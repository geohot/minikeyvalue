import os
import time
import json
import lmdb
import base64
import random
import shutil
import socket
import hashlib
import tempfile
import requests

# *** Global ***

stype = os.environ.get("TYPE", None)
print("hello", stype, os.getpid())

def resp(start_response, code, headers=None, body=b''):
  headers = [('Content-Type', 'text/plain')] if headers is None else headers
  headers.append(('Content-Length', str(len(body))))
  start_response(code, headers)
  return [body]

# assert key == key2path(base64.b64decode(os.path.basename(key)))
def key2path(key):
  mkey = hashlib.md5(key).digest()
  b64key = base64.b64encode(key).decode("utf-8")

  # 2 byte layers deep, meaning a fanout of 256
  # optimized for 2^24 = 16M files per volume server
  return "/%02x/%02x/%s" % (mkey[0], mkey[1], b64key)

def key2volume(key, volumes):
  # this is an intelligent way to pick the volume server for a file
  # should be stable in the volume server name (not position!)
  # and if more are added the correct portion will move (yay md5!)
  # note that this would be trivial to extend to replicas using k top
  best_score = None
  ret = None
  for v in volumes:
    # hash the volume + the key
    kk = v.encode('utf-8') + key
    score = int.from_bytes(hashlib.md5(kk).digest(), byteorder='big')
    # find the biggest hash (strcmp)
    if best_score is None or best_score < score:
      best_score = score
      ret = v
  return ret

# *** Master Server ***

class LmdbCache(object):
  # this is a single computer on disk key json store optimized for small things

  def __init__(self, basedir):
    self.db = lmdb.open(basedir)

  def get(self, key):
    with self.db.begin() as txn:
      metakey = txn.get(key)
      if metakey is None:
        return None
      return json.loads(metakey.decode('utf-8'))

  def delete(self, key):
    with self.db.begin(write=True) as txn:
      metakey = txn.get(key)
      if metakey is not None:
        ret = json.loads(metakey.decode('utf-8'))
        txn.delete(key)
        return ret    # return last value of the key
    return None

  def put(self, key, dat):
    with self.db.begin(write=True) as txn:
      metakey = txn.get(key)
      if metakey is None:
        txn.put(key, json.dumps(dat).encode('utf-8'))
        return True
      else:
        return False

if stype == "master":
  volumes = os.environ['VOLUMES'].split(",")
  print("volume servers:", volumes)
  kc = LmdbCache(os.environ['DB'])

def remote_put(remote, dat):
  try:
    req = requests.put(remote, dat)
    # 201 is a new file, 204 is an overwrite
    ret = req.status_code in [201, 204]
  except Exception:
    ret = False
  if not ret:
    print("remote put failed: %s %d" % (remote, len(dat)))
  return ret

def remote_delete(remote):
  try:
    req = requests.delete(remote)
    ret = req.status_code == 204
  except Exception:
    ret = False
  if not ret:
    print("remote delete failed: %s" % remote)
  return ret

def master(env, sr):
  key = env['PATH_INFO'].encode("utf-8")

  # 302 redirect for GET/HEAD
  if env['REQUEST_METHOD'] in ['GET', 'HEAD']:
    meta = kc.get(key)
    if meta is None:
      return resp(sr, '404 Not Found')

    # send the redirect
    remote = 'http://%s%s' % (meta['volume'], key2path(key))
    return resp(sr, '302 Found', headers=[('Location', remote)])

  # proxy for DELETE
  elif env['REQUEST_METHOD'] == 'DELETE':
    # first do the local delete while fetching the volume
    meta = kc.delete(key)
    if meta is None:
      return resp(sr, '404 Not Found (delete on master)')

    # now do the remote delete
    remote = 'http://%s%s' % (meta['volume'], key2path(key))

    if remote_delete(remote):
      return resp(sr, '204 No Content')
    else:
      # NOTE: The delete can succeed locally and fail remotely
      # This will cause an orphan file, worst case it wastes space
      return resp(sr, '500 Internal Server Error (remote delete failed)')

  # proxy for PUT
  elif env['REQUEST_METHOD'] == 'PUT':
    # first check that we don't already have this
    if kc.get(key) is not None:
      return resp(sr, '409 Conflict')

    # select the volume
    volume = key2volume(key, volumes)

    # no empty values
    flen = int(env.get('CONTENT_LENGTH', '0'))
    if flen <= 0:
      return resp(sr, '411 Length Required')

    # now do the remote write
    # TODO: stream this
    dat = env['wsgi.input'].read()
    if len(dat) != flen:
      return resp(sr, '500 Internal Server Error (length mismatch)')

    remote = 'http://%s%s' % (volume, key2path(key))
    if not remote_put(remote, dat):
      return resp(sr, '500 Internal Server Error (remote write failed)')

    # NOTE: The put can succeed remotely and fail locally
    # This will cause an orphan file, worst case it wastes space

    # now do the local write (after it's safe on the remote server)
    if not kc.put(key, {"volume": volume, "size": flen}):
      # someone else wrote in the mean time, they won the race
      remote_delete(remote)
      return resp(sr, '409 Conflict')

    # both writes succeed
    return resp(sr, '201 Created')

