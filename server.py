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
  mkey = hashlib.md5(key).hexdigest()
  b64key = base64.b64encode(key).decode("utf-8")

  # 2 byte layers deep, meaning a fanout of 256
  # optimized for 2^24 = 16M files per volume server
  return "/%s/%s/%s" % (mkey[0:2], mkey[2:4], b64key)

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

def master(env, sr):
  key = env['PATH_INFO'].encode("utf-8")

  # 302 redirect for GET
  if env['REQUEST_METHOD'] == 'GET':
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
    req = requests.delete(remote)

    if req.status_code == 200:
      return resp(sr, '200 OK')
    else:
      # TODO: think through this case more
      # worst case it wastes space on the remote
      return resp(sr, '500 Internal Server Error (remote delete failed)')

  # proxy for PUT
  elif env['REQUEST_METHOD'] == 'PUT':
    # first check that we don't already have this
    if kc.get(key) is not None:
      return resp(sr, '409 Conflict')

    # TODO: make volume selection intelligent
    volume = random.choice(volumes)

    # no empty values
    flen = int(env.get('CONTENT_LENGTH', '0'))
    if flen <= 0:
      return resp(sr, '411 Length Required')

    # now do the remote write
    # TODO: stream this
    dat = env['wsgi.input'].read()
    remote = 'http://%s%s' % (volume, key2path(key))
    req = requests.put(remote, dat)
    if req.status_code != 201:
      return resp(sr, '500 Internal Server Error (remote write failed)')

    # now do the local write (after it's safe on the remote server)
    if not kc.put(key, {"volume": volume}):
      # someone else wrote in the mean time
      requests.delete(remote)
      return resp(sr, '409 Conflict')

    # both writes succeed
    return resp(sr, '201 Created')


# *** Volume Server (can be replaced by nginx) ***

class FileCache(object):
  # this is a single computer on disk key value store

  def __init__(self, basedir):
    self.basedir = os.path.realpath(basedir)
    self.tmpdir = os.path.join(self.basedir, "tmp")
    assert os.path.isdir(self.tmpdir)

    print("FileCache in %s" % basedir)

  def _k2p(self, key):
    # well formed key, not actually required to check
    assert key == key2path(base64.b64decode(os.path.basename(key)))

    path = self.basedir + key
    return path

  def delete(self, key):
    try:
      os.unlink(self._k2p(key))
      return True
    except FileNotFoundError:
      pass
    return False

  def get(self, key):
    try:
      return open(self._k2p(key), "rb")
    except FileNotFoundError:
      return None

  def put(self, key, stream):
    ret = False
    try:
      with tempfile.NamedTemporaryFile(dir=self.tmpdir, delete=True) as f:
        shutil.copyfileobj(stream, f)
        os.rename(f.name, self._k2p(key))
        ret = True
    except FileNotFoundError:
      # If the rename succeeds, the unlink should throw this
      return ret
    return False

if stype == "volume":
  # create the filecache
  fc = FileCache(os.environ['VOLUME'])

def volume(env, sr):
  key = env['PATH_INFO']

  if env['REQUEST_METHOD'] == 'GET':
    f = fc.get(key)
    if f is None:
      return resp(sr, '404 Not Found (from volume server)')

    # TODO: read in chunks, don't waste RAM
    if 'HTTP_RANGE' in env:
      b,e = [int(x) for x in env['HTTP_RANGE'].split("=")[1].split("-")]
      f.seek(b)
      ret = f.read(e-b)
    else:
      ret = f.read()

    # close file and send back
    f.close()
    return resp(sr, '200 OK', body=ret)

  elif env['REQUEST_METHOD'] == 'DELETE':
    if fc.delete(key):
      return resp(sr, '200 OK')
    else:
      return resp(sr, '500 Internal Server Error (not on disk)')

  elif env['REQUEST_METHOD'] == 'PUT':
    if fc.put(key, env['wsgi.input']):
      return resp(sr, '201 Created')
    else:
      return resp(sr, '500 Internal Server Error (volume write failed)')

