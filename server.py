import os
import time
import json
import xattr
import random
import socket
import hashlib
import tempfile
import requests

# *** Global ***

print("hello", os.environ['TYPE'], os.getpid())

def resp(start_response, code, headers=[('Content-type', 'text/plain')], body=b''):
  start_response(code, headers)
  return [body]

# *** Master Server ***

# TODO: don't use leveldb cause it's single process
class SimpleKV(object):
  def __init__(self, fn):
    import plyvel
    self.db = plyvel.DB(fn, create_if_missing=True)

  def get(self, k):
    return self.db.get(k)

  def put(self, k, v):
    self.db.put(k, v)

  def delete(self, k):
    self.db.delete(k)


if os.environ['TYPE'] == "master":
  # check on volume servers
  volumes = os.environ['VOLUMES'].split(",")

  for v in volumes:
    print(v)

  db = SimpleKV(os.environ['DB'])

def master(env, sr):
  host = env['SERVER_NAME'] + ":" + env['SERVER_PORT']
  key = env['PATH_INFO']

  if env['REQUEST_METHOD'] == 'POST':
    # POST is called by the volume servers to write to the database
    flen = int(env.get('CONTENT_LENGTH', '0'))
    print("posting", key, flen)
    # TODO: overwrite shouldn't be allowed, need transactions
    if flen > 0:
      db.put(key.encode('utf-8'), env['wsgi.input'].read())
    else:
      db.delete(key.encode('utf-8'))
    return resp(sr, '200 OK')

  metakey = db.get(key.encode('utf-8'))
  print(key, metakey)
  if metakey is None:
    if env['REQUEST_METHOD'] == 'PUT':
      # handle putting key
      # TODO: make volume selection intelligent
      volume = random.choice(volumes)
    else:
      # this key doesn't exist and we aren't trying to create it
      return resp(sr, '404 Not Found')
  else:
    # key found 
    if env['REQUEST_METHOD'] == 'PUT':
      # we are trying to put it. delete first!
      return resp(sr, '409 Conflict')
    meta = json.loads(metakey.decode('utf-8'))
    volume = meta['volume']

  # send the redirect
  headers = [('Location', 'http://%s%s?%s' % (volume, key, host))]

  return resp(sr, '307 Temporary Redirect', headers)

# *** Volume Server ***

class FileCache(object):
  # this is a single computer on disk key value store

  def __init__(self, basedir):
    self.basedir = os.path.realpath(basedir)
    self.tmpdir = os.path.join(self.basedir, "tmp")
    os.makedirs(self.tmpdir, exist_ok=True)
    print("FileCache in %s" % basedir)

  def k2p(self, key, mkdir_ok=False):
    key = hashlib.md5(key.encode('utf-8')).hexdigest()

    # 2 layers deep in nginx world
    path = self.basedir+"/"+key[0:2]+"/"+key[0:4]
    if not os.path.isdir(path) and mkdir_ok:
      # exist ok is fine, could be a race
      os.makedirs(path, exist_ok=True)

    return os.path.join(path, key)

  def exists(self, key):
    return os.path.isfile(self.k2p(key))

  def delete(self, key):
    try:
      os.unlink(self.k2p(key))
      return True
    except FileNotFoundError:
      pass
    return False

  def get(self, key):
    return open(self.k2p(key), "rb")

  def put(self, key, stream):
    with tempfile.NamedTemporaryFile(dir=self.tmpdir, delete=False) as f:
      # TODO: in chunks, don't waste RAM
      f.write(stream.read())

      # save the real name in xattr in case we rebuild cache
      xattr.setxattr(f.name, 'user.key', key.encode('utf-8'))

      # TODO: check hash
      os.rename(f.name, self.k2p(key, True))

if os.environ['TYPE'] == "volume":

  # create the filecache
  fc = FileCache(os.environ['VOLUME'])

def volume(env, sr):
  host = env['SERVER_NAME'] + ":" + env['SERVER_PORT']
  key = env['PATH_INFO']
  master_url = "http://"+env['QUERY_STRING']+key

  if env['REQUEST_METHOD'] == 'PUT':
    if fc.exists(key):
      req = requests.post(master_url, json={"volume": host})
      # can't write, already exists
      return resp(sr, '409 Conflict')

    flen = int(env.get('CONTENT_LENGTH', '0'))
    if flen > 0:
      fc.put(key, env['wsgi.input'])
      req = requests.post(master_url, json={"volume": host})
      if req.status_code == 200:
        return resp(sr, '201 Created')
      else:
        fc.delete(key)
        return resp(sr, '500 Internal Server Error')
    else:
      return resp(sr, '411 Length Required')

  if env['REQUEST_METHOD'] == 'DELETE':
    req = requests.post(master_url, data='')
    if req.status_code == 200:
      if fc.delete(key):
        return resp(sr, '200 OK')
      else:
        # file wasn't on our disk
        return resp(sr, '500 Internal Server Error (not on disk)')
    else:
      return resp(sr, '500 Internal Server Error (master db write fail)')

  if not fc.exists(key):
    # key not in the FileCache, 404
    return resp(sr, '404 Not Found')

  if env['REQUEST_METHOD'] == 'GET':
    # TODO: in chunks, don't waste RAM
    if 'HTTP_RANGE' in env:
      b,e = [int(x) for x in env['HTTP_RANGE'].split("=")[1].split("-")]
      f = fc.get(key)
      f.seek(b)
      ret = f.read(e-b)
      f.close()
      return resp(sr, '200 OK', body=ret)
    else:
      return resp(sr, '200 OK', body=fc.get(key).read())

