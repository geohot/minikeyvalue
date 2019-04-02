import os
import time
import json
import random
import socket
import hashlib

print("hello", os.environ['TYPE'], os.getpid())

# *** Master Server ***

if os.environ['TYPE'] == "master":
  # check on volume servers
  volumes = os.environ['VOLUMES'].split(",")

  for v in volumes:
    print(v)

  import plyvel
  db = plyvel.DB(os.environ['DB'], create_if_missing=True)

def master(env, start_response):
  key = env['REQUEST_URI']
  metakey = db.get(key.encode('utf-8'))

  if metakey is None:
    if env['REQUEST_METHOD'] == 'PUT':
      # handle putting key
      # TODO: make volume selection intelligent
      volume = random.choice(volumes)

      # save volume to database
      meta = {"volume": volume}
      db.put(key.encode('utf-8'), json.dumps(meta).encode('utf-8'))
    else:
      # this key doesn't exist and we aren't trying to create it
      start_response('404 Not Found', [('Content-type', 'text/plain')])
      return [b'key not found']
  else:
    # key found and we are trying to put it
    """
    if env['REQUEST_METHOD'] == 'PUT':
      start_response('409 Conflict', [('Content-type', 'text/plain')])
      return [b'key already exists']
    """
    meta = json.loads(metakey.decode('utf-8'))

  # send the redirect
  print(meta)
  volume = meta['volume']
  headers = [('Location', 'http://%s%s' % (volume, key))]
  start_response('307 Temporary Redirect', headers)
  return [b""]

# *** Volume Server ***

class FileCache(object):
  def __init__(self, basedir):
    self.basedir = os.path.realpath(basedir)
    os.makedirs(self.basedir, exist_ok=True)
    print("FileCache in %s" % basedir)

  def k2p(self, key, mkdir_ok=False):
    # must be MD5 hash
    assert len(key) == 32

    # 2 layers deep in nginx world
    path = self.basedir+"/"+key[0:2]+"/"+key[0:4]
    if not os.path.isdir(path) and mkdir_ok:
      # exist ok is fine, could be a race
      os.makedirs(path, exist_ok=True)

    return os.path.join(path, key)

  def exists(self, key):
    return os.path.isfile(self.k2p(key))

  def delete(self, key):
    os.unlink(self.k2p(key))

  def get(self, key):
    return open(self.k2p(key), "rb").read()

  def put(self, key, value):
    # TODO: refactor to use a tempfile and symlink
    with open(self.k2p(key, True), "wb") as f:
      f.write(value)

if os.environ['TYPE'] == "volume":
  host = socket.gethostname()

  # create the filecache
  fc = FileCache(os.environ['VOLUME'])

def volume(env, start_response):
  key = env['REQUEST_URI'].encode('utf-8')
  hkey = hashlib.md5(key).hexdigest()
  print(hkey)

  if env['REQUEST_METHOD'] == 'GET':
    if not fc.exists(hkey):
      # key not in the FileCache
      start_response('404 Not Found', [('Content-type', 'text/plain')])
      return [b'key not found']
    start_response('200 OK', [('Content-type', 'text/plain')])
    return [fc.get(hkey)]

  if env['REQUEST_METHOD'] == 'PUT':
    flen = int(env.get('CONTENT_LENGTH', '0'))
    if flen > 0:
      fc.put(hkey, env['wsgi.input'].read(flen))
      start_response('200 OK', [('Content-type', 'text/plain')])
      return [b'']
    else:
      start_response('411 Length Required', [('Content-type', 'text/plain')])
      return [b'']

  if env['REQUEST_METHOD'] == 'DELETE':
    fc.delete(hkey)

