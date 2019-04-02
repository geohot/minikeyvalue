import os
import time
import json
import socket
import hashlib

print("hello", os.environ['TYPE'], os.getpid())

# *** Master Server ***

if os.environ['TYPE'] == "master":
  import plyvel
  db = plyvel.DB(os.environ['DB'], create_if_missing=True)

def master(env, start_response):
  key = env['REQUEST_URI'].encode('utf-8')
  metakey = db.get(key)

  if metakey is None:
    if env['REQUEST_METHOD'] in ['PUT']:
      # TODO: handle putting key
      pass

    # this key doesn't exist and we aren't trying to create it
    start_response('404 Not Found', [('Content-type', 'text/plain')])
    return [b'key not found']

  # key found
  meta = json.loads(metakey)

  # send the redirect for either GET or DELETE
  headers = [('location', 'http://%s%s' % (meta['volume'], key)), ('expires', '0')]
  start_response('302 Found', headers)
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
    with open(self.k2p(key, True), "wb") as f:
      f.write(value)

if os.environ['TYPE'] == "volume":
  host = socket.gethostname()
  
  # register with master
  master = os.environ['MASTER']

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
    return [fc.get(hkey)]

  if env['REQUEST_METHOD'] == 'PUT':
    flen = int(env.get('CONTENT_LENGTH', '0'))
    fc.put(hkey, env['wsgi.input'].read(flen))

  if env['REQUEST_METHOD'] == 'DELETE':
    fc.delete(hkey)

