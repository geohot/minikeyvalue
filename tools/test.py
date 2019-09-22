#!/usr/bin/env python3
import os
import socket
import hashlib
import binascii
import unittest
import requests
from urllib.parse import quote_plus
import time
import timeit
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(format='%(name)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestMiniKeyValue(unittest.TestCase):
  maxDiff = None
  
  def get_fresh_key(self):
    return b"http://localhost:3000/swag-" + binascii.hexlify(os.urandom(10))

  def test_getputdelete(self):
    key = self.get_fresh_key()

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.get(key)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.text, "onyou")

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

  def test_deleteworks(self):
    key = self.get_fresh_key()

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_doubledelete(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

    r = requests.delete(key)
    self.assertNotEqual(r.status_code, 204)

  def test_doubleput(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.put(key, data="onyou")
    self.assertNotEqual(r.status_code, 201)

  def test_doubleputwdelete(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

  def test_10keys(self):
    keys = [self.get_fresh_key() for i in range(10)]

    for k in keys:
      r = requests.put(k, data=hashlib.md5(k).hexdigest())
      self.assertEqual(r.status_code, 201)

    for k in keys:
      r = requests.get(k)
      self.assertEqual(r.status_code, 200)
      self.assertEqual(r.text, hashlib.md5(k).hexdigest())

    for k in keys:
      r = requests.delete(k)
      self.assertEqual(r.status_code, 204)

  def test_range_request(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.get(key, headers={"Range": "bytes=2-5"})
    self.assertEqual(r.status_code, 206)
    self.assertEqual(r.text, "you")

  def test_nonexistent_key(self):
    key = self.get_fresh_key()
    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_head_request(self):
    # head not exist
    key = self.get_fresh_key()
    r = requests.head(key, allow_redirects=True)
    self.assertEqual(r.status_code, 404)
    # no redirect, content length should be zero
    self.assertEqual(int(r.headers['content-length']), 0)

    # head exist
    key = self.get_fresh_key()
    data = "onyou"
    r = requests.put(key, data=data)
    self.assertEqual(r.status_code, 201)
    r = requests.head(key, allow_redirects=True)
    self.assertEqual(r.status_code, 200)
    # redirect, content length should be size of data
    self.assertEqual(int(r.headers['content-length']), len(data))

  def test_large_key(self):
    key = self.get_fresh_key()

    data = b"a"*(16*1024*1024)

    r = requests.put(key, data=data)
    self.assertEqual(r.status_code, 201)

    r = requests.get(key)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.content, data)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 204)

  def test_json_list(self):
    key = self.get_fresh_key()
    data = "eh"
    r = requests.put(key+b"1", data=data)
    self.assertEqual(r.status_code, 201)
    r = requests.put(key+b"2", data=data)
    self.assertEqual(r.status_code, 201)

    r = requests.get(key+b"?list")
    self.assertEqual(r.status_code, 200)
    bkey = key.decode('utf-8')
    bkey = "/"+bkey.split("/")[-1]
    self.assertEqual(r.json(), {"next": "", "keys": [bkey+"1", bkey+"2"]})

  def test_json_list_null(self):
    r = requests.get(self.get_fresh_key()+b"/DOES_NOT_EXIST?list")
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.json(), {"next": "", "keys": []})

  def test_json_list_limit(self):
    prefix = self.get_fresh_key()
    keys = []
    data = "0"
    limit = 10
    for i in range(limit+2):
      key = prefix+str(i).encode()
      r = requests.put(key, data=data)
      self.assertEqual(r.status_code, 201)
      keys.append("/"+key.decode().split("/")[-1])
    # leveldb is sorted alphabetically
    keys = sorted(keys)
    # should return first page
    r = requests.get(prefix+b"?list&limit="+str(limit).encode())
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.json(), {"next": keys[limit], "keys": keys[:limit]})
    start = quote_plus(r.json()["next"]).encode()
    # should return last page
    r = requests.get(prefix+b"?list&limit="+str(limit).encode()+b"&start="+start)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.json(), {"next": "", "keys": keys[limit:]})

  def test_noemptykey(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="")
    self.assertEqual(r.status_code, 411)

  def test_content_hash(self):
    for i in range(100):
      key = self.get_fresh_key()
      r = requests.put(key, data=key)
      self.assertEqual(r.status_code, 201)

      r = requests.head(key, allow_redirects=False)
      self.assertEqual(r.headers['Content-Md5'], hashlib.md5(key).hexdigest())

if __name__ == '__main__':
  # wait for servers
  for port in range(3000,3006):
    print("check port %d" % port)
    while 1:
      try:
        s = socket.create_connection(("localhost", port), timeout=0.5)
        s.close()
        break
      except (ConnectionRefusedError, OSError):
        time.sleep(0.5)
        continue
      print("waiting for servers")
  
  unittest.main()

