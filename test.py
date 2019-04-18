#!/usr/bin/env python3
import os
import socket
import hashlib
import binascii
import unittest
import requests
import time
import timeit
import logging
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(format='%(name)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestMiniKeyValue(unittest.TestCase):
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
    self.assertEqual(r.status_code, 200)

  def test_deleteworks(self):
    key = self.get_fresh_key()

    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 200)

    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_doubledelete(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 200)

    r = requests.delete(key)
    self.assertNotEqual(r.status_code, 200)

  def test_doubleput(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.put(key, data="onyou")
    self.assertNotEqual(r.status_code, 201)

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
      self.assertEqual(r.status_code, 200)

  def test_range_request(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="onyou")
    self.assertEqual(r.status_code, 201)

    r = requests.get(key, headers={"Range": "bytes=2-5"})
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.text, "you")

  def test_nonexistent_key(self):
    key = self.get_fresh_key()
    r = requests.get(key)
    self.assertEqual(r.status_code, 404)

  def test_large_key(self):
    key = self.get_fresh_key()

    data = b"a"*(16*1024*1024)

    r = requests.put(key, data=data)
    self.assertEqual(r.status_code, 201)

    r = requests.get(key)
    self.assertEqual(r.status_code, 200)
    self.assertEqual(r.content, data)

    r = requests.delete(key)
    self.assertEqual(r.status_code, 200)

  def test_noemptykey(self):
    key = self.get_fresh_key()
    r = requests.put(key, data="")
    self.assertEqual(r.status_code, 411)

  def test_put_speed(self):
    PUT_COUNT = 64
    MAX_WORKERS = 8
    keys = [self.get_fresh_key() for i in range(PUT_COUNT)]

    def put(x):
      r = requests.put(x, data=b"onyou-"+x)
      return r.status_code

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      start = time.perf_counter()
      for status_code in executor.map(put, keys):
        self.assertEqual(status_code, 201)
      elapsed = time.perf_counter()-start

    logger.debug("%.2f ms for %d writes (%.2f writes/second)" %
      (elapsed*1000., PUT_COUNT, PUT_COUNT/elapsed))

    def get(x):
      r = requests.get(x)
      return r.status_code, r.content

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
      start = time.perf_counter()
      for x,o in zip(keys, executor.map(get, keys)):
        status_code, text = o
        self.assertEqual(status_code, 200)
        self.assertEqual(text, b"onyou-"+x)
      elapsed = time.perf_counter()-start

    logger.debug("%.2f ms for %d reads (%.2f reads/second)" %
      (elapsed*1000., PUT_COUNT, PUT_COUNT/elapsed))


if __name__ == '__main__':
  # wait for servers
  for port in [3000,3001,3002]:
    while 1:
      try:
        s = socket.create_connection(("localhost", port), timeout=0.5)
        s.close()
        break
      except ConnectionRefusedError, OSError:
        time.sleep(0.5)
        continue
      print("waiting for servers")
  
  unittest.main()

