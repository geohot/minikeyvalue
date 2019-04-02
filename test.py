#!/usr/bin/env python3
import os
import hashlib
import binascii
import unittest
import requests

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
    self.assertNotEqual(r.status_code, 200)

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
      
if __name__ == '__main__':
  unittest.main()

