#!/usr/bin/env python3
import os
import binascii
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
s3 = fs.S3FileSystem(endpoint_override="localhost:3000", scheme="http")

class TestS3PyArrow(unittest.TestCase):
  def get_fresh_key(self):
    return "bucket/swag-" + binascii.hexlify(os.urandom(10)).decode('utf-8')

  def test_smallrw(self):
    key = self.get_fresh_key()

    a = pa.array([0,1,2,3])
    tbl = pa.table([a], ['a'])
    pq.write_table(tbl, key, filesystem=s3)

    tbl2 = pq.read_table(key, filesystem=s3)

if __name__ == '__main__':
  unittest.main()