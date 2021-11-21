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

  @unittest.expectedFailure
  def test_deletedir(self):
    s3.delete_dir('bucket')

  # this needs multipart uploads to work
  def test_largerw(self):
    tbl = pa.table([pa.array(range(10000000))], ['a'])

    key = self.get_fresh_key()
    pq.write_table(tbl, key, filesystem=s3)
    tbl2 = pq.read_table(key, filesystem=s3)
    self.assertEqual(tbl, tbl2) # unclear what sort of equality this checks
    s3.delete_file(key)

  def test_smallrw(self):
    tbl = pa.table([pa.array([0,1,2,3])], ['a'])

    key = self.get_fresh_key()
    pq.write_table(tbl, key, filesystem=s3)
    tbl2 = pq.read_table(key, filesystem=s3)
    self.assertEqual(tbl, tbl2) # unclear what sort of equality this checks
    s3.delete_file(key)

if __name__ == '__main__':
  unittest.main()