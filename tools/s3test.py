#!/usr/bin/env python3
import os
import binascii
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

class TestS3PyArrow(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # why is this so slow?
    cls.s3 = fs.S3FileSystem(endpoint_override="localhost:3000", scheme="http")

  def get_fresh_key(self):
    return "bucket/swag-" + binascii.hexlify(os.urandom(10)).decode('utf-8')

  def write_file(self, fn, dat):
    with self.s3.open_output_stream(fn) as f:
      f.write(dat)

  def test_fileinfo(self):
    fn = self.get_fresh_key()+"-fileinfo"
    self.write_file(fn, b"hello1")
    inf = self.s3.get_file_info(fn)
    self.assertEqual(inf.size, 6)

  def test_fileinfo_list(self):
    fn = self.get_fresh_key()+"-listdir"
    self.write_file(fn, b"hello1")
    infs = self.s3.get_file_info(fs.FileSelector("bucket/", recursive=True))
    fns = [x.path for x in infs]
    self.assertIn(fn, fns)

  # need to support file delete with POST
  # which requires parsing XML
  @unittest.expectedFailure
  def test_deletedir(self):
    fn = self.get_fresh_key()+"-deltest"
    self.write_file(fn, b"hello1")
    self.s3.delete_dir_contents('bucket')
    inf = self.s3.get_file_info(fn)
    self.assertEqual(inf.type, fs.FileType.NotFound)

  # this needs multipart uploads to work
  def test_largerw(self):
    tbl = pa.table([pa.array(range(10000000))], ['a'])

    key = self.get_fresh_key()
    pq.write_table(tbl, key, filesystem=self.s3)
    tbl2 = pq.read_table(key, filesystem=self.s3)
    self.assertEqual(tbl, tbl2) # unclear what sort of equality this checks
    self.s3.delete_file(key)

  def test_smallrw(self):
    tbl = pa.table([pa.array([0,1,2,3])], ['a'])

    key = self.get_fresh_key()
    pq.write_table(tbl, key, filesystem=self.s3)
    tbl2 = pq.read_table(key, filesystem=self.s3)
    self.assertEqual(tbl, tbl2) # unclear what sort of equality this checks
    self.s3.delete_file(key)

if __name__ == '__main__':
  unittest.main()