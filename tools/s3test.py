#!/usr/bin/env python3
import os
import binascii
import unittest
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
import boto3

class TestS3Boto(unittest.TestCase):
  def get_fresh_key(self):
    return "swag-" + binascii.hexlify(os.urandom(10)).decode('utf-8')

  @classmethod
  def setUpClass(cls):
    cls.s3 = boto3.client('s3', endpoint_url="http://127.0.0.1:3000", aws_access_key_id="user", aws_secret_access_key="password")

  def test_writelist(self):
    key = self.get_fresh_key()
    self.s3.put_object(Body=b'hello1', Bucket='boto', Key=key)
    response = self.s3.list_objects_v2(Bucket='boto')
    keys = [x['Key'] for x in response['Contents']]
    self.assertIn(key, keys)

  @unittest.expectedFailure
  def test_writeread(self):
    key = self.get_fresh_key()
    self.s3.put_object(Body=b'hello1', Bucket='boto', Key=key)
    # sadly this doesn't work because it won't follow the redirect
    self.s3.get_object(Bucket="boto", Key=key)

class TestS3PyArrow(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    # this prevents stupid requests to 169.254.169.254 which take a while
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    cls.s3 = fs.S3FileSystem(endpoint_override="127.0.0.1:3000", scheme="http", anonymous=True)

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
  def test_deletedir(self):
    fn = self.get_fresh_key()+"-deltest"
    self.write_file(fn, b"hello1")
    self.s3.delete_dir_contents('bucket')
    inf = self.s3.get_file_info(fn)
    self.assertEqual(inf.type, fs.FileType.NotFound)

  def test_deletefile(self):
    fn = self.get_fresh_key()+"-delftest"
    self.write_file(fn, b"hello1")
    inf = self.s3.get_file_info(fn)
    self.assertEqual(inf.size, 6)
    self.s3.delete_file(fn)
    inf = self.s3.get_file_info(fn)
    self.assertEqual(inf.type, fs.FileType.NotFound)

  # this needs multipart uploads to work
  def test_largerw(self):
    tbl = pa.table([pa.array(range(2000000)), pa.array(range(2000000))], ['a', 'b'])

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