#!/usr/bin/env python3
import os, sys

if __name__ == "__main__":
  basedir = os.path.realpath(sys.argv[1])
  tmpdir = os.path.join(basedir, "tmp")

  if not os.path.isdir(tmpdir):
    print("creating 65k seed directories")
    # create all 65k directories
    for i in range(65536):
      path = os.path.join(basedir, "%02x" % (i//256), "%02x" % (i%256))
      os.makedirs(path, exist_ok=True)

    # create tmpdir last as a sentinal
    os.makedirs(tmpdir, exist_ok=True)

  # remove all files in tmpdir
  for fn in os.listdir(tmpdir):
    os.unlink(os.path.join(tmpdir, fn))

