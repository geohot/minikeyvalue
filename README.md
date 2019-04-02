# minikeyvalue

[![Build Status](https://travis-ci.org/geohot/minikeyvalue.svg?branch=master)](https://travis-ci.org/geohot/minikeyvalue)

A ~200 line (not including tests!) non-proxying distributed key value store. Optimized for reading files between 1MB and 1GB. Inspired by SeaweedFS, but simple. Should scale to 1 billion files.

Even if this code is crap, the on disk format is super simple! It's like the nginx cache with MD5 hashes for filenames and the real name in an xattr.

### API

- GET /key
  - Supports range requests.
  - 302 redirect to volume server.
- {PUT, DELETE} /key
  - Blocks. 200 = written, anything else = nothing happened.

### Start Master Server (default port 3000)

```
./master localhost:3001,localhost:3002 /tmp/cachedb/
```

### Start Volume Server (default port 3001)

```
./volume /tmp/volume1/
PORT=3002 ./volume /tmp/volume2/
```

### Usage

```
# put "bigswag" in key "wehave"
curl -X PUT -d bigswag localhost:3000/wehave

# get key "wehave" (should be "bigswag")
curl localhost:3000/wehave

# delete key "wehave"
curl -X DELETE localhost:3000/wehave
```

