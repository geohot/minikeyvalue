# minikeyvalue

[![Build Status](https://travis-ci.org/geohot/minikeyvalue.svg?branch=master)](https://travis-ci.org/geohot/minikeyvalue)

Fed up with the complexity of distributed filesystems?

minikeyvalue is a ~200 line (not including tests!) distributed key value store. Optimized for reading files between 1MB and 1GB. Inspired by SeaweedFS, but simple. Should scale to billions of files and petabytes of data.

Even if this code is crap, the on disk format is super simple! We rely on a filesystem for blob storage and a LevelDB for cache. The cache can be reconstructed from the filesystem.

### API

- GET /key
  - 302 redirect to nginx volume server.
- PUT /key
  - Blocks. 201 = written, anything else = probably not written.
- DELETE /key
  - Blocks. 204 = deleted, anything else = probably deleted.

### Start Master Server (default port 3000)

```
# this is using the code in server.go
./master localhost:3001,localhost:3002 /tmp/cachedb/
```

### Start Volume Server (default port 3001)

```
# this is just nginx under the hood
./volume /tmp/volume1/
PORT=3002 ./volume /tmp/volume2/
```

### Usage

```
# put "bigswag" in key "wehave"
curl -L -X PUT -d bigswag localhost:3000/wehave

# get key "wehave" (should be "bigswag")
curl -L localhost:3000/wehave

# delete key "wehave"
curl -L -X DELETE localhost:3000/wehave
```

### Performance

```
# Fetching non-existent key: 116338 req/sec
wrk -t2 -c100 -d10s http://localhost:3000/key

starting thrasher
1000 write/read/delete in 295.76852ms
```

