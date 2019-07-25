# minikeyvalue

[![Build Status](https://travis-ci.org/geohot/minikeyvalue.svg?branch=master)](https://travis-ci.org/geohot/minikeyvalue)

Fed up with the complexity of distributed filesystems?

minikeyvalue is a ~200 line (not including tests!) distributed key value store. Optimized for values between 1MB and 1GB. Inspired by SeaweedFS, but simple. Should scale to billions of files and petabytes of data.

Now with support for 3x replication. Just like the cloud!

Even if this code is crap, the on disk format is super simple! We rely on a filesystem for blob storage and a LevelDB for indexing. The index can be reconstructed with rebuild. Volumes can be added or removed with rebalance.

### API

- GET /key
  - 302 redirect to nginx volume server.
- PUT /key
  - Blocks. 201 = written, anything else = probably not written.
- DELETE /key
  - Blocks. 204 = soft deleted, anything else = probably not deleted.

### Start Master Server (default port 3000)

```
# this is using the code in server.go
./mkv -volumes localhost:3001,localhost:3002 -db /tmp/indexdb/ server
```

### Start Volume Servers (default port 3001)

```
# this is just nginx under the hood
PORT=3001 ./volume /tmp/volume1/
PORT=3002 ./volume /tmp/volume2/
```

### Usage

```
# put "bigswag" in key "wehave"
curl -v -L -X PUT -d bigswag localhost:3000/wehave

# get key "wehave" (should be "bigswag")
curl -v -L localhost:3000/wehave

# delete key "wehave"
curl -v -L -X DELETE localhost:3000/wehave

# unlink key "wehave", this is a virtual delete
curl -v -L -X UNLINK localhost:3000/wehave

# list keys starting with "we"
curl -v -L localhost:3000/we?list

# list unlinked keys ripe for DELETE
curl -v -L localhost:3000/?unlinked

# put file in key "file.txt"
curl -v -L -X PUT -T /path/to/local/file.txt localhost:3000/file.txt

# get file in key "file.txt"
curl -v -L -o /path/to/local/file.txt localhost:3000/file.txt
```

### Parameters (in lib.go)

- fallback: A (volume) server to fallback to if the key is not found
- replicas: The amount of times to replicate the file
- subvolumes: The amount of drives to use in a single machine

### Rebalancing (to change the amount of volume servers)

```
# must shut down master first, since LevelDB can only be accessed by one process
./mkv -volumes localhost:3001,localhost:3002 -db /tmp/indexdb/ rebalance
```

### Rebuilding (to regenerate the LevelDB)

```
./mkv -volumes localhost:3001,localhost:3002 -db /tmp/indexdbalt/ rebuild
```

### Performance

```
# Fetching non-existent key: 116338 req/sec
wrk -t2 -c100 -d10s http://localhost:3000/key

# go run thrasher.go lib.go
starting thrasher
10000 write/read/delete in 2.620922675s
thats 3815.40/sec
```

