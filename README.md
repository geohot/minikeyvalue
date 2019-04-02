# minikeyvalue

A sub 1000 line (not including tests!) non-proxying distributed key value store. Optimized for reading files between 1MB and 1GB. Inspired by SeaweedFS, but simple. Should scale to 1 billion files.

### API

- GET /key
  - Supports range requests.
  - 302 redirect to volume server.
- {PUT, DELETE} /key
  - Blocks. 200 = written, anything else = nothing happened.

### Start Master Server (default port 3000)

```
./master /tmp/cachedb/
```

### Start Volume Server (default port 3001)

```
./volume /tmp/volume1/ localhost:3000
PORT=3002 ./volume -p 3002 /tmp/volume2/ localhost:3000
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

