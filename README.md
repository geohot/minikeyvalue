# minikeyvalue

[![Build Status](https://travis-ci.org/geohot/minikeyvalue.svg?branch=master)](https://travis-ci.org/geohot/minikeyvalue)

Fed up with the complexity of distributed filesystems?

minikeyvalue is a ~1000 line distributed key value store, with support for replication, multiple machines, and multiple drives per machine. Optimized for values between 1MB and 1GB. Inspired by SeaweedFS, but simple. Should scale to billions of files and petabytes of data. Used in production at [comma.ai](https://comma.ai/).

A key part of minikeyvalue's simplicity is using stock nginx as the volume server.

Even if this code is crap, the on disk format is super simple! We rely on a filesystem for blob storage and a LevelDB for indexing. The index can be reconstructed with rebuild. Volumes can be added or removed with rebalance.

### API

- GET /key
  - 302 redirect to nginx volume server.
- PUT /key
  - Blocks. 201 = written, anything else = probably not written.
- DELETE /key
  - Blocks. 204 = deleted, anything else = probably not deleted.

### Start Volume Servers (default port 3001)

```
# this is just nginx under the hood
PORT=3001 ./volume /tmp/volume1/
PORT=3002 ./volume /tmp/volume2/
PORT=3003 ./volume /tmp/volume3/
```

### Start Master Server (default port 3000)

```
./mkv -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdb/ server
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

### ./mkv Usage

```
Usage: ./mkv <server, rebuild, rebalance>

  -basicauth
        Activate basic authorization
  -db string
        Path to leveldb
  -fallback string
        Fallback server for missing keys
  -port int
        Port for the server to listen on (default 3000)
  -protect
        Force UNLINK before DELETE
  -replicas int
        Amount of replicas to make of the data (default 3)
  -subvolumes int
        Amount of subvolumes, disks per machine (default 10)
  -userpass string
        Rebuild/rebalance with basic authorization (-userpass username:password)
  -volumes string
        Volumes to use for storage, comma separated
```

### Rebalancing (to change the amount of volume servers)

```
# must shut down master first, since LevelDB can only be accessed by one process
./mkv -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdb/ rebalance
```

### Rebuilding (to regenerate the LevelDB)

```
./mkv -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdbalt/ rebuild
```

### Performance

```
# Fetching non-existent key: 116338 req/sec
wrk -t2 -c100 -d10s http://localhost:3000/key

# go run thrasher.go
starting thrasher
10000 write/read/delete in 2.620922675s
thats 3815.40/sec
```

### Basic Authorization

Basic authorization is implemented via a htpasswd file used on the nginx volumes.

Generate a valid htpasswd file using `apache2-utils`:

```
# generate htpasswd file (using apache2-utils) and move it /etc/nginx
htpasswd -b -c .htpasswd admin thisisatest
mv .htpasswd /etc/nginx
```

Configure nginx volumes with basic authorization via `AUTH` enviromental variable:

```
# start nginx with the path of .htpasswd in AUTH enviromental variable
AUTH=/etc/nginx/.htpasswd PORT=3001 ./volume /tmp/volume1/
AUTH=/etc/nginx/.htpasswd PORT=3002 ./volume /tmp/volume2/
AUTH=/etc/nginx/.htpasswd PORT=3003 ./volume /tmp/volume3/
```

To secure the go master server start `mkv` with `-basicauth` command:

```
./mkv -basicauth -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdb/ server
```

To rebuild or rebalance with basic authorization the user and password need to be passed with `-userpass`:

```
./mkv -userpass admin:thisisatest -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdb/ rebalance
./mkv -userpass admin:thisisatest -volumes localhost:3001,localhost:3002,localhost:3003 -db /tmp/indexdbalt/ rebuild
```

Example of curl with basic authorization:

```
# curl with --user <USER>:<PASSWORD>
curl --user admin:thisisatest -v -L -X PUT -d bigswag localhost:3000/wehave
```

### Docker usage

Create docker image:
```
docker image build -t minikeyvalue:1.0 .
```

Create docker container:
```
docker container run -it \
    -p 3000:3000 \
    -p 3001:3001 \
    -p 3002:3002 \
    -p 3003:3003 \
    -p 3004:3004 \
    -p 3005:3005 \
    --detach --name minikeyvalue minikeyvalue:1.0
```
