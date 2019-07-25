#!/bin/bash
ALTDB=/tmp/indexdbalt/
ALTDB2=/tmp/indexdbalt2/
echo "rebuild and rebalance test"

# take down main server (now leaves nginx running)
kill $(pgrep -f "indexdb")
set -e

# rebuild and compare the database
./mkv -volumes localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 -db $ALTDB rebuild
go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB

# do a rebalance, then put it back
./mkv -volumes localhost:3001,localhost:3002,localhost:3003 -db $ALTDB rebalance
./mkv -volumes localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 -db $ALTDB rebalance
go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB

# rebuild and compare the database
./mkv -volumes localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 -db $ALTDB2 rebuild
go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB2

