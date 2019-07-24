#!/bin/bash
ALTDB=/tmp/indexdbalt/
ALTDB2=/tmp/indexdbalt2/
echo "rebuild and rebalance test"

# take down main server (now leaves nginx running)
kill $(pgrep -f "indexdb")
set -e

# rebuild and compare the database
go run rebuild.go lib.go localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 $ALTDB
go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB

# do a rebalance, then put it back
#go run rebalance.go lib.go localhost:3001,localhost:3002,localhost:3003 $ALTDB
#go run rebalance.go lib.go localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 $ALTDB
#go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB

# rebuild and compare the database
#go run rebuild.go lib.go localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 $ALTDB2
#go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB2

