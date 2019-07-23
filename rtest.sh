#!/bin/bash -e
ALTDB=/tmp/indexdbalt/
echo "rebuild and rebalance test"

# take down main server
kill $(pgrep -f "./master")

# rebuild and compare the database
go run rebuild.go lib.go localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 $ALTDB
go run tools/leveldb_compare.go /tmp/indexdb/ $ALTDB

# do a few rebalances
go run rebalance.go lib.go localhost:3001 $ALTDB
go run rebalance.go lib.go localhost:3002 $ALTDB
go run rebalance.go lib.go localhost:3001,localhost:3002 $ALTDB

