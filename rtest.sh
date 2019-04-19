#!/bin/bash -e
ALTDB=/tmp/indexdbalt/
echo "rebuild and rebalance test"
echo "Although this works with the master server running, you shouldn't do that!"
go run rebuild.go lib.go localhost:3001,localhost:3002 $ALTDB
go run rebalance.go lib.go localhost:3001 $ALTDB
go run rebalance.go lib.go localhost:3002 $ALTDB
go run rebalance.go lib.go localhost:3001,localhost:3002 $ALTDB

