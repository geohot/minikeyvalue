#!/bin/bash
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

# have to wait for this
./precreate.py /tmp/volume1/
./precreate.py /tmp/volume2/

PORT=3001 ./volume /tmp/volume1/ &
PORT=3002 ./volume /tmp/volume2/ &

./master localhost:3001,localhost:3002 /tmp/cachedb/

