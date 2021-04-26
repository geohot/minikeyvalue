#!/bin/bash
#trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
kill $(pgrep -f nginx)

PORT=3001 ./volume /tmp/volume1/ &
PORT=3002 ./volume /tmp/volume2/ &
PORT=3003 ./volume /tmp/volume3/ &
PORT=3004 ./volume /tmp/volume4/ &
PORT=3005 ./volume /tmp/volume5/ &

./mkv -port 3000 -volumes localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 -db /tmp/indexdb/ server

