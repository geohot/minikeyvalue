#!/bin/bash
#trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

PORT=3001 ./volume /tmp/volume1/ 2 &
PORT=3002 ./volume /tmp/volume2/ 2 &
PORT=3003 ./volume /tmp/volume3/ 2 &
PORT=3004 ./volume /tmp/volume4/ 2 &
PORT=3005 ./volume /tmp/volume5/ 2 &

./master localhost:3001,localhost:3002,localhost:3003,localhost:3004,localhost:3005 /tmp/indexdb/

