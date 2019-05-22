#!/bin/bash
docker build -t minikeyvalue -f Dockerfile .
docker run --hostname localhost -p 3000:3000 -p 3001:3001 -p 3002:3002 --name mkv --rm minikeyvalue -it ./bringup.sh
