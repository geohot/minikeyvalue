FROM ubuntu:16.04

# system basics
RUN apt-get update && \
  apt-get -y --no-install-recommends install build-essential curl python3 python3-dev python3-setuptools python3-pip libffi-dev nginx golang git && \
  apt-get clean

RUN pip3 install --no-cache-dir requests

ENV GOPATH $HOME/go  
COPY lib.go thrasher.go server.go bringup.sh master volume test.py /tmp/

