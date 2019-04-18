FROM ubuntu:16.04

# system basics
RUN apt-get update && \
  apt-get -y --no-install-recommends install build-essential curl python3 python3-pip libffi-dev nginx golang && \
  apt-get clean

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

COPY server.go bringup.sh master volume test.py /tmp/

