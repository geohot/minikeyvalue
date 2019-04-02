FROM ubuntu:16.04

# system basics
RUN apt-get update && apt-get -y install build-essential curl python3 python3-pip libffi-dev

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

COPY src /tmp/src
COPY test /tmp/test
COPY master volume /tmp/

