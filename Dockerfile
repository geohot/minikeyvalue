FROM ubuntu:20.04

# system basics
RUN apt-get update && \
  apt-get -y --no-install-recommends install \
    build-essential \
    curl \
    python3 \
    python3-dev \
    python3-setuptools \
    python3-pip \
    libffi-dev \
    nginx \
    golang \
    git \
    apache2-utils && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /
ENV GOPATH /usr/lib/go
ENV PATH ${PATH}:/mkv

COPY requirements.txt mkv/requirements.txt
RUN pip3 install --no-cache-dir -r mkv/requirements.txt
RUN htpasswd -b -c .htpasswd admin thisisatest && mv .htpasswd /etc/nginx

WORKDIR /mkv

COPY go.mod mkv volume ./
COPY src/*.go ./src/
COPY tools/* ./tools/
