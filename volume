#!/bin/bash -e
export VOLUME=${1:-/tmp/volume1/}
export TYPE=volume
export PORT=${PORT:-3001}

# create 65536 directories for files
for I in 0 1 2 3 4 5 6 7 8 9 a b c d e f
do
  mkdir -m 777 -p $VOLUME/${I}{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}/{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}{0,1,2,3,4,5,6,7,8,9,a,b,c,d,e,f}
done

CONF=$(mktemp)
echo "
daemon off;
worker_processes auto;

error_log /dev/stderr;
pid $VOLUME/nginx.pid;

events {
  multi_accept on;
  worker_connections 4096;
}

http {
  sendfile on;
  sendfile_max_chunk 1024k;

  tcp_nopush on;
  tcp_nodelay on;

  open_file_cache off;
  types_hash_max_size 2048;

  server_tokens off;

  default_type application/octet-stream;

  server {
    listen $PORT default_server;
    location / {
      root $VOLUME;

      client_body_temp_path $VOLUME/body_temp;
      client_max_body_size 0;

      # this causes tests to fail
      #client_body_buffer_size 0;

      dav_methods PUT DELETE;
      dav_access group:rw all:r;

      autoindex on;
      autoindex_format json;
    }
  }
}
" > $CONF
echo "starting nginx on $PORT"
nginx -c $CONF -p $VOLUME/tmp

