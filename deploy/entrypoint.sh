#!/bin/sh
set -e

if [ -z "${JM_SESSION_SECRET}" ]; then
  JM_SESSION_SECRET=$(python -c "import secrets; print(secrets.token_hex(32))")
  export JM_SESSION_SECRET
fi

mkdir -p /data
if [ -e /app/data ] && [ ! -L /app/data ]; then
  if [ -f /app/data/banuser.log ] && [ ! -f /data/banuser.log ]; then
    cp /app/data/banuser.log /data/banuser.log
  fi
  rm -rf /app/data
fi
if [ ! -e /app/data ]; then
  ln -s /data /app/data
fi

exec "$@"
