#!/bin/sh
set -e

if [ -z "${JM_SESSION_SECRET}" ]; then
  JM_SESSION_SECRET=$(python -c "import secrets; print(secrets.token_hex(32))")
  export JM_SESSION_SECRET
fi

exec "$@"
