#!/usr/bin/env bash
# MongoDB single-member replica set with authentication.
# A replica set with auth enabled needs an internal keyFile; it is generated per container
# (never committed) and must be owned by the mongod user (uid 999) with mode 400.
# The replica set itself is initiated by the compose healthcheck (rs.initiate on first run).
# VERIFY: official image's docker-entrypoint.sh accepts --replSet together with MONGO_INITDB_ROOT_* on first init.
set -euo pipefail
KEYFILE=/etc/mongo-keyfile
if [[ ! -f "$KEYFILE" ]]; then
  head -c 756 /dev/urandom | base64 | tr -d '\n' > "$KEYFILE"
  chmod 400 "$KEYFILE"
  chown 999:999 "$KEYFILE"
fi
exec docker-entrypoint.sh mongod --replSet rs0 --keyFile "$KEYFILE" --bind_ip_all
