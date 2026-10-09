#!/bin/bash
# Physical hot standby: clone the primary once with pg_basebackup (-R writes standby.signal
# and primary_conninfo), then run postgres in recovery mode against the replication slot.
set -euo pipefail

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h "$PRIMARY_HOST" -p 5432 -q; do
    echo "replica: waiting for $PRIMARY_HOST"
    sleep 1
  done
  rm -rf "${PGDATA:?}"/*
  PGPASSWORD="$REPLICATION_PASSWORD" pg_basebackup \
    --host="$PRIMARY_HOST" --port=5432 --username="$REPLICATION_USER" \
    --pgdata="$PGDATA" --wal-method=stream --slot="$REPLICATION_SLOT" \
    --write-recovery-conf --checkpoint=fast --no-password
  chmod 0700 "$PGDATA"
fi

exec postgres -c hot_standby=on -c hot_standby_feedback=on -c max_connections=200
