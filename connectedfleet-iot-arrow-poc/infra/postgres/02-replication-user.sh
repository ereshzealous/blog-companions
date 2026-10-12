#!/usr/bin/env bash
# Least-privilege login for Debezium: REPLICATION + SELECT on the two captured tables.
# Publications already exist (01-schema.sql), so the connector never needs CREATE privileges.
# https://debezium.io/documentation/reference/3.6/connectors/postgresql.html (Setting up permissions)
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v dbz_user="$DEBEZIUM_PG_USER" -v dbz_password="$DEBEZIUM_PG_PASSWORD" -v db_name="$POSTGRES_DB" <<'EOSQL'
CREATE ROLE :"dbz_user" WITH LOGIN REPLICATION PASSWORD :'dbz_password';
GRANT CONNECT ON DATABASE :"db_name" TO :"dbz_user";
GRANT USAGE ON SCHEMA public TO :"dbz_user";
GRANT SELECT ON device_registry, outbox_event TO :"dbz_user";
EOSQL
