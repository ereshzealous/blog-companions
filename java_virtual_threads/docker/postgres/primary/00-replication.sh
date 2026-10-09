#!/bin/bash
# Runs once, inside the init-time server, before schema and seed. Creates the physical
# replication role and slot the standby streams from; the slot keeps WAL until the replica
# has consumed it, so a slow replica shows up as lag instead of a broken stream.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
	CREATE ROLE ${REPLICATION_USER} WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD}';
	SELECT pg_create_physical_replication_slot('replica_1');
SQL

echo "host replication ${REPLICATION_USER} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
