#!/usr/bin/env bash
# Create .env with image versions, lab tunables and random local-only credentials.
# Existing .env files are left untouched.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  echo ".env already exists; delete it to regenerate."
  exit 0
fi
secret() { openssl rand -hex 16; }
cat > .env <<EOF
# Image versions used for the published runs.
POSTGRES_VERSION=18.6
KAFKA_VERSION=4.3.1
DEBEZIUM_VERSION=3.6.2.Final
CLICKHOUSE_VERSION=26.8.3.105

# Seed data (synthetic device platform; no personal or clinical data).
SEED_REGISTRY_ROWS=2000000
SEED_STATUS_ROWS=500000

# Kafka Connect settings the lab measures. Kafka's defaults are 60000 and 300000.
CONNECT_OFFSET_FLUSH_INTERVAL_MS=60000
CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS=300000

# Sink batching.
SINK_BATCH_MAX=10000
SINK_BATCH_WAIT_S=0.5

# Load generator defaults (overridden per scenario).
GENERATOR_RATE=20000
GENERATOR_WORKERS=6

# Freshness objective used by the collector's alert (source commit -> queryable).
FRESHNESS_SLO_MS=60000

# Local-only credentials, generated $(date -u +%Y-%m-%dT%H:%M:%SZ).
PG_ADMIN_PASSWORD=$(secret)
CDC_CAPTURE_PASSWORD=$(secret)
APP_PASSWORD=$(secret)
MONITOR_PASSWORD=$(secret)
CLICKHOUSE_PASSWORD=$(secret)
CLICKHOUSE_SINK_PASSWORD=$(secret)
EOF
echo "wrote .env"
