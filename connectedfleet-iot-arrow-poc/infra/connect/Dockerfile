# Kafka Connect with Debezium 3.6.3.Final (PostgreSQL, MongoDB, Outbox Event Router SMT bundled)
# plus the official ClickHouse Kafka Connect Sink v1.6.0, downloaded and checksum-verified at build time.
#
# Debezium image layout: plugins load from $KAFKA_CONNECT_PLUGINS_DIR=/kafka/connect, user `kafka`.
#   https://github.com/debezium/container-images (connect-base Dockerfile / docker-entrypoint.sh)
# Sink release + digest (GitHub release asset, sha256 published by GitHub):
#   https://github.com/ClickHouse/clickhouse-kafka-connect/releases/tag/v1.6.0

ARG DEBEZIUM_IMAGE=quay.io/debezium/connect:3.6.3.Final

FROM alpine:3.24.1 AS clickhouse-sink
ARG SINK_VERSION=v1.6.0
ARG SINK_SHA256=8c7a013708b68d16beeb7cb99cbd5e76c185526441a499412fbb65e37878d5e4
RUN apk add --no-cache curl unzip \
 && curl -fsSL -o /tmp/sink.zip \
      "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/${SINK_VERSION}/clickhouse-kafka-connect-${SINK_VERSION}.zip" \
 && echo "${SINK_SHA256}  /tmp/sink.zip" | sha256sum -c - \
 && unzip -q /tmp/sink.zip -d /tmp/sink \
 && mkdir -p /out/clickhouse-kafka-connect \
 && cp /tmp/sink/clickhouse-kafka-connect-${SINK_VERSION}/lib/*.jar /out/clickhouse-kafka-connect/

FROM ${DEBEZIUM_IMAGE}
COPY --from=clickhouse-sink --chown=kafka:kafka /out/clickhouse-kafka-connect /kafka/connect/clickhouse-kafka-connect
