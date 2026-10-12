"""Kafka admin helpers: idempotent topic creation, message counts and consumer lag."""

from __future__ import annotations

from connectedfleet import topics as topic_names


def create_topics(bootstrap: str, specs: dict[str, int], replication: int = 1) -> dict[str, str]:
    """Create topics that do not exist yet. Returns {topic: "created" | "exists"}."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=15).topics)
    result = {t: "exists" for t in specs if t in existing}
    todo = [NewTopic(t, num_partitions=p, replication_factor=replication) for t, p in specs.items() if t not in existing]
    if todo:
        for topic, future in admin.create_topics(todo).items():
            try:
                future.result(30)
                result[topic] = "created"
            except KafkaException as err:
                if err.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
                result[topic] = "exists"
    return result


def delete_topics(bootstrap: str, names: list[str]) -> None:
    from confluent_kafka.admin import AdminClient

    if not names:
        return
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.delete_topics(names).values():
        try:
            future.result(30)
        except Exception:  # noqa: BLE001 - best-effort cleanup of benchmark topics
            pass


def all_topic_specs() -> dict[str, int]:
    return dict(topic_names.PARTITIONS)


def message_count(bootstrap: str, topic: str) -> int:
    """Sum over partitions of (high watermark - low watermark): records currently retained."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "cf-admin-count", "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(topic, timeout=15)
        partitions = metadata.topics[topic].partitions
        total = 0
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=15)
            total += high - low
        return total
    finally:
        consumer.close()


def group_lag(bootstrap: str, group: str, topic: str) -> int | None:
    """Total lag of a consumer group on one topic (None if the group has no committed offsets yet)."""
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "cf-admin-lag", "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(topic, timeout=15)
        parts = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
        from confluent_kafka import ConsumerGroupTopicPartitions

        futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, parts)])
        committed = futures[group].result(15).topic_partitions
        lag = 0
        seen = False
        for tp in committed:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=15)
            if tp.offset >= 0:
                seen = True
                lag += max(0, high - tp.offset)
            else:
                lag += high
        return lag if seen else None
    finally:
        consumer.close()
