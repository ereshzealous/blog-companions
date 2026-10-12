"""Worker consumer settings and the app-image record (D-031)."""

import json
import subprocess

from connectedfleet.instrumentation import env
from connectedfleet.lanes import worker


def test_prefetch_does_not_back_off_for_a_second():
    # librdkafka defaults (queued.min.messages=100000, fetch.queue.backoff.ms=1000) cap a consumer that keeps up at
    # roughly 100K-136K messages/s: once 100,000 messages are queued, the next fetch waits a full second.
    assert worker.CONSUMER_CONFIG["fetch.queue.backoff.ms"] <= 50


def test_keeps_the_long_poll_interval():
    assert worker.CONSUMER_CONFIG["max.poll.interval.ms"] == 300_000


def test_image_info_records_id_and_build_time(monkeypatch):
    def fake_run(cmd, **kwargs):
        assert cmd[:3] == ["docker", "image", "inspect"]
        return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps([{"Id": "sha256:abc", "Created": "2026-10-10T21:00:00Z"}]))

    monkeypatch.setattr(env.subprocess, "run", fake_run)
    assert env.image_info("connectedfleet/app:0.1.0") == {
        "image": "connectedfleet/app:0.1.0", "id": "sha256:abc", "created": "2026-10-10T21:00:00Z"}


def test_image_info_is_none_without_docker(monkeypatch):
    def fail(cmd, **kwargs):
        raise OSError("no docker")

    monkeypatch.setattr(env.subprocess, "run", fail)
    assert env.image_info("connectedfleet/app:0.1.0") is None


def test_consume_probe_counts_records_from_the_first_one(monkeypatch):
    from connectedfleet.bench import consume_probe

    class FakeConsumer:
        def __init__(self, bootstrap, group, topics, extra):
            self.left = 2500
            self.closed = False

        def consume(self, n, timeout):
            take = min(n, self.left)
            self.left -= take
            return [object()] * take

        def close(self):
            self.closed = True

    monkeypatch.setattr(consume_probe, "KafkaConsumer", FakeConsumer)
    result = consume_probe.drain("kafka:19092", "t", 2500, 1000, {})
    assert result["records"] == 2500
    assert result["records_per_s"] > 0
