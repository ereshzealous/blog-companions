"""Gateway generator, CSV importer and benchmark workload: determinism, stable ids, bounded chunks."""

import json
from pathlib import Path

import pytest

from connectedfleet import topics
from connectedfleet.bench.workload import WorkloadSpec, digest_and_nulls, events
from connectedfleet.normalize.mappers import MappingError, map_csv_row, map_gateway
from connectedfleet.producers import csv_import
from connectedfleet.producers.gateway import generate_raw

ROOT = Path(__file__).resolve().parents[2]
CSV = ROOT / "fixtures" / "diagnostics" / "batch-001.csv"


def test_gateway_generator_is_deterministic_and_tagged():
    a = list(generate_raw(200, seed=7, run_tag="t1"))
    b = list(generate_raw(200, seed=7, run_tag="t1"))
    assert a == b
    assert a[0]["msg_id"] == "gw-t1-000000000"
    assert all(r["sent"] is None for r in a)  # stamped at send time, never generated


def test_gateway_poison_positions_and_mapping():
    records = list(generate_raw(100, seed=7, run_tag="p", poison_every=50))
    poison = [i for i, r in enumerate(records) if r["ts"] == "yesterday"]
    assert poison == [49, 99]
    for i, record in enumerate(records):
        record["sent"] = 1_791_534_600_000
        if i in poison:
            with pytest.raises(MappingError):
                map_gateway(record)
        else:
            map_gateway(record)


def test_csv_fixture_has_expected_header_and_20_rows():
    rows = csv_import.read_rows(CSV.read_text(encoding="utf-8"))
    assert len(rows) == 20


def test_csv_messages_have_stable_ids_and_bounded_chunks():
    chunks = list(csv_import.iter_messages(CSV, "2026-10-09T09:00:00.000Z", chunk_rows=6))
    assert [len(c) for c in chunks] == [6, 6, 6, 2]
    first = json.loads(chunks[0][0][1])
    assert first["row_number"] == 1 and first["file_sha256"] == csv_import.file_sha256(CSV)
    again = list(csv_import.iter_messages(CSV, "2026-10-10T00:00:00.000Z", chunk_rows=6))
    ids = lambda cs: [map_csv_row(json.loads(v))["event_id"] for c in cs for _, v in c]  # noqa: E731
    assert ids(chunks) == ids(again), "re-import must reproduce identical logical ids"
    assert len(set(ids(chunks))) == 20


def test_csv_header_mismatch_is_rejected(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("device,when\nx,y\n")
    with pytest.raises(csv_import.CsvSchemaError):
        list(csv_import.iter_messages(bad, "2026-10-09T09:00:00Z"))


def test_benchmark_workload_is_deterministic_and_contract_valid():
    from connectedfleet.contracts import canonical

    spec = WorkloadSpec(500, 11, null_fraction=0.3)
    first = list(events(spec))
    assert first == list(events(spec))
    for e in first:
        canonical.validate(canonical.TELEMETRY, {**e, "emitted_at": e["event_time"]})


def test_workload_digest_and_null_counts_follow_the_spec():
    dense = digest_and_nulls(WorkloadSpec(2000, 1, null_fraction=0.0))
    heavy = digest_and_nulls(WorkloadSpec(2000, 1, null_fraction=0.5))
    assert dense[1] == {"temperature_c": 0, "battery_pct": 0, "signal_dbm": 0}
    assert all(800 < v < 1200 for v in heavy[1].values())
    assert dense[0] != heavy[0]
    assert sum(heavy[2].values()) == 2000


def test_string_heavy_shape_produces_long_strings():
    e = next(events(WorkloadSpec(1, 1, string_heavy=True)))
    assert len(e["firmware_version"]) > 40 and len(e["site_id"]) > 40


def test_bench_topic_names_are_kafka_safe():
    name = topics.bench_topic("lab03-smoke-20261010T120000Z-abc1234", "e1-n10000")
    assert name.startswith("cfbench.") and all(c.isalnum() or c in "._-" for c in name) and len(name) < 249


class _CapturingProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value, headers=None):
        self.sent.append(json.loads(value))

    def flush(self):
        pass


def test_benchmark_poison_is_sent_mid_stream_inside_a_batch():
    """E4 poison must land inside a batch, not alone after the last event.

    Sent last, the poison sat alone in the official sink's trailing buffer, so the scenario could not show
    batch-level failure handling (Lab 03 probe 2026-10-10: one poison dead-lettered its whole 501-record batch).
    """
    from connectedfleet.bench.producer import poison_positions, produce

    positions = poison_positions(5000, 1)
    assert positions == [2501]
    assert all(p % 500 not in (0, 499) for p in positions)  # never the first or last record of a 500-record batch
    capture = _CapturingProducer()
    produce(capture, "t", WorkloadSpec(5000, 11, 0.04, False, id_prefix="e4"), poison=1)
    ids = [m["event_id"] for m in capture.sent]
    assert len(ids) == 5001 and ids[2501].startswith("poison-")
    assert sum(i.startswith("poison-") for i in ids) == 1


def test_benchmark_poison_positions_spread_and_stay_in_range():
    from connectedfleet.bench.producer import poison_positions

    assert poison_positions(100, 0) == []
    positions = poison_positions(1000, 3)
    assert positions == sorted(set(positions)) and 0 < positions[0] and positions[-1] < 1000
