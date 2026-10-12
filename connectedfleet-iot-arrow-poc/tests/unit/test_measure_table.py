"""E1/E2 table measurement: hash without the memory-heavy GROUP BY unless the table really holds duplicates (D-029)."""

from types import SimpleNamespace

import pytest

from connectedfleet.bench import run


@pytest.mark.parametrize("physical,logical,dedupe", [(1_000_000, 1_000_000, False), (1_000_500, 1_000_000, True)])
def test_content_hash_dedupes_only_when_duplicates_exist(monkeypatch, physical, logical, dedupe):
    seen = {}
    monkeypatch.setattr(run.queries, "counts", lambda ch, db, t: {"physical": physical, "logical": logical})
    monkeypatch.setattr(run.queries, "latency_ms", lambda ch, db, t: {"samples": 0, "insert_batches": 0})
    monkeypatch.setattr(run.queries, "null_counts", lambda ch, db, t, cols: {})
    monkeypatch.setattr(run.queries, "active_parts", lambda ch, db, t: 1)
    monkeypatch.setattr(run.queries, "describe", lambda ch, db, t: [])

    def fake_hash(ch, db, table, columns, exclude=(), dedupe=True, **kw):
        seen["dedupe"] = dedupe
        return "h/1"

    monkeypatch.setattr(run.queries, "content_hash", fake_hash)
    run.Bench.measure_table(SimpleNamespace(ch=None), "bench_row", "telemetry_fact", ["event_id"], "drain")
    assert seen["dedupe"] is dedupe
