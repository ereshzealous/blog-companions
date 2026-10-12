"""LaneSink.prepare for every compute x insert variant, without any network access."""

import json
from datetime import timezone
from pathlib import Path

import pyarrow as pa
import pytest

from connectedfleet import topics
from connectedfleet.bench.workload import WorkloadSpec, events
from connectedfleet.contracts import canonical
from connectedfleet.contracts.canonical import format_timestamp_ms
from connectedfleet.lanes.sink import LaneSink

DUMMY = object()  # prepare() never touches the clients
ROOT = Path(__file__).resolve().parents[2]


def _values(n=300, null_fraction=0.2):
    out = []
    for e in events(WorkloadSpec(n, 3, null_fraction)):
        out.append(json.dumps({**e, "emitted_at": format_timestamp_ms(1_791_534_600_000)}).encode())
    return out


def _sink(compute, insert, transform="none"):
    return LaneSink("t", compute, insert, "bench_x", transform, http=DUMMY, native_client=DUMMY)


def _ipc_table(payload: bytes) -> pa.Table:
    return pa.ipc.open_stream(payload).read_all()


def test_arrow_arrowstream_payload_is_a_valid_ipc_stream_with_contract_schema():
    prepared = _sink("arrow", "arrowstream").prepare(topics.TELEMETRY, _values())
    table = _ipc_table(prepared.payload)
    assert table.schema == canonical.TELEMETRY.arrow_schema
    assert table.num_rows == prepared.rows == 300
    assert prepared.columns == canonical.TELEMETRY.names
    assert prepared.payload_bytes == len(prepared.payload)
    assert prepared.phase_ns["build"] > 0 and prepared.phase_ns["serialize"] > 0


def test_row_native_payload_has_aware_datetimes_and_no_serialize_phase():
    prepared = _sink("row", "native").prepare(topics.TELEMETRY, _values(10))
    first = prepared.payload[0]
    assert first[2].tzinfo == timezone.utc and first[3].tzinfo == timezone.utc
    assert prepared.phase_ns["serialize"] == 0  # Native encoding happens inside the client insert


def test_risk_transform_identical_across_all_four_variants():
    values = _values(500)
    risk_by_variant = {}
    for compute, insert in (("row", "native"), ("row", "arrowstream"), ("arrow", "arrowstream"), ("arrow", "native")):
        prepared = _sink(compute, insert, "risk").prepare(topics.TELEMETRY, values)
        assert prepared.table == "telemetry_enriched" and prepared.columns[-1] == "risk_flag"
        if insert == "arrowstream":
            risk = _ipc_table(prepared.payload).column("risk_flag").to_pylist()
        elif compute == "row":
            risk = [row[-1] for row in prepared.payload]
        else:  # column-oriented python lists
            risk = prepared.payload[-1]
        risk_by_variant[(compute, insert)] = risk
    assert len({tuple(r) for r in risk_by_variant.values()}) == 1
    assert {True, False, None} <= set(next(iter(risk_by_variant.values())))


def test_row_arrowstream_and_arrow_arrowstream_send_identical_tables():
    values = _values(200)
    a = _ipc_table(_sink("row", "arrowstream").prepare(topics.TELEMETRY, values).payload)
    b = _ipc_table(_sink("arrow", "arrowstream").prepare(topics.TELEMETRY, values).payload)
    assert a.equals(b)


def test_rejects_point_back_to_input_positions():
    values = _values(5)
    bad = json.loads(values[3])
    bad["battery_pct"] = "x"
    values[3] = json.dumps(bad).encode()
    prepared = _sink("arrow", "arrowstream").prepare(topics.TELEMETRY, values)
    assert [(r.index, r.reason) for r in prepared.rejects] == [(3, "BAD_TYPE")]
    assert prepared.rows == 4


def test_non_telemetry_topic_uses_its_contract():
    example = json.loads((ROOT / "contracts" / "examples" / "cf.device-state.v1.json").read_text())
    prepared = _sink("arrow", "arrowstream").prepare(topics.DEVICE_STATE, [json.dumps(example).encode()])
    table = _ipc_table(prepared.payload)
    assert table.schema == canonical.DEVICE_STATE.arrow_schema
    assert table.column("is_deleted").to_pylist() == [0]


def test_all_rejected_batch_has_no_payload():
    prepared = _sink("row", "native").prepare(topics.TELEMETRY, [b"{broken", b"[]"])
    assert prepared.rows == 0 and prepared.payload is None and len(prepared.rejects) == 2


def test_unknown_variant_rejected():
    with pytest.raises(ValueError):
        LaneSink("t", "rows", "native", "db", native_client=DUMMY)
