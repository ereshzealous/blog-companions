"""Physical Arrow layout claims used in Article 01 / figure 01-F03, checked on real buffers."""

import pyarrow as pa

from connectedfleet.codec import telemetry as codec
from connectedfleet.contracts import telemetry as contract
from connectedfleet.fixtures import telemetry as fixtures
from connectedfleet.lab01 import arrow_path, buffers


def test_toy_validity_bitmap_is_lsb_first():
    layout = buffers.illustrative_layout()
    # sensor-03 has a null temperature: slots 0,1,3 valid, slot 2 null.
    assert layout["temperature_c"]["validity_bits_lsb_first"] == "1101"
    assert layout["temperature_c"]["null_count"] == 1
    assert layout["temperature_c"]["values_float64"][2] is None


def test_no_validity_buffer_without_nulls():
    layout = buffers.illustrative_layout()
    assert layout["battery_pct"]["validity_bits_lsb_first"] is None
    assert layout["battery_pct"]["buffers"][0] == {"role": "validity", "present": False, "size_bytes": None}


def test_string_offsets_have_length_plus_one_entries():
    layout = buffers.illustrative_layout()
    assert layout["device_id"]["offsets_int32"] == [0, 9, 18, 27, 36]
    assert layout["device_id"]["data_utf8"] == "sensor-01sensor-02sensor-03sensor-04"


def test_uint8_values_are_contiguous_bytes():
    assert buffers.illustrative_layout()["battery_pct"]["values_uint8"] == [88, 17, 64, 12]


def test_null_slot_still_occupies_data_buffer_space():
    batch = buffers.toy_batch()
    temperature = batch.column("temperature_c")
    assert temperature.buffers()[1].size >= 4 * 8


def test_recordbatch_columns_share_one_length():
    fx = fixtures.generate(1_000)
    decoded, _ = codec.decode(fx.lines)
    rows, _, _ = codec.validate(decoded)
    batch = arrow_path.build(rows)
    assert batch.schema == contract.ARROW_SCHEMA
    assert {len(col) for col in batch.columns} == {batch.num_rows}


def test_ipc_stream_roundtrip_preserves_schema_and_values():
    fx = fixtures.generate(600)
    decoded, _ = codec.decode(fx.lines)
    rows, _, _ = codec.validate(decoded)
    enriched, _ = arrow_path.transform(arrow_path.build(rows), {})
    payload = arrow_path.serialize(enriched)
    reader = pa.ipc.open_stream(payload)
    assert reader.schema == contract.ENRICHED_ARROW_SCHEMA
    roundtrip = reader.read_all()
    assert roundtrip.to_batches()[0].equals(enriched)


def test_observed_layout_reports_every_column():
    fx = fixtures.generate(500)
    decoded, _ = codec.decode(fx.lines)
    rows, _, _ = codec.validate(decoded)
    observed = buffers.observed_layout(arrow_path.build(rows))
    assert set(observed["columns"]) == set(contract.ARROW_SCHEMA.names)
    assert observed["columns"]["device_id"]["buffers"][1]["role"] == "offsets"
