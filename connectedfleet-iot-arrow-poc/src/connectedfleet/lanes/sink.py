"""Lane sinks: compute representation x insert format.

    lane B  = LaneSink(compute="row",   insert="native")       row control
    lane C  = LaneSink(compute="arrow", insert="arrowstream")  Arrow candidate
    2x2     = (row, arrowstream) and (arrow, native)            Lab 03 E2 isolation; each pays a labelled
                                                               extra conversion inside its serialize phase

Phases (time.perf_counter_ns), aligned with Lab 01:
  decode     JSON bytes -> dicts                   shared codec, all variants
  validate   dicts -> typed tuples                 shared codec, all variants
  build      row: client values (datetimes)  |  Arrow: typed arrays/RecordBatch
  transform  risk flag (only with transform="risk"; telemetry only)
  serialize  Arrow IPC stream (arrowstream) | row->Arrow (row,arrowstream) | Arrow->Python columns (arrow,native)
  insert     measured by the loop: HTTP round trip until ClickHouse acknowledges.
             For "native" the Native encoding happens inside clickhouse-connect's insert,
             so it is part of insert, not serialize. Compare serialize+insert across lanes.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import perf_counter_ns

import pyarrow as pa
import pyarrow.compute as pc

from connectedfleet import topics
from connectedfleet.clickhouse.http import SYNC_INSERT, ClickHouseError, ClickHouseHTTP
from connectedfleet.codec import canonical as codec
from connectedfleet.contracts import canonical
from connectedfleet.contracts import telemetry as tel
from connectedfleet.lanes.loop import InsertFailed, Prepared

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
COMPUTE = ("row", "arrow")
INSERT = ("native", "arrowstream")


def _is_telemetry(topic: str) -> bool:
    return topic == topics.TELEMETRY or topic.startswith("cfbench.")


def _ms_to_datetime(value: int | None):
    return None if value is None else _EPOCH + timedelta(milliseconds=value)


class LaneSink:
    def __init__(self, name: str, compute: str, insert: str, database: str, transform: str = "none",
                 http: ClickHouseHTTP | None = None, native_client=None) -> None:
        if compute not in COMPUTE or insert not in INSERT:
            raise ValueError(f"unsupported variant compute={compute} insert={insert}")
        if insert == "arrowstream" and http is None:
            raise ValueError("arrowstream insert needs a ClickHouseHTTP client")
        if insert == "native" and native_client is None:
            raise ValueError("native insert needs a clickhouse-connect client")
        self.name = name
        self.compute = compute
        self.insert_format = insert
        self.database = database
        self.transform_mode = transform
        self.http = http
        self.native = native_client

    # ------------------------------------------------------------------ helpers
    def _target(self, topic: str) -> tuple[canonical.TopicContract, str, list[canonical.Field]]:
        contract = codec.contract_for(topic)
        if _is_telemetry(topic) and self.transform_mode == "risk":
            return contract, "telemetry_enriched", list(contract.fields) + list(canonical.ENRICHED_EXTRA)
        return contract, contract.table, list(contract.fields)

    def _timestamp_positions(self, fields: list[canonical.Field]) -> list[int]:
        return [i for i, f in enumerate(fields) if f.kind == canonical.TIMESTAMP]

    # ------------------------------------------------------------------ prepare
    def prepare(self, topic: str, values: list[bytes]) -> Prepared:
        contract, table, fields = self._target(topic)
        columns = [f.name for f in fields]
        risky = len(fields) > len(contract.fields)
        phase = dict.fromkeys(("decode", "validate", "build", "transform", "serialize"), 0)

        t0 = perf_counter_ns()
        decoded, decode_rejects = codec.decode(values)
        t1 = perf_counter_ns()
        rows, validate_rejects = codec.validate(topic, decoded)
        t2 = perf_counter_ns()
        phase["decode"], phase["validate"] = t1 - t0, t2 - t1
        rejects = decode_rejects + validate_rejects

        payload: object
        payload_bytes: int | None = None
        if not rows:
            return Prepared(topic, table, columns, None, 0, rejects, phase, 0)

        if self.compute == "row":
            stamp_cols = self._timestamp_positions(list(contract.fields))
            t3 = perf_counter_ns()
            built = []
            for row in rows:
                values_ = list(row)
                for i in stamp_cols:
                    values_[i] = _ms_to_datetime(values_[i])
                built.append(values_)
            t4 = perf_counter_ns()
            if risky:
                temp_i, batt_i = contract.names.index("temperature_c"), contract.names.index("battery_pct")
                for values_ in built:
                    values_.append(tel.risk_flag(values_[batt_i], values_[temp_i]))
            t5 = perf_counter_ns()
            phase["build"], phase["transform"] = t4 - t3, t5 - t4
            if self.insert_format == "native":
                payload = built
            else:  # row compute -> ArrowStream: extra row->column conversion, labelled as serialize
                schema = pa.schema([pa.field(f.name, f.arrow_type, nullable=f.nullable) for f in fields])
                columns_data = list(zip(*built))
                batch = pa.RecordBatch.from_arrays(
                    [pa.array(col, type=f.arrow_type) for col, f in zip(columns_data, fields)], schema=schema
                )
                payload = _ipc(batch)
                payload_bytes = len(payload)
                phase["serialize"] = perf_counter_ns() - t5
        else:
            schema = pa.schema([pa.field(f.name, f.arrow_type, nullable=f.nullable) for f in contract.fields])
            t3 = perf_counter_ns()
            columns_data = list(zip(*rows))
            batch = pa.RecordBatch.from_arrays(
                [pa.array(col, type=f.arrow_type) for col, f in zip(columns_data, contract.fields)], schema=schema
            )
            t4 = perf_counter_ns()
            if risky:
                risk = pc.and_kleene(
                    pc.less(batch.column("battery_pct"), tel.BATTERY_RISK_BELOW),
                    pc.greater(batch.column("temperature_c"), tel.TEMPERATURE_RISK_ABOVE),
                )
                batch = pa.RecordBatch.from_arrays(
                    [*batch.columns, risk],
                    schema=schema.append(pa.field("risk_flag", pa.bool_(), nullable=True)),
                )
            t5 = perf_counter_ns()
            phase["build"], phase["transform"] = t4 - t3, t5 - t4
            if self.insert_format == "arrowstream":
                payload = _ipc(batch)
                payload_bytes = len(payload)
            else:  # Arrow compute -> Native: extra column->Python conversion, labelled as serialize
                payload = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
            phase["serialize"] = perf_counter_ns() - t5

        return Prepared(topic, table, columns, payload, len(rows), rejects, phase, payload_bytes)

    # ------------------------------------------------------------------ insert
    def insert(self, prepared: Prepared) -> int:
        started = perf_counter_ns()
        if self.insert_format == "arrowstream":
            try:
                self.http.insert_arrow_stream(self.database, prepared.table, prepared.columns, prepared.payload)
            except ClickHouseError as err:
                raise InsertFailed(str(err), err.retriable) from err
            except OSError as err:
                raise InsertFailed(str(err), True) from err
        else:
            from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

            contract_types = _column_types(prepared)
            try:
                self.native.insert(
                    prepared.table,
                    prepared.payload,
                    column_names=prepared.columns,
                    database=self.database,
                    column_type_names=contract_types,
                    column_oriented=self.compute == "arrow",
                )
            except OperationalError as err:  # network / server unavailable
                raise InsertFailed(str(err), True) from err
            except DatabaseError as err:  # data/schema errors: retrying will not help
                raise InsertFailed(str(err), False) from err
        return perf_counter_ns() - started


def _ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _column_types(prepared: Prepared) -> list[str]:
    contract = codec.contract_for(prepared.topic)
    by_name = {f.name: f.ch_type for f in list(contract.fields) + list(canonical.ENRICHED_EXTRA)}
    return [by_name[c] for c in prepared.columns]


def build_sink(lane: str, database: str, transform: str, compute: str | None, insert: str | None, settings) -> LaneSink:
    """Default variants: row -> (row, native); arrow -> (arrow, arrowstream)."""
    compute = compute or ("row" if lane == "row" else "arrow")
    insert = insert or ("native" if lane == "row" else "arrowstream")
    http = None
    native = None
    if insert == "arrowstream":
        http = ClickHouseHTTP(settings.clickhouse_http_url, settings.clickhouse_user, settings.clickhouse_password)
    else:
        import urllib.parse

        import clickhouse_connect

        url = urllib.parse.urlsplit(settings.clickhouse_http_url)
        native = clickhouse_connect.get_client(
            host=url.hostname,
            port=url.port or 8123,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=database,
            interface="http",
            compress=False,  # both custom lanes send uncompressed bodies; compression is a separate variable
            settings=dict(SYNC_INSERT),
        )
    default_variant = (compute, insert) in (("row", "native"), ("arrow", "arrowstream"))
    name = lane if default_variant else f"{lane}:{compute}-{insert}"
    return LaneSink(name, compute, insert, database, transform, http, native)
