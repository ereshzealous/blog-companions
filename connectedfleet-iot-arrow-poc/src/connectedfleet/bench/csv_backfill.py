"""Lab 03 E3: historical CSV backfill, three ways, same file, identical target tables.

    direct  streamed HTTP upload, parsed by ClickHouse: INSERT ... SELECT ... FROM input(...) FORMAT CSVWithNames
    row     Python csv module -> typed tuples -> clickhouse-connect insert (Native), bounded batches
    arrow   pyarrow.csv streaming reader -> RecordBatches -> IPC stream -> INSERT ... FORMAT ArrowStream

This is the file path, separate from the CSV->Kafka importer. Runs inside the
`app` container (Linux) so the client numbers are comparable:

    python -m connectedfleet.bench.csv_backfill generate --rows 100000 --out /work/diag.csv [--gzip]
    python -m connectedfleet.bench.csv_backfill run --method arrow --file /work/diag.csv --database bench_arrow --batch 10000

Client-side CPU/RSS come from getrusage of this process. For `direct` almost
all work happens inside the ClickHouse server, so its client numbers are not a
fair cost comparison (reported with that label).
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import random
import resource
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from connectedfleet.config import Settings

TABLE = "diagnostic_backfill"
CSV_COLUMNS = ("row_number", "device_id", "diagnosed_at", "error_code", "severity", "battery_pct",
               "temperature_c", "firmware_version", "note")
INSERT_COLUMNS = (*CSV_COLUMNS, "file_sha256")
CH_TYPES = ("UInt32", "String", "DateTime64(3, 'UTC')", "LowCardinality(String)", "LowCardinality(String)",
            "Nullable(UInt8)", "Nullable(Float64)", "Nullable(String)", "Nullable(String)", "String")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def generate(rows: int, out: Path, seed: int = 20261010, compress: bool = False) -> dict:
    rng = random.Random(seed)
    codes = [("E-BATT-017", "warning"), ("E-TEMP-102", "error"), ("E-RADIO-004", "info"), ("E-FW-221", "critical")]
    notes = ["battery sag under load", "", "antenna re-seated", "Kühlkette unterbrochen", "", "sensor recalibrated"]
    opener = (lambda p: gzip.open(p, "wt", encoding="utf-8", newline="")) if compress else (
        lambda p: open(p, "w", encoding="utf-8", newline=""))
    with opener(out) as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(CSV_COLUMNS)
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for n in range(1, rows + 1):
            code, severity = codes[n % len(codes)]
            moment = base + timedelta(seconds=rng.randrange(0, 270 * 86400))
            writer.writerow([
                n, f"sensor-{rng.randrange(1000):05d}", moment.strftime("%Y-%m-%dT%H:%M:%SZ"), code, severity,
                "" if rng.random() < 0.05 else rng.randint(0, 100),
                "" if rng.random() < 0.05 else f"{rng.uniform(-20, 80):.1f}",
                "" if rng.random() < 0.02 else rng.choice(["3.2.1", "3.2.0", "3.1.9"]),
                rng.choice(notes),
            ])
    return {"file": str(out), "rows": rows, "bytes": out.stat().st_size, "sha256": file_sha256(out), "gzip": compress}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _usage() -> dict:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {"cpu_s": usage.ru_utime + usage.ru_stime, "max_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)}


# ---------------------------------------------------------------------- direct
def run_direct(http, database: str, path: Path, sha: str, tag_settings: dict | None = None) -> dict:
    structure = ("row_number UInt32, device_id String, diagnosed_at String, error_code String, severity String, "
                 "battery_pct Nullable(UInt8), temperature_c Nullable(Float64), firmware_version Nullable(String), "
                 "note Nullable(String)")
    query = (
        f"INSERT INTO `{database}`.`{TABLE}` ({', '.join(INSERT_COLUMNS)}) "
        f"SELECT row_number, device_id, parseDateTime64BestEffort(diagnosed_at, 3, 'UTC'), error_code, severity, "
        f"battery_pct, temperature_c, firmware_version, note, '{sha}' FROM input('{structure}') FORMAT CSVWithNames"
    )
    encoding = "gzip" if path.suffix == ".gz" else None
    # The file is streamed as the HTTP body (never read whole into memory); ClickHouse parses the CSV.
    # Empty CSV fields become NULL for Nullable columns (input_format_csv_empty_as_default).
    http.insert_file({"query": query, "input_format_csv_empty_as_default": 1, **(tag_settings or {})}, path,
                     {"Content-Encoding": encoding} if encoding else None)
    return {"body_bytes": path.stat().st_size, "batches": 1, "transport": "streamed HTTP upload, parsed by ClickHouse",
            "client_cost_note": "ClickHouse does the parsing: compare server cost (`server`), not client CPU alone"}


# ---------------------------------------------------------------------- row
def _row_values(row: list[str], sha: str) -> list:
    number, device, diagnosed, code, severity, battery, temperature, firmware, note = row
    moment = datetime.fromisoformat(diagnosed)
    return [int(number), device, moment, code, severity,
            None if battery == "" else int(battery),
            None if temperature == "" else float(temperature),
            None if firmware == "" else firmware,
            None if note == "" else note, sha]


def run_row(native, database: str, path: Path, sha: str, batch: int) -> dict:
    opener = (lambda p: gzip.open(p, "rt", encoding="utf-8", newline="")) if path.suffix == ".gz" else (
        lambda p: open(p, "r", encoding="utf-8", newline=""))
    batches = 0
    with opener(path) as handle:
        reader = csv.reader(handle)
        header = next(reader)
        if tuple(header) != CSV_COLUMNS:
            raise ValueError(f"unexpected header {header}")
        rows: list[list] = []
        for raw in reader:
            rows.append(_row_values(raw, sha))
            if len(rows) >= batch:
                native.insert(TABLE, rows, column_names=list(INSERT_COLUMNS), database=database, column_type_names=list(CH_TYPES))
                batches += 1
                rows = []
        if rows:
            native.insert(TABLE, rows, column_names=list(INSERT_COLUMNS), database=database, column_type_names=list(CH_TYPES))
            batches += 1
    return {"batches": batches}


# ---------------------------------------------------------------------- arrow
def run_arrow(http, database: str, path: Path, sha: str, batch: int, tag_settings: dict | None = None) -> dict:
    import pyarrow as pa
    import pyarrow.csv as pacsv

    column_types = {
        "row_number": pa.uint32(), "device_id": pa.string(), "diagnosed_at": pa.timestamp("ms", tz="UTC"),
        "error_code": pa.string(), "severity": pa.string(), "battery_pct": pa.uint8(), "temperature_c": pa.float64(),
        "firmware_version": pa.string(), "note": pa.string(),
    }
    # VERIFY: Arrow's ISO-8601 timestamp parser accepts the trailing 'Z' for a tz-aware target type.
    reader = pacsv.open_csv(
        pa.input_stream(str(path), compression="detect"),
        read_options=pacsv.ReadOptions(block_size=1 << 20),
        convert_options=pacsv.ConvertOptions(column_types=column_types, strings_can_be_null=True,
                                             include_columns=list(CSV_COLUMNS)),
    )
    batches, body_bytes = 0, 0
    for record_batch in reader:
        for offset in range(0, record_batch.num_rows, batch):
            part = record_batch.slice(offset, batch)
            part = pa.RecordBatch.from_arrays([*part.columns, pa.array([sha] * part.num_rows, pa.string())],
                                              names=list(INSERT_COLUMNS))
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, part.schema) as writer:
                writer.write_batch(part)
            payload = sink.getvalue().to_pybytes()
            http.insert_arrow_stream(database, TABLE, list(INSERT_COLUMNS), payload, settings=tag_settings)
            body_bytes += len(payload)
            batches += 1
    return {"batches": batches, "body_bytes": body_bytes,
            "note": "pyarrow CSV reader blocks are re-sliced to the requested batch size"}


def run(method: str, path: Path, database: str, batch: int, settings: Settings, tag: str | None = None) -> dict:
    from connectedfleet.clickhouse.http import SYNC_INSERT, ClickHouseHTTP

    http = ClickHouseHTTP(settings.clickhouse_http_url, settings.clickhouse_user, settings.clickhouse_password, timeout_s=600)
    http.command(f"TRUNCATE TABLE IF EXISTS `{database}`.`{TABLE}`")
    sha = file_sha256(path)
    tag_settings = {"log_comment": tag} if tag else {}
    before = _usage()
    started = time.perf_counter()
    if method == "direct":
        detail = run_direct(http, database, path, sha, tag_settings)
    elif method == "row":
        import urllib.parse

        import clickhouse_connect

        url = urllib.parse.urlsplit(settings.clickhouse_http_url)
        native = clickhouse_connect.get_client(host=url.hostname, port=url.port or 8123, username=settings.clickhouse_user,
                                               password=settings.clickhouse_password, database=database,
                                               interface="http", compress=False,
                                               settings={**SYNC_INSERT, **tag_settings})
        detail = run_row(native, database, path, sha, batch)
    elif method == "arrow":
        detail = run_arrow(http, database, path, sha, batch, tag_settings)
    else:
        raise ValueError(method)
    wall = time.perf_counter() - started
    after = _usage()
    rows = int(http.scalar(f"SELECT count() FROM `{database}`.`{TABLE}`") or 0)
    return {
        "method": method, "database": database, "file": str(path), "file_bytes": path.stat().st_size,
        "file_sha256": sha, "compressed": path.suffix == ".gz", "batch_size": batch,
        "wall_s": wall, "rows_loaded": rows, "rows_per_s": rows / wall if wall else None,
        "client_cpu_s": after["cpu_s"] - before["cpu_s"], "client_max_rss_bytes": after["max_rss_bytes"],
        "rss_scope": "process high-water mark (includes interpreter + imports)", **detail,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate")
    gen.add_argument("--rows", type=int, required=True)
    gen.add_argument("--out", type=Path, required=True)
    gen.add_argument("--seed", type=int, default=20261010)
    gen.add_argument("--gzip", action="store_true")
    go = sub.add_parser("run")
    go.add_argument("--method", choices=("direct", "row", "arrow"), required=True)
    go.add_argument("--file", type=Path, required=True)
    go.add_argument("--database", required=True)
    go.add_argument("--batch", type=int, default=10_000)
    go.add_argument("--tag", default=None, help="log_comment for every insert, so server cost can be read from query_log")
    args = parser.parse_args(argv)
    if args.command == "generate":
        print(json.dumps(generate(args.rows, args.out, args.seed, args.gzip)))
    else:
        print(json.dumps(run(args.method, args.file, args.database, args.batch, Settings.from_env(), args.tag)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
