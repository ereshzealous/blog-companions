"""Import historical diagnostics CSV files into Kafka (stream path), in bounded chunks.

    python -m connectedfleet.producers.csv_import --file fixtures/diagnostics/batch-001.csv

Logical id = first 16 hex of the file's SHA-256 + 1-based data row number, so
re-importing the same file yields the same event ids (logical duplicates with a
new `imported_at`). This is the Kafka path; the direct file-to-ClickHouse
backfill (Lab 03 E3) is a different code path and never touches Kafka.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from connectedfleet import topics
from connectedfleet.config import Settings
from connectedfleet.kafka_io import KafkaProducer

COLUMNS = ("device_id", "diagnosed_at", "error_code", "severity", "battery_pct", "temperature_c", "firmware_version", "note")


class CsvSchemaError(Exception):
    pass


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def iter_messages(path: Path, imported_at: str, chunk_rows: int = 500):
    """Yield lists of (key, value) messages, at most `chunk_rows` per list (bounded memory)."""
    sha = file_sha256(path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != COLUMNS:
            raise CsvSchemaError(f"unexpected header {reader.fieldnames}; expected {list(COLUMNS)}")
        chunk = []
        for row_number, row in enumerate(reader, start=1):
            message = {"file_sha256": sha, "row_number": row_number, "imported_at": imported_at, "row": row}
            chunk.append((row["device_id"].encode("utf-8"), json.dumps(message, ensure_ascii=False).encode("utf-8")))
            if len(chunk) >= chunk_rows:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


def import_file(producer, path: Path, imported_at: str | None = None, chunk_rows: int = 500,
                topic: str = topics.RAW_DIAGNOSTICS) -> dict:
    imported_at = imported_at or datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    rows = 0
    for chunk in iter_messages(path, imported_at, chunk_rows):
        for key, value in chunk:
            producer.produce(topic, key, value)
        producer.flush()  # one chunk in flight at a time
        rows += len(chunk)
    return {"file": str(path), "sha256": file_sha256(path), "rows": rows, "imported_at": imported_at}


def read_rows(text: str) -> list[dict]:
    """Parse CSV text with the expected header (helper for tests and the backfill experiment)."""
    reader = csv.DictReader(io.StringIO(text))
    if tuple(reader.fieldnames or ()) != COLUMNS:
        raise CsvSchemaError(f"unexpected header {reader.fieldnames}")
    return list(reader)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--chunk-rows", type=int, default=500)
    parser.add_argument("--imported-at", default=None)
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    print(json.dumps(import_file(KafkaProducer(settings.kafka_bootstrap), args.file, args.imported_at, args.chunk_rows)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
