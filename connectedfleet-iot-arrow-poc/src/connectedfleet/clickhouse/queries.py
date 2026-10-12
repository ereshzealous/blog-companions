"""Verification queries shared by Lab 02 and Lab 03.

The content hash is order-independent and lane-independent:
  * group by event_id and take min() of every contract column, so physical
    duplicates (and CSV re-imports that differ only in emitted_at) collapse
    deterministically into one logical row;
  * render each logical row with formatRowNoNewline('TSV', ...) so NULLs
    become `\\N` instead of making the hash NULL;
  * XOR the cityHash64 of every row.
`ingested_at` (server insert time) is never part of the hash.
"""

from __future__ import annotations

from connectedfleet.clickhouse.http import ClickHouseHTTP


def _where(where: str | None) -> str:
    return f"WHERE {where}" if where else ""


def counts(ch: ClickHouseHTTP, database: str, table: str, where: str | None = None) -> dict:
    row = ch.query_json(
        f"SELECT count() AS physical, uniqExact(event_id) AS logical FROM `{database}`.`{table}` {_where(where)}"
    )[0]
    return {"physical": int(row["physical"]), "logical": int(row["logical"])}


def content_hash(ch: ClickHouseHTTP, database: str, table: str, columns: list[str], where: str | None = None,
                 exclude: tuple[str, ...] = ("ingested_at",), key: str = "event_id", dedupe: bool = True) -> str:
    """Order-independent hash of a table's logical content: XOR of cityHash64 over (key, columns...).

    dedupe=True collapses at-least-once duplicates (GROUP BY key, min per column) and may spill to disk instead of
    failing on memory. dedupe=False hashes rows as they are: use it only when rows are known to be unique (it then
    gives the same result in constant memory)."""
    cols = [c for c in columns if c != key and c not in exclude]
    if not dedupe:
        rendered = ", ".join(f"`{c}`" for c in [key, *cols])
        sql = (f"SELECT toString(groupBitXor(cityHash64(formatRowNoNewline('TSV', {rendered})))) AS h, count() AS n "
               f"FROM `{database}`.`{table}` {_where(where)}")
        row = ch.query_json(sql)[0]
        return f"{row['h']}/{row['n']}"
    # Aliases deliberately differ from column names: ClickHouse resolves an alias that shadows a
    # column inside the aggregate itself ("aggregate inside aggregate").
    inner = ", ".join(f"min(`{c}`) AS `m_{i}`" for i, c in enumerate(cols))
    rendered = ", ".join([f"`{key}`", *[f"`m_{i}`" for i in range(len(cols))]])
    sql = (
        f"SELECT toString(groupBitXor(cityHash64(formatRowNoNewline('TSV', {rendered})))) AS h, count() AS n "
        f"FROM (SELECT `{key}`, {inner} FROM `{database}`.`{table}` {_where(where)} GROUP BY `{key}`)"
    )
    row = ch.query_json(sql, {"max_bytes_before_external_group_by": str(256 * 1024 * 1024)})[0]
    return f"{row['h']}/{row['n']}"


def current_state(ch: ClickHouseHTTP, database: str, table: str, where: str | None = None) -> list[dict]:
    """Rows visible after ReplacingMergeTree semantics are applied at read time (FINAL hides is_deleted=1)."""
    return ch.query_json(f"SELECT * FROM `{database}`.`{table}` FINAL {_where(where)} ORDER BY device_id")


def active_parts(ch: ClickHouseHTTP, database: str, table: str) -> int:
    return int(ch.scalar(
        f"SELECT count() FROM system.parts WHERE database = '{database}' AND table = '{table}' AND active"
    ) or 0)


def latency_ms(ch: ClickHouseHTTP, database: str, table: str, where: str | None = None) -> dict:
    """Event-to-queryable latency: server insert time (ingested_at) minus producer send time (emitted_at)."""
    delay = "(toUnixTimestamp64Milli(ingested_at) - toUnixTimestamp64Milli(emitted_at))"
    row = ch.query_json(
        f"SELECT quantilesExact(0.5, 0.95, 0.99)({delay}) AS q, max({delay}) AS max_ms, countIf({delay} > 5000) AS over_5s, "
        "count() AS n, "
        "toUnixTimestamp64Milli(min(ingested_at)) AS first_insert_ms, toUnixTimestamp64Milli(max(ingested_at)) AS last_insert_ms "
        f"FROM `{database}`.`{table}` {_where(where)}"
    )[0]
    samples = int(row["n"])
    p50, p95, p99 = (float(x) for x in row["q"]) if samples else (None, None, None)
    return {
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "max_ms": float(row["max_ms"]) if samples else None,
        "over_5s": int(row["over_5s"]) if samples else None,
        "samples": samples,
        "first_insert_ms": int(row["first_insert_ms"]) if samples else None,
        "last_insert_ms": int(row["last_insert_ms"]) if samples else None,
        "insert_batches": int(ch.scalar(f"SELECT uniqExact(ingested_at) FROM `{database}`.`{table}` {_where(where)}") or 0),
    }


def null_counts(ch: ClickHouseHTTP, database: str, table: str, columns: list[str], where: str | None = None) -> dict:
    exprs = ", ".join(f"countIf(isNull(`{c}`)) AS `n_{i}`" for i, c in enumerate(columns))
    row = ch.query_json(f"SELECT {exprs} FROM (SELECT * FROM `{database}`.`{table}` {_where(where)} LIMIT 1 BY event_id)")[0]
    # 64-bit integers arrive JSON-quoted by default (output_format_json_quote_64bit_integers); int() accepts both.
    return {c: int(row[f"n_{i}"]) for i, c in enumerate(columns)}


def describe(ch: ClickHouseHTTP, database: str, table: str) -> list[tuple[str, str]]:
    return [(r["name"], r["type"]) for r in ch.query_json(
        f"SELECT name, type FROM system.columns WHERE database = '{database}' AND table = '{table}' ORDER BY position"
    )]


def server_cost(ch: ClickHouseHTTP, log_comment: str) -> dict:
    """Server-side cost of every finished query tagged with `log_comment` (inserts of one benchmark case).

    CPU is the server's OSCPUVirtualTimeMicroseconds for those queries; memory is the largest per-query peak."""
    if "'" in log_comment:
        raise ValueError("log_comment must not contain quotes")
    ch.command("SYSTEM FLUSH LOGS")
    row = ch.query_json(
        "SELECT count() AS queries, sum(query_duration_ms) AS query_ms, "
        "sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us, max(memory_usage) AS peak_mem "
        f"FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '{log_comment}'"
    )[0]
    return {"queries": int(row["queries"]), "query_s": int(row["query_ms"]) / 1000, "cpu_s": int(row["cpu_us"]) / 1e6,
            "peak_mem_bytes": int(row["peak_mem"])}
