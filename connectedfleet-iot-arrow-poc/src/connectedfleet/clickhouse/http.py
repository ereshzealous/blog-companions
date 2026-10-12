"""Minimal ClickHouse HTTP interface client (stdlib only).

Used for DDL, verification queries and lane C's `INSERT ... FORMAT ArrowStream`.
It keeps one persistent HTTP/1.1 connection, the same connection-reuse
behaviour the row lane gets from clickhouse-connect, and raises
`ClickHouseError` with the server's own message on any non-200 response.

HTTP interface: https://clickhouse.com/docs/interfaces/http
ArrowStream format: https://clickhouse.com/docs/interfaces/formats/ArrowStream
"""

from __future__ import annotations

import http.client
import json
import time
import urllib.parse


# Every lane inserts synchronously, as the official sink does (it sends async_insert=0). ClickHouse 26.x
# defaults to async_insert=1 + wait_for_async_insert=1, which holds each single-writer insert in the async
# queue for >= 50 ms (adaptive busy timeout); observed in Lab 03 on 2026-10-10 (decision D-019).
SYNC_INSERT = {"async_insert": "0"}


class ClickHouseError(Exception):
    def __init__(self, status: int, message: str, query: str) -> None:
        super().__init__(f"HTTP {status}: {message[:500]}")
        self.status = status
        self.message = message
        self.query = query[:300]

    @property
    def retriable(self) -> bool:
        # 5xx and "too many parts / simultaneous queries" style overloads are worth a retry; 4xx data errors are not.
        return self.status >= 500 or "TOO_MANY" in self.message


class ClickHouseHTTP:
    def __init__(self, url: str, user: str, password: str, timeout_s: float = 60.0) -> None:
        parsed = urllib.parse.urlsplit(url)
        self._host = parsed.hostname or "localhost"
        self._port = parsed.port or 8123
        self._headers = {"X-ClickHouse-User": user, "X-ClickHouse-Key": password}
        self._timeout = timeout_s
        self._conn: http.client.HTTPConnection | None = None
        self.bytes_sent = 0

    def _connection(self) -> http.client.HTTPConnection:
        if self._conn is None:
            self._conn = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _post(self, params: dict, body: bytes, extra_headers: dict | None = None) -> bytes:
        query_string = urllib.parse.urlencode(params)
        headers = {**self._headers, "Content-Length": str(len(body)), **(extra_headers or {})}
        for attempt in (1, 2):  # one transparent reconnect if a kept-alive socket was closed by the server
            conn = self._connection()
            try:
                conn.request("POST", f"/?{query_string}", body=body, headers=headers)
                response = conn.getresponse()
                data = response.read()
            except (http.client.HTTPException, ConnectionError, OSError):
                self.close()
                if attempt == 2:
                    raise
                continue
            self.bytes_sent += len(body)
            if response.status != 200:
                raise ClickHouseError(response.status, data.decode("utf-8", errors="replace"), str(params.get("query", "")))
            return data
        raise RuntimeError("unreachable")

    def insert_file(self, params: dict, path, extra_headers: dict | None = None) -> float:
        """Stream a file as the request body (never read whole into memory); returns seconds until ack.

        Used for the direct CSV backfill, where ClickHouse parses the body itself."""
        params = {**SYNC_INSERT, **params}
        size = path.stat().st_size
        headers = {**self._headers, "Content-Length": str(size), **(extra_headers or {})}
        started = time.perf_counter()
        conn = self._connection()
        with path.open("rb") as handle:
            try:
                conn.request("POST", f"/?{urllib.parse.urlencode(params)}", body=handle, headers=headers)
                response = conn.getresponse()
                data = response.read()
            except (http.client.HTTPException, ConnectionError, OSError):
                self.close()
                raise
        self.bytes_sent += size
        if response.status != 200:
            raise ClickHouseError(response.status, data.decode("utf-8", errors="replace"), str(params.get("query", "")))
        return time.perf_counter() - started

    def command(self, sql: str, settings: dict | None = None) -> str:
        """Run a statement sent as the request body (DDL/DML). Returns the raw text response."""
        return self._post(dict(settings or {}), sql.encode("utf-8")).decode("utf-8")

    def query_json(self, sql: str, settings: dict | None = None) -> list[dict]:
        text = self.command(f"{sql.rstrip().rstrip(';')} FORMAT JSONEachRow", settings)
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def scalar(self, sql: str):
        rows = self.query_json(sql)
        if not rows:
            return None
        return next(iter(rows[0].values()))

    def insert_arrow_stream(self, database: str, table: str, columns: list[str], ipc_stream: bytes, settings: dict | None = None) -> float:
        """POST one Arrow IPC stream as `INSERT INTO db.table (cols) FORMAT ArrowStream`; returns seconds until ack."""
        column_list = ", ".join(f"`{c}`" for c in columns)
        params = {"query": f"INSERT INTO `{database}`.`{table}` ({column_list}) FORMAT ArrowStream", **SYNC_INSERT,
                  **(settings or {})}
        started = time.perf_counter()
        self._post(params, ipc_stream, {"Content-Type": "application/vnd.apache.arrow.stream"})
        return time.perf_counter() - started

    def insert_raw(self, database: str, table: str, fmt: str, body: bytes, columns: list[str] | None = None,
                   settings: dict | None = None, content_encoding: str | None = None) -> float:
        """Generic `INSERT ... FORMAT <fmt>` with a raw body (used by the CSV backfill experiment)."""
        target = f"`{database}`.`{table}`" + (f" ({', '.join(f'`{c}`' for c in columns)})" if columns else "")
        params = {"query": f"INSERT INTO {target} FORMAT {fmt}", **SYNC_INSERT, **(settings or {})}
        headers = {"Content-Encoding": content_encoding} if content_encoding else None
        started = time.perf_counter()
        self._post(params, body, headers)
        return time.perf_counter() - started

    def ping(self) -> bool:
        try:
            return self.command("SELECT 1").strip() == "1"
        except (ClickHouseError, OSError, http.client.HTTPException):
            return False
