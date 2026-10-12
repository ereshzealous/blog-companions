"""Lab 03 orchestrator: executes benchmarks/workloads/<profile>.yaml and writes one run directory.

Fairness rules enforced here:
  * lanes run one at a time; the main pipeline connectors are paused during the run;
  * every comparison uses the same workload topic (same offsets) and the same batch size
    and flush timeout; destination tables are truncated before each lane run;
  * lane order rotates per repetition; warm-up cases are recorded but flagged;
  * every measurement records its scope (container vs process) and its mode (drain vs live).
Nothing is estimated: a value that is not observable for a lane is stored as null with a reason.
"""

from __future__ import annotations

import json
import secrets
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

from connectedfleet import compose, kafka_admin, topics
from connectedfleet import connect as connect_mod
from connectedfleet.bench import hostload, sampling, sinkcfg
from connectedfleet.bench.workload import MEASUREMENTS, WorkloadSpec, digest_and_nulls
from connectedfleet.clickhouse import queries, schema
from connectedfleet.clickhouse.http import ClickHouseHTTP
from connectedfleet.config import POC_ROOT, Settings
from connectedfleet.contracts import canonical
from connectedfleet.instrumentation.env import git_state, host_info, image_info, versions

APP_IMAGE = "connectedfleet/app:0.1.0"  # B/C workers run from this image (compose x-app); `make lab-03` rebuilds it

RESULTS_ROOT = POC_ROOT / "benchmarks" / "results"
WORKLOADS = POC_ROOT / "benchmarks" / "workloads"
METRICS_HOST_DIR = POC_ROOT / ".data" / "metrics"
LANE_DB = topics.LANE_DATABASES
TELEMETRY_COLUMNS = canonical.TELEMETRY.names
ENRICHED_COLUMNS = TELEMETRY_COLUMNS + ["risk_flag"]

MEASUREMENT_DEFINITIONS = {
    "wall_rows_per_s": "validated logical rows / seconds from lane start (connector PUT or container start) until all rows are queryable; includes startup",
    "insert_window_rows_per_s": "validated logical rows / (max(ingested_at) - min(ingested_at)); steady-state insert rate, excludes startup",
    "effective_batch_rows": "logical rows / number of distinct server insert timestamps (ingested_at); shows the batch size each lane really used",
    "latency": "live mode only: ingested_at (ClickHouse server clock, Docker VM) - emitted_at (producer send clock, same Docker VM); p50/p95/p99 over all rows of the run",
    "resources.lane_a": "Kafka Connect container (JVM): cgroup v2 cpu.stat usage delta + peak docker-stats memory; source connectors paused",
    "resources.lane_bc": "worker process: getrusage CPU (user+sys) and ru_maxrss; container docker-stats peak also recorded",
    "phases": "lanes B/C only: summed perf_counter_ns per phase from the worker metrics; lane A internals are not observable",
}


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_experiments(experiments, enabled, log, checkpoint) -> list[dict]:
    """Run each enabled experiment; a failure is recorded and the rest still run. The manifest is checkpointed after
    every experiment, so a crash never discards measured cases (a confirm run lost ~60 min of cases on 2026-10-10)."""
    failed = []
    for name, fn in experiments:
        if not enabled(name):
            continue
        log(f"== {name}")
        try:
            fn()
        except Exception as err:  # noqa: BLE001 - recorded in the manifest, never hidden
            failed.append({"experiment": name, "reason": f"ERROR: {type(err).__name__}: {str(err)[:300]}"})
            log(f"{name} failed: {type(err).__name__}: {str(err)[:300]}")
        checkpoint()
    return failed


def stale_bench_connectors(names: list[str]) -> list[str]:
    """Benchmark connectors (cfbench-*) still registered from earlier runs; the pipeline's own are never touched."""
    return sorted(n for n in names if n.startswith("cfbench-"))


class Bench:
    def __init__(self, settings: Settings, profile: str, only: list[str] | None = None) -> None:
        self.settings = settings
        self.profile = profile
        self.spec = yaml.safe_load((WORKLOADS / f"{profile}.yaml").read_text())
        self.only = set(only or [])
        self.ch = ClickHouseHTTP(settings.clickhouse_http_url, settings.clickhouse_user, settings.clickhouse_password, 600)
        self.connect = connect_mod.ConnectClient(settings.connect_url)
        self.code = {**git_state(POC_ROOT), "app_image": image_info(APP_IMAGE)}
        self.started = _utc()
        self.run_id = f"lab03-{profile}-{self.started.replace('-', '').replace(':', '')}-{(self.code['git_sha'] or 'nogit')[:7]}"
        self.run_dir = RESULTS_ROOT / "runs" / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (METRICS_HOST_DIR / self.run_id).mkdir(parents=True, exist_ok=True)
        (METRICS_HOST_DIR / self.run_id).chmod(0o777)
        self.cases: list[dict] = []
        self.not_run: list[dict] = []
        self.samples: list[dict] = []
        self.workloads: dict[str, dict] = {}
        self._log = (self.run_dir / "stdout.log").open("a", encoding="utf-8")
        self.created_topics: list[str] = []
        guard_cfg = self.spec.get("host_guard")
        self.guard = hostload.HostGuard(**guard_cfg, log=self.log) if guard_cfg else None

    # ------------------------------------------------------------------ utilities
    def log(self, message: str) -> None:
        line = f"{_utc()} {message}"
        print(line, flush=True)
        self._log.write(line + "\n")
        self._log.flush()

    def timed(self, label: str, fn) -> dict:
        """Run one timing case; with a host guard, only on a quiet host and again if other work overlapped it."""
        return self.guard.run_case(label, fn) if self.guard else fn()

    def enabled(self, experiment: str) -> bool:
        cfg = self.spec["experiments"].get(experiment, {})
        return bool(cfg.get("enabled")) and (not self.only or experiment in self.only)

    def workload(self, key: str, spec: WorkloadSpec) -> dict:
        """Digest + expected null/risk counts, computed once per workload from the deterministic generator."""
        if key not in self.workloads:
            self.log(f"computing workload digest {key} ({spec.count} events)")
            digest, nulls, risk = digest_and_nulls(spec)
            self.workloads[key] = {**spec.describe(), "digest_sha256": digest, "expected_nulls": nulls, "expected_risk": risk}
        return self.workloads[key]

    def topic(self, case: str) -> str:
        name = topics.bench_topic(self.run_id, case)
        kafka_admin.create_topics(self.settings.kafka_bootstrap, {name: int(self.spec.get("kafka_partitions", 1))})
        self.created_topics.append(name)
        return name

    def produce(self, topic: str, spec: WorkloadSpec, rate: float = 0.0, poison: int = 0) -> dict:
        args = ["-m", "connectedfleet.bench.producer", "--topic", topic, "--count", str(spec.count), "--seed", str(spec.seed),
                "--null-fraction", str(spec.null_fraction), "--id-prefix", spec.id_prefix, "--rate", str(rate),
                "--poison", str(poison)]
        if spec.string_heavy:
            args.append("--string-heavy")
        proc = compose.run_once("app", *args, timeout=float(self.spec["timeouts"]["live_s"]) + 600)
        if proc.returncode != 0:
            raise RuntimeError(f"producer failed: {proc.stderr[-800:]}")
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def wait_rows(self, db: str, table: str, expected: int, timeout_s: float, done=None,
                  physical: int | None = None, stall_s: float = 120.0) -> bool:
        """Wait until `expected` logical (and optionally `physical`) rows are queryable.

        Stops early when the lane process exited (`done`) or when the row count has not moved
        for `stall_s` seconds (e.g. a sink that dropped a batch); both cases return False if incomplete.
        """
        deadline = time.monotonic() + timeout_s
        last_total, last_change = -1, time.monotonic()

        def reached(c: dict) -> bool:
            return c["logical"] >= expected and (physical is None or c["physical"] >= physical)

        while time.monotonic() < deadline:
            c = queries.counts(self.ch, db, table)
            if reached(c):
                return True
            if c["physical"] != last_total:
                last_total, last_change = c["physical"], time.monotonic()
            elif time.monotonic() - last_change > stall_s:
                self.log(f"wait_rows {db}.{table}: no progress for {stall_s:.0f}s at {c}; giving up")
                return False
            if done is not None and done():
                return reached(queries.counts(self.ch, db, table))
            time.sleep(0.2)  # completion poll; 1 s quantized wall-clock rates of 1-2 s runs
        return False

    def measure_table(self, db: str, table: str, columns: list[str], mode: str) -> dict:
        counts = queries.counts(self.ch, db, table)
        latency = queries.latency_ms(self.ch, db, table)
        window_s = ((latency["last_insert_ms"] - latency["first_insert_ms"]) / 1000) if latency["samples"] else None
        # A drain run loads into an emptied table, so rows are unique unless a lane re-sent a batch; the GROUP BY
        # form needs ~1 GiB at 1M rows and ClickHouse in its 3 GiB container refused it (D-029, D-033).
        dedupe = counts["physical"] != counts["logical"]
        result = {
            "counts": counts,
            # emitted_at is the producer's send clock (differs per production run), so like the workload
            # digest the benchmark content hash covers every column except emitted_at and ingested_at.
            "content_hash": queries.content_hash(self.ch, db, table, columns, exclude=("ingested_at", "emitted_at"),
                                                 dedupe=dedupe),
            "nulls": queries.null_counts(self.ch, db, table, list(MEASUREMENTS)),
            "parts_active": queries.active_parts(self.ch, db, table),
            "insert_batches": latency["insert_batches"],
            "effective_batch_rows": (counts["logical"] / latency["insert_batches"]) if latency["insert_batches"] else None,
            "insert_window_s": window_s,
            "insert_window_rows_per_s": (counts["logical"] / window_s) if window_s else None,
            "latency_mode": mode,
            "latency_ms": {k: latency[k] for k in ("p50_ms", "p95_ms", "p99_ms", "samples")} if mode == "live" else None,
            "schema": queries.describe(self.ch, db, table),
        }
        if table == "telemetry_enriched":
            row = self.ch.query_json(
                f"SELECT countIf(risk_flag = true) AS t, countIf(risk_flag = false) AS f, countIf(isNull(risk_flag)) AS u "
                f"FROM (SELECT risk_flag FROM `{db}`.`{table}` LIMIT 1 BY event_id)")[0]
            result["risk_counts"] = {"true": int(row["t"]), "false": int(row["f"]), "unknown": int(row["u"])}
        return result

    # ------------------------------------------------------------------ lanes
    def run_lane_a(self, case_id: str, topic: str, table: str, batch: int, flush_ms: int, expected: int, timeout_s: float,
                   truncate: bool = True, expect_physical: int | None = None, stall_s: float = 120.0,
                   variant: str = "sink") -> dict:
        db = sinkcfg.db_for(variant)
        if truncate:
            self.ch.command(f"TRUNCATE TABLE `{db}`.`{table}`")
        name = f"cfbench-a-{secrets.token_hex(4)}"
        container = compose.container_id("connect")
        base_settings = connect_mod.load(connect_mod.SINK_CONNECTOR)["config"]["clickhouseSettings"]
        overrides = sinkcfg.overrides(variant, topic, table, db, batch, flush_ms, base_settings)
        cpu_before = sampling.cgroup_cpu_usage_s(container) if container else None
        stats = sampling.ContainerSampler(container) if container else None
        lag = sampling.LagSampler(self.settings.kafka_bootstrap, f"connect-{name}", topic)
        if stats:
            stats.start()
        lag.start()
        started = time.monotonic()
        error = None
        try:
            try:
                connect_mod.register(self.connect, connect_mod.SINK_CONNECTOR, overrides, rename=name)
                complete = self.wait_rows(db, table, expected, timeout_s, physical=expect_physical, stall_s=stall_s)
            except Exception as err:  # noqa: BLE001 - recorded, never hidden
                complete, error = False, f"{type(err).__name__}: {err}"
            wall = time.monotonic() - started
            cpu_after = sampling.cgroup_cpu_usage_s(container) if container else None
            container_stats = stats.stop() if stats else {}
            lag_stats = lag.stop()
        finally:
            # Always remove the benchmark connector: three leaked from aborted runs on 2026-10-10 and idled in the
            # Connect JVM through later runs (D-028).
            try:
                self.connect.delete(name)
            except connect_mod.ConnectError:
                pass
        measured = self.measure_table(db, table, ENRICHED_COLUMNS if table == "telemetry_enriched" else TELEMETRY_COLUMNS,
                                      "drain")
        return {
            **measured,
            "lane_runtime": {"connector": name, "variant": variant, "complete": complete, "error": error},
            "wall_s": wall,
            "wall_rows_per_s": measured["counts"]["logical"] / wall if wall else None,
            "connector_overrides": {k: v for k, v in overrides.items() if k not in ("topics", "topic2TableMap")},
            "resources": {"scope": "container (Kafka Connect JVM)",
                          "cpu_s": (cpu_after - cpu_before) if cpu_before is not None and cpu_after is not None else None,
                          "cpu_s_integrated": container_stats.get("cpu_s_integrated"),
                          "peak_mem_bytes": container_stats.get("peak_mem_bytes")},
            "phases_ns": None,
            "phases_note": "not observable inside the connector",
            "payload_bytes": None,
            "lag": {k: v for k, v in lag_stats.items() if k != "series"},
            "_series": {"lag": lag_stats["series"], "container": stats.samples if stats else []},
        }

    def run_lane_bc(self, case_id: str, lane: str, topic: str, table: str, batch: int, flush_ms: int, expected_records: int,
                    expected_rows: int, timeout_s: float, transform: str = "none", compute: str | None = None,
                    insert: str | None = None, fault: str | None = None, group: str | None = None,
                    database: str | None = None, truncate: bool = True) -> dict:
        db = database or LANE_DB[lane]
        if truncate:
            self.ch.command(f"TRUNCATE TABLE `{db}`.`{table}`")
        service = "worker-row" if lane == "row" else "worker-arrow"
        name = f"cfbench-{lane}-{secrets.token_hex(4)}"
        group = group or f"cfbench-{case_id}-{secrets.token_hex(3)}"
        metrics_rel = f"{self.run_id}/{case_id}-{name}.jsonl"
        args = ["-m", "connectedfleet.lanes.worker", "--lane", lane, "--database", db, "--topics", topic,
                "--group", group, "--batch-size", str(batch), "--flush-ms", str(flush_ms), "--transform", transform,
                "--max-records", str(expected_records), "--idle-exit", "60", "--metrics-out", f"/metrics/{metrics_rel}"]
        if compute:
            args += ["--compute", compute]
        if insert:
            args += ["--insert", insert]
        env = {"FAULT": fault, "FAULT_AFTER_BATCHES": "1"} if fault else {}
        env_args = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
        compose.run("run", "-d", "--name", name, *env_args, service, *args)
        stats = sampling.ContainerSampler(name)
        lag = sampling.LagSampler(self.settings.kafka_bootstrap, group, topic)
        stats.start()
        lag.start()
        started = time.monotonic()

        def exited() -> bool:
            out = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name], capture_output=True, text=True)
            return out.stdout.strip() == "false"

        complete = self.wait_rows(db, table, expected_rows, timeout_s, done=exited) if not fault else False
        exit_code = int(subprocess.run(["docker", "wait", name], capture_output=True, text=True, timeout=timeout_s).stdout.strip() or -1)
        wall = time.monotonic() - started
        container_stats = stats.stop()
        lag_stats = lag.stop()
        logs = subprocess.run(["docker", "logs", "--tail", "50", name], capture_output=True, text=True).stdout
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        phases, process = self._read_worker_metrics(METRICS_HOST_DIR / metrics_rel)
        measured = self.measure_table(db, table, ENRICHED_COLUMNS if table == "telemetry_enriched" else TELEMETRY_COLUMNS,
                                      "drain")
        return {
            **measured,
            "lane_runtime": {"container": name, "group": group, "exit_code": exit_code, "complete": complete,
                             "log_tail": logs[-2000:]},
            "wall_s": wall,
            "wall_rows_per_s": measured["counts"]["logical"] / wall if wall else None,
            "resources": {"scope": "worker process (getrusage) + container (docker stats)",
                          "cpu_s": process.get("cpu_s"), "max_rss_bytes": process.get("max_rss_bytes"),
                          "cpu_s_integrated": container_stats.get("cpu_s_integrated"),
                          "peak_mem_bytes": container_stats.get("peak_mem_bytes")},
            "phases_ns": phases.get("phases_ns"),
            "flushes": phases.get("flushes"),
            "payload_bytes": phases.get("payload_bytes"),
            "lag": {k: v for k, v in lag_stats.items() if k != "series"},
            "_series": {"lag": lag_stats["series"], "container": stats.samples},
        }

    @staticmethod
    def _read_worker_metrics(path: Path) -> tuple[dict, dict]:
        phases = dict.fromkeys(("decode", "validate", "build", "transform", "serialize", "insert"), 0)
        flushes, payload = 0, 0
        payload_seen = False
        process: dict = {}
        if not path.is_file():
            return {"phases_ns": None, "flushes": 0, "payload_bytes": None}, process
        for line in path.read_text().splitlines():
            m = json.loads(line)
            if "decode_ns" in m:
                flushes += 1
                for key in phases:
                    phases[key] += int(m.get(f"{key}_ns") or 0)
                if m.get("payload_bytes") is not None:
                    payload += m["payload_bytes"]
                    payload_seen = True
            elif m.get("event") == "stopped":
                process = {"cpu_s": m.get("cpu_user_s", 0) + m.get("cpu_sys_s", 0), "max_rss_bytes": m.get("max_rss_bytes")}
        return {"phases_ns": phases, "flushes": flushes, "payload_bytes": payload if payload_seen else None}, process

    # ------------------------------------------------------------------ case bookkeeping
    def record(self, experiment: str, lane: str, variant: dict, mode: str, workload_key: str | None, config: dict,
               warmup: bool, repetition: int, measurement: dict | None, status: str, error: str | None = None,
               extra: dict | None = None) -> dict:
        case = {
            "case_id": f"{experiment}-{len(self.cases):04d}",
            "experiment": experiment,
            "lane": lane,
            "variant": variant,
            "mode": mode,
            "warmup": warmup,
            "repetition": repetition,
            "workload_key": workload_key,
            "config": config,
            "status": status,
            "error": error,
            "measurements": {k: v for k, v in (measurement or {}).items() if not k.startswith("_")},
            **(extra or {}),
        }
        if measurement and "_series" in measurement:
            self.samples.append({"case_id": case["case_id"], **measurement["_series"]})
        self.cases.append(case)
        logical = case["measurements"].get("counts", {}).get("logical")
        self.log(f"case {case['case_id']} {experiment}/{lane}{'/' + variant.get('compute', '') + '-' + variant.get('insert', '') if variant else ''} "
                 f"{'warmup ' if warmup else ''}rep={repetition} status={status} logical={logical} "
                 f"wall_rows_per_s={case['measurements'].get('wall_rows_per_s')}"
                 + (f" error={str(error)[:300]}" if error else ""))
        return case

    def reps(self, cfg: dict) -> int:
        """Warm-ups + measured repetitions; an experiment may ask for more measured repetitions than the profile."""
        return int(self.spec["warmups"]) + int(cfg.get("repetitions", self.spec["repetitions"]))

    def lanes_for(self, lanes: list[str], repetition: int) -> list[str]:
        if self.spec.get("lane_order") == "rotate" and lanes:
            k = repetition % len(lanes)
            return lanes[k:] + lanes[:k]
        return lanes

    # ------------------------------------------------------------------ E1 drain + live
    def e1(self) -> None:
        cfg = self.spec["experiments"]["E1"]
        shape = cfg["shape"]
        timeout = float(self.spec["timeouts"]["lane_drain_s"])
        reps = self.reps(cfg)
        for count in cfg["event_counts"]:
            spec = WorkloadSpec(count, int(self.spec["seed"]) + count, shape["null_fraction"], shape["string_heavy"],
                                id_prefix=f"e1n{count}")
            key = f"E1-{shape['name']}-{count}"
            workload = self.workload(key, spec)
            topic = self.topic(f"e1-n{count}")
            workload["topic"] = topic
            workload["produced"] = self.produce(topic, spec)
            for batch in cfg["batch_sizes"]:
                excluded = next((x for x in cfg.get("exclusions", []) if x["event_count"] == count and x["batch_size"] == batch), None)
                if excluded:
                    self.not_run.append({"experiment": "E1", "event_count": count, "batch_size": batch, "reason": excluded["reason"]})
                    self.log(f"E1 n={count} batch={batch} NOT_RUN: {excluded['reason']}")
                    continue
                for rep in range(reps):
                    warm = rep < int(self.spec["warmups"])
                    for lane in self.lanes_for(cfg["lanes"], rep):
                        config = {"batch_size": batch, "flush_timeout_ms": cfg["flush_timeout_ms"], "source_format": "canonical JSON",
                                  "sink_format": "connector JSONEachRow" if sinkcfg.is_sink(lane) else
                                                 {"row": "Native (clickhouse-connect)", "arrow": "ArrowStream (HTTP)"}[lane],
                                  "transform": "none", "shape": shape["name"], "kafka_partitions": self.spec["kafka_partitions"],
                                  "consumer_parallelism": self.spec["consumer_parallelism"]}
                        try:
                            if sinkcfg.is_sink(lane):
                                m = self.timed(f"E1 {lane} b{batch} r{rep}", lambda: self.run_lane_a(
                                    key, topic, "telemetry_fact", batch, cfg["flush_timeout_ms"], count, timeout, variant=lane))
                            else:
                                m = self.timed(f"E1 {lane} b{batch} r{rep}", lambda: self.run_lane_bc(
                                    key, lane, topic, "telemetry_fact", batch, cfg["flush_timeout_ms"], count, count, timeout))
                            self.record("E1", lane, {}, "drain", key, config, warm, rep, m, "COMPLETED")
                        except Exception as err:  # noqa: BLE001
                            self.record("E1", lane, {}, "drain", key, config, warm, rep, None, "ERROR", f"{type(err).__name__}: {err}")
        live = cfg.get("live", {})
        if live.get("enabled"):
            self.e1_live(cfg, live)

    def e1_live(self, cfg: dict, live: dict) -> None:
        reps = self.reps(live)
        count = int(live["event_count"])
        shape = cfg["shape"]
        timeout = float(self.spec["timeouts"]["live_s"])
        for batch in live["batch_sizes"]:
            for flush_ms in live["flush_timeouts_ms"]:
                for rep in range(reps):
                    warm = rep < int(self.spec["warmups"])
                    for lane in self.lanes_for(cfg["lanes"], rep):
                        spec = WorkloadSpec(count, int(self.spec["seed"]) + 7, shape["null_fraction"], shape["string_heavy"],
                                            id_prefix="e1live")
                        key = f"E1live-{shape['name']}-{count}"
                        self.workload(key, spec)
                        config = {"batch_size": batch, "flush_timeout_ms": flush_ms, "rate_eps": live["rate_eps"],
                                  "transform": "none", "shape": shape["name"], "kafka_partitions": self.spec["kafka_partitions"]}
                        attempt = [0]

                        def live_case(lane=lane, batch=batch, flush_ms=flush_ms, rep=rep, spec=spec, key=key, attempt=attempt):
                            attempt[0] += 1  # a re-run needs a fresh topic: the old one already holds the events
                            topic = self.topic(f"e1live-{lane}-b{batch}-f{flush_ms}-r{rep}-a{attempt[0]}")
                            return self._live_case(lane, key, topic, batch, flush_ms, spec, float(live["rate_eps"]), timeout)

                        try:
                            m = self.timed(f"E1 live {lane} f{flush_ms} r{rep}", live_case)
                            self.record("E1", lane, {}, "live", key, config, warm, rep, m, "COMPLETED")
                        except Exception as err:  # noqa: BLE001
                            self.record("E1", lane, {}, "live", key, config, warm, rep, None, "ERROR", f"{type(err).__name__}: {err}")

    def _live_case(self, lane: str, key: str, topic: str, batch: int, flush_ms: int, spec: WorkloadSpec, rate: float,
                   timeout: float) -> dict:
        """Start the lane first, then produce at a fixed rate; latency = ingested_at - emitted_at."""
        import threading

        holder: dict = {}

        def lane_thread():
            try:
                if sinkcfg.is_sink(lane):
                    holder["m"] = self.run_lane_a(key, topic, "telemetry_fact", batch, flush_ms, spec.count, timeout,
                                                  variant=lane)
                else:
                    holder["m"] = self.run_lane_bc(key, lane, topic, "telemetry_fact", batch, flush_ms, spec.count, spec.count, timeout)
            except Exception as err:  # noqa: BLE001
                holder["error"] = err

        thread = threading.Thread(target=lane_thread)
        thread.start()
        time.sleep(15)  # let the connector/worker subscribe before the first event is sent
        produced = self.produce(topic, spec, rate=rate)
        thread.join(timeout + 120)
        if "error" in holder:
            raise holder["error"]
        m = holder["m"]
        db = sinkcfg.db_for(lane)
        latency = queries.latency_ms(self.ch, db, "telemetry_fact")
        m["latency_mode"] = "live"
        m["latency_ms"] = {k: latency[k] for k in ("p50_ms", "p95_ms", "p99_ms", "max_ms", "over_5s", "samples")}
        m["produced"] = produced
        # Lag while events were being sent (+5 s); a group with no committed offsets in that window is unmeasured.
        m["lag"] = {**sampling.window_lag(m["_series"]["lag"], produced["started_at"], produced["ended_at"] + 5),
                    "window": "produce window + 5 s"}
        return m

    # ------------------------------------------------------------------ E2 transform isolation
    def e2(self) -> None:
        cfg = self.spec["experiments"]["E2"]
        timeout = float(self.spec["timeouts"]["lane_drain_s"])
        reps = self.reps(cfg)
        count = int(cfg["event_count"])
        for shape in cfg["shapes"]:
            spec = WorkloadSpec(count, int(self.spec["seed"]) + 2, shape["null_fraction"], shape["string_heavy"],
                                id_prefix=f"e2{shape['name']}")
            key = f"E2-{shape['name']}-{count}"
            workload = self.workload(key, spec)
            topic = self.topic(f"e2-{shape['name']}")
            workload["topic"] = topic
            workload["produced"] = self.produce(topic, spec)
            for batch in cfg["batch_sizes"]:
                for rep in range(reps):
                    warm = rep < int(self.spec["warmups"])
                    variants = cfg["variants"]
                    k = rep % len(variants)
                    for compute, insert in variants[k:] + variants[:k]:
                        lane = "row" if compute == "row" else "arrow"
                        variant = {"compute": compute, "insert": insert}
                        config = {"batch_size": batch, "flush_timeout_ms": cfg["flush_timeout_ms"], "transform": "risk",
                                  "shape": shape["name"], "kafka_partitions": self.spec["kafka_partitions"],
                                  "extra_conversion": {("row", "arrowstream"): "rows -> Arrow before IPC (in serialize)",
                                                       ("arrow", "native"): "Arrow -> Python columns before Native (in serialize)"}
                                  .get((compute, insert))}
                        try:
                            m = self.timed(f"E2 {shape['name']} {compute}-{insert} r{rep}", lambda: self.run_lane_bc(
                                key, lane, topic, "telemetry_enriched", batch, cfg["flush_timeout_ms"], count, count, timeout,
                                transform="risk", compute=compute, insert=insert))
                            self.record("E2", lane, variant, "drain", key, config, warm, rep, m, "COMPLETED")
                        except Exception as err:  # noqa: BLE001
                            self.record("E2", lane, variant, "drain", key, config, warm, rep, None, "ERROR", f"{type(err).__name__}: {err}")

    # ------------------------------------------------------------------ E3 CSV backfill
    def e3(self) -> None:
        cfg = self.spec["experiments"]["E3"]
        reps = self.reps(cfg)
        method_db = {"direct": LANE_DB["sink"], "row": LANE_DB["row"], "arrow": LANE_DB["arrow"]}
        for rows in cfg["rows"]:
            for compression in cfg["compression"]:
                suffix = ".csv.gz" if compression == "gzip" else ".csv"
                path = f"/work/{self.run_id}-diag-{rows}{suffix}"
                gen_args = ["-m", "connectedfleet.bench.csv_backfill", "generate", "--rows", str(rows), "--out", path]
                if compression == "gzip":
                    gen_args.append("--gzip")
                gen = compose.run_once("app", *gen_args)
                if gen.returncode != 0:
                    self.record("E3", "-", {}, "file", None, {"rows": rows, "compression": compression}, False, 0, None,
                                "ERROR", gen.stderr[-800:])
                    continue
                file_info = json.loads(gen.stdout.strip().splitlines()[-1])
                key = f"E3-{rows}-{compression}"
                self.workloads[key] = {"file": file_info}
                for rep in range(reps):
                    warm = rep < int(self.spec["warmups"])
                    methods = cfg["methods"]
                    k = rep % len(methods)
                    for method in methods[k:] + methods[:k]:
                        config = {"rows": rows, "compression": compression, "batch_size": cfg["batch_size"], "method": method}
                        attempt = [0]

                        def load(method=method, rows=rows, compression=compression, rep=rep, path=path, attempt=attempt):
                            attempt[0] += 1  # each attempt gets its own query-log tag
                            tag = f"e3-{self.run_id}-{rows}-{compression}-{method}-r{rep}-a{attempt[0]}"
                            proc = compose.run_once("app", "-m", "connectedfleet.bench.csv_backfill", "run", "--method", method,
                                                    "--file", path, "--database", method_db[method], "--batch",
                                                    str(cfg["batch_size"]), "--tag", tag)
                            if proc.returncode != 0:
                                raise RuntimeError(proc.stderr[-800:])
                            return {**json.loads(proc.stdout.strip().splitlines()[-1]), "tag": tag}

                        try:
                            result = self.timed(f"E3 {rows} {compression} {method} r{rep}", load)
                        except RuntimeError as err:
                            self.record("E3", method, {}, "file", key, config, warm, rep, None, "ERROR", str(err))
                            continue
                        result["server"] = queries.server_cost(self.ch, result["tag"])
                        db = method_db[method]
                        from connectedfleet.bench.csv_backfill import INSERT_COLUMNS, TABLE

                        result["counts"] = {"physical": int(self.ch.scalar(f"SELECT count() FROM `{db}`.`{TABLE}`") or 0),
                                            "logical": int(self.ch.scalar(f"SELECT uniqExact(row_number) FROM `{db}`.`{TABLE}`") or 0)}
                        # one load into an emptied table: dedupe only if duplicates actually exist (memory, see D-029)
                        dedupe = result["counts"]["physical"] != result["counts"]["logical"]
                        result["content_hash"] = queries.content_hash(self.ch, db, TABLE, list(INSERT_COLUMNS), key="row_number",
                                                                      dedupe=dedupe)
                        self.record("E3", method, {}, "file", key, config, warm, rep, result, "COMPLETED")

    # ------------------------------------------------------------------ E4 correctness / failure
    def e4(self) -> None:
        cfg = self.spec["experiments"]["E4"]
        timeout = float(self.spec["timeouts"]["lane_drain_s"])
        count, batch, flush_ms = int(cfg["event_count"]), int(cfg["batch_size"]), int(cfg["flush_timeout_ms"])
        poison = int(cfg.get("poison_records", 0))
        spec = WorkloadSpec(count, int(self.spec["seed"]) + 4, 0.04, False, id_prefix="e4")
        key = f"E4-{count}"
        workload = self.workload(key, spec)
        topic = self.topic("e4")
        workload["topic"] = topic
        workload["produced"] = self.produce(topic, spec, poison=poison)
        workload["poison_records"] = poison
        config = {"batch_size": batch, "flush_timeout_ms": flush_ms}

        def dlq(topic_name: str) -> int:
            return kafka_admin.message_count(self.settings.kafka_bootstrap, topic_name)

        def poison_rows(lane: str) -> int:
            return int(self.ch.scalar(f"SELECT count() FROM `{sinkcfg.db_for(lane)}`.`telemetry_fact` WHERE startsWith(event_id, 'poison-')") or 0)

        # 1) happy path with a mid-stream poison record, then 2) replay from offset 0 with a new consumer group
        # (no truncate). Lane A gets a 70 s stall window: a batch dead-lettered with the poison never reaches the
        # expected count, and its trailing partial buffer is only flushed on Kafka Connect's 60 s offset flush.
        # Extra sink variants (sinkcfg) run the poison case only; the original sink is then recovered from its DLQ.
        sink_lanes = list(cfg.get("sink_variants", ["sink"]))
        for lane in [*sink_lanes, "row", "arrow"]:
            is_sink = sinkcfg.is_sink(lane)
            dlq_topic = topics.DLQ_SINK if is_sink else topics.DLQ_LANE[lane]
            before = dlq(dlq_topic)
            try:
                if is_sink:
                    m = self.run_lane_a(key, topic, "telemetry_fact", batch, flush_ms, count, timeout, stall_s=70.0,
                                        variant=lane)
                else:
                    m = self.run_lane_bc(key, lane, topic, "telemetry_fact", batch, flush_ms, count + poison, count, timeout)
                m["dlq_delta"] = dlq(dlq_topic) - before
                m["poison_in_table"] = poison_rows(lane)
                self.record("E4", lane, {}, "scenario", key, {**config, "scenario": "happy_path_with_poison"}, False, 0, m, "COMPLETED")
                if lane not in ("sink", "row", "arrow"):
                    continue
                first = dict(m["counts"])
                if lane == "sink":
                    # Replay re-inserts whatever the first pass inserted: wait for 2x the first pass's physical rows.
                    r = self.run_lane_a(key, topic, "telemetry_fact", batch, flush_ms, first["logical"], timeout, truncate=False,
                                        expect_physical=2 * first["physical"], stall_s=70.0)
                else:
                    r = self.run_lane_bc(key, lane, topic, "telemetry_fact", batch, flush_ms, count + poison, count, timeout, truncate=False)
                r["first_pass_counts"] = first
                self.record("E4", lane, {}, "scenario", key, {**config, "scenario": "replay_new_group"}, False, 0, r, "COMPLETED")
                if lane == "sink":
                    self.e4_recovery(key, batch, flush_ms, count, timeout, dlq_topic, before, config)
            except Exception as err:  # noqa: BLE001
                self.record("E4", lane, {}, "scenario", key, {**config, "scenario": "happy_path_with_poison"}, False, 0, None,
                            "ERROR", f"{type(err).__name__}: {err}")

        # 3) crash after ClickHouse ack, before Kafka commit (custom lanes only)
        for lane in ("row", "arrow"):
            group = f"cfbench-e4crash-{lane}-{secrets.token_hex(3)}"
            try:
                crashed = self.run_lane_bc(key, lane, topic, "telemetry_fact", batch, flush_ms, count + poison, count, timeout,
                                           fault="after_insert_before_commit", group=group)
                resumed = self.run_lane_bc(key, lane, topic, "telemetry_fact", batch, flush_ms, count + poison, count, timeout,
                                           group=group, truncate=False)
                resumed["crash_run"] = {"exit_code": crashed["lane_runtime"]["exit_code"], "counts": crashed["counts"]}
                self.record("E4", lane, {}, "scenario", key, {**config, "scenario": "crash_after_insert_before_commit"}, False, 0,
                            resumed, "COMPLETED")
            except Exception as err:  # noqa: BLE001
                self.record("E4", lane, {}, "scenario", key, {**config, "scenario": "crash_after_insert_before_commit"}, False, 0,
                            None, "ERROR", f"{type(err).__name__}: {err}")
        self.not_run.append({"experiment": "E4", "lane": "sink", "scenario": "crash_after_insert_before_commit",
                             "reason": "NOT_APPLICABLE: offsets are managed by the Kafka Connect framework; no injection point in this POC"})

        # 4) null-heavy edge: every lane must preserve null counts exactly
        edge = WorkloadSpec(2000, int(self.spec["seed"]) + 5, float(cfg.get("null_edge_fraction", 0.9)), False, id_prefix="e4null")
        edge_key = "E4-null-edge"
        edge_workload = self.workload(edge_key, edge)
        edge_topic = self.topic("e4-null")
        edge_workload["topic"] = edge_topic
        edge_workload["produced"] = self.produce(edge_topic, edge)
        for lane in ("sink", "row", "arrow"):
            try:
                if lane == "sink":
                    m = self.run_lane_a(edge_key, edge_topic, "telemetry_fact", 500, 500, edge.count, timeout)
                else:
                    m = self.run_lane_bc(edge_key, lane, edge_topic, "telemetry_fact", 500, 500, edge.count, edge.count, timeout)
                self.record("E4", lane, {}, "scenario", edge_key, {"batch_size": 500, "scenario": "schema_null_edge"}, False, 0, m, "COMPLETED")
            except Exception as err:  # noqa: BLE001
                self.record("E4", lane, {}, "scenario", edge_key, {"scenario": "schema_null_edge"}, False, 0, None, "ERROR", str(err))

        # 5) source-level cases come from the latest Lab 02 run (executed there, against the real connectors)
        latest = RESULTS_ROOT / "lab-02" / "latest.json"
        if latest.is_file():
            pointer = json.loads(latest.read_text())
            lab02 = json.loads((RESULTS_ROOT / "lab-02" / "runs" / pointer["run_id"] / "run.json").read_text())
            self.workloads["E4-lab02-link"] = {"lab02_run_id": pointer["run_id"], "lab02_status": pointer["status"],
                                              "checks": lab02.get("checks", [])}
        else:
            self.not_run.append({"experiment": "E4", "scenario": "source-level (CSV re-import, PG delete/update, outbox rollback, Mongo update/delete)",
                                 "reason": "no Lab 02 run found; run `make lab-02` first"})

    def e4_recovery(self, key: str, batch: int, flush_ms: int, count: int, timeout: float, dlq_topic: str, dlq_from: int,
                    config: dict) -> None:
        """Recover the original sink's dead-lettered records: re-validate, replay the valid ones to a recovery topic,
        let the same connector configuration insert them, and check the table ends with every good row once."""
        from connectedfleet import recovery
        from connectedfleet.kafka_io import KafkaProducer

        target = self.topic("e4-recovery")
        try:
            records = recovery.read_topic(self.settings.kafka_bootstrap, dlq_topic, dlq_from)
            result = recovery.replay(records, KafkaProducer(self.settings.kafka_bootstrap), target)
            m = self.run_lane_a(key, target, "telemetry_fact", batch, flush_ms, count, timeout, truncate=False,
                                stall_s=70.0, variant="sink")
            m["recovery"] = {k: v for k, v in result.items() if k != "quarantine"}
            m["recovery"]["quarantined_ids"] = sorted({q["event_id"] or "" for q in result["quarantine"]})
            self.record("E4", "sink", {}, "scenario", key, {**config, "scenario": "dlq_recovery"}, False, 0, m, "COMPLETED")
        except Exception as err:  # noqa: BLE001
            self.record("E4", "sink", {}, "scenario", key, {**config, "scenario": "dlq_recovery"}, False, 0, None,
                        "ERROR", f"{type(err).__name__}: {err}")

    # ------------------------------------------------------------------ E5 interop (optional)
    def e5(self) -> None:
        cfg = self.spec["experiments"]["E5"]
        try:
            from connectedfleet.bench import interop
        except ImportError as err:
            self.not_run.append({"experiment": "E5", "reason": f"optional dependencies missing ({err}); `uv sync --group interop`"})
            return
        try:
            result = interop.measure(int(cfg["event_count"]), int(self.spec["seed"]) + 9)
            self.record("E5", "host", {}, "in-process", None, {"event_count": cfg["event_count"]}, False, 0, result, "COMPLETED")
        except ImportError as err:
            self.not_run.append({"experiment": "E5", "reason": f"optional dependencies missing ({err}); `uv sync --group interop`"})

    # ------------------------------------------------------------------ main
    def execute(self) -> int:
        self.log(f"run {self.run_id} profile={self.profile}")
        missing = self.connect.missing_plugins()
        if missing:
            raise SystemExit(f"Kafka Connect plugins missing: {missing}; run `make up init`")
        schema.apply(self.ch)
        stale = stale_bench_connectors(self.connect.list())
        for name in stale:
            self.connect.delete(name)
        if stale:
            self.log(f"deleted {len(stale)} stale benchmark connectors left by earlier runs: {stale}")
        self.stale_connectors_removed = stale
        paused = [n for n in (*connect_mod.SOURCE_CONNECTORS, connect_mod.SINK_CONNECTOR) if n in self.connect.list()]
        for name in paused:
            self.connect.pause(name)
        compose.stop("normalizer", "worker-row", "worker-arrow")
        self.log(f"paused connectors {paused} and stopped pipeline workers for isolation")
        experiments = (("E1", self.e1), ("E2", self.e2), ("E3", self.e3), ("E4", self.e4), ("E5", self.e5))
        for experiment, _ in experiments:
            if not self.enabled(experiment):
                self.not_run.append({"experiment": experiment, "reason": "disabled in workload or excluded by --only"})
        try:
            self.failed_experiments = run_experiments(experiments, self.enabled, self.log, self.checkpoint)
            self.not_run.extend(self.failed_experiments)
        finally:
            for name in paused:
                try:
                    self.connect.resume(name)
                except connect_mod.ConnectError:
                    pass
            kafka_admin.delete_topics(self.settings.kafka_bootstrap, self.created_topics)
        return self.write()

    def checkpoint(self) -> None:
        """Persist the manifest so far (status RUNNING); the final write() replaces it."""
        manifest = self._manifest("RUNNING")
        (self.run_dir / "run.json").write_text(json.dumps(manifest, indent=2, default=str, ensure_ascii=False))

    def _manifest(self, status: str) -> dict:
        stack_versions = json.loads((POC_ROOT / "versions.json").read_text()) if (POC_ROOT / "versions.json").is_file() else None
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "lab": "lab-03",
            "profile": self.profile,
            "status": status,
            "started_at_utc": self.started,
            "ended_at_utc": _utc(),
            "code": self.code,
            "host": host_info(),
            "versions": {**versions(), "stack": stack_versions},
            "workload_spec": self.spec,
            "workloads": self.workloads,
            "measurement_definitions": MEASUREMENT_DEFINITIONS,
            "cases": self.cases,
            "not_run": self.not_run,
            "stale_connectors_removed": getattr(self, "stale_connectors_removed", []),
            "host_guard": None if not self.guard else {
                "config": self.spec.get("host_guard"), "waited_s": round(self.guard.waited_s, 1),
                "discarded_cases": self.guard.discarded},
            "known_interference": "single Docker VM shared by all services; source connectors paused; one lane at a time",
            "limitations": [
                "one local machine (Docker Desktop VM); not production scaling",
                "lane A is a different runtime (JVM connector) and insert format (JSONEachRow): A vs C is a system comparison",
                "B vs C share decode/validate code; differences come from representation, compute and insert format",
            ],
        }

    def write(self) -> int:
        from connectedfleet.bench import report as report_mod
        from connectedfleet.bench import validate as validate_mod

        if getattr(self, "failed_experiments", None):
            status = "PARTIAL"  # an experiment failed; its error is in not_run, completed cases stay valid
        elif self.cases and all(c["status"] == "COMPLETED" for c in self.cases):
            status = "COMPLETED"
        else:
            status = "COMPLETED_WITH_ERRORS"
        manifest = self._manifest(status)
        (self.run_dir / "run.json").write_text(json.dumps(manifest, indent=2, default=str, ensure_ascii=False))
        with (self.run_dir / "samples.jsonl").open("w") as handle:
            for sample in self.samples:
                handle.write(json.dumps(sample, default=str) + "\n")
        validation = validate_mod.validate_run(manifest)
        (self.run_dir / "validation.json").write_text(json.dumps(validation, indent=2))
        report_mod.write_run_metrics(self.run_dir, manifest, validation)
        report_mod.regenerate(RESULTS_ROOT)
        valid = validation["run"]["valid"]
        self.log(f"run {self.run_id}: {len(self.cases)} cases, run valid={valid}; evidence {self.run_dir.relative_to(POC_ROOT)}")
        return 0 if valid else 1


def main(argv: list[str] | None = None) -> int:
    import argparse

    from connectedfleet.config import load_dotenv

    parser = argparse.ArgumentParser(description="Lab 03 benchmark orchestrator")
    parser.add_argument("--profile", choices=("smoke", "confirm", "full"), default="smoke")
    parser.add_argument("--only", default="", help="comma-separated experiments, e.g. E1,E4")
    args = parser.parse_args(argv)
    load_dotenv(POC_ROOT / ".env")
    bench = Bench(Settings.from_env(), args.profile, [x for x in args.only.split(",") if x])
    return bench.execute()


if __name__ == "__main__":
    sys.exit(main())
