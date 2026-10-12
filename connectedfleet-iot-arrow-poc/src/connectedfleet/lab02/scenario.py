"""Lab 02 scenario (docs/ARCHITECTURE.md §9.1).

Run identifiers make every expectation exact even when the stack has history:
devices are `dev-<tag>-NN`, telemetry ids `gw-<tag>-…`, outbox ids are uuid5(tag, step).
Cross-lane equivalence is checked on whole tables (all lanes read the same
canonical topics), after each lane has caught up.

Nothing here simulates CDC: PostgreSQL and MongoDB are mutated through their
real drivers and Debezium does the capture. A check that cannot be evaluated
is recorded as failed, not skipped.
"""

from __future__ import annotations

import json
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from connectedfleet import compose, kafka_admin, topics
from connectedfleet import connect as connect_mod
from connectedfleet.clickhouse import queries
from connectedfleet.clickhouse.http import ClickHouseHTTP
from connectedfleet.config import POC_ROOT, Settings
from connectedfleet.contracts import canonical
from connectedfleet.instrumentation.env import git_state, host_info, versions
from connectedfleet.kafka_io import KafkaProducer
from connectedfleet.producers import csv_import
from connectedfleet.producers.telemetry import produce as produce_telemetry
from connectedfleet.sources import mongo_state, registry_app

RESULTS_ROOT = POC_ROOT / "benchmarks" / "results" / "lab-02"
CSV_FIXTURE = POC_ROOT / "fixtures" / "diagnostics" / "batch-001.csv"
LANES = topics.LANE_DATABASES  # sink -> bench_sink, row -> bench_row, arrow -> bench_arrow


@dataclass
class Check:
    name: str
    ok: bool
    expected: object = None
    observed: object = None
    lane: str | None = None
    note: str = ""


@dataclass
class Plan:
    tag: str
    telemetry_count: int
    poison_every: int
    fault_count: int
    steps: dict = field(default_factory=dict)

    @property
    def dev1(self) -> str:
        return f"dev-{self.tag}-01"

    @property
    def dev2(self) -> str:
        return f"dev-{self.tag}-02"

    @property
    def expected_telemetry(self) -> int:
        poison = self.telemetry_count // self.poison_every if self.poison_every else 0
        return self.telemetry_count - poison


def _wait(predicate, timeout_s: float, interval_s: float = 3.0):
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last is True or (isinstance(last, tuple) and last[0] is True):
            return last
        time.sleep(interval_s)
    return last


class Lab02:
    def __init__(self, settings: Settings, smoke: bool = False, timeout_s: float = 300.0) -> None:
        self.settings = settings
        self.ch = ClickHouseHTTP(settings.clickhouse_http_url, settings.clickhouse_user, settings.clickhouse_password)
        self.connect = connect_mod.ConnectClient(settings.connect_url)
        self.checks: list[Check] = []
        self.timeout_s = timeout_s
        tag = secrets.token_hex(3)
        self.plan = Plan(tag=tag, telemetry_count=100 if smoke else 500, poison_every=50 if smoke else 250,
                         fault_count=60 if smoke else 120)
        self.smoke = smoke
        self.started = datetime.now(timezone.utc)

    # ------------------------------------------------------------------ helpers
    def check(self, name: str, ok: bool, expected=None, observed=None, lane: str | None = None, note: str = "") -> bool:
        self.checks.append(Check(name, bool(ok), expected, observed, lane, note))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}{f' ({lane})' if lane else ''}: expected={expected} observed={observed} {note}", flush=True)
        return bool(ok)

    def _q(self, sql: str):
        return self.ch.query_json(sql)

    # ------------------------------------------------------------------ preflight
    def preflight(self) -> bool:
        print("== preflight", flush=True)
        ok = self.check("clickhouse reachable", self.ch.ping(), True, True)
        missing = self.connect.missing_plugins()
        ok &= self.check("connect plugins loaded", not missing, [], missing)
        for name in (*connect_mod.SOURCE_CONNECTORS, connect_mod.SINK_CONNECTOR):
            try:
                self.connect.wait_running(name, timeout_s=60)
                running = True
            except connect_mod.ConnectError as err:
                running = False
                print(f"     {err}")
            ok &= self.check(f"connector {name} RUNNING", running, "RUNNING", running)
        return ok

    # ------------------------------------------------------------------ pipeline processes
    def start_pipeline(self) -> None:
        print("== start normalizer + lanes B/C (lane A runs inside Kafka Connect)", flush=True)
        compose.up("normalizer", "worker-row", "worker-arrow")

    # ------------------------------------------------------------------ source mutations
    def mutate_sources(self) -> None:
        plan, s = self.plan, self.settings
        print(f"== mutate sources (tag {plan.tag})", flush=True)
        conn = registry_app.connect(s.postgres_dsn)
        ids = {step: registry_app.outbox_id(plan.tag, step) for step in ("activate1", "activate2", "firmware1", "retire2", "rollback1")}
        registry_app.activate_device(conn, plan.dev1, "acme-logistics", "factory-03", "3.2.0", ids["activate1"])
        registry_app.activate_device(conn, plan.dev2, "polar-foods", "工場-東京", "3.1.9", ids["activate2"])
        registry_app.update_firmware(conn, plan.dev1, "3.2.1", ids["firmware1"])
        registry_app.set_state(conn, plan.dev2, "maintenance")  # CDC only, no business event
        registry_app.retire_and_delete(conn, plan.dev2, ids["retire2"], "decommissioned")
        registry_app.rolled_back_firmware_update(conn, plan.dev1, "9.9.9", ids["rollback1"])
        self.check("rollback left no outbox row", not registry_app.outbox_exists(conn, ids["rollback1"]), False,
                   registry_app.outbox_exists(conn, ids["rollback1"]))
        row = registry_app.registry_row(conn, plan.dev1)
        self.check("rollback left business row unchanged", row and row["firmware_version"] == "3.2.1", "3.2.1",
                   row and row["firmware_version"])
        conn.close()
        plan.steps["outbox_ids"] = ids

        coll = mongo_state.collection(s.mongo_uri)
        mongo_state.upsert_state(coll, plan.dev1, "cold-chain", 30, "normal", "3.2.1")
        mongo_state.patch_state(coll, plan.dev1, {"sampling_interval_s": 60})
        mongo_state.upsert_state(coll, plan.dev2, "standard", 120, "power_saving", "3.1.9")
        mongo_state.delete_state(coll, plan.dev2)

        producer = KafkaProducer(s.kafka_bootstrap)
        plan.steps["dlq_before"] = kafka_admin.message_count(s.kafka_bootstrap, topics.DLQ_NORMALIZER)
        plan.steps["telemetry"] = produce_telemetry(producer, topics.RAW_TELEMETRY, plan.telemetry_count, seed=7,
                                                    run_tag=plan.tag, rate=0, poison_every=plan.poison_every)
        plan.steps["csv_first"] = csv_import.import_file(producer, CSV_FIXTURE)
        plan.steps["csv_second"] = csv_import.import_file(producer, CSV_FIXTURE)

    # ------------------------------------------------------------------ waiting
    def wait_for_lanes(self) -> None:
        plan = self.plan
        print("== wait for every lane to reach the expected logical counts", flush=True)
        where = f"startsWith(event_id, 'gw-{plan.tag}-')"
        for lane, db in LANES.items():
            def ready(db=db):
                c = queries.counts(self.ch, db, "telemetry_fact", where)["logical"]
                r = queries.counts(self.ch, db, "device_registry_versions", f"startsWith(device_id, 'dev-{plan.tag}-')")["logical"]
                m = queries.counts(self.ch, db, "device_state_versions", f"startsWith(device_id, 'dev-{plan.tag}-')")["logical"]
                return (c >= plan.expected_telemetry and r >= 5 and m >= 4, (c, r, m))
            result = _wait(ready, self.timeout_s)
            self.check("lane caught up (telemetry, registry versions, state versions)", result and result[0] is True,
                       (plan.expected_telemetry, 5, 4), result[1] if result else None, lane)

    # ------------------------------------------------------------------ per-lane verification
    def verify_lanes(self) -> None:
        plan = self.plan
        ids = plan.steps["outbox_ids"]
        sha = plan.steps["csv_first"]["sha256"]
        print("== verify each lane", flush=True)
        for lane, db in LANES.items():
            tel = queries.counts(self.ch, db, "telemetry_fact", f"startsWith(event_id, 'gw-{plan.tag}-')")
            self.check("telemetry logical rows", tel["logical"] == plan.expected_telemetry, plan.expected_telemetry, tel, lane)

            life = {r["event_id"]: r["event_type"] for r in self._q(
                f"SELECT DISTINCT event_id, event_type FROM `{db}`.lifecycle_events WHERE event_id IN "
                f"({', '.join(repr(v) for v in ids.values())})")}
            expected_life = {ids["activate1"]: "DeviceActivated", ids["activate2"]: "DeviceActivated",
                             ids["firmware1"]: "FirmwareUpdated", ids["retire2"]: "DeviceDeactivated"}
            self.check("committed outbox events present (exact ids/types)",
                       all(life.get(k) == v for k, v in expected_life.items()), expected_life, life, lane)
            self.check("rolled-back outbox event absent", ids["rollback1"] not in life, "absent",
                       "present" if ids["rollback1"] in life else "absent", lane)

            ops = sorted(r["operation"] for r in self._q(
                f"SELECT operation FROM `{db}`.device_registry_versions WHERE startsWith(device_id, 'dev-{plan.tag}-') "
                "LIMIT 1 BY event_id"))
            self.check("registry CDC versions (insert/update/delete)", ops == sorted(["insert", "insert", "update", "update", "delete"]),
                       sorted(["insert", "insert", "update", "update", "delete"]), ops, lane)
            current = queries.current_state(self.ch, db, "device_registry_current", f"startsWith(device_id, 'dev-{plan.tag}-')")
            self.check("registry current state after FINAL (dev1 3.2.1 present, dev2 deleted)",
                       [(r["device_id"], r["firmware_version"]) for r in current] == [(plan.dev1, "3.2.1")],
                       [(plan.dev1, "3.2.1")], [(r["device_id"], r["firmware_version"]) for r in current], lane)

            states = self._q(
                f"SELECT operation, is_deleted, full_document, sampling_interval_s FROM `{db}`.device_state_versions "
                f"WHERE startsWith(device_id, 'dev-{plan.tag}-') LIMIT 1 BY event_id")
            self.check("mongo change versions (insert/update/insert/delete)",
                       sorted(s["operation"] for s in states) == sorted(["insert", "update", "insert", "delete"]),
                       4, [s["operation"] for s in states], lane)
            cur_state = queries.current_state(self.ch, db, "device_state_current", f"startsWith(device_id, 'dev-{plan.tag}-')")
            self.check("mongo current state after FINAL (dev1 interval 60, dev2 deleted)",
                       [(r["device_id"], int(r["sampling_interval_s"])) for r in cur_state] == [(plan.dev1, 60)],
                       [(plan.dev1, 60)], [(r["device_id"], r["sampling_interval_s"]) for r in cur_state], lane,
                       "full-document lookup can reflect a later update; checked on final state only")

            diag = queries.counts(self.ch, db, "diagnostic_events", f"file_sha256 = '{sha}'")
            self.check("CSV re-import: 20 logical rows, physical >= 2x", diag["logical"] == 20 and diag["physical"] >= 40,
                       {"logical": 20, "physical": ">=40"}, diag, lane)

            snapshot = int(self.ch.scalar(f"SELECT count() FROM `{db}`.device_registry_versions WHERE operation = 'snapshot'") or 0)
            self.check("initial PostgreSQL snapshot rows captured (op r)", snapshot >= 3, ">=3", snapshot, lane,
                       "present once the lane consumed the topic from the beginning")

        dlq_after = kafka_admin.message_count(self.settings.kafka_bootstrap, topics.DLQ_NORMALIZER)
        poison = plan.telemetry_count // plan.poison_every
        self.check("poison telemetry routed to normalizer DLQ (once each)",
                   dlq_after - plan.steps["dlq_before"] == poison, poison, dlq_after - plan.steps["dlq_before"])

    # ------------------------------------------------------------------ cross-lane equivalence
    def verify_equivalence(self) -> None:
        print("== cross-lane equivalence (logical content hash per table)", flush=True)
        for topic, contract in canonical.CONTRACTS.items():
            def hashes(contract=contract):
                return {lane: queries.content_hash(self.ch, db, contract.table, contract.names) for lane, db in LANES.items()}
            result = _wait(lambda: (len(set(hashes().values())) == 1, hashes()), 120, 5)
            self.check(f"{contract.table}: identical logical content in all three databases",
                       result and result[0] is True, "equal", result[1] if result else None)

    # ------------------------------------------------------------------ crash window (lane B)
    def fault_crash_window(self) -> None:
        plan, s = self.plan, self.settings
        tag = f"{plan.tag}f"
        print("== fault: lane B crashes after ClickHouse ack, before Kafka commit", flush=True)
        compose.stop("worker-row")
        produce_telemetry(KafkaProducer(s.kafka_bootstrap), topics.RAW_TELEMETRY, plan.fault_count, seed=11, run_tag=tag, rate=0)
        where = f"startsWith(event_id, 'gw-{tag}-')"
        _wait(lambda: queries.counts(self.ch, "bench_arrow", "telemetry_fact", where)["logical"] >= plan.fault_count, self.timeout_s)
        proc = compose.run_once(
            "worker-row", "-m", "connectedfleet.lanes.worker", "--lane", "row", "--database", "bench_row",
            "--group", "cf-lane-b", "--batch-size", "50", "--flush-ms", "500", "--metrics-out", "/metrics/lane-b-fault.jsonl",
            env={"FAULT": "after_insert_before_commit", "FAULT_AFTER_BATCHES": "1"},
        )
        self.check("fault worker exited inside the crash window", proc.returncode == 86, 86, proc.returncode, "row")
        crashed = queries.counts(self.ch, "bench_row", "telemetry_fact", where)
        compose.up("worker-row")
        result = _wait(lambda: (queries.counts(self.ch, "bench_row", "telemetry_fact", where)["logical"] >= plan.fault_count,
                                queries.counts(self.ch, "bench_row", "telemetry_fact", where)), self.timeout_s)
        after = result[1] if result else None
        self.check("after restart: logical rows complete", after and after["logical"] == plan.fault_count,
                   plan.fault_count, after, "row")
        self.check("after restart: physical duplicates exist (at-least-once, not exactly-once)",
                   after and after["physical"] > after["logical"], "physical > logical", after, "row",
                   f"rows inserted before the crash: {crashed}")

    # ------------------------------------------------------------------ connector restart
    def fault_connector_restart(self) -> None:
        plan = self.plan
        print("== fault: restart the PostgreSQL registry connector, then change data", flush=True)
        self.connect.restart("cf-pg-registry")
        self.connect.wait_running("cf-pg-registry", timeout_s=120)
        conn = registry_app.connect(self.settings.postgres_dsn)
        registry_app.set_state(conn, plan.dev1, "maintenance")
        conn.close()
        where = f"device_id = '{plan.dev1}' AND operation = 'update' AND state = 'maintenance'"
        result = _wait(lambda: all(queries.counts(self.ch, db, "device_registry_versions", where)["logical"] == 1
                                   for db in LANES.values()), self.timeout_s)
        self.check("change after connector restart reaches all lanes exactly once (logically)", result is True, 1,
                   {lane: queries.counts(self.ch, db, "device_registry_versions", where) for lane, db in LANES.items()})

    # ------------------------------------------------------------------ evidence
    def write_evidence(self) -> tuple[Path, str]:
        ended = datetime.now(timezone.utc)
        code = git_state(POC_ROOT)
        run_id = f"lab02-{self.started:%Y%m%dT%H%M%SZ}-{(code['git_sha'] or 'nogit')[:7]}"
        status = "PASS" if self.checks and all(c.ok for c in self.checks) else "FAIL"
        run_dir = RESULTS_ROOT / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        counts = {lane: {t: queries.counts(self.ch, db, t) for t in topics.TOPIC_TABLE.values()} for lane, db in LANES.items()}
        manifest = {
            "schema_version": 1,
            "run_id": run_id,
            "lab": "lab-02",
            "mode": "smoke" if self.smoke else "full-scenario",
            "status": status,
            "started_at_utc": self.started.isoformat(timespec="seconds"),
            "ended_at_utc": ended.isoformat(timespec="seconds"),
            "code": code,
            "host": host_info(),
            "versions": {**versions(), "stack": _read_versions_file()},
            "plan": {"tag": self.plan.tag, "telemetry_count": self.plan.telemetry_count,
                     "poison_every": self.plan.poison_every, "fault_count": self.plan.fault_count,
                     "steps": self.plan.steps},
            "checks": [c.__dict__ for c in self.checks],
            "table_counts": counts,
            "limitations": [
                "single-node local stack; no HA, no production sizing",
                "lanes are at-least-once; physical duplicates are expected after the injected crash",
                "MongoDB update events use a post-change full-document lookup (may include later updates)",
            ],
        }
        (run_dir / "run.json").write_text(json.dumps(manifest, indent=2, default=str, ensure_ascii=False))
        (RESULTS_ROOT / "latest.json").write_text(json.dumps({"run_id": run_id, "status": status}, indent=2))
        return run_dir, status


def _read_versions_file() -> object:
    path = POC_ROOT / "versions.json"
    return json.loads(path.read_text()) if path.is_file() else "versions.json missing: run `make init`"


def run(settings: Settings, smoke: bool = False, skip_faults: bool = False) -> int:
    lab = Lab02(settings, smoke=smoke)
    try:
        if lab.preflight():
            lab.start_pipeline()
            lab.mutate_sources()
            lab.wait_for_lanes()
            lab.verify_lanes()
            lab.verify_equivalence()
            if not skip_faults:
                lab.fault_crash_window()
                lab.fault_connector_restart()
    except Exception as err:  # noqa: BLE001 - any crash is recorded as a failed check, never hidden
        lab.check("lab completed without exceptions", False, "no exception", f"{type(err).__name__}: {err}")
    run_dir, status = lab.write_evidence()
    print(f"\nLab 02 {status}: {sum(c.ok for c in lab.checks)}/{len(lab.checks)} checks passed")
    print(f"evidence: {run_dir.relative_to(POC_ROOT)}")
    return 0 if status == "PASS" else 1
