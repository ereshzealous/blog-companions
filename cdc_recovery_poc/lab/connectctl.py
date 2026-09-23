#!/usr/bin/env python3
"""Kafka Connect REST control for the lab connector.

Usage: connectctl.py register [key=value ...] | status | task-worker | wait STATE [timeout_s] | stop | resume
       | restart | offsets | reset-offsets | delete | versions
"""
import json
import sys
import time

import requests

from common import CONNECT_WORKERS, CONNECTOR


def call(method, path, **kw):
    last = None
    for _ in range(30):
        for base in CONNECT_WORKERS:
            try:
                r = requests.request(method, base + path, timeout=15, **kw)
            except requests.RequestException as exc:
                last = exc
                continue
            if r.status_code == 409:  # rebalance in progress
                last = r.text
                continue
            return r
        time.sleep(2)
    raise SystemExit(f"Connect REST unavailable: {last}")


def status():
    r = call("GET", f"/connectors/{CONNECTOR}/status")
    return r.json() if r.ok else {"connector": {"state": "ABSENT"}, "tasks": []}


def main(argv):
    cmd = argv[0] if argv else "status"
    if cmd == "register":
        config = json.load(open("/connect/pg-devices.json"))
        for pair in argv[1:]:
            key, value = pair.split("=", 1)
            config[key] = value
        r = call("PUT", f"/connectors/{CONNECTOR}/config", json=config)
        print(r.status_code, r.text[:500])
        r.raise_for_status()
    elif cmd == "status":
        print(json.dumps(status(), indent=1))
    elif cmd == "task-worker":
        tasks = status().get("tasks") or []
        print(tasks[0]["worker_id"].split(":")[0] if tasks else "")
    elif cmd == "wait":
        want, timeout = argv[1], float(argv[2]) if len(argv) > 2 else 300
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            s = status()
            tasks = s.get("tasks") or []
            state = tasks[0]["state"] if tasks else "UNASSIGNED"
            if state == want or s["connector"]["state"] == want == "STOPPED":
                print(state)
                return
            time.sleep(1)
        raise SystemExit(f"timeout waiting for {want}; last status: {json.dumps(status())[:800]}")
    elif cmd in ("stop", "resume"):
        r = call("PUT", f"/connectors/{CONNECTOR}/{cmd}")
        print(r.status_code, r.text[:300])
    elif cmd == "restart":
        r = call("POST", f"/connectors/{CONNECTOR}/restart?includeTasks=true&onlyFailed=false")
        print(r.status_code, r.text[:300])
    elif cmd == "offsets":
        print(json.dumps(call("GET", f"/connectors/{CONNECTOR}/offsets").json(), indent=1))
    elif cmd == "reset-offsets":
        r = call("DELETE", f"/connectors/{CONNECTOR}/offsets")
        print(r.status_code, r.text[:500])
        r.raise_for_status()
    elif cmd == "delete":
        r = call("DELETE", f"/connectors/{CONNECTOR}")
        print(r.status_code, r.text[:300])
    elif cmd == "versions":
        root = call("GET", "/").json()
        plugins = [p for p in call("GET", "/connector-plugins").json() if "postgres" in p["class"].lower()]
        print(json.dumps({"kafka_connect": root.get("version"), "postgres_plugin": plugins}, indent=1))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
