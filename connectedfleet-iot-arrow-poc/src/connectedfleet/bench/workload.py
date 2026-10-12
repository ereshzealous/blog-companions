"""Deterministic canonical-telemetry workloads for Lab 03.

Unlike the Lab 01 fixture, benchmark events are always contract-valid (no
malformed records), so every lane, including the official sink, receives
identical, insertable input. Shapes vary the parts that stress columnar vs
row processing differently:
  dense         few nulls, short strings
  null_heavy    many null measurements (validity bitmaps do real work)
  string_heavy  long firmware/site strings (offset + data buffers dominate)

`emitted_at` is stamped with the real clock at send time, so it is excluded
from the workload digest; everything else is reproducible from the spec.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass

from connectedfleet.contracts.canonical import format_timestamp_ms
from connectedfleet.fixtures.telemetry import BASE_EVENT_TIME_MS, EVENT_SPACING_MS, FIRMWARE, SITES

MEASUREMENTS = ("temperature_c", "battery_pct", "signal_dbm")


@dataclass(frozen=True)
class WorkloadSpec:
    count: int
    seed: int
    null_fraction: float = 0.04
    string_heavy: bool = False
    devices: int = 1000
    id_prefix: str = "bench"

    def describe(self) -> dict:
        return asdict(self)


def _long_text(rng: random.Random, base: str) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-"
    return f"{base}-{''.join(rng.choice(alphabet) for _ in range(48))}"[:64]


def events(spec: WorkloadSpec):
    """Yield canonical telemetry dicts WITHOUT emitted_at (stamped at send time)."""
    rng = random.Random(spec.seed)
    for index in range(spec.count):
        event = {
            "event_id": f"{spec.id_prefix}-{spec.seed}-{index:010d}",
            "device_id": f"sensor-{rng.randrange(spec.devices):05d}",
            "event_time": format_timestamp_ms(BASE_EVENT_TIME_MS + index * EVENT_SPACING_MS),
            "source_system": "iot-gateway",
            "schema_version": 1,
        }
        temperature = round(min(85.0, max(-20.0, rng.gauss(33.0, 7.0))), 1)
        battery = rng.randint(0, 19) if rng.random() < 0.15 else rng.randint(20, 100)
        signal = rng.randint(-115, -45)
        draws = [rng.random() for _ in MEASUREMENTS]
        for name, value, draw in zip(MEASUREMENTS, (temperature, battery, signal), draws):
            event[name] = None if draw < spec.null_fraction else value
        firmware = rng.choice(FIRMWARE)
        site = rng.choice(SITES)
        if spec.string_heavy:
            firmware, site = _long_text(rng, firmware), _long_text(rng, site)
        event["firmware_version"] = firmware
        event["site_id"] = site
        yield event


def digest_and_nulls(spec: WorkloadSpec) -> tuple[str, dict[str, int], dict[str, int]]:
    """(content sha256 over sorted-key JSON lines, null counts per measurement, risk counts)."""
    from connectedfleet.contracts.telemetry import risk_flag

    digest = hashlib.sha256()
    nulls = dict.fromkeys(MEASUREMENTS, 0)
    risk = {"true": 0, "false": 0, "unknown": 0}
    for event in events(spec):
        digest.update(json.dumps(event, sort_keys=True, ensure_ascii=False).encode())
        digest.update(b"\n")
        for name in MEASUREMENTS:
            if event[name] is None:
                nulls[name] += 1
        flag = risk_flag(event["battery_pct"], event["temperature_c"])
        risk["true" if flag is True else "false" if flag is False else "unknown"] += 1
    return digest.hexdigest(), nulls, risk
