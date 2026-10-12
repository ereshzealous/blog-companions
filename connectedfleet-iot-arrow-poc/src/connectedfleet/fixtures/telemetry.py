"""Seeded `cf.telemetry.v1` JSON-lines fixture.

The fixture is a synthetic experiment input, not a claim about real fleet
data. Every edge case the lab must handle is injected at a fixed position
(index modulo a period), so any fixture of a few hundred records or more
contains all of them, and the expected counts are known without running
either processing path.
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone

DEFAULT_SEED = 20261009
BASE_EVENT_TIME_MS = int(datetime(2026, 10, 9, 8, 0, tzinfo=timezone.utc).timestamp() * 1000)
EVENT_SPACING_MS = 50

DEVICE_COUNT = 1000
SITES = (
    *(f"factory-{n:02d}" for n in range(1, 17)),
    "werk-münchen",
    "工場-東京",
    "planta-são-paulo",
    "zavod-новосибирск",
)
FIRMWARE = ("3.2.1", "3.2.0", "3.1.9", "3.3.0-rc1")

# Boundary injections: index % BOUNDARY_PERIOD == offset.
BOUNDARY_PERIOD = 50
BATTERY_AT_20 = 7  # battery_pct == 20 -> not low battery (strict <)
TEMP_AT_40 = 13  # temperature_c == 40.0 -> not hot (strict >)
JUST_RISKY = 21  # battery 19, temperature 40.1 -> risky
BOTH_AT_BOUNDARY = 29  # battery 20, temperature 40.0 -> not risky

# Malformed / unusual injections: index % MALFORMED_PERIOD == MALFORMED_OFFSET.
MALFORMED_PERIOD = 200
MALFORMED_OFFSET = 101
# (kind, expected reject reason or None when the record must stay valid)
MALFORMED_KINDS = (
    ("temperature_string", "BAD_TYPE"),
    ("battery_negative", "OUT_OF_RANGE"),
    ("battery_over_100", "OUT_OF_RANGE"),
    ("battery_float", "BAD_TYPE"),
    ("signal_string", "BAD_TYPE"),
    ("event_time_garbage", "BAD_TIMESTAMP"),
    ("missing_device_id", "MISSING_REQUIRED"),
    ("truncated_json", "MALFORMED_JSON"),
    ("schema_version_2", "UNSUPPORTED_SCHEMA_VERSION"),
    ("temperature_nan", "OUT_OF_RANGE"),
    ("naive_timestamp", "BAD_TIMESTAMP"),
    ("unknown_extra_field", None),
)

# Four observations used by figure 01-F03 and the lab's illustrative buffer report.
TOY_OBSERVATIONS = (
    {"device_id": "sensor-01", "temperature_c": 21.5, "battery_pct": 88},
    {"device_id": "sensor-02", "temperature_c": 43.6, "battery_pct": 17},
    {"device_id": "sensor-03", "temperature_c": None, "battery_pct": 64},
    {"device_id": "sensor-04", "temperature_c": 41.2, "battery_pct": 12},
)


@dataclass
class Fixture:
    seed: int
    count: int
    lines: list[bytes]
    sha256: str
    injected: dict[str, int] = field(default_factory=dict)
    expected_rejects: dict[str, int] = field(default_factory=dict)

    @property
    def byte_size(self) -> int:
        return sum(len(line) + 1 for line in self.lines)


def _iso_ms(epoch_ms: int) -> str:
    moment = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{epoch_ms % 1000:03d}Z"


def _device_id(number: int) -> str:
    if number % 97 == 0:
        return f"capteur-é{number:05d}"
    if number % 89 == 0:
        return f"センサー-{number:05d}"
    return f"sensor-{number:05d}"


def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


def generate(count: int, seed: int = DEFAULT_SEED) -> Fixture:
    """Generate `count` JSON lines deterministically from `seed`."""
    rng = random.Random(seed)
    injected: dict[str, int] = {}
    expected_rejects: dict[str, int] = {}
    lines: list[bytes] = []
    dumps = json.dumps

    for index in range(count):
        event_ms = BASE_EVENT_TIME_MS + index * EVENT_SPACING_MS
        if rng.random() < 0.02:
            event_ms -= rng.randint(1_000, 600_000)  # late event: event time well before arrival order
            _bump(injected, "late_event")
        emitted_ms = event_ms + rng.randint(20, 1_500)

        record: dict = {
            "event_id": f"evt-{index:08d}",
            "device_id": _device_id(rng.randrange(DEVICE_COUNT)),
            "event_time": _iso_ms(event_ms),
            "emitted_at": _iso_ms(emitted_ms),
            "source_system": "iot-gateway",
            "schema_version": 1,
        }

        temperature = round(min(85.0, max(-20.0, rng.gauss(33.0, 7.0))), 1)
        battery = rng.randint(0, 19) if rng.random() < 0.15 else rng.randint(20, 100)
        signal = rng.randint(-115, -45)
        firmware = None if rng.random() < 0.02 else rng.choice(FIRMWARE)
        site = None if rng.random() < 0.01 else rng.choice(SITES)

        slot = index % BOUNDARY_PERIOD
        if slot == BATTERY_AT_20:
            battery = 20
            _bump(injected, "battery_at_20")
        elif slot == TEMP_AT_40:
            temperature = 40.0
            _bump(injected, "temperature_at_40")
        elif slot == JUST_RISKY:
            battery, temperature = 19, 40.1
            _bump(injected, "just_risky")
        elif slot == BOTH_AT_BOUNDARY:
            battery, temperature = 20, 40.0
            _bump(injected, "both_at_boundary")

        # Optional fields: explicit null, absent, or a value. Draws happen
        # unconditionally so injections never shift the random stream.
        temperature_draw = rng.random()
        battery_draw = rng.random()
        signal_draw = rng.random()
        if temperature_draw < 0.03:
            record["temperature_c"] = None
            _bump(injected, "temperature_null")
        elif temperature_draw < 0.04:
            _bump(injected, "temperature_absent")
        else:
            record["temperature_c"] = temperature
        if battery_draw < 0.03:
            record["battery_pct"] = None
            _bump(injected, "battery_null")
        elif battery_draw < 0.04:
            _bump(injected, "battery_absent")
        else:
            record["battery_pct"] = battery
        record["signal_dbm"] = None if signal_draw < 0.02 else signal
        record["firmware_version"] = firmware
        record["site_id"] = site

        truncate = False
        if index % MALFORMED_PERIOD == MALFORMED_OFFSET:
            kind, reason = MALFORMED_KINDS[(index // MALFORMED_PERIOD) % len(MALFORMED_KINDS)]
            _bump(injected, kind)
            if reason is not None:
                _bump(expected_rejects, reason)
            if kind == "temperature_string":
                record["temperature_c"] = "hot"
            elif kind == "battery_negative":
                record["battery_pct"] = -5
            elif kind == "battery_over_100":
                record["battery_pct"] = 101
            elif kind == "battery_float":
                record["battery_pct"] = 17.5
            elif kind == "signal_string":
                record["signal_dbm"] = "x"
            elif kind == "event_time_garbage":
                record["event_time"] = "not-a-time"
            elif kind == "missing_device_id":
                del record["device_id"]
            elif kind == "truncated_json":
                truncate = True
            elif kind == "schema_version_2":
                record["schema_version"] = 2
            elif kind == "temperature_nan":
                record["temperature_c"] = float("nan")
            elif kind == "naive_timestamp":
                record["event_time"] = record["event_time"].rstrip("Z")
            elif kind == "unknown_extra_field":
                record["humidity_pct"] = 41

        line = dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if truncate:
            line = line[: len(line) // 2]
        lines.append(line)

    digest = hashlib.sha256()
    for line in lines:
        digest.update(line)
        digest.update(b"\n")
    return Fixture(
        seed=seed,
        count=count,
        lines=lines,
        sha256=digest.hexdigest(),
        injected=injected,
        expected_rejects=expected_rejects,
    )
