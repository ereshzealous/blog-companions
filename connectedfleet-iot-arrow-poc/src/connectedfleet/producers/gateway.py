"""Seeded raw IoT gateway records (docs/ARCHITECTURE.md §3.1).

The raw format deliberately differs from `cf.telemetry.v1`: short keys,
deci-degrees Celsius and epoch-millisecond times, so the normalizer has real
mapping work. `sent` is left as None here and stamped with the real clock at
send time by the producer; event time is generated (`ts`) and never measured.
"""

from __future__ import annotations

import random

from connectedfleet.fixtures.telemetry import BASE_EVENT_TIME_MS, EVENT_SPACING_MS, FIRMWARE, SITES


def generate_raw(count: int, seed: int, run_tag: str, devices: int = 200, poison_every: int = 0,
                 base_ms: int = BASE_EVENT_TIME_MS):
    """Yield raw gateway dicts. Every `poison_every`-th record (if > 0) is structurally invalid on purpose."""
    rng = random.Random(seed)
    for index in range(count):
        ts = base_ms + index * EVENT_SPACING_MS
        if rng.random() < 0.02:
            ts -= rng.randint(1_000, 600_000)  # late event
        measurements: dict = {}
        t_draw, b_draw, s_draw = rng.random(), rng.random(), rng.random()
        temperature = int(round(min(85.0, max(-20.0, rng.gauss(33.0, 7.0))) * 10))
        battery = rng.randint(0, 19) if rng.random() < 0.15 else rng.randint(20, 100)
        if t_draw < 0.03:
            measurements["t_dC"] = None
        elif t_draw >= 0.04:
            measurements["t_dC"] = temperature
        if b_draw < 0.03:
            measurements["bat"] = None
        elif b_draw >= 0.04:
            measurements["bat"] = battery
        measurements["rssi"] = None if s_draw < 0.02 else rng.randint(-115, -45)
        record = {
            "msg_id": f"gw-{run_tag}-{index:09d}",
            "dev": f"sensor-{rng.randrange(devices):05d}",
            "ts": ts,
            "sent": None,
            "m": measurements,
            "fw": None if rng.random() < 0.02 else rng.choice(FIRMWARE),
            "site": None if rng.random() < 0.01 else rng.choice(SITES),
        }
        if poison_every and index % poison_every == poison_every - 1:
            record["ts"] = "yesterday"  # normalizer must route this to the DLQ, once
        yield record
