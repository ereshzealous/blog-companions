"""Per-site aggregate state shared by both paths, so outputs are directly comparable."""

from __future__ import annotations

# Index layout of a site's aggregate list.
EVENTS, RISK_TRUE, RISK_UNKNOWN, TEMP_COUNT, TEMP_SUM, BATTERY_MIN = range(6)


def new_site() -> list:
    return [0, 0, 0, 0, 0.0, None]


def merge_partial(
    state: dict,
    site: str | None,
    events: int,
    risk_true: int,
    risk_unknown: int,
    temp_count: int,
    temp_sum: float | None,
    battery_min: int | None,
) -> None:
    """Fold one partial (per-batch, per-site) aggregate into the running state."""
    agg = state.get(site)
    if agg is None:
        agg = state[site] = new_site()
    agg[EVENTS] += events
    agg[RISK_TRUE] += risk_true
    agg[RISK_UNKNOWN] += risk_unknown
    agg[TEMP_COUNT] += temp_count
    agg[TEMP_SUM] += temp_sum or 0.0
    if battery_min is not None and (agg[BATTERY_MIN] is None or battery_min < agg[BATTERY_MIN]):
        agg[BATTERY_MIN] = battery_min


def finalize(state: dict) -> list[dict]:
    """Stable, sorted site summary. `None` (unknown site) sorts first."""
    summary = []
    for site in sorted(state, key=lambda s: (s is not None, s or "")):
        agg = state[site]
        summary.append(
            {
                "site_id": site,
                "events": agg[EVENTS],
                "risk_true": agg[RISK_TRUE],
                "risk_unknown": agg[RISK_UNKNOWN],
                "temperature_count": agg[TEMP_COUNT],
                "temperature_mean": (agg[TEMP_SUM] / agg[TEMP_COUNT]) if agg[TEMP_COUNT] else None,
                "battery_min": agg[BATTERY_MIN],
            }
        )
    return summary
