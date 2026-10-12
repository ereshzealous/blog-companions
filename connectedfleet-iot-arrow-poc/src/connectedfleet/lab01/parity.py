"""Compare row-path and Arrow-path outputs for the same fixture and batch size."""

from __future__ import annotations

import math

INT_SUMMARY_FIELDS = ("events", "risk_true", "risk_unknown", "temperature_count", "battery_min")


def _check(name: str, ok: bool, detail: str = "") -> dict:
    return {"check": name, "ok": bool(ok), "detail": detail}


def compare(row: dict, arrow: dict, expected_rejects: dict[str, int]) -> list[dict]:
    checks = [
        _check("fixture_sha256", row["fixture_sha256"] == arrow["fixture_sha256"]),
        _check("valid_rows", row["valid_rows"] == arrow["valid_rows"], f"{row['valid_rows']} vs {arrow['valid_rows']}"),
        _check("rejects_equal", row["rejects"] == arrow["rejects"]),
        _check(
            "rejects_match_injected",
            row["rejects"] == expected_rejects,
            f"observed {row['rejects']} expected {expected_rejects}",
        ),
        _check("field_stats", row["field_stats"] == arrow["field_stats"]),
        _check("risk_counts", row["risk_counts"] == arrow["risk_counts"], f"{row['risk_counts']}"),
        _check("risk_vector_digest", row["risk_digest"] == arrow["risk_digest"]),
        _check(
            "alerts",
            row["alert_count"] == arrow["alert_count"] and row["alert_digest"] == arrow["alert_digest"],
            f"{row['alert_count']} vs {arrow['alert_count']}",
        ),
    ]

    row_sites = {s["site_id"]: s for s in row["site_summary"]}
    arrow_sites = {s["site_id"]: s for s in arrow["site_summary"]}
    same_keys = row_sites.keys() == arrow_sites.keys()
    mismatches = []
    if same_keys:
        for site, r in row_sites.items():
            a = arrow_sites[site]
            for name in INT_SUMMARY_FIELDS:
                if r[name] != a[name]:
                    mismatches.append(f"{site}.{name}")
            rm, am = r["temperature_mean"], a["temperature_mean"]
            if (rm is None) != (am is None) or (
                rm is not None and not math.isclose(rm, am, rel_tol=1e-9, abs_tol=1e-9)
            ):
                mismatches.append(f"{site}.temperature_mean")
    checks.append(
        _check(
            "site_summary",
            same_keys and not mismatches,
            "float means compared with rel_tol=1e-9 (summation order differs)" if not mismatches else ", ".join(mismatches),
        )
    )
    return checks
