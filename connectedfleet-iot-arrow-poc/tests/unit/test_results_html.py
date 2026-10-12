"""Results pages: helpers that decide what is shown and how numbers are rendered."""

from connectedfleet.bench import results_html as rh


def test_usable_accepts_cases_of_an_interrupted_run_but_not_rejected_cases():
    assert rh.usable({"valid": True})
    assert rh.usable({"valid": False, "logical_rows": 1000, "invalid_reasons": ""})       # run-level STATUS only
    assert not rh.usable({"valid": False, "logical_rows": 1000, "invalid_reasons": "HOST: busy"})
    assert not rh.usable({"valid": False, "logical_rows": None, "invalid_reasons": ""})


def test_stat_uses_measured_repetitions_only():
    rows = [{"warmup": True, "valid": True, "lane": "row", "x": 1.0},
            *[{"warmup": False, "valid": True, "lane": "row", "x": v} for v in (2.0, 4.0, 3.0)],
            {"warmup": False, "valid": True, "lane": "arrow", "x": 9.0}]
    assert rh.stat(rows, "x", lane="row") == {"med": 3.0, "min": 2.0, "max": 4.0, "n": 3}


def test_labels():
    assert rh.events_label(1_000_000) == "1M events"
    assert rh.events_label(100_000) == "100K events"
    assert rh.k(183_900) == "183.9K" and rh.k(2_163_000) == "2.16M"
    assert rh.ms(24_513) == "24.5 s" and rh.ms(266) == "266 ms" and rh.ms(None) == "n/m"


def test_bars_never_exceed_the_scale():
    out = rh.bars("t", "s", [("a", 50.0, "teal"), ("b", 200.0, "neutral")], rh.k, maximum=100.0)
    assert 'style="width:50.000%"' in out and 'style="width:100.000%"' in out
