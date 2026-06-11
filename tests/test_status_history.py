"""Tests for gwmsmon.status_history.StatusHistory.

Covers binning, flushing, pruning, and disk persistence.
"""

import json
import os

import pytest

from gwmsmon import status_history
from gwmsmon.status_history import METRICS, TIERS, StatusHistory


@pytest.fixture
def freeze_time(monkeypatch):
    """Yield a callable that sets the wall-clock time used by
    status_history."""
    state = {"now": 1_700_000_000.0}

    def fake_time():
        return state["now"]

    monkeypatch.setattr(status_history.time, "time", fake_time)

    def setter(t):
        state["now"] = t

    return setter


def test_record_within_same_bin_keeps_one_point(freeze_time, tmp_path):
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    # Three records in same hour bin
    h.record(cycle_time=100, rss_mb=1000, state_size_mb=10)
    freeze_time(1_700_000_010.0)
    h.record(cycle_time=200, rss_mb=2000, state_size_mb=20)
    freeze_time(1_700_000_020.0)
    h.record(cycle_time=300, rss_mb=3000, state_size_mb=30)

    h.flush(str(tmp_path))

    data = json.loads((tmp_path / "status_history.json").read_text())
    pts = data["cycle_time"]["24h"]
    assert len(pts["t"]) == 1
    # average of 100, 200, 300
    assert pts["v"] == [200.0]


def test_record_crosses_bin_boundary(freeze_time, tmp_path):
    h = StatusHistory()
    base = 1_700_000_000.0  # aligned to a 24h-tier (1h) bin start
    # 1-hour bins → cross boundary at base+3600
    freeze_time(base)
    h.record(100, 1000, 10)
    freeze_time(base + 1800)
    h.record(200, 2000, 20)
    freeze_time(base + 3600)  # new bin
    h.record(400, 4000, 40)

    h.flush(str(tmp_path))
    data = json.loads((tmp_path / "status_history.json").read_text())
    pts = data["cycle_time"]["24h"]
    assert len(pts["t"]) == 2
    assert pts["v"][0] == 150.0  # avg(100, 200)
    assert pts["v"][1] == 400.0  # only entry in 2nd bin


def test_prune_drops_old_points(freeze_time, tmp_path):
    h = StatusHistory()
    base = 1_700_000_000.0

    # Retention is the 24h chart window + 2 bins (26h): the 25h-old
    # point survives as the left-edge interpolation anchor, the
    # 27h-old point is dropped.
    h.series["cycle_time"]["24h"]["t"] = [base - 27 * 3600,
                                          base - 25 * 3600,
                                          base - 1 * 3600]
    h.series["cycle_time"]["24h"]["v"] = [40.0, 50.0, 99.0]

    freeze_time(base)
    h.prune()
    pts = h.series["cycle_time"]["24h"]
    assert pts["t"] == [base - 25 * 3600, base - 1 * 3600]
    assert pts["v"] == [50.0, 99.0]


def test_restore_round_trip(freeze_time, tmp_path):
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    h.record(123, 4567, 8)
    h.flush(str(tmp_path))

    h2 = StatusHistory()
    h2.restore(str(tmp_path))
    assert h2.series["cycle_time"]["24h"]["v"] == [123.0]
    assert h2.series["rss_mb"]["24h"]["v"] == [4567.0]
    assert h2.series["state_size_mb"]["24h"]["v"] == [8.0]


def test_restore_missing_file_is_noop(tmp_path):
    h = StatusHistory()
    h.restore(str(tmp_path))  # no status_history.json present
    for metric in METRICS:
        for name, _, _ in TIERS:
            assert h.series[metric][name]["t"] == []


def test_restore_corrupt_file_logs_and_continues(tmp_path):
    (tmp_path / "status_history.json").write_text("not json {{")
    h = StatusHistory()
    h.restore(str(tmp_path))  # must not raise
    for metric in METRICS:
        assert h.series[metric]["24h"]["t"] == []


def test_partial_bin_visible_in_flush(freeze_time, tmp_path):
    """A partial (not yet rolled) bin must still be visible in the
    flushed JSON, so the dashboard sees current data."""
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    h.record(50, 500, 5)
    h.flush(str(tmp_path))
    data = json.loads((tmp_path / "status_history.json").read_text())
    assert data["cycle_time"]["24h"]["v"] == [50.0]


def test_atomic_write_no_partial_files(freeze_time, tmp_path):
    """After flush, no .tmp files should remain in the directory."""
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    h.record(50, 500, 5)
    h.flush(str(tmp_path))
    leftover = [n for n in os.listdir(str(tmp_path)) if n.endswith(".tmp")]
    assert leftover == []


def test_leak_report_warmup(freeze_time):
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    for c in range(3):
        h.record(50, 4000, 5, cycle=c)
    rep = h.leak_report()
    assert rep["verdict"] == "warmup"
    assert rep["n"] == 3


def test_leak_report_stable(freeze_time):
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    # RSS hovering around 4500 MB — no slope
    for c in range(40):
        h.record(50, 4500 + (c % 5), 5, cycle=c)
    rep = h.leak_report()
    assert rep["verdict"] == "stable"
    assert abs(rep["slope_mb_per_cycle"]) < 1


def test_leak_report_leak(freeze_time):
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    # RSS climbs 100 MB per cycle — clear leak
    for c in range(20):
        h.record(50, 4000 + 100 * c, 5, cycle=c)
    rep = h.leak_report()
    assert rep["verdict"] == "leak"
    assert 95 < rep["slope_mb_per_cycle"] < 105


def test_leak_report_drift(freeze_time):
    h = StatusHistory()
    freeze_time(1_700_000_000.0)
    # 20 MB/cycle — between drift and leak
    for c in range(20):
        h.record(50, 4000 + 20 * c, 5, cycle=c)
    rep = h.leak_report()
    assert rep["verdict"] == "drift"
    assert 18 < rep["slope_mb_per_cycle"] < 22
