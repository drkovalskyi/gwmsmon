"""Sparse time-series edge behavior: zero crossings and retention.

Charts interpolate their left edge from the last sample before the
plotting window, so:
- maintenance() must retain a margin past the longest chart window
  (30d) instead of pruning exactly at it;
- _ts_append/_ts_close_silent must record explicit 0 points when a
  sparse key starts or stops, so the renderer never extrapolates a
  stale non-zero value across a silent gap.
"""

import time

from gwmsmon.state import (
    HOURLY_RES_SECONDS,
    RETENTION_MARGIN_SECONDS,
    State,
)


def _series(state, view, entity, key):
    return state.timeseries[view][entity][key]


# --- closing zeros (entity vanishes from snapshot) ---

def test_closing_zero_when_entity_vanishes():
    s = State()
    s._ts_append("prodview", "request:wf1", {"Running": 5}, 1000)
    s._ts_close_silent(1000)

    # cycle 2: wf1 gone, but the view still got appends
    s._ts_views_updated.clear()
    s._ts_append("prodview", "request:wf2", {"Running": 3}, 1060)
    s._ts_close_silent(1060)

    pts = _series(s, "prodview", "request:wf1", "Running")
    assert pts["t"] == [1000, 1060]
    assert pts["v"] == [5, 0]

    # cycle 3: still gone — exactly one closing zero, then silence
    s._ts_views_updated.clear()
    s._ts_append("prodview", "request:wf2", {"Running": 3}, 1120)
    s._ts_close_silent(1120)
    assert pts["v"] == [5, 0]


def test_closing_zero_per_key_within_active_entity():
    s = State()
    s._ts_append("prodview", "request:wf1",
                 {"Running": 5, "MatchingIdle": 9}, 1000)
    s._ts_close_silent(1000)

    # MatchingIdle drops to 0 while Running stays active
    s._ts_views_updated.clear()
    s._ts_append("prodview", "request:wf1",
                 {"Running": 6, "MatchingIdle": 0}, 1060)
    s._ts_close_silent(1060)

    idle = _series(s, "prodview", "request:wf1", "MatchingIdle")
    assert idle["t"] == [1000, 1060]
    assert idle["v"] == [9, 0]
    running = _series(s, "prodview", "request:wf1", "Running")
    assert running["v"] == [5, 6]


def test_no_closing_zero_when_view_skips_cycle():
    s = State()
    s._ts_append("prodview", "request:wf1", {"Running": 5}, 1000)
    s._ts_close_silent(1000)

    # cycle 2: prodview got no appends at all (collection hiccup) —
    # don't fabricate zeros for the whole view
    s._ts_views_updated.clear()
    s._ts_append("poolview", "_summary", {"TotalRunning": 9}, 1060)
    s._ts_close_silent(1060)

    pts = _series(s, "prodview", "request:wf1", "Running")
    assert pts["t"] == [1000]
    assert pts["v"] == [5]


# --- opening zeros (key becomes active after a silent gap) ---

def test_opening_zero_after_silent_gap():
    s = State()
    s._ts_append("prodview", "request:wf1", {"Running": 5}, 1000)
    s._ts_close_silent(1000)

    # two silent cycles (closing zero lands at 1060)
    for now in (1060, 1120):
        s._ts_views_updated.clear()
        s._ts_append("prodview", "request:wf2", {"Running": 1}, now)
        s._ts_close_silent(now)

    # wf1 comes back: opening 0 at the previous cycle, then the value
    s._ts_views_updated.clear()
    s._ts_append("prodview", "request:wf1", {"Running": 7}, 1180)
    s._ts_close_silent(1180)

    pts = _series(s, "prodview", "request:wf1", "Running")
    assert pts["t"] == [1000, 1060, 1120, 1180]
    assert pts["v"] == [5, 0, 0, 7]


def test_no_opening_zero_on_first_sample():
    s = State()
    s._ts_prev_now = 940
    s._ts_append("prodview", "request:wf1", {"Running": 5}, 1000)
    pts = _series(s, "prodview", "request:wf1", "Running")
    assert pts["t"] == [1000]
    assert pts["v"] == [5]


def test_consecutive_samples_get_no_extra_zeros():
    s = State()
    for now in (1000, 1060, 1120):
        s._ts_views_updated.clear()
        s._ts_append("prodview", "request:wf1", {"Running": 5}, now)
        s._ts_close_silent(now)
    pts = _series(s, "prodview", "request:wf1", "Running")
    assert pts["t"] == [1000, 1060, 1120]
    assert pts["v"] == [5, 5, 5]


# --- retention margin ---

def test_maintenance_keeps_margin_past_chart_window():
    s = State()
    now = int(time.time())
    in_margin = now - HOURLY_RES_SECONDS - 86400      # 31d: kept
    too_old = (now - HOURLY_RES_SECONDS
               - RETENTION_MARGIN_SECONDS - 86400)    # 33d: dropped
    s.timeseries.setdefault("prodview", {})["request:wf1"] = {
        "Running": {"t": [too_old, in_margin, now], "v": [1, 2, 3]},
    }
    s.maintenance()
    pts = _series(s, "prodview", "request:wf1", "Running")
    assert 1 not in pts["v"]
    # the 31d point survives, downsampled into its hourly bucket
    assert any(abs(t - in_margin) <= 3600 for t in pts["t"])
    assert 2 in pts["v"]
    assert 3 in pts["v"]
