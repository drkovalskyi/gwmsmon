"""View-level efficiency is the per-site weighted aggregate, excluding
the no-site bucket, and per-site output carries raw weights so the
frontend can recompute it over a filtered subset.
"""

import configparser
import json
import time

from gwmsmon.state import EXIT_CODE_BUCKET, State


def _make_cfg(tmp_path):
    cfg = configparser.ConfigParser()
    for view in ("prodview", "analysisview", "globalview",
                 "poolview", "factoryview"):
        cfg.add_section(view)
        d = tmp_path / view
        d.mkdir()
        cfg.set(view, "basedir", str(d))
    return cfg


def _seed(state, minute):
    wf = "alice/alice_crab_task1"
    # Real site: 800/1000 = 0.80 running_eff, 8 completions.
    # Unknown bucket: 1/9000 ~ 0 eff, huge wall — must be excluded.
    state.exit_codes["analysisview"] = {wf: {minute: {"0": 10}}}
    state.exit_codes_by_site["analysisview"] = {wf: {
        "T2_US_MIT": {minute: {"0": 8}},
        "Unknown": {minute: {"0": 2}},
    }}
    state.efficiency_by_site["analysisview"] = {wf: {
        "T2_US_MIT": {minute: {"cpu": 800, "wall_cpus": 1000,
                               "slot_ok": 900, "slot_all": 1000}},
        "Unknown": {minute: {"cpu": 1, "wall_cpus": 9000,
                             "slot_ok": 0, "slot_all": 9000}},
    }}
    # Legacy per-workflow aggregate still mixes both (no longer read by
    # the headline): 801/10000 = 0.08 if it were used.
    state.efficiency["analysisview"] = {wf: {
        minute: {"cpu": 801, "wall_cpus": 10000,
                 "slot_ok": 900, "slot_all": 10000}}}


def test_headline_excludes_unknown_and_emits_weights(tmp_path):
    s = State()
    s.updated = int(time.time())
    minute = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET - EXIT_CODE_BUCKET
    _seed(s, minute)
    cfg = _make_cfg(tmp_path)

    s._flush_one_view(cfg, "analysisview")

    ec = json.loads((tmp_path / "analysisview" / "exit_codes.json").read_text())
    # Headline = real site only: 800/1000 = 0.80, not 801/10000 = 0.08.
    assert ec["efficiency"]["7d"]["running_eff"] == 0.8
    assert ec["efficiency"]["7d"]["processing_eff"] == 0.9

    sec = json.loads(
        (tmp_path / "analysisview" / "site_exit_codes.json").read_text())
    mit = sec["sites"]["T2_US_MIT"]["efficiency"]["7d"]
    # Raw weights present so the client can re-weight a filtered subset.
    assert mit["cpu"] == 800
    assert mit["wall_cpus"] == 1000
    assert mit["slot_ok"] == 900
    assert mit["slot_all"] == 1000
    assert mit["running_eff"] == 0.8
    # 1h carries the same weights so the "Current (1h)" headline is
    # filter-reactive too (the seeded bucket falls in the 1h window).
    mit_1h = sec["sites"]["T2_US_MIT"]["efficiency"]["1h"]
    assert mit_1h["cpu"] == 800
    assert mit_1h["wall_cpus"] == 1000


def test_completion_xref_keyed_by_user_with_7d_weights(tmp_path):
    """analysisview completion_cross_reference is keyed by user (not
    user/task) and each (user, site) entry carries 1h + 7d weights, so
    the JS can re-weight the headline when filtering by user. The
    no-site bucket never appears."""
    s = State()
    s.updated = int(time.time())
    minute = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET - EXIT_CODE_BUCKET
    _seed(s, minute)
    cfg = _make_cfg(tmp_path)

    s._flush_one_view(cfg, "analysisview")

    xref = json.loads(
        (tmp_path / "analysisview" / "completion_cross_reference.json")
        .read_text())
    # Keyed by user "alice", not "alice/alice_crab_task1".
    assert "alice" in xref
    assert "alice/alice_crab_task1" not in xref
    # No-site bucket excluded.
    assert "Unknown" not in xref["alice"]
    vals = xref["alice"]["T2_US_MIT"]
    assert len(vals) == 12
    # 1h block: done, fail, cpu, wall, slot_ok, slot_all
    assert vals[:6] == [8, 0, 800, 1000, 900, 1000]
    # 7d block mirrors it (single recent bucket is in both windows)
    assert vals[6:] == [8, 0, 800, 1000, 900, 1000]
