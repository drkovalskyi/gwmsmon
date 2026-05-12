"""End-to-end fixture-replay for gwmsmon.state.State.

Feeds a small but realistic batch of job dicts through State.update()
and flush_snapshot(), then asserts the snapshot shape and that the
expected JSON files are written.

This is the canary for the data pipeline. Any time someone:
- Drops a JOB_PROJECTION field that aggregation reads
- Renames a snapshot key
- Changes the schedd-type → view routing
- Breaks tool detection
…this test should fail loudly.
"""

import configparser
import json
import os

import pytest

from gwmsmon.state import State

# --- helpers ---

def _job(**overrides):
    """Build a job dict with sensible defaults; pass kwargs to override.

    Mirrors the dicts produced by query.convert_ad on a real classad.
    """
    base = {
        "JobStatus": 2,        # 1=Idle, 2=Running, 5=Held
        "JobUniverse": 5,
        "RequestCpus": 1,
        "RequestMemory": 2000,
        "Owner": "alice",
        "AccountingGroup": "production.alice",
        "AcctGroup": "production",
        "DESIRED_Sites": "T2_CH_CERN,T2_US_MIT",
        "MATCH_GLIDEIN_CMSSite": "T2_CH_CERN",
        "JobPrio": 0,
        "_schedd": "vocms0100.cern.ch",
        "_schedd_type": "prodschedd",
    }
    base.update(overrides)
    return base


def _make_cfg(tmp_path):
    cfg = configparser.ConfigParser()
    for view in ("prodview", "analysisview", "globalview",
                 "poolview", "factoryview"):
        cfg.add_section(view)
        d = tmp_path / view
        d.mkdir()
        cfg.set(view, "basedir", str(d))
    return cfg


# --- update ---

def test_empty_update_does_not_crash():
    s = State()
    s.update([], summary_ads={}, factory_data={})
    assert s.snapshot
    for view in ("prodview", "analysisview", "globalview", "poolview",
                 "factoryview"):
        assert view in s.snapshot


def test_prodview_workflow_aggregates_running():
    """A WMAgent prod job → workflow appears in prodview."""
    jobs = [
        _job(WMAgent_RequestName="ReReco-Run3-2024", JobStatus=2,
             RequestCpus=4),
        _job(WMAgent_RequestName="ReReco-Run3-2024", JobStatus=2,
             RequestCpus=4),
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    workflows = s.snapshot["prodview"]["workflows"]
    assert "ReReco-Run3-2024" in workflows


def test_analysisview_picks_up_crabschedd_only():
    """Only crabschedd jobs go to analysisview."""
    jobs = [
        _job(_schedd_type="crabschedd",
             CRAB_UserHN="bob",
             CRAB_ReqName="bob_my_task",
             JobStatus=2),
        # Same job from prodschedd should NOT land in analysisview
        _job(_schedd_type="prodschedd",
             WMAgent_RequestName="some-wf",
             CRAB_UserHN="bob", JobStatus=2),
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    workflows = s.snapshot["analysisview"]["workflows"]
    assert any(k.startswith("bob/") for k in workflows), \
        f"expected bob/* key, got {list(workflows.keys())}"


def test_globalview_aggregates_per_user():
    jobs = [
        _job(Owner="alice", JobStatus=2, RequestCpus=4),
        _job(Owner="bob", JobStatus=2, RequestCpus=2),
        _job(Owner="bob", JobStatus=1, RequestCpus=2),  # Idle
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    users = s.snapshot["globalview"]["users"]
    assert "alice" in users
    assert "bob" in users


def test_held_jobs_excluded_from_prodview_running():
    """JobStatus=5 (Held) jobs should not be aggregated into prodview
    workflow lists (only Idle=1 and Running=2 matter there)."""
    jobs = [
        _job(WMAgent_RequestName="held-only-wf", JobStatus=5),
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    # The workflow may or may not exist, but a Held job alone should
    # not produce a Running/Idle entry.
    workflows = s.snapshot["prodview"]["workflows"]
    if "held-only-wf" in workflows:
        # If created, summary should be all-zero
        for st_data in workflows["held-only-wf"].values():
            if isinstance(st_data, dict):
                summary = st_data.get("Summary", {})
                assert summary.get("Running", 0) == 0


def test_desired_sites_as_list():
    """DESIRED_Sites can come from the classad as either a comma-string
    or a Python list (after classad_to_python). Either shape must be
    accepted by the aggregator without crashing, and produce the same
    site-pressure routing."""
    jobs_str = [
        _job(WMAgent_RequestName="wf-str", JobStatus=1,
             DESIRED_Sites="T2_CH_CERN,T2_US_MIT"),
    ]
    jobs_list = [
        _job(WMAgent_RequestName="wf-list", JobStatus=1,
             DESIRED_Sites=["T2_CH_CERN", "T2_US_MIT"]),
    ]
    s = State()
    s.update(jobs_str + jobs_list, summary_ads={}, factory_data={})
    sites = s.snapshot["prodview"]["sites"]
    # Both jobs are idle on the same two sites — pressure for each
    # site should reflect both jobs.
    assert sites["T2_CH_CERN"]["MatchingIdle"] == 2
    assert sites["T2_US_MIT"]["MatchingIdle"] == 2
    # _metadata DESIRED_Sites must be a string (normalized) regardless
    # of source shape.
    wfs = s.snapshot["prodview"]["workflows"]
    for wf in ("wf-str", "wf-list"):
        meta = wfs[wf]["_metadata"]
        assert isinstance(meta["DESIRED_Sites"], str)
        assert "T2_CH_CERN" in meta["DESIRED_Sites"]
        assert "T2_US_MIT" in meta["DESIRED_Sites"]


def test_non_vanilla_universe_jobs_excluded():
    """Scheduler-universe (7) and local-universe (12) jobs run on the
    AP itself (DAGMan, local helpers) and must not be counted as grid
    workload. Only JobUniverse==5 (Vanilla) is real grid load."""
    jobs = [
        # Real grid job
        _job(WMAgent_RequestName="prod-wf-1", JobStatus=2, JobUniverse=5),
        # DAGMan helper — must be skipped from globalview/prodview/etc.
        _job(JobStatus=2, JobUniverse=7, Owner="crabtw"),
        # Local-universe — must be skipped
        _job(JobStatus=2, JobUniverse=12, Owner="crabtw"),
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})

    # crabtw should NOT appear at all (its only jobs were universe 7/12)
    users = s.snapshot["globalview"]["users"]
    assert "crabtw" not in users, \
        f"crabtw aggregated despite all its jobs being non-vanilla: {users.get('crabtw')}"
    # The real prod workflow IS aggregated
    assert "prod-wf-1" in s.snapshot["prodview"]["workflows"]


def test_poolview_schedds_universe_breakdown():
    """Per-universe (vanilla/scheduler/local/other) Running/Idle/Held
    counts are stamped onto poolview.schedds for each schedd, so the
    Schedds table can show grid load and AP-side load separately."""
    jobs = [
        # 2 vanilla running, 1 vanilla idle, 1 vanilla held on schedd A
        _job(_schedd="schedA", JobUniverse=5, JobStatus=2),
        _job(_schedd="schedA", JobUniverse=5, JobStatus=2),
        _job(_schedd="schedA", JobUniverse=5, JobStatus=1),
        _job(_schedd="schedA", JobUniverse=5, JobStatus=5),
        # Sched-universe: 1 running + 3 idle on schedA (DAGMans)
        _job(_schedd="schedA", JobUniverse=7, JobStatus=2),
        _job(_schedd="schedA", JobUniverse=7, JobStatus=1),
        _job(_schedd="schedA", JobUniverse=7, JobStatus=1),
        _job(_schedd="schedA", JobUniverse=7, JobStatus=1),
        # Local-universe: 2 running on schedA
        _job(_schedd="schedA", JobUniverse=12, JobStatus=2),
        _job(_schedd="schedA", JobUniverse=12, JobStatus=2),
        # Grid-universe (9) → bucketed as "other"
        _job(_schedd="schedA", JobUniverse=9, JobStatus=2),
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    sd = s.snapshot["poolview"]["schedds"]["schedA"]
    assert sd["vanillaRunning"] == 2
    assert sd["vanillaIdle"] == 1
    assert sd["vanillaHeld"] == 1
    assert sd["schedulerRunning"] == 1
    assert sd["schedulerIdle"] == 3
    assert sd["schedulerHeld"] == 0
    assert sd["localRunning"] == 2
    assert sd["localIdle"] == 0
    assert sd["otherRunning"] == 1
    assert sd["otherIdle"] == 0


def test_tool_detection_kraken():
    """Kraken jobs are identified by KRAKEN_EXE in Environment."""
    job = _job(
        Owner="paus",
        Environment="KRAKEN_EXE=foo;OTHER=x",
        Iwd="/home/submit/paus/cms/data/nanoao/536/MyDataset",
    )
    s = State()
    s.update([job], summary_ads={}, factory_data={})
    user_summary = s.snapshot["globalview"].get("user_summary", {})
    if "paus" in user_summary:
        tool = user_summary["paus"].get("Tool", "")
        assert "Kraken" in tool, f"expected Kraken, got {tool!r}"


def test_tool_detection_crab():
    """CRAB jobs identified via CRAB_ReqName."""
    job = _job(
        _schedd_type="crabschedd",
        Owner="charlie",
        CRAB_UserHN="charlie",
        CRAB_ReqName="charlie:my_task",
    )
    s = State()
    s.update([job], summary_ads={}, factory_data={})
    user_summary = s.snapshot["globalview"].get("user_summary", {})
    if "charlie" in user_summary:
        tool = user_summary["charlie"].get("Tool", "")
        assert "CRAB" in tool


# --- flush_snapshot ---

@pytest.fixture
def populated_state():
    """A State with a small mixed batch already aggregated."""
    jobs = [
        _job(WMAgent_RequestName="prod-wf-1", JobStatus=2,
             RequestCpus=4),
        _job(_schedd_type="crabschedd",
             CRAB_UserHN="bob",
             CRAB_ReqName="bob_task", JobStatus=2),
        _job(Owner="charlie", JobStatus=1),
    ]
    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    return s


def test_flush_writes_summary_per_view(populated_state, tmp_path):
    cfg = _make_cfg(tmp_path)
    populated_state.flush_snapshot(cfg)

    for view in ("prodview", "analysisview", "globalview"):
        path = tmp_path / view / "summary.json"
        assert path.exists(), f"missing {path}"
        data = json.loads(path.read_text())
        assert "updated" in data
        assert "totals" in data


def test_flush_writes_globalview_fairshare(populated_state, tmp_path):
    cfg = _make_cfg(tmp_path)
    populated_state.flush_snapshot(cfg)
    fs = json.loads((tmp_path / "globalview" / "fairshare.json").read_text())
    assert "categories" in fs


def test_flush_writes_atomic_no_tmp_leftovers(populated_state, tmp_path):
    cfg = _make_cfg(tmp_path)
    populated_state.flush_snapshot(cfg)
    leftovers = []
    for _root, _dirs, files in os.walk(str(tmp_path)):
        leftovers.extend(f for f in files if f.endswith(".tmp"))
    assert leftovers == []


def test_flush_writes_globalview_owner_rollup(tmp_path):
    """Regression: /globalview/request/<owner> reads
    basedir/<owner>/exit_codes.json. Exit codes are tracked per
    <owner>/<task> key so we must roll them up per owner before
    flushing."""
    import time as _time
    cfg = _make_cfg(tmp_path)
    s = State()
    # Use the current minute bucket so the 1h window includes the data
    minute = int(_time.time()) // 60 * 60
    s.updated = minute
    s.exit_codes["globalview"] = {
        "alice/taskA": {minute: {"0": 5, "1": 1}},
        "alice/taskB": {minute: {"0": 3, "11": 2}},
        "bob/taskC":   {minute: {"0": 7}},
    }
    s.flush_exit_codes(cfg)
    alice_path = tmp_path / "globalview" / "alice" / "exit_codes.json"
    bob_path = tmp_path / "globalview" / "bob" / "exit_codes.json"
    assert alice_path.exists(), "alice owner roll-up missing"
    assert bob_path.exists(), "bob owner roll-up missing"
    alice = json.loads(alice_path.read_text())
    # 1h window should sum: total=5+1+3+2=11, failures=1+2=3
    assert alice["windows"]["1h"]["total"] == 11
    assert alice["windows"]["1h"]["failures"] == 3
    assert alice["windows"]["1h"]["codes"] == {"0": 8, "1": 1, "11": 2}
    bob = json.loads(bob_path.read_text())
    assert bob["windows"]["1h"]["total"] == 7
    assert bob["windows"]["1h"]["failures"] == 0
    assert (tmp_path / "globalview" / "alice"
            / "completion_histogram.json").exists()


def test_flush_skips_missing_basedir(populated_state, tmp_path):
    """If a view's basedir doesn't exist, flush should skip silently
    (not crash). Mirrors the running collector if a basedir is misconfigured."""
    cfg = _make_cfg(tmp_path)
    # Remove globalview basedir
    import shutil
    shutil.rmtree(str(tmp_path / "globalview"))
    # Should not raise
    populated_state.flush_snapshot(cfg)
    # Other views still wrote
    assert (tmp_path / "prodview" / "summary.json").exists()
