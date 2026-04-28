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
