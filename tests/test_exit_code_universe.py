"""update_exit_codes must count only real-site, vanilla-universe payload.

Two gates protect site/efficiency aggregation:
- JobUniverse != 5 -> skip. Scheduler-universe (7) DAGMan bootstraps
  and local-universe (12) CRAB service jobs run on the schedd, not a
  site; their data belongs only to the schedd view.
- no resolved MATCH_GLIDEIN_CMSSite -> skip. Jobs with site
  None/""/"Unknown"/"unknown" are not attributable to a site and must
  never enter the "Unknown" bucket that once crushed the wall-weighted
  CPU-efficiency headline.
"""

import time

import pytest

from gwmsmon.state import State


def _hist_job(universe, **overrides):
    """A completed crabschedd history job dict."""
    base = {
        "JobUniverse": universe,
        "ExitCode": 0,
        "CompletionDate": int(time.time()) - 60,
        "_schedd_type": "crabschedd",
        "_schedd": "vocms0100.cern.ch",
        "MATCH_GLIDEIN_CMSSite": "T2_US_MIT",
        "CRAB_UserHN": "alice",
        "CRAB_ReqName": "alice_crab_task1",
        "RemoteUserCpu": 3600,
        "RemoteSysCpu": 0,
        "RemoteWallClockTime": 3600,
        "RequestCpus": 1,
    }
    base.update(overrides)
    return base


def test_vanilla_job_counted():
    s = State()
    s.update_exit_codes([_hist_job(5)])
    assert "alice/alice_crab_task1" in s.exit_codes["analysisview"]
    assert "alice/alice_crab_task1" in s.efficiency["analysisview"]


def test_scheduler_and_local_jobs_skipped():
    s = State()
    # DAGMan bootstrap (7) and CRAB local service (12): no site, no CPU,
    # large wall — exactly the jobs that polluted the Unknown bucket.
    s.update_exit_codes([
        _hist_job(7, MATCH_GLIDEIN_CMSSite=None,
                  RemoteUserCpu=0, RemoteWallClockTime=50000),
        _hist_job(12, MATCH_GLIDEIN_CMSSite=None,
                  RemoteUserCpu=0, RemoteWallClockTime=50000),
    ])
    assert s.exit_codes.get("analysisview", {}) == {}
    assert s.efficiency.get("analysisview", {}) == {}
    assert s.efficiency_by_site.get("analysisview", {}) == {}


def test_mixed_batch_keeps_only_vanilla():
    s = State()
    s.update_exit_codes([
        _hist_job(5),                       # payload, counted
        _hist_job(7, MATCH_GLIDEIN_CMSSite=None, RemoteUserCpu=0,
                  RemoteWallClockTime=50000),  # service, skipped
        _hist_job(12, MATCH_GLIDEIN_CMSSite=None, RemoteUserCpu=0,
                  RemoteWallClockTime=50000),  # service, skipped
    ])
    eff = s.efficiency_by_site["analysisview"]["alice/alice_crab_task1"]
    # Only the vanilla job's site is present; no "Unknown" bucket.
    assert set(eff.keys()) == {"T2_US_MIT"}
    # Efficiency reflects only the payload: 3600 cpu / 3600 wall_cpus.
    bucket = next(iter(eff["T2_US_MIT"].values()))
    assert bucket["cpu"] == 3600
    assert bucket["wall_cpus"] == 3600


@pytest.mark.parametrize("nosite", [None, "", "Unknown", "unknown"])
def test_vanilla_job_without_site_skipped(nosite):
    s = State()
    s.update_exit_codes([_hist_job(5, MATCH_GLIDEIN_CMSSite=nosite)])
    assert s.exit_codes.get("analysisview", {}) == {}
    assert s.efficiency_by_site.get("analysisview", {}) == {}


def test_no_unknown_bucket_ever_created():
    s = State()
    s.update_exit_codes([
        _hist_job(5, MATCH_GLIDEIN_CMSSite="T2_US_MIT"),
        _hist_job(5, MATCH_GLIDEIN_CMSSite="Unknown",
                  RemoteUserCpu=0, RemoteWallClockTime=50000),
        _hist_job(5, MATCH_GLIDEIN_CMSSite="unknown",
                  RemoteUserCpu=0, RemoteWallClockTime=50000),
    ])
    all_sites = set()
    for wfs in s.efficiency_by_site.get("analysisview", {}).values():
        all_sites.update(wfs.keys())
    assert all_sites == {"T2_US_MIT"}
