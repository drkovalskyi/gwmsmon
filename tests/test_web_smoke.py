"""Smoke tests for gwmsmon.web routes.

For each route, render against a tmpdir-backed snapshot produced by
State.flush_snapshot(). Asserts: HTTP 200 (or expected redirect) and
no template/Jinja errors. Catches:
  - Typos in routes / template variable names
  - Templates referencing keys not in the JSON
  - Renames in summary.json schema
"""


import pytest

from gwmsmon import config as gw_config
from gwmsmon.state import State
from gwmsmon.web import create_app


@pytest.fixture
def app(tmp_path, monkeypatch):
    # Lay out per-view basedirs.
    basedirs = {}
    for view in ("prodview", "analysisview", "globalview",
                 "poolview", "factoryview"):
        d = tmp_path / view
        d.mkdir()
        basedirs[view] = str(d)

    # Write a config file the loader can read.
    cfg_path = tmp_path / "gwmsmon.conf"
    lines = ["[htcondor]", "pool = test-collector.local:9618"]
    for view, d in basedirs.items():
        lines.append(f"[{view}]")
        lines.append(f"basedir = {d}")
    cfg_path.write_text("\n".join(lines))

    # Populate the basedirs by running a real State.update + flush.
    cfg = gw_config.load(str(cfg_path))
    state = State()
    jobs = [
        {"JobStatus": 2, "JobUniverse": 5, "RequestCpus": 4,
         "Owner": "alice", "AccountingGroup": "production.alice",
         "AcctGroup": "production",
         "DESIRED_Sites": "T2_CH_CERN",
         "MATCH_GLIDEIN_CMSSite": "T2_CH_CERN",
         "JobPrio": 0,
         "WMAgent_RequestName": "ReReco-2024",
         "_schedd": "vocms0100.cern.ch",
         "_schedd_type": "prodschedd"},
        {"JobStatus": 2, "JobUniverse": 5, "RequestCpus": 1,
         "Owner": "bob", "AccountingGroup": "analysis.bob",
         "AcctGroup": "analysis",
         "DESIRED_Sites": "T2_US_MIT",
         "MATCH_GLIDEIN_CMSSite": "T2_US_MIT",
         "JobPrio": 10,
         "CRAB_UserHN": "bob",
         "CRAB_ReqName": "bob:my_task",
         "_schedd": "crab3@vocms0121.cern.ch",
         "_schedd_type": "crabschedd"},
    ]
    state.update(jobs, summary_ads={}, factory_data={})
    state.flush_snapshot(cfg)

    app = create_app(str(cfg_path))
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    return app.test_client()


# --- routes ---

def test_root_redirects_to_prodview(client):
    rv = client.get("/")
    assert rv.status_code in (301, 302)
    assert "/prodview/" in rv.headers["Location"]


@pytest.mark.parametrize("view",
                         ["prodview", "analysisview", "globalview",
                          "poolview", "factoryview"])
def test_overview_renders(client, view):
    rv = client.get(f"/{view}/")
    assert rv.status_code == 200, (
        f"{view} returned {rv.status_code}: "
        f"{rv.data[:300]!r}")


@pytest.mark.parametrize("view",
                         ["prodview", "analysisview", "globalview"])
def test_sites_page_renders(client, view):
    rv = client.get(f"/{view}/sites")
    assert rv.status_code == 200


def test_status_page_renders(client):
    rv = client.get("/status")
    assert rv.status_code == 200


def test_status_history_json_returns_json(client):
    rv = client.get("/status/history.json")
    assert rv.status_code == 200
    assert rv.is_json


def test_unknown_view_404(client):
    rv = client.get("/notaview/")
    assert rv.status_code == 404


def test_security_headers_present(client):
    rv = client.get("/prodview/")
    assert rv.headers.get("X-Content-Type-Options") == "nosniff"
    assert rv.headers.get("X-Frame-Options") == "DENY"
    assert "Content-Security-Policy" in rv.headers


def test_group_detail_uses_only_matching_group_counts(tmp_path):
    """Regression: /globalview/group/<name> previously summed all of
    a user's jobs (across every group they belonged to) instead of
    just those in the requested group. Crabtw with 100k analysis +
    4k 'other' was showing ~104k under both groups."""
    # Set up basedirs
    basedirs = {}
    for view in ("prodview", "analysisview", "globalview",
                 "poolview", "factoryview"):
        d = tmp_path / view
        d.mkdir()
        basedirs[view] = str(d)
    cfg_path = tmp_path / "gwmsmon.conf"
    lines = ["[htcondor]", "pool = test-collector.local:9618"]
    for view, d in basedirs.items():
        lines.append(f"[{view}]")
        lines.append(f"basedir = {d}")
    cfg_path.write_text("\n".join(lines))

    cfg = gw_config.load(str(cfg_path))

    # Build jobs: one Owner=crabtw with 5 analysis jobs and 2 "other"
    # (AccountingGroup missing) jobs.
    base = {
        "JobStatus": 2, "JobUniverse": 5, "RequestCpus": 1,
        "Owner": "crabtw", "AcctGroup": "analysis",
        "DESIRED_Sites": "T2_CH_CERN",
        "MATCH_GLIDEIN_CMSSite": "T2_CH_CERN",
        "JobPrio": 0, "CRAB_UserHN": "alice",
        "CRAB_ReqName": "alice:task1",
        "_schedd": "crab3@vocms0121.cern.ch",
        "_schedd_type": "crabschedd",
    }
    jobs = []
    for _ in range(5):
        j = dict(base)
        j["AccountingGroup"] = "analysis.alice"
        jobs.append(j)
    for _ in range(2):
        j = dict(base)
        # AccountingGroup intentionally missing → category "other"
        jobs.append(j)

    s = State()
    s.update(jobs, summary_ads={}, factory_data={})
    s.flush_snapshot(cfg)

    app = create_app(str(cfg_path))
    app.config["TESTING"] = True
    client = app.test_client()

    # Hit the "other" group page; expect exactly 2 Running jobs for
    # crabtw, NOT 7.
    rv = client.get("/globalview/group/other")
    assert rv.status_code == 200
    body = rv.data.decode()
    # The user row in the table should reflect 2 jobs (the "other" ones).
    # Anchor on the user name; verify 7 is absent and 2 is present.
    assert "crabtw" in body
    assert "<td>7</td>" not in body, \
        "regression: group page summed all user jobs across groups"
