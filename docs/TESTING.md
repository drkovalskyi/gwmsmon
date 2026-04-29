# gwmsmon — Testing Guide

## Quick start

```bash
pip install -e ".[test]"   # one-time
pytest                     # ~2s, 59 tests
ruff check tests           # lint, ~1s
```

Tests live in `tests/`. They run on any Python ≥3.9 without HTCondor
installed — `tests/conftest.py` installs an in-process stub for the
`classad` module so imports work.

## Layers

| Layer | File | What it covers |
|---|---|---|
| Unit | `tests/test_convert.py` | `classad_to_python`, `convert_ad`: scalar fast path, ExprTree eval, list/dict recursion, projection filtering. |
| Unit | `tests/test_exitcodes.py` | `describe()` + signal table. |
| Unit | `tests/test_status_history.py` | Multi-tier binning, flush, prune, restore round-trip. |
| Integration | `tests/test_state_aggregate.py` | Feeds synthetic job dicts through `State.update` + `State.flush_snapshot`, asserts shape of every view's snapshot, tool detection (Kraken, CRAB), atomic JSON write, missing-basedir tolerance. |
| Smoke | `tests/test_web_smoke.py` | Flask test client GETs every overview/sites/status route against a tmpdir snapshot; checks 200 + security headers. |
| Static | `tests/test_projection.py` | AST scan: every field in `JOB_PROJECTION` (in `query.py`) must be read in `state.py`. Catches the failure mode "we fetch a field nobody uses" or "we use a field nobody fetches". |

## Pre-deploy validation

`./deploy.sh --restart` runs in-process safety checks beyond the unit
suite:

1. **Canary cycle**: between stopping and starting services, one real
   `gwmsmon-collect --config /etc/gwmsmon.conf --once` is invoked
   against the live pool with the new code. If the cycle exits
   non-zero, the deploy aborts with services left stopped — easier to
   roll back than to fix a crashloop.
2. **Multi-route health check**: after services are up, every overview
   route + `/status` is requested. Any non-200 fails the deploy.

## CI

`.github/workflows/ci.yml` runs `ruff check tests` and `pytest` on
Python 3.9 (matching RHEL9 prod) on every push and PR to `main`.

## Adding a test

- Tests must run without HTCondor. If you need a `classad.ExprTree`
  or `classad.Value`, the stub in `tests/conftest.py` mimics enough
  surface for the conversion code path. Extend it rather than
  importing the real binding.
- Use `tmp_path` (pytest fixture) for any disk I/O — never write
  outside of a tmpdir.
- For tests that touch wall-clock-sensitive code (`status_history`),
  use the `freeze_time` fixture in `tests/test_status_history.py` as
  a template.

## When to add what

| You changed… | Add to… |
|---|---|
| `JOB_PROJECTION` (added/removed a field) | `test_projection.py` will fail automatically if state.py doesn't read the new field. If trimming, also update `test_state_aggregate.py` if any test relied on the field. |
| `convert_ad` / `classad_to_python` | `test_convert.py` — every type branch must have a case. |
| `state.update` aggregation logic | `test_state_aggregate.py` — add a synthetic job covering the new code path. |
| `State.flush_snapshot` / new JSON file | `test_state_aggregate.py` — assert the new file is written. Also add a `_load_json` site in `test_web_smoke.py` if a route reads it. |
| New Flask route | `test_web_smoke.py` — at minimum a 200-status assertion against a tmpdir snapshot. |
| New persistent state in `State.__init__` | If it must survive restart, add a round-trip test mirroring `test_status_history.py::test_restore_round_trip`. |

## Not covered

- HTCondor binding behavior (real schedd queries, GIL release, fork
  safety). The canary in `deploy.sh` is the only check; relies on the
  real pool.
- ProcessPoolExecutor worker crashes / `BrokenProcessPool` recovery.
- Long-running RSS growth.
- Apache / mod_auth_openidc / SSL termination — out of process scope.
- `graphs.py` / `log_analyzer.py` — uncovered.

If you hit a regression in any of these, write a regression test
where it makes sense and document the gap if it doesn't.
