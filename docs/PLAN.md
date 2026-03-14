# gwmsmon2 — Development Plan

## Principles

- Follow SPEC.md exactly
- Run as gwmsmon user, never root
- Test on vocms860 against the real pool at each step
- Each step produces something verifiable before moving on

---

## Step 1: Project skeleton

Create package structure and build config. No application logic.

```
gwmsmon2/
├── pyproject.toml
├── src/
│   └── gwmsmon/
│       └── __init__.py
```

Entry point: `gwmsmon-collect` (long-running collector process).

Verify: `pip install -e .` works on vocms860 as gwmsmon user.

---

## Step 2: Configuration

`src/gwmsmon/config.py`

- Load INI from `/etc/gwmsmon2.conf` (or path argument)
- Sections: htcondor, prodview, analysisview, globalview, poolview,
  factoryview, utilization
- Sensible defaults

Verify: Load config, print pool address and basedirs.

---

## Step 3: Classad conversion boundary

`src/gwmsmon/convert.py`

- `classad_to_python(value)` — recursive conversion
- ExprTree → eval() then convert
- Value.Undefined → None
- Lists → recurse
- Primitives pass through
- Everything else → str()
- No classad objects leak past this layer

Verify: Unit test with mock classad values.

---

## Step 4: HTCondor query layer

`src/gwmsmon/query.py`

Three query types that together collect everything all views need
in a single collection round.

### 4a. Job-level queries

Query collector for schedd ads, then query each schedd in parallel
for individual job classads.

**No JobStatus constraint** — fetch all jobs regardless of status.
Each view filters what it needs in the aggregation layer:
- prodview/analysisview: use only Idle (1) and Running (2)
- globalview UserSummary: uses all statuses including
  Held/Completed/Removed

**View routing:**
- Each schedd ad carries a `CMSGWMS_Type` attribute
- Tag every job with its source schedd name and type
- prodview: aggregates jobs that have `WMAgent_RequestName`
  (any schedd type — the job attribute is the filter)
- analysisview: only aggregates jobs from `crabschedd` schedds
  (schedd type is the filter, per SPEC 4.4)
- globalview: aggregates jobs from ALL schedds regardless of type

**Projection** — union of all attributes needed across all views:
```
JobStatus, JobUniverse, RequestCpus, RequestMemory,
DESIRED_Sites, MATCH_GLIDEIN_CMSSite, Owner,
EnteredCurrentStatus, AccountingGroup,
WMAgent_RequestName, WMAgent_SubTaskName, JobPrio,
CRAB_UserHN, CRAB_ReqName, QDate, CRAB_UserWebDir,
Dashboard_TaskId, BLTaskID, SubmitFile,
DAG_NodesTotal, DAG_NodesDone, DAG_NodesFailed,
DAG_NodesQueued, DAG_NodesReady
```

Functions:
- `get_schedds(pool)` — query collector for schedd ads
- `query_schedd(ad, projection)` — query single schedd,
  return list of plain Python dicts (uses convert layer)
- `query_schedds_parallel(ads, projection)` — thread pool
  for parallel schedd queries, each job tagged with `_schedd` name
  and `_schedd_type`

**Failure handling:** Individual schedd queries may fail (network,
timeout, schedd down). Skip failed schedds and continue with the
rest — never let one bad schedd block the entire collection cycle.
Log failures for debugging.

### 4b. Summary ad queries

Separate lightweight queries to the collector for pool-wide data:

- **Schedd summary ads**: total jobs, running, idle per schedd
- **Pilot/slot ads**: GLIDEIN_CMSSite, SlotType, Cpus, Memory,
  GLIDEIN_ToRetire (retirement time)
- **Negotiator ads**: LastNegotiationCycleDuration
- **Collector claimed-slot counts**

These feed globalview's pool-wide summary and poolview.

### 4c. Factory XML feeds

HTTP fetch of GlideinWMS factory status:
- `descript.xml` — factory entry configuration
- `rrd_Status_Attributes.xml` — entry status metrics

These feed factoryview and globalview's factory entry data.

Verify: Run on vocms860 as gwmsmon. Print:
- schedd count and total job count (by status)
- job counts per CMSGWMS_Type
- pilot slot count
- factory entry count

---

## Step 5: In-memory state — current snapshot

`src/gwmsmon/state.py`

The core data structure that lives in memory for the lifetime of
the process.

```python
class State:
    def __init__(self):
        self.snapshot = {}    # current cycle's aggregated data
        self.timeseries = {}  # sparse time-series per entity
        self.updated = 0      # timestamp of last update

    def update(self, jobs, summary_ads, factory_data):
        """Rebuild snapshot from fresh data.
        Append to time-series (sparse — only active entities)."""

    def flush(self, basedirs):
        """Write snapshot + time-series to JSON files (atomic)."""

    def restore(self, basedirs):
        """Load previous state from JSON files on startup."""
```

The snapshot contains all views' aggregated data:

```python
snapshot = {
    "prodview": {
        "workflows": { ... },   # request -> subtask -> site -> counts
        "sites": { ... },       # site -> counts
        "totals": { ... },      # aggregate counts
        "priorities": { ... },  # block -> counts
        "schedds": { ... },     # schedd -> counts
    },
    "analysisview": {
        "workflows": { ... },   # user/request -> site -> counts
        "sites": { ... },
        "totals": { ... },
        "schedds": { ... },
    },
    "globalview": {
        "users": { ... },       # owner -> task -> site -> counts (all statuses)
        "sites": { ... },
        "totals": { ... },
        "schedds": { ... },     # all schedds, with summary ad data
        "pilots": { ... },      # site -> slot type -> CPU/memory/retirement
        "fairshare": { ... },   # production vs analysis vs other
        "user_summary": { ... },# from scheduler-universe jobs (JobUniverse == 7)
        "negotiator": { ... },  # cycle duration
    },
    "poolview": {
        "schedds": { ... },     # scheduler health, summary ad data
        "negotiator": { ... },  # negotiation times
    },
    "factoryview": {
        "entries": { ... },     # factory entry -> site mapping + status
    },
}
```

The `update()` method:
1. Receives all jobs, summary ads, and factory data from query layer
2. Iterates once through all jobs
3. For each job, checks source schedd type and updates relevant views:
   - All jobs → globalview (by user/task, all statuses)
   - Jobs with WMAgent_RequestName → prodview (Idle + Running only)
   - Jobs from crabschedd with CRAB_UserHN → analysisview
     (Idle + Running only)
   - Jobs with JobUniverse == 7 → globalview UserSummary
4. Processes summary ads → globalview pool-wide, poolview
5. Processes factory data → factoryview, globalview
6. After aggregation, appends data points to time-series for active
   entities only (sparse)

Verify: Run update() with real data, inspect snapshot structure.
Print counts per view. Compare prodview counts against old system.

---

## Step 6: In-memory state — sparse time-series

Extend `State` with time-series support.

Each entity (view summary, request, site, user) has a time-series:

```python
timeseries = {
    "prodview": {
        "_summary": {           # view-level summary
            "Running": [{"t": ..., "v": ...}, ...],
            "Idle": [...],
            "CpusInUse": [...],
        },
        "request:<name>": {     # per-request
            "Running": [...],
            "Idle": [...],
        },
        "site:<name>": {        # per-site
            "Running": [...],
        },
    },
    "globalview": {
        "_summary": { ... },
        "_fairshare": {         # fairshare breakdown
            "ProdCpusUse": [...],
            "AnalCpusUse": [...],
            "ProdRunning": [...],
            "AnalRunning": [...],
        },
        "_pilots": {            # pilot usage
            "PartCpusUse": [...],
            "PartCpusFree": [...],
            "StatCpusUse": [...],
            "StatCpusFree": [...],
        },
    },
    ...
}
```

Standard series per entity type (from SPEC):

| Type       | Series                                                  |
|------------|---------------------------------------------------------|
| summary    | Running, Idle, UniquePressure, CpusUse, CpusPending    |
| site       | Running, MatchingIdle, MaxRunning, CpusUse, CpusPen    |
| request    | Running, Idle, HighPrioIdle, LowPrioRun, CpusUse, CpusPending |
| subtask    | Running, Idle, CpusUse, CpusPending                    |
| fairshare  | ProdCpusUse, AnalCpusUse, ProdRunning, AnalRunning     |
| utilization| Running, MaxWasRunning, CpusUse, MaxWasCpusUse         |
| pilot usage| PartCpusUse, PartCpusFree, StatCpusUse, StatCpusFree   |

Rules:
- Append only when entity has data (sparse)
- Full resolution kept for 3.5 days
- Hourly downsample kept for 365 days
- Points older than 365 days dropped

Verify: Run several cycles, inspect time-series growth. Confirm
inactive entities get no new points.

---

## Step 7: Persistence — flush and restore

Extend `State.flush()` and `State.restore()`.

Flush:
- Write snapshot JSON files to each view's basedir (atomic: tmp+rename)

  Per job-level view (prodview, analysisview, globalview):
  - `summary.json`, `totals.json`, `site_summary.json`
  - Per-request, per-site, per-user directories

  globalview additionally:
  - `poolview/summary.json` — scheduler health + UserSummary
  - `factoryview/fact_entries.json` — factory entry-to-site mapping
  - Pilot inventory and fairshare data

- Write time-series JSON files (periodic, not every cycle)
- All writes as gwmsmon user

Restore:
- On startup, load time-series from JSON files
- Snapshot is always rebuilt from the next collection cycle
  (no need to restore snapshot — it's ephemeral)

Verify: Run collector, kill it, restart it. Confirm time-series
survives the restart. Confirm snapshot is fresh after restart.

---

## Step 8: Main collector loop

`src/gwmsmon/collector.py`

The long-running process:

```python
def main():
    cfg = config.load()
    state = State()
    state.restore(cfg)

    while True:
        jobs, summary_ads, factory_data = query_all(cfg)
        state.update(jobs, summary_ads, factory_data)
        state.flush_snapshot(cfg)       # every cycle
        state.flush_timeseries(cfg)     # every N cycles
        state.maintenance()             # downsample, prune, cleanup
        sleep(cooldown)
```

- Entry point: `gwmsmon-collect`
- Flags: `--config`, `--once` (single cycle for testing), `--verbose`
- Adaptive pacing placeholder (fixed cooldown initially)
- Graceful shutdown on SIGTERM

`query_all()` executes all three query types from Step 4:
job-level queries, summary ad queries, and factory XML fetches.

`maintenance()` performs:
- Downsample full-res points older than 3.5 days → hourly
- Drop points older than 365 days
- Prune workflow/request directories inactive for 30+ days

Verify: Run as gwmsmon user on vocms860 via systemd or manually.
Confirm continuous operation. Confirm JSON files are updated each
cycle. Kill and restart — confirm time-series restored.

---

## Step 9: Exit code collection

Extend the collection cycle with `schedd.history()` queries.

- Track a `last_query_time` watermark per schedd
- Each cycle, query `schedd.history()` with
  `since=CompletionDate < last_query_time`
- Projection: ExitCode, workflow/task identifier, CompletionDate
- Aggregate into rolling exit code counters per workflow/task
- Update watermark to current time

Output files:
- `exit_codes.json` — overall exit code summary (top codes, failure rate)
- `{request}/exit_codes.json` — per-workflow exit code distribution

Runs alongside live job queries in the same collection cycle,
adding ~3s sequential (~1s parallel).

Verify: Run a few cycles, inspect exit_codes.json. Confirm
per-workflow files are created. Confirm watermark advances.

---

## Step 10: Flask web application

`src/gwmsmon/web.py`

- Serves pre-computed JSON files from each view's basedir
- Jinja2 templates for HTML pages
- Matplotlib graph endpoints (on-demand initially)
- Data freshness indicator on every page
- Exit code summary on overview pages
- Exit code distribution on request detail pages
- URL routing per spec:
  ```
  /{view}/                          — overview page
  /{view}/request/{name}            — request/workflow detail
  /{view}/request/{name}/{subtask}  — subtask detail
  /{view}/site/{name}               — site detail
  /{view}/json/...                  — JSON data endpoints
  /{view}/graphs/...                — graph endpoints
  ```
- CERN SSO (OpenID Connect) authentication

Entry point: `gwmsmon-web`

Verify: Start web app, browse prodview overview page. Confirm data
loads, freshness shows correct age. Verify authentication works.

---

## Step 11: Deployment

- systemd unit files for collector and web app (run as gwmsmon)
- Apache reverse proxy to Flask
- CERN SSO (OpenID Connect) integration with Apache
- Config file at `/etc/gwmsmon2.conf`
- Data directories owned by gwmsmon

---

## Step 12: Testing and polish

Testing:
- HTCondor interface tests: run against real pool, verify expected
  classad attributes exist with expected types (contract tests,
  run on a schedule)
- Aggregation unit tests: feed mock Python dicts into aggregation,
  verify output (no HTCondor dependency)
- The boundary between the two is the classad-to-Python conversion

Polish:
- Adaptive pacing (tiered polling: active views → short cooldown,
  inactive views → long cooldown)
- Adaptive graph pre-rendering (track request frequency, pre-render
  top N graphs each cycle)
- Utilization post-processor (derive rolling stats from time-series,
  compute daily max → write maxused.json)

---

## What NOT to do

- Don't run anything as root
- Don't write cron jobs
- Don't create separate collection passes per view
- Don't write files as the primary output — in-memory state is primary
- Don't build views as independent modules that each own their
  collection — one collection feeds all views
- Don't skip steps — each step must be verified before moving on
- Don't let classad objects leak past the query boundary
- Don't filter by JobStatus in the query — let views filter in
  aggregation
