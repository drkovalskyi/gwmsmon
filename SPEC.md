# gwmsmon2 — Design Specification

## 1. Purpose

gwmsmon2 monitors the CMS global HTCondor pool — the distributed computing
infrastructure that runs CMS production and analysis workloads.

### Users

The **primary users are production operators**. They need to see what
production is running right now, and they need the full picture of the
pool: production workflows, but also analysis/user load, site health,
and pilot inventory — because everything competes for the same resources.

**Site and factory operators** are the second-tier audience. They check
specific sites: are pilots arriving, what's the utilization, are factory
entries in error state.

**Analysis users** are the third tier. The system can be used by
individual analyzers, but it's primarily used by operators to detect
abnormalities in user workloads.

### Core Design Principles

1. **Responsiveness**: Primary views must load in sub-second time. All
   data and graphs for the main pages must be pre-computed. It is not
   acceptable for primary information to have any loading delay.

2. **Visual stability**: No dynamic resizing or layout shifts as data
   loads. All containers have predetermined dimensions. Data fills
   space — it never reshapes it. Layout shifts interfere with cognitive
   analysis and are pure noise.

3. **Adaptive pre-rendering**: Start with on-demand graph generation.
   Track request frequency. Pre-render the most accessed graphs. Serve
   everything else on demand. The system learns what matters.

4. **Data freshness**: Every view must show how old its data is. A clear
   indicator ("12s ago") that turns to a warning when data is stale.

5. **Minimal pool load**: Use tiered polling (background vs active) and
   investigate HTCondor push/callback mechanisms. Only query aggressively
   for what's being watched.

---

## 2. Architecture Overview

```
┌─────────────────────┐     ┌───────────────────────┐
│   HTCondor Pool      │     │   GlideinWMS Factories │
│  (collectors/schedds)│     │  (XML status feeds)    │
└────────┬────────────┘     └──────────┬────────────┘
         │  htcondor API                │  HTTP/XML
         │  (parallel schedd queries)   │
         ▼                              ▼
┌─────────────────────────────────────────────────────┐
│         Long-Running Collection Process               │
│                                                       │
│  Continuous collection with adaptive pacing.          │
│  Two polling tiers:                                   │
│    Background — slow baseline for all views           │
│    Active — fast for views with active users           │
│                                                       │
│  HTCondor push/callbacks (to be investigated)         │
│                                                       │
│  Classad → Python conversion at the query boundary.   │
│  No classad objects in application data structures.    │
│  In-memory time-series rolling buffers.               │
└────────┬───────────────────────────┬─────────────────┘
         │                           │
         ▼                           ▼
   ┌───────────┐            ┌──────────────┐
   │ JSON files │            │ Time-series   │
   │ (pre-       │            │ JSON files    │
   │  computed   │            │ (periodic     │
   │  snapshots) │            │  flush from   │
   └─────┬─────┘            │  memory)      │
         │                   └──────┬───────┘
         │                          │
         ▼                          ▼
┌─────────────────────────────────────────────────────┐
│             Flask Web Application                     │
│                                                       │
│  JSON endpoints — serve pre-computed files            │
│  Graph endpoints — Matplotlib (adaptive pre-render)   │
│  HTML endpoints — Jinja2 templates                    │
│                                                       │
│  Graph popularity tracking for pre-render decisions   │
└────────────────────────┬────────────────────────────┘
                         │  HTTPS
                         ▼
                    Web Browser
              (server-rendered HTML,
               minimal JS for refresh,
               fixed-geometry layout)
```

---

## 3. Monitoring Views

Five views, each with its own data directory and URL namespace. All
views are equal peers in navigation — operators switch freely between
them.

| View         | URL prefix        | Content                                           |
|--------------|-------------------|---------------------------------------------------|
| Production   | `/prodview`       | WMAgent production workflows by request/subtask   |
| Analysis     | `/analysisview`   | CRAB analysis workflows (CMS production service)  |
| Global       | `/globalview`     | All schedds — per-user/task breakdown, site utilization, pilot inventory |
| Pool         | `/poolview`       | Scheduler health, negotiation times               |
| Factory      | `/factoryview`    | Glidein factory entries, pilot delivery status     |

### Changes from gwmsmon

**New globalview** replaces totalview, cmsconnectview, and
institutionalview. It queries **all schedds** regardless of type and
shows per-user, per-task information across the entire pool. This
eliminates the problem where schedds with unknown or changed
`CMSGWMS_Type` were invisible. Since we already query all schedds
across the other views, globalview reuses the same data with a
different aggregation (by user/task instead of by workflow).

**Kept separate**: analysisview stays its own view. CRAB is a CMS
production service with different characteristics than ad-hoc user
submissions.

**Dropped**: ElasticSearch history integration (exit codes, runtime
distributions). Exit codes now collected directly from HTCondor via
`schedd.history()` (see section 4.5).

### URL Routing

Explicit path prefixes for all entity types — no ambiguous routing:

```
/{view}/                          — overview page
/{view}/request/{name}            — request/workflow detail
/{view}/request/{name}/{subtask}  — subtask detail
/{view}/site/{name}               — site detail
/{view}/json/...                  — JSON data endpoints
/{view}/graphs/...                — graph endpoints
```

---

## 4. Data Collection

### 4.1 Collection Types

Three distinct collection patterns:

**Job-level collectors** (prodview, analysisview, globalview):
Query individual job classads from each schedd, aggregate into
summaries. Globalview queries **all schedds** — the same ones
prodview and analysisview query, plus any others (cmsconnect,
institutional, tier0, unknown types). Since we're hitting all
schedds anyway, globalview adds no extra pool load — it just
aggregates the data differently (by user/task).

**Summary collectors** (globalview, poolview):
Query schedd/pilot/factory summary ads for pool-wide metrics.
Globalview subsumes the old totalview role: site utilization,
pilot inventory, fairshare, and scheduler-universe jobs for
UserSummary.

**Post-processors** (utilization):
Derive rolling statistics from existing time-series data.

### 4.2 Collection Mechanism

**Adaptive pacing, not fixed cron intervals.** Each collector runs
continuously:
1. Query collector for schedd ads (one lightweight query)
2. Query each schedd in **parallel** for job classads
3. Aggregate and write output
4. Wait for cooldown, then repeat

The cooldown is the tuning parameter — run as fast as the pool allows.

**Tiered polling:**
- Views with active users get fast polling (short cooldown)
- Views with no active users get background polling (longer cooldown)
- The web layer signals which views are active

**HTCondor push/callbacks:** Investigate whether HTCondor supports
notification or streaming interfaces that could replace polling. To be
sorted out with HTCondor team. (TODO)

### 4.3 Classad Conversion

**All classad values are converted to plain Python types at the query
boundary.** This is a hard rule. The conversion layer:

- `classad.ExprTree` → evaluate via `.eval()`, then convert
- `classad.Value.Undefined` → `None`
- Numeric types (`int`, `long`, `float`) → Python native
- Lists → recursively convert elements
- Everything else → `str()`

No classad objects are ever stored in application data structures or
serialized to JSON.

### 4.4 Per-View Job Attributes

Attributes are categorized by role:

#### Production (prodview)

Grouping keys:
```
WMAgent_RequestName, WMAgent_SubTaskName, DESIRED_Sites, JobPrio
```

Status/metric fields:
```
JobStatus, RequestCpus, RequestMemory, EnteredCurrentStatus
```

Metadata (displayed, not aggregated):
```
MATCH_GLIDEIN_CMSSite, time()
```

Special features: Priority blocks (B0–B7) derived from JobPrio thresholds.

#### Analysis (analysisview)

Grouping keys:
```
CRAB_UserHN, CRAB_ReqName, DESIRED_Sites
```

Status/metric fields:
```
JobStatus, RequestCpus, RequestMemory
```

Metadata:
```
MATCH_GLIDEIN_CMSSite, QDate, CRAB_UserWebDir, DAG node counts
```

Queries schedds with `CMSGWMS_Type == "crabschedd"`.

#### Global (globalview)

Queries **all schedds** regardless of `CMSGWMS_Type`. Two roles:

**Per-user/task aggregation** (like old cmsconnect/institutional views):

Grouping keys:
```
AccountingGroup (or user identity), task identifier, DESIRED_Sites
```

Status/metric fields:
```
JobStatus, RequestCpus, RequestMemory
```

Metadata:
```
MATCH_GLIDEIN_CMSSite, QDate,
schedd origin (CMSGWMS_Type of the schedd)
```

Task identifier fallback chain: CRAB_ReqName → WMAgent_RequestName →
Dashboard_TaskId → BLTaskID → SubmitFile.

**Pool-wide summary** (like old totalview):
- Schedd summary ads (all schedds)
- CRAB scheduler-universe jobs (JobUniverse == 7) for UserSummary
- Pilot slot inventory (GLIDEIN_CMSSite, SlotType, CPU/memory/retirement)
- Factory XML feeds (descript.xml, rrd_Status_Attributes.xml)
- Collector claimed-slot counts
- Negotiator cycle duration
- Fairshare breakdown (production vs analysis vs other)

### 4.5 Exit Code Collection

Job-level views (prodview, analysisview, globalview) collect exit codes
from recently completed jobs using `schedd.history()` with the `since`
parameter.

**Why `since` matters:** HTCondor stores history most-recent-first.
Without `since`, `schedd.history()` scans the entire history file
(~15-17s per schedd regardless of result count). The `since` parameter
is a *stop condition* — the scan stops as soon as it hits a job older
than the cutoff. This makes incremental queries fast:

| Query type              | Time (busiest schedd) | Time (all 24 prod schedds, sequential) |
|-------------------------|-----------------------|----------------------------------------|
| Full history scan       | ~15s                  | ~6-7 min                               |
| Since 3 min ago         | ~0.6s                 | ~3s                                    |
| Since 1 min ago         | ~0.3s                 | <1s                                    |

**Collection pattern:**

1. Track a `last_query_time` watermark per schedd
2. Each cycle, query `schedd.history()` with
   `since=CompletionDate < last_query_time`
3. Projection: `ExitCode`, workflow/task identifier, `CompletionDate`
4. Aggregate into rolling exit code counters per workflow/task
5. Update watermark to current time

This runs alongside the live job queries in the same collection cycle,
adding ~3s sequential (~1s parallel) — negligible overhead.

**Aggregation:**

Exit codes are accumulated into rolling time windows:

```python
exit_codes = {
    "<request_name>": {
        "<exit_code>": count,  # rolling window (e.g., last hour)
    }
}
```

Flushed to JSON per view: `exit_codes.json` (overall) and
`{request}/exit_codes.json` (per-workflow).

**Display:** Overview pages show a summary of non-zero exit codes
(failure rate, top exit codes). Request detail pages show the full
exit code distribution for that workflow.

### 4.6 Job Status Mapping

| JobStatus | Name      | Tracked in job-level views? |
|-----------|-----------|-----------------------------|
| 1         | Idle      | Yes (MatchingIdle)          |
| 2         | Running   | Yes                         |
| 3         | Removed   | Skipped                     |
| 4         | Completed | Skipped                     |
| 5         | Held      | Skipped                     |

Exception: globalview UserSummary tracks all statuses including
Held/Completed/Removed for the per-user summary.

---

## 5. Data Model

### 5.1 Core Aggregation Structure

The job-level collectors build nested dictionaries:

```python
workflows = {
    "<request_name>": {
        "<subtask_name>": {
            "Summary": {
                "Running": N,
                "MatchingIdle": N,
                "CpusInUse": N,
                "CpusPending": N,
            },
            "<site_name>": {
                "Running": N,
                "MatchingIdle": N,
                "UniquePressure": N,  # idle targeting only this site
                "CpusInUse": N,
                "CpusPending": N,
            }
        }
    }
}
```

Note: The priority dimension exists in the current system but is only
meaningful for prodview (8 priority blocks). Other views hardcode it.
Decision on whether to keep it as a universal dimension is **open**.

### 5.2 Output JSON Files

Each job-level view produces:

| File                         | Content                                |
|------------------------------|----------------------------------------|
| `summary.json`               | Top-level totals + per-schedd summaries |
| `totals.json`                | Aggregate counts + per-request summary  |
| `site_summary.json`          | Per-site aggregate metrics              |
| `{request}/summary.json`     | Per-request subtask breakdown           |
| `{request}/totals.json`      | Per-request aggregate                   |
| `{request}/exit_codes.json`  | Exit code distribution for this request |
| `{site}/summary.json`        | Per-site request breakdown              |
| `exit_codes.json`            | Overall exit code summary (top codes, failure rate) |

Globalview additionally produces:
- `poolview/summary.json` — scheduler health + UserSummary
- `factoryview/fact_entries.json` — factory entry-to-site mapping
- Pilot inventory and fairshare data (previously in totalview)

Note: The current totalview `site_summary.json` is 24MB because it
includes per-pilot debug data. **Needs review** — may need to split
into a lightweight summary for overview pages and detailed data for
drill-down. (TODO)

All JSON writes are atomic (write to tmp file, then rename).

### 5.3 Time-Series Storage

**In-memory rolling buffers with periodic JSON flush.** No RRD, no
database, no external daemon.

The collection process (long-running) maintains time-series in memory:
- Each cycle appends a data point to the relevant buffers
- Every N cycles, the full buffers are flushed to disk as JSON
- On startup, buffers are restored from the JSON files
- If the process crashes, at most a few minutes of data are lost

**Two retention tiers:**
- **Full resolution** (~3.5 days at collection-step intervals): `summary.json`
- **Downsampled hourly** (~140 days): `summary_history.json`

Downsampling (averaging full-res points into hourly buckets) runs as
part of the periodic flush.

**File format:**
```json
{
  "step": 180,
  "updated": 1772820180,
  "series": {
    "Running":   [{"t": 1772820000, "v": 45231}, ...],
    "Idle":      [{"t": 1772820000, "v": 12044}, ...],
    "CpusInUse": [{"t": 1772820000, "v": 98332}, ...]
  }
}
```

Plain JSON — easy to inspect with `cat`, `jq`, or load in a notebook.
Matplotlib reads these files directly to render graphs.

**Standard series per type:**

| Type       | Series                                                  |
|------------|---------------------------------------------------------|
| summary    | Running, Idle, UniquePressure, CpusUse, CpusPending    |
| site       | Running, MatchingIdle, MaxRunning, CpusUse, CpusPen    |
| request    | Running, Idle, HighPrioIdle, LowPrioRun, CpusUse, CpusPending |
| subtask    | Running, Idle, CpusUse, CpusPending                    |
| fairshare  | ProdCpusUse, AnalCpusUse, ProdRunning, AnalRunning     |
| utilization| Running, MaxWasRunning, CpusUse, MaxWasCpusUse         |
| pilot usage| PartCpusUse, PartCpusFree, StatCpusUse, StatCpusFree   |

---

## 6. Web Application

### 6.1 Pages

Three page types per view:

**Overview page** (`/{view}/`):
- Summary numbers: Running, Idle, CpusInUse, CpusPending, RequestCount
- Summary time-series graphs (hourly/daily/weekly)
- Sortable, filterable request/workflow table
- Exit code summary (failure rate, top non-zero exit codes)
- Site summary table
- Data freshness indicator

**Request detail page** (`/{view}/request/{name}`):
- Subtask breakdown table
- Site allocation table
- Time-series graphs (request-level + per-subtask)
- Exit code distribution (full breakdown for this workflow)
- Task configuration (memory, cpus, desired sites)

**Site detail page** (`/{view}/site/{name}`):
- Workflow list at this site
- Utilization graphs
- Fairshare breakdown (globalview)
- Pilot statistics (globalview)
- Factory entry status (factoryview)

### 6.2 Graph Rendering

**Matplotlib** generates all graphs server-side as PNG images. A custom
style is defined once (color palette, fonts, transparency, line weights,
grid style) and applied consistently across all views. Graphs should
look modern and clean — not like rrdtool output from the 1990s.

**Adaptive pre-rendering:**

1. All graph requests are logged with a hit counter
2. A background process periodically ranks graphs by frequency
3. The top N most-requested graphs are pre-rendered during each
   update cycle and served as static files
4. All other graphs are generated on demand
5. N is tuned based on pre-rendering time budget

Graphs that were popular but stop being requested naturally fall off
the pre-render list. No cleanup needed.

**Fixed dimensions:** All graph containers have predetermined pixel
sizes set in the HTML template. `<img>` tags have explicit `width` and
`height` attributes. Whether the graph is pre-rendered, on-demand, or
still loading, the space is reserved. No layout shifts.

### 6.3 Frontend

**Server-rendered HTML with minimal JavaScript.**

- **Templates**: Jinja2 (Flask built-in)
- **Graphs**: Server-rendered Matplotlib PNGs, delivered as `<img>` tags
- **JavaScript**: Minimal — vanilla JS or a small utility library for:
  - Auto-refresh (periodic reload of data and graph images)
  - Table sorting and filtering
  - Data freshness indicator (live-updating age display)
- **CSS**: Clean, modern styling. No heavy framework required.

No single-page app framework. No heavy charting library. The server
decides what the page looks like and delivers finished HTML + images.
JavaScript is glue, not architecture.

**Key principles:**
- Fixed layout geometry — all containers have known dimensions
- Data freshness shown on every view with age indicator
- Overview pages load fully from pre-computed data
- Detail pages may have on-demand elements with acceptable delay
- Light on client resources — no multi-megabyte JS bundles

---

## 7. Technology Stack

| Layer              | Choice                | Rationale                                    |
|--------------------|-----------------------|----------------------------------------------|
| Language           | Python 3              | HTCondor bindings, team expertise             |
| Web framework      | Flask                 | Simple, fits a mostly-static-file app         |
| Template engine    | Jinja2                | Comes with Flask, well-documented             |
| Graph rendering    | Matplotlib            | Publication-quality server-side PNGs          |
| Time-series storage| In-memory + JSON flush| No external dependencies, easy to debug       |
| Snapshot storage   | JSON files            | Pre-computed, atomic writes, trivially inspectable |
| Frontend JS        | Vanilla / minimal     | Auto-refresh, table sort, freshness display   |
| Authentication     | CERN SSO (OpenID Connect) | Existing infrastructure                  |
| Process model      | Long-running collector + Flask app | Not cron — adaptive pacing        |

---

## 8. Configuration

INI format (`/etc/gwmsmon2.conf`):

```ini
[prodview]
basedir = /var/www/prodview/

[analysisview]
basedir = /var/www/analysisview/

[globalview]
basedir = /var/www/globalview/

[poolview]
basedir = /var/www/poolview/

[factoryview]
basedir = /var/www/factoryview/

[htcondor]
pool = cmsgwms-collector-global.fnal.gov:9620
# pool1 = backup-collector:9620   (optional)

[utilization]
timespan = 31
```

---

## 9. Operational Requirements

### Security
- Run all components as an unprivileged service account. No root.
- CERN SSO (OpenID Connect) for web access.

### Reliability
- Atomic file writes (tmp + rename) for all JSON output
- Graceful handling of schedd query failures (skip and continue)
- Guard against overlapping collection runs
- Staleness detection — frontend shows data age, warns when stale

### Testing

Two test tiers:

**HTCondor interface tests**: Run against a real pool. Verify that
expected classad attributes exist and have expected types. These are
contract tests that catch schema changes in HTCondor or CMS job
infrastructure. Run on a schedule.

**Aggregation unit tests**: Feed mock Python dicts into the aggregation
logic, verify output. Pure Python, fast, no HTCondor dependency.

The boundary between the two is the classad-to-Python conversion layer.

---

## 10. Open Questions

| # | Topic | Notes |
|---|-------|-------|
| 1 | Consolidate update scripts into shared library vs single process | Shared library = independent failure domains. Single process = less overhead. Either way, common logic must be factored out. |
| 2 | Priority as universal dimension vs prodview-specific | Only prodview uses priority blocks meaningfully. Others hardcode it. |
| 3 | site_summary.json content | Currently 24MB with per-pilot debug data. May need summary vs detail split. |
| 4 | Containerization | Good for deployment, but need to sort out persistent state (JSON files, htcondor module access). |
| 5 | HTCondor push/callbacks | Can we subscribe to changes instead of polling? To be investigated with HTCondor team. |
| 6 | Parallel schedd queries — collector impact | Querying schedds in parallel should be safe (separate daemons on separate machines) but need to confirm no collector contention. |

---

## 11. Data Flow Summary

```
Continuous (adaptive pacing):

  Collector ──query schedd list──▶ list of schedd ads
       │
       ▼
  Parallel schedd queries ──job classads──▶ convert to Python dicts
       │
       ▼
  Aggregate:
    workflows[request][subtask][site] → {Running, Idle, CpusInUse, ...}
    taskInfo[user][task] → {metadata}
    gsites[site][request] → {Running, Idle, ...}
       │
       ├──▶ JSON files (atomic write)
       │
       └──▶ Time-series updates

  Adaptive pacing: if view has active users → short cooldown
                    if view has no users   → long cooldown

Periodic (hourly):
  Read time-series → compute daily max → write maxused.json

On HTTP request:
  JSON endpoints   → stream pre-computed file
  Graph endpoints  → serve pre-rendered (if popular) or generate on demand
  HTML endpoints   → render template

Background:
  Track graph request frequency → pre-render top N graphs each cycle
```

---

## Appendix A: Migration from gwmsmon

| gwmsmon                | gwmsmon2              | Change                          |
|------------------------|-----------------------|---------------------------------|
| analysisview           | analysisview          | Kept separate (CRAB is a CMS production service) |
| cmsconnectview         | globalview            | Absorbed into globalview        |
| institutionalview      | globalview            | Absorbed into globalview        |
| totalview              | globalview            | Absorbed into globalview        |
| prodview               | prodview              | Same concept                    |
| poolview               | poolview              | Same concept                    |
| factoryview            | factoryview           | Same concept, update factory URLs |
| ElasticSearch history  | (dropped)             | Exit codes now come from schedd.history() directly |
| Walltime over-use plots| (dropped)             | Jobs are killed anyway, not actionable |
| Memory over-use plots  | (dropped)             | Not useful for operators        |
| Python 2.7             | Python 3              | Full rewrite                    |
| Cheetah templates      | Jinja2                | Flask built-in                  |
| mod_wsgi 3.4           | Flask                 | Modern, simple                  |
| jQuery + Google Charts | Minimal vanilla JS    | No heavy frameworks             |
| rrdtool graphs         | Matplotlib            | Modern, clean visuals           |
| RRD files + rrdcached  | In-memory + JSON flush| No external daemons, debuggable |
| Fixed cron intervals   | Adaptive pacing       | Continuous collection           |
| Sequential schedd queries | Parallel            | Major speedup                   |
| On-demand graphs only  | Adaptive pre-rendering| Usage-driven                    |
| No freshness indicator | Age display + warning | Always visible                  |
| Running as root        | Unprivileged account  | Security                        |
| No tests               | Two-tier testing      | Interface + aggregation         |
