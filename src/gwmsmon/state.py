"""In-memory state: snapshot aggregation and sparse time-series.

The State object lives for the lifetime of the collection process.
Each cycle, update() rebuilds the snapshot from fresh job data and
appends to time-series for active entities.
"""

import json
import logging
import os
import re
import tempfile
import time

log = logging.getLogger(__name__)

# Safe pattern for workflow/request/site names used in filesystem paths
_SAFE_NAME_RE = re.compile(r'^[a-zA-Z0-9._:@#-]+(/[a-zA-Z0-9._:@#-]+)*$')


def _safe_name(name):
    """Return name if safe for use in filesystem paths, else None."""
    if not name or not _SAFE_NAME_RE.match(name):
        return None
    # Reject path traversal via ".." components
    if any(part == ".." for part in name.split("/")):
        return None
    return name


# Import negotiator tier mapping from query module to avoid duplication
def _negotiator_tier(neg_name):
    """Map a NegotiatorName to a site tier label."""
    from gwmsmon.query import negotiator_tier
    return negotiator_tier(neg_name)

# Retention: full resolution for 3.5 days, hourly for 30 days
FULL_RES_SECONDS = int(3.5 * 86400)  # 302400
HOURLY_RES_SECONDS = 30 * 86400      # 2592000
# Keep hourly points this much past the longest chart window (30 days)
# so left-edge interpolation on monthly charts always has an anchor
# point outside the plotting range.
RETENTION_MARGIN_SECONDS = 2 * 86400
PRUNE_INACTIVE_DAYS = 30
EXIT_CODE_WINDOW = 7 * 86400       # retain 7 days of buckets
EXIT_CODE_BUCKET = 600              # 10-minute bucket resolution
EXIT_CODE_WINDOWS = {"1h": 3600, "24h": 86400, "7d": 7 * 86400}


def _window_cutoffs(now_site):
    """Pre-computed (label, cutoff) pairs and global oldest cutoff for
    single-pass windowed bucket aggregation."""
    items = [(wl, now_site - wsec) for wl, wsec in EXIT_CODE_WINDOWS.items()]
    return items, min(c for _, c in items)


def _window_codes(buckets, now_site):
    """Single-pass per-window code aggregation.

    buckets: {ts: {code: count}}
    Returns {window_label: {code: count}} (one pass over buckets).
    """
    cutoffs, oldest = _window_cutoffs(now_site)
    out = {wl: {} for wl, _ in cutoffs}
    for ts, codes in buckets.items():
        if ts < oldest:
            continue
        for wl, cutoff in cutoffs:
            if ts >= cutoff:
                wcodes = out[wl]
                for code, cnt in codes.items():
                    wcodes[code] = wcodes.get(code, 0) + cnt
    return out


def _window_totals(buckets, now_site):
    """Single-pass per-window (total, failures) tallies.

    Returns {window_label: [total, failures]}.
    """
    cutoffs, oldest = _window_cutoffs(now_site)
    out = {wl: [0, 0] for wl, _ in cutoffs}
    for ts, codes in buckets.items():
        if ts < oldest:
            continue
        tot = sum(codes.values())
        fail = tot - codes.get("0", 0)
        for wl, cutoff in cutoffs:
            if ts >= cutoff:
                pair = out[wl]
                pair[0] += tot
                pair[1] += fail
    return out


EOS_LOG_BASE = "/eos/cms/store/logs/prod/recent"

_EOS_PREFIXES = [
    ("PromptReco_", "PromptReco"),
    ("Repack_", "Repack"),
    ("Express_", "Express"),
]


# --- Tool detection ---
# Each entry: (detect_func, tool_name, task_extract_func)
# detect_func(job) -> bool
# task_extract_func(job) -> task_name or None

def _detect_kraken(job):
    """Detect MIT Kraken jobs by environment variable."""
    env = job.get("Environment", "")
    return "KRAKEN_EXE=" in env or "KRAKEN_CONDOR" in env


def _kraken_task(job):
    """Extract Kraken task from Iwd path.

    Iwd looks like: /home/submit/paus/cms/data/nanoao/536/Dataset+Name
    We want: nanoao/536/Dataset+Name
    """
    iwd = job.get("Iwd", "")
    # Find the part after .../cms/data/ or .../cms/logs/
    for marker in ("/cms/data/", "/cms/logs/"):
        idx = iwd.find(marker)
        if idx >= 0:
            return iwd[idx + len(marker):]
    # Fallback: last 3 path components
    parts = iwd.rstrip("/").split("/")
    if len(parts) >= 3:
        return "/".join(parts[-3:])
    return iwd


_TOOL_DETECTORS = [
    (_detect_kraken, "Kraken", _kraken_task),
]


def detect_tool(job):
    """Detect which tool submitted a job.

    Returns (tool_name, task_name) or (None, None).
    """
    for detect_fn, tool_name, task_fn in _TOOL_DETECTORS:
        if detect_fn(job):
            return tool_name, task_fn(job)
    return None, None


def eos_log_dir(request_name):
    """Return EOS base directory for a request's logs."""
    for prefix, subdir in _EOS_PREFIXES:
        if request_name.startswith(prefix):
            return f"{EOS_LOG_BASE}/{subdir}"
    return f"{EOS_LOG_BASE}/PRODUCTION"


def _ensure(d, *keys):
    """Ensure nested dict path exists, return innermost dict."""
    for k in keys:
        if k not in d:
            d[k] = {}
        d = d[k]
    return d


_ZERO_KEYS = ("Running", "MatchingIdle", "CpusInUse", "CpusPending")
_ZERO_TEMPLATE = {k: 0 for k in _ZERO_KEYS}


def _parse_desired_sites(value):
    """Coerce a DESIRED_Sites classad value to a list of site names.

    Both shapes appear in production:
    - comma-separated string: "T1_DE_KIT,T2_CH_CERN, T2_US_MIT"
    - Python list: ["T1_DE_KIT", "T2_CH_CERN", "T2_US_MIT"]
      (a classad list field, already converted by classad_to_python)
    """
    if not value:
        return []
    if isinstance(value, str):
        return [s.strip() for s in value.split(",") if s.strip()]
    if isinstance(value, list):
        return [s.strip() for s in value
                if isinstance(s, str) and s.strip()]
    return []


def _zero_counts():
    return _ZERO_TEMPLATE.copy()


def _ensure_counts(d):
    """Initialize d with zero counts on first call. Replaces the
    `for k in _ZERO_KEYS: d.setdefault(k, 0)` pattern: one dict
    lookup + one C-level update vs four setdefaults. Saves ~30s/cycle
    when called millions of times in the per-job aggregation."""
    if "Running" not in d:
        d.update(_ZERO_TEMPLATE)


def _add_counts(target, status, cpus):
    """Increment counters based on job status."""
    if status == 2:  # Running
        target["Running"] += 1
        target["CpusInUse"] += cpus
    elif status == 1:  # Idle
        target["MatchingIdle"] += 1
        target["CpusPending"] += cpus


# --- Priority blocks for prodview ---

# B0 (highest) through B7 (lowest), derived from JobPrio thresholds
_PRIO_THRESHOLDS = [
    ("B0", 130000),
    ("B1", 110000),
    ("B2", 90000),
    ("B3", 85000),
    ("B4", 80000),
    ("B5", 70000),
    ("B6", 63000),
    ("B7", 0),
]


def _prio_block(job_prio):
    """Map a JobPrio value to a priority block name."""
    if job_prio is None:
        return "B7"
    for name, threshold in _PRIO_THRESHOLDS:
        if job_prio >= threshold:
            return name
    return "B7"


# Job-chunk aggregation workers. Set by State.update() before forking
# the Pool; children inherit via fork's COW. Each worker processes
# jobs[start:end] and runs the full per-job pipeline (universe filter,
# per-schedd task, all 3 view aggregators), returning a partial snap.
_PARENT_STATE = None  # State instance, set in parent before fork
_PARENT_JOBS = None   # list of job dicts


def _reset_worker_signals():
    """Reset SIGTERM/SIGINT handlers in a forked worker to SIG_DFL.

    Workers inherit the parent's signal handlers via fork. The parent's
    SIGTERM handler sets a module-level _shutdown flag the workers
    never read, and the Python-level handler doesn't interrupt the
    C-level multiprocessing.connection.recv() workers wait on for new
    tasks. Net effect: a cgroup SIGTERM logs nicely in each worker but
    leaves them wedged for hours (incident 2026-05-11, 22h hang).
    Reverting to SIG_DFL lets the kernel terminate the worker.
    """
    import signal
    try:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    except (ValueError, OSError):
        pass  # not main thread; nothing to do


# Keys that should NOT be summed during partial-snap merge. They're
# config attributes captured once per (request, subtask, cfg_key) and
# happen to be ints, so the generic int+int rule would wrongly sum
# identical values from different workers.
_FIRST_ONLY_INT_KEYS = frozenset({"WallTime", "Memory", "Cpus"})
# Keys whose int value should be merged with min() — used for
# job-priority tracking ("_min" = lowest priority seen across workers).
_MIN_INT_KEYS = frozenset({"_min"})


def _merge_partial(dst, src):
    """Recursively merge a partial snap (or sub-tree) into the parent.

    Default rules:
      dict + dict   -> recurse
      int  + int    -> sum (or min for "_min", or first-wins for cfg keys)
      list + list   -> element-wise sum (handles nested int lists)
      str  + *      -> keep dst (first-wins)
    """
    for k, v in src.items():
        if k not in dst:
            dst[k] = v
            continue
        d = dst[k]
        if isinstance(v, dict) and isinstance(d, dict):
            _merge_partial(d, v)
        elif isinstance(v, int) and isinstance(d, int):
            if k in _MIN_INT_KEYS:
                dst[k] = min(d, v)
            elif k in _FIRST_ONLY_INT_KEYS:
                pass  # keep first
            else:
                dst[k] = d + v
        elif isinstance(v, list) and isinstance(d, list) and len(v) == len(d):
            for i in range(len(v)):
                a, b = d[i], v[i]
                if isinstance(a, int) and isinstance(b, int):
                    d[i] = a + b
                elif isinstance(a, list) and isinstance(b, list):
                    for j in range(min(len(a), len(b))):
                        if isinstance(a[j], int) and isinstance(b[j], int):
                            a[j] = a[j] + b[j]
        # else: type mismatch or non-mergeable — keep dst as-is


def _worker_process_chunk(chunk):
    """Process jobs[chunk[0]:chunk[1]] in a forked subprocess.

    Replicates the parent's per-job loop in full: universe bucketing,
    per-schedd task breakdown, DESIRED_Sites parsing, all 3 view
    aggregators. Also collects per-schedd service-job detail
    (DAGMan instances, scheduler-other, local, held-by-reason,
    long-idle) for the /poolview/schedd/<name> page. Returns
    (partial_snap, univ_counts, site_cpus_cat, counters).
    """
    _reset_worker_signals()
    state = _PARENT_STATE
    jobs = _PARENT_JOBS
    snap = state._empty_snapshot()
    site_cpus_cat = {}
    univ_counts = {}
    n_jobs = n_vanilla = n_gv = n_pv = n_av = 0
    now_ts = int(time.time())
    LONG_IDLE_SEC = 3600  # idle ≥ 1h flagged as "long idle"
    start, end = chunk

    for i in range(start, end):
        job = jobs[i]
        n_jobs += 1
        schedd_name = job.get("_schedd", "unknown")
        universe = job.get("JobUniverse")
        status = job.get("JobStatus")
        if universe == 5:
            bucket = "vanilla"
        elif universe == 7:
            bucket = "scheduler"
        elif universe == 12:
            bucket = "local"
        else:
            bucket = "other"
        sd_uc = univ_counts.setdefault(
            schedd_name,
            {"vanilla": [0, 0, 0], "scheduler": [0, 0, 0],
             "local": [0, 0, 0], "other": [0, 0, 0]})
        if status == 2:
            sd_uc[bucket][0] += 1
        elif status == 1:
            sd_uc[bucket][1] += 1
        elif status == 5:
            sd_uc[bucket][2] += 1

        # Per-schedd service-job aux — for /poolview/schedd/<name>.
        # Collected for ALL universes/statuses (not gated by vanilla).
        sd_aux = _ensure(snap["poolview"], "schedds",
                         schedd_name, "_aux")
        for k in ("dagman_count", "scheduler_other",
                  "local_universe", "long_idle"):
            sd_aux.setdefault(k, 0)
        sd_aux.setdefault("held_by_reason", {})
        sd_aux.setdefault("dagman", {})

        if universe == 7:
            # DAGMan instance OR other scheduler-universe driver
            dag_total = job.get("DAG_NodesTotal")
            if dag_total is not None:
                sd_aux["dagman_count"] += 1
                cid = job.get("ClusterId", 0)
                pid = job.get("ProcId", 0)
                sd_aux["dagman"][f"{cid}.{pid}"] = {
                    "cluster": cid,
                    "proc": pid,
                    "owner": job.get("Owner") or "?",
                    "iwd": job.get("Iwd") or "",
                    "q_date": job.get("QDate") or 0,
                    "status": status,
                    "dag_status": job.get("DAG_Status") or 0,
                    "in_recovery": bool(job.get("DAG_InRecovery")),
                    "nodes_total": dag_total or 0,
                    "nodes_done": job.get("DAG_NodesDone") or 0,
                    "nodes_queued": job.get("DAG_NodesQueued") or 0,
                    "nodes_ready": job.get("DAG_NodesReady") or 0,
                    "nodes_unready": job.get("DAG_NodesUnready") or 0,
                    "nodes_prerun": job.get("DAG_NodesPrerun") or 0,
                    "nodes_postrun": job.get("DAG_NodesPostrun") or 0,
                    "nodes_failed": job.get("DAG_NodesFailed") or 0,
                }
            else:
                sd_aux["scheduler_other"] += 1
        elif universe == 12:
            sd_aux["local_universe"] += 1

        if status == 5:
            code = job.get("HoldReasonCode") or 0
            key = str(int(code)) if isinstance(code, (int, float)) else "0"
            sd_aux["held_by_reason"][key] = (
                sd_aux["held_by_reason"].get(key, 0) + 1)
        elif status == 1:
            qdate = job.get("QDate") or 0
            if qdate and (now_ts - int(qdate)) > LONG_IDLE_SEC:
                sd_aux["long_idle"] += 1

        if universe != 5:
            continue
        n_vanilla += 1
        cpus = job.get("RequestCpus", 1) or 1
        schedd_type = job.get("_schedd_type", "unknown")

        ds_list = _parse_desired_sites(job.get("DESIRED_Sites"))
        job["_desired_sites_list"] = ds_list
        job["_desired_unique"] = (len(ds_list) == 1)

        owner = job.get("Owner", "unknown")
        dagman_id = job.get("DAGManJobId")
        condora_req = job.get("CONDORA_RequestName")
        sched_task = (job.get("CRAB_ReqName")
                      or job.get("WMAgent_RequestName")
                      or condora_req
                      or (f"{schedd_name}#{dagman_id}"
                          if dagman_id else None)
                      or job.get("SubmitFile")
                      or "unknown")
        condora_round = job.get("CONDORA_Round")
        if condora_req and condora_round is not None:
            sched_task = f"{sched_task}/{condora_round}"
        sd_task = _ensure(snap["poolview"], "schedds",
                          schedd_name, "_tasks", sched_task)
        sd_task.setdefault("Owner", owner)
        for k in ("Running", "MatchingIdle", "Held",
                  "CpusInUse", "CpusPending"):
            sd_task.setdefault(k, 0)
        if status == 2:
            sd_task["Running"] += 1
            sd_task["CpusInUse"] += cpus
        elif status == 1:
            sd_task["MatchingIdle"] += 1
            sd_task["CpusPending"] += cpus
        elif status == 5:
            sd_task["Held"] += 1

        state._aggregate_globalview(snap["globalview"], job, status, cpus)
        n_gv += 1

        if status not in (1, 2):
            continue

        if status == 2:
            site = job.get("MATCH_GLIDEIN_CMSSite")
            if site:
                acct = job.get("AcctGroup", "") or ""
                sc = site_cpus_cat.setdefault(
                    site, {"tier0": 0, "production": 0,
                           "analysis": 0, "other": 0})
                if acct in ("tier0", "production", "analysis"):
                    sc[acct] += cpus
                else:
                    sc["other"] += cpus

        request = job.get("WMAgent_RequestName")
        if request:
            state._aggregate_prodview(snap["prodview"], job, status,
                                      cpus, request, schedd_name)
            n_pv += 1

        if schedd_type == "crabschedd":
            user = job.get("CRAB_UserHN")
            if user:
                state._aggregate_analysisview(
                    snap["analysisview"], job, status, cpus,
                    user, schedd_name)
                n_av += 1

    return (snap, univ_counts, site_cpus_cat,
            n_jobs, n_vanilla, n_gv, n_pv, n_av)


class State:
    def __init__(self):
        self.snapshot = {}
        self.timeseries = {}
        self.updated = 0
        self.exit_codes = {}         # {view: {workflow: {minute_ts: {code: count}}}}
        self.exit_codes_by_site = {} # {view: {workflow: {site: {minute_ts: {code: count}}}}}
        # prodview-only: per-(request, subtask) breakdown to surface
        # failures on the Subtasks table of /prodview/request/<wf>
        self.exit_codes_by_subtask = {}  # {request: {subtask: {minute_ts: {code: count}}}}
        self.exit_code_detail = {}   # {view: {code: {workflow: count, site: count, ...}}}
        self.failed_job_records = {} # {view: {site: {workflow: [record, ...]}}}
        self.efficiency = {}         # {view: {wf: {ts: {cpu, wall_cpus, slot_ok, slot_all}}}}
        self.efficiency_by_site = {} # {view: {wf: {site: {ts: {cpu, wall_cpus, slot_ok, slot_all}}}}}
        self.efficiency_lifetime = {} # {wf: {cpu, wall_cpus, slot_ok, slot_all}}
        self.history_watermarks = {} # {schedd_name: timestamp}
        # Per-view set of workflows touched since last flush_exit_codes.
        # Empty after a successful flush. Used to skip rewriting per-wf
        # JSON files for workflows that didn't gain completions and whose
        # window cutoffs haven't shifted.
        self._dirty_wfs = {v: set() for v in
                           ("prodview", "analysisview", "globalview")}
        # now_site at the previous flush. When unchanged across cycles,
        # window cutoffs are identical and only dirty wfs need rewrite.
        self._last_flush_now_site = 0
        # Per-view set of timeseries entities that got a new sample this
        # cycle. flush_timeseries writes only these instead of all
        # ~200K entities — lets us flush every cycle cheaply rather
        # than every 5, so a SIGKILL (OOM / watchdog escalation /
        # deploy timeout) loses at most one cycle of data instead of
        # up to 25 min. Cleared after each successful flush.
        self._dirty_ts = {v: set() for v in
                          ("prodview", "analysisview", "globalview",
                           "poolview", "factoryview")}
        # Set to True after maintenance() rewrites series in place;
        # flush_timeseries then writes every entity once before going
        # back to incremental mode.
        self._ts_full_flush_needed = True
        # Timestamp of the previous _append_timeseries cycle and the
        # views that received appends this cycle — used to place
        # opening/closing zeros around silent gaps in sparse series.
        self._ts_prev_now = 0
        self._ts_views_updated = set()

    def update(self, jobs, summary_ads, factory_data, accounting_ads=None,
               unclaimed_by_tier=None):
        """Rebuild snapshot from fresh data.

        One pass through all jobs, routing each to the relevant views
        based on job attributes and source schedd type.
        """
        # One-shot cProfile of state.update for diagnosing hot
        # functions. Triggered by env var; logs top 25 cumtime.
        _profile_this = os.environ.get("GWMSMON_PROFILE_UPDATE") == "1"
        _prof = None
        if _profile_this:
            import cProfile
            _prof = cProfile.Profile()
            _prof.enable()
        t0 = time.time()
        snap = self._empty_snapshot()

        # Per-site CPU breakdown by accounting category
        site_cpus_cat = {}  # {site: {tier0, production, analysis, other}}

        # Per-schedd, per-universe-bucket counts (Running/Idle/Held).
        # Buckets: vanilla (5), scheduler (7), local (12), other (rest).
        # Surfaced on /poolview/ so AP-side load is visible alongside grid
        # load. We need to count BEFORE filtering out non-Vanilla universes.
        univ_counts = {}
        t_loop_start = time.time()
        n_jobs_seen = 0
        n_vanilla = 0
        n_globalview = 0
        n_prodview = 0
        n_analysisview = 0

        # Run the per-job loop across N forked workers — each handles a
        # contiguous chunk of jobs and runs the full pipeline (universe
        # filter, per-schedd task, all 3 view aggregators). Workers
        # inherit jobs via fork's COW and pickle a partial snap back.
        n_workers = int(os.environ.get("GWMSMON_AGGREGATE_WORKERS", "8"))
        n_total = len(jobs)
        global _PARENT_STATE, _PARENT_JOBS
        _PARENT_STATE = self
        _PARENT_JOBS = jobs
        try:
            if n_workers > 1 and n_total >= 1000:
                import multiprocessing as mp
                chunk = (n_total + n_workers - 1) // n_workers
                chunks = [(i * chunk, min((i + 1) * chunk, n_total))
                          for i in range(n_workers)]
                ctx = mp.get_context("fork")
                t_aggr = time.time()
                with ctx.Pool(n_workers) as pool:
                    results = pool.map(_worker_process_chunk, chunks)
                t_workers = time.time() - t_aggr
            else:
                # Inline path: tests, small data, or worker count = 1.
                t_aggr = time.time()
                results = [_worker_process_chunk((0, n_total))]
                t_workers = time.time() - t_aggr
        finally:
            _PARENT_STATE = None
            _PARENT_JOBS = None

        t_merge = time.time()
        for (partial_snap, w_univ, w_site_cpus,
             w_n_jobs, w_n_van, w_n_gv, w_n_pv, w_n_av) in results:
            _merge_partial(snap, partial_snap)
            _merge_partial(univ_counts, w_univ)
            _merge_partial(site_cpus_cat, w_site_cpus)
            n_jobs_seen += w_n_jobs
            n_vanilla += w_n_van
            n_globalview += w_n_gv
            n_prodview += w_n_pv
            n_analysisview += w_n_av
        t_merge = time.time() - t_merge

        t_loop_end = time.time()
        actual_workers = (n_workers
                          if n_workers > 1 and n_total >= 1000 else 1)
        log.info(
            "update loop: %.1fs over %d jobs (%d vanilla, %d gv, %d pv, "
            "%d av) | workers=%d wall=%.1fs merge=%.1fs",
            t_loop_end - t_loop_start, n_jobs_seen, n_vanilla,
            n_globalview, n_prodview, n_analysisview,
            actual_workers, t_workers, t_merge)

        snap["_site_cpus_cat"] = site_cpus_cat

        # Merge batched per-view per-site idle pressure into view["sites"].
        # Replaces ~80M _ensure+_ensure_counts+setdefault calls done
        # inside the per-job loop with a few hundred dict ops here.
        for view_name in ("prodview", "analysisview", "globalview"):
            view = snap[view_name]
            sites_idle = view.pop("_sites_idle", {})
            view_sites = view["sites"]
            for s, b in sites_idle.items():
                sv = view_sites.get(s)
                if sv is None:
                    sv = view_sites[s] = _ZERO_TEMPLATE.copy()
                elif "Running" not in sv:
                    sv.update(_ZERO_TEMPLATE)
                sv["MatchingIdle"] += b[0]
                sv["CpusPending"] += b[1]
                sv["UniquePressure"] = sv.get("UniquePressure", 0) + b[2]

        # Stamp per-universe-bucket counts onto poolview schedds.
        for schedd_name, buckets in univ_counts.items():
            sd = _ensure(snap["poolview"], "schedds", schedd_name)
            for bucket, counts in buckets.items():
                sd[f"{bucket}Running"] = counts[0]
                sd[f"{bucket}Idle"] = counts[1]
                sd[f"{bucket}Held"] = counts[2]

        # Copy fairshare from globalview → poolview (single source of truth)
        snap["poolview"]["fairshare"] = snap["globalview"]["fairshare"]

        t_proc = time.time()
        # --- Summary ads → globalview pool-wide, poolview ---
        self._process_summary_ads(snap, summary_ads)
        t_summ = time.time()
        # --- Factory data → factoryview, globalview ---
        self._process_factory_data(snap, factory_data)
        t_fact = time.time()
        # --- Accounting ads → globalview ---
        self._process_accounting_ads(snap, accounting_ads or [],
                                     unclaimed_by_tier or {})
        t_acct = time.time()
        log.info(
            "update post-loop: summary=%.1fs factory=%.1fs accounting=%.1fs",
            t_summ - t_proc, t_fact - t_summ, t_acct - t_fact)

        self.snapshot = snap
        self.updated = time.time()

        if _prof is not None:
            _prof.disable()
            import io
            import pstats
            buf = io.StringIO()
            pstats.Stats(_prof, stream=buf) \
                .sort_stats("cumulative").print_stats(25)
            log.info("cProfile state.update top 25 cumulative:\n%s",
                     buf.getvalue())

        log.info("snapshot updated in %.2fs: "
                 "prodview=%d workflows, analysisview=%d users, "
                 "globalview=%d users",
                 time.time() - t0,
                 len(snap["prodview"]["workflows"]),
                 len(snap["analysisview"]["workflows"]),
                 len(snap["globalview"]["users"]))

    def _empty_snapshot(self):
        return {
            "prodview": {
                "workflows": {},
                "sites": {},
                "totals": _zero_counts(),
                "priorities": {},
                "site_priorities": {},
                "schedds": {},
                # Transient: per-site idle pressure accumulator.
                # Merged into "sites" after the per-job loop.
                # {site: [matching_idle, cpus_pending, unique_pressure]}
                "_sites_idle": {},
            },
            "analysisview": {
                "workflows": {},
                "sites": {},
                "totals": _zero_counts(),
                "schedds": {},
                "_sites_idle": {},
            },
            "globalview": {
                "users": {},
                "sites": {},
                "totals": _zero_counts(),
                "schedds": {},
                "pilots": {},
                "fairshare": {},
                "user_summary": {},
                "negotiator": {},
                "accounting": {},
                "_sites_idle": {},
            },
            "poolview": {
                "schedds": {},
                "negotiator": {},
                "user_summary": {},
                "fairshare": {},
                "totals": {},
            },
            "factoryview": {
                "sites": {},
                "totals": {},
                "errors": [],
            },
        }

    # --- Per-view aggregation ---

    def _aggregate_prodview(self, view, job, status, cpus, request,
                            schedd_name):
        subtask = job.get("WMAgent_SubTaskName", request)
        site = job.get("MATCH_GLIDEIN_CMSSite")
        prio = _prio_block(job.get("JobPrio"))
        desired_sites_list = job.get("_desired_sites_list", ())
        unique_site = job.get("_desired_unique", False)

        # workflows[request][subtask]["Summary"]
        st = _ensure(view["workflows"], request, subtask, "Summary")
        _ensure_counts(st)
        _add_counts(st, status, cpus)

        # per-request metadata (capture once from first job)
        req_meta = _ensure(view["workflows"], request, "_metadata")
        if not req_meta:
            for attr in ("CMS_JobType", "CMS_RequestType",
                         "CMS_CampaignName", "CMS_Type",
                         "CMSSW_Versions", "OriginalMaxWallTimeMins",
                         "OriginalMemory", "RequestDisk", "Owner"):
                val = job.get(attr)
                if val is not None:
                    req_meta[attr] = val
            # DESIRED_Sites is normalized to a comma-string regardless
            # of source shape (string or classad list) so the template
            # renders it consistently.
            if desired_sites_list:
                req_meta["DESIRED_Sites"] = ",".join(desired_sites_list)

        # per-subtask priority
        st_prio = _ensure(view["workflows"], request, subtask, "_priority")
        st_prio.setdefault(prio, 0)
        st_prio[prio] += 1
        job_prio_st = job.get("JobPrio") or 0
        if "_min" not in st_prio or job_prio_st < st_prio["_min"]:
            st_prio["_min"] = job_prio_st

        # workflows[request][subtask][site] (if running at a site)
        if site and status == 2:
            ss = _ensure(view["workflows"], request, subtask, site)
            _ensure_counts(ss)
            _add_counts(ss, status, cpus)

        # Per-site idle pressure (if idle with desired sites)
        if status == 1 and desired_sites_list:
            for s in desired_sites_list:
                ss = _ensure(view["workflows"], request, subtask, s)
                _ensure_counts(ss)
                _add_counts(ss, status, cpus)
                if unique_site:
                    ss.setdefault("UniquePressure", 0)
                    ss["UniquePressure"] += 1

        # Per-subtask debug breakdown: group by unique job config
        walltime = str(job.get("OriginalMaxWallTimeMins", 0))
        memory = str(job.get("OriginalMemory", 0))
        req_cpus = str(job.get("RequestCpus", 1))
        desired_sites = ",".join(sorted(desired_sites_list)) if desired_sites_list else ""
        cfg_key = "||".join([walltime, memory, req_cpus,
                             desired_sites, schedd_name])
        dbg = _ensure(view["workflows"], request, subtask,
                      "_debug", cfg_key)
        _ensure_counts(dbg)
        _add_counts(dbg, status, cpus)
        if "Schedd" not in dbg:
            dbg["Schedd"] = schedd_name
            dbg["WallTime"] = int(walltime)
            dbg["Memory"] = int(memory)
            dbg["Cpus"] = int(req_cpus)
            dbg["DesiredSites"] = desired_sites

        # view totals
        _add_counts(view["totals"], status, cpus)

        # per-site totals (running)
        if site and status == 2:
            s = _ensure(view["sites"], site)
            _ensure_counts(s)
            _add_counts(s, status, cpus)

        # per-site idle pressure (matching idle per desired site).
        # Batched: see _sites_idle merge in update().
        if status == 1 and desired_sites_list:
            sites_idle = view["_sites_idle"]
            for s in desired_sites_list:
                b = sites_idle.get(s)
                if b is None:
                    b = [0, 0, 0]
                    sites_idle[s] = b
                b[0] += 1
                b[1] += cpus
                if unique_site:
                    b[2] += 1

        # per-schedd totals
        sd = _ensure(view["schedds"], schedd_name)
        _ensure_counts(sd)
        _add_counts(sd, status, cpus)

        # priority block
        p = _ensure(view["priorities"], prio)
        _ensure_counts(p)
        _add_counts(p, status, cpus)
        # Track unique-site idle per priority block
        if status == 1 and desired_sites_list:
            if unique_site:
                p.setdefault("UniqueIdle", 0)
                p.setdefault("UniqueCpusPend", 0)
                p["UniqueIdle"] += 1
                p["UniqueCpusPend"] += cpus

        # per-site priority (running only — for site detail priority chart)
        if site and status == 2:
            sp = _ensure(view["site_priorities"], site, prio)
            sp.setdefault("CpusInUse", 0)
            sp["CpusInUse"] += cpus

        # per-request priority tracking
        req_prio = _ensure(view["workflows"], request, "_priority")
        req_prio.setdefault("_jobs", {})
        req_prio["_jobs"].setdefault(prio, 0)
        req_prio["_jobs"][prio] += 1
        job_prio_val = job.get("JobPrio") or 0
        if "_min" not in req_prio or job_prio_val < req_prio["_min"]:
            req_prio["_min"] = job_prio_val

    def _aggregate_analysisview(self, view, job, status, cpus, user,
                                schedd_name):
        request = job.get("CRAB_ReqName", "unknown")
        site = job.get("MATCH_GLIDEIN_CMSSite")
        desired_sites_list = job.get("_desired_sites_list", ())
        unique_site = job.get("_desired_unique", False)

        # workflows[user/request]["Summary"]
        wf_key = f"{user}/{request}"
        st = _ensure(view["workflows"], wf_key, "Summary")
        _ensure_counts(st)
        _add_counts(st, status, cpus)

        # Per-site
        if site and status == 2:
            ss = _ensure(view["workflows"], wf_key, site)
            _ensure_counts(ss)
            _add_counts(ss, status, cpus)

        if status == 1 and desired_sites_list:
            for s in desired_sites_list:
                ss = _ensure(view["workflows"], wf_key, s)
                _ensure_counts(ss)
                _add_counts(ss, status, cpus)
                if unique_site:
                    ss.setdefault("UniquePressure", 0)
                    ss["UniquePressure"] += 1

        # view totals
        _add_counts(view["totals"], status, cpus)

        # per-site totals (running)
        if site and status == 2:
            s = _ensure(view["sites"], site)
            _ensure_counts(s)
            _add_counts(s, status, cpus)

        # per-site idle pressure — batched, see merge in update()
        if status == 1 and desired_sites_list:
            sites_idle = view["_sites_idle"]
            for s in desired_sites_list:
                b = sites_idle.get(s)
                if b is None:
                    b = [0, 0, 0]
                    sites_idle[s] = b
                b[0] += 1
                b[1] += cpus
                if unique_site:
                    b[2] += 1

        # per-schedd
        sd = _ensure(view["schedds"], schedd_name)
        _ensure_counts(sd)
        _add_counts(sd, status, cpus)

    def _aggregate_globalview(self, view, job, status, cpus):
        owner = job.get("Owner", "unknown")
        schedd_name = job.get("_schedd", "unknown")
        site = job.get("MATCH_GLIDEIN_CMSSite")
        desired_sites_list = job.get("_desired_sites_list", ())
        unique_site = job.get("_desired_unique", False)

        # Tool detection (custom submission frameworks)
        tool_name, tool_task = detect_tool(job)

        # Task identifier fallback chain
        dagman_id = job.get("DAGManJobId")
        condora_req = job.get("CONDORA_RequestName")
        task = (job.get("CRAB_ReqName")
                or job.get("WMAgent_RequestName")
                or condora_req
                or (f"{schedd_name}#{dagman_id}" if dagman_id else None)
                or tool_task
                or job.get("SubmitFile")
                or "unknown")
        # CONDORA subtask: append round to task name
        condora_round = job.get("CONDORA_Round")
        if condora_req and condora_round is not None:
            task = f"{task}/{condora_round}"

        # Determine tool: CMS classads → standard frameworks → fallback
        if not tool_name:
            cms_tool = job.get("CMS_WMTool") or job.get("CMS_SubmissionTool")
            if cms_tool and cms_tool not in ("User", "unknown"):
                tool_name = str(cms_tool)
            elif job.get("CRAB_ReqName"):
                tool_name = "CRAB"
            elif job.get("WMAgent_RequestName"):
                tool_name = "WMAgent"
            elif condora_req:
                tool_name = "CONDORA"
            # else: no tool detected — leave unlabeled

        # Accumulate all tools per user
        if tool_name:
            user_tools = _ensure(view["users"], owner, "_tools")
            user_tools.setdefault(tool_name, 0)
            user_tools[tool_name] += 1

        # Accumulate schedd types per user
        stype = job.get("_schedd_type", "unknown")
        if stype and stype != "unknown":
            user_stypes = _ensure(view["users"], owner, "_schedd_types")
            user_stypes.setdefault(stype, 0)
            user_stypes[stype] += 1

        # users[owner][task]["Summary"]
        st = _ensure(view["users"], owner, task, "Summary")
        for k in ("Running", "MatchingIdle", "Held", "Completed",
                   "Removed", "CpusInUse", "CpusPending"):
            st.setdefault(k, 0)

        if status == 2:
            st["Running"] += 1
            st["CpusInUse"] += cpus
        elif status == 1:
            st["MatchingIdle"] += 1
            st["CpusPending"] += cpus
        elif status == 5:
            st["Held"] += 1
        elif status == 4:
            st["Completed"] += 1
        elif status == 3:
            st["Removed"] += 1

        # Per-site (running only)
        if site and status == 2:
            ss = _ensure(view["users"], owner, task, site)
            _ensure_counts(ss)
            _add_counts(ss, status, cpus)

        # view totals (idle + running only, matching other views)
        if status in (1, 2):
            _add_counts(view["totals"], status, cpus)

        # per-site totals (running)
        if site and status == 2:
            s = _ensure(view["sites"], site)
            _ensure_counts(s)
            _add_counts(s, status, cpus)

        # per-site idle pressure — accumulate into a transient list
        # bucket per site; merged into view["sites"] after the for-job
        # loop. Replaces an _ensure + _ensure_counts + setdefault per
        # site per idle job (~80M calls/cycle) with three int adds.
        if status == 1 and desired_sites_list:
            sites_idle = view["_sites_idle"]
            for s in desired_sites_list:
                b = sites_idle.get(s)
                if b is None:
                    b = [0, 0, 0]
                    sites_idle[s] = b
                b[0] += 1
                b[1] += cpus
                if unique_site:
                    b[2] += 1

        # per-schedd
        sd = _ensure(view["schedds"], schedd_name)
        for k in ("Running", "MatchingIdle", "Held", "Total"):
            sd.setdefault(k, 0)
        sd["Total"] += 1
        if status == 2:
            sd["Running"] += 1
        elif status == 1:
            sd["MatchingIdle"] += 1
        elif status == 5:
            sd["Held"] += 1

        # Accounting group category aggregation
        acct_group = job.get("AccountingGroup", "")
        category = acct_group.split(".")[0] if acct_group else "other"
        if status in (1, 2):
            fs = _ensure(view["fairshare"], category)
            _ensure_counts(fs)
            _add_counts(fs, status, cpus)

        # per-user accounting groups (count jobs per category)
        user_acct = _ensure(view["users"], owner, "_acct")
        user_acct.setdefault(category, 0)
        user_acct[category] += 1

        # per-user per-group resource counts (idle + running)
        if status in (1, 2):
            gs = _ensure(view["users"], owner, "_group_stats", category)
            _ensure_counts(gs)
            _add_counts(gs, status, cpus)

        # UserSummary from scheduler-universe jobs (JobUniverse == 7)
        if job.get("JobUniverse") == 7:
            us = _ensure(view["user_summary"], owner)
            for k in ("Running", "Idle", "Held", "Completed",
                       "Removed", "Total"):
                us.setdefault(k, 0)
            us["Total"] += 1
            if status == 2:
                us["Running"] += 1
            elif status == 1:
                us["Idle"] += 1
            elif status == 5:
                us["Held"] += 1
            elif status == 4:
                us["Completed"] += 1
            elif status == 3:
                us["Removed"] += 1

    def _process_summary_ads(self, snap, summary_ads):
        """Process summary ads into globalview and poolview."""
        if not summary_ads:
            return

        # Submitter ads → schedd summaries
        for ad in summary_ads.get("submitters", []):
            name = ad.get("ScheddName") or ad.get("Name", "unknown")
            for view_key in ("globalview", "poolview"):
                sd = _ensure(snap[view_key], "schedds", name)
                sd["SubmitterRunning"] = ad.get("RunningJobs", 0)
                sd["SubmitterIdle"] = ad.get("IdleJobs", 0)
                sd["SubmitterHeld"] = ad.get("HeldJobs", 0)

        # Schedd health ads → poolview. Pull every field the query
        # surfaced; the template decides what to render.
        for name, health in summary_ads.get("schedd_health", {}).items():
            sd = _ensure(snap["poolview"], "schedds", name)
            sd["CMSGWMS_Type"] = health.get("CMSGWMS_Type", "unknown")
            for k in (
                "TotalRunningJobs", "TotalIdleJobs", "TotalHeldJobs",
                "MaxJobsRunning", "TotalSubmitters", "TotalOwners",
                "RecentDaemonCoreDutyCycle", "ShadowsRunning",
                "MonitorSelfAge", "MonitorSelfImageSize",
                "MonitorSelfCPUUsage", "MonitorSelfResidentSetSize",
                "RecentJobsStarted", "RecentJobsCompleted",
                "RecentJobsExited", "JobsStarted", "JobsCompleted",
                "TransferQueueNumWaitingToUpload",
                "TransferQueueNumWaitingToDownload",
                "CondorVersion", "CondorPlatform",
            ):
                sd[k] = health.get(k, 0)
            max_jobs = sd["MaxJobsRunning"]
            if max_jobs > 0:
                sd["PercentUse"] = round(
                    sd["TotalRunningJobs"] * 100 / max_jobs)
            else:
                sd["PercentUse"] = 0

        # Slot ads → pilot inventory
        for ad in summary_ads.get("slots", []):
            site = ad.get("GLIDEIN_CMSSite")
            if not site:
                continue
            slot_type = ad.get("SlotType", "Unknown")
            p = _ensure(snap["globalview"]["pilots"], site, slot_type)
            p.setdefault("count", 0)
            p.setdefault("cpus", 0)
            p.setdefault("memory", 0)
            p["count"] += 1
            p["cpus"] += ad.get("Cpus", 0) or 0
            p["memory"] += ad.get("Memory", 0) or 0

        # Negotiator ads
        for ad in summary_ads.get("negotiator", []):
            duration = ad.get("LastNegotiationCycleDuration0")
            if duration is not None:
                snap["globalview"]["negotiator"]["duration"] = duration
                snap["poolview"]["negotiator"]["duration"] = duration

    def _process_factory_data(self, snap, factory_data):
        """Process factory XML data into factoryview."""
        if not factory_data:
            return

        sites_raw = factory_data.get("sites", {})
        snap["factoryview"]["errors"] = factory_data.get("errors", [])

        total_running = 0
        total_idle = 0
        total_held = 0
        site_summaries = {}

        for site, entries in sites_raw.items():
            site_running = 0
            site_idle = 0
            site_held = 0
            entry_count = 0

            for entry_name, factories in entries.items():
                for factory_name, edata in factories.items():
                    r = edata.get("running", 0)
                    i = edata.get("idle", 0)
                    h = edata.get("held", 0)
                    site_running += r
                    site_idle += i
                    site_held += h
                entry_count += 1

            site_summaries[site] = {
                "Running": site_running,
                "Idle": site_idle,
                "Held": site_held,
                "Entries": entry_count,
                "entries": entries,
            }
            total_running += site_running
            total_idle += site_idle
            total_held += site_held

        snap["factoryview"]["sites"] = site_summaries
        snap["factoryview"]["totals"] = {
            "Running": total_running,
            "Idle": total_idle,
            "Held": total_held,
            "Sites": len(site_summaries),
        }

    def _process_accounting_ads(self, snap, ads, unclaimed_by_tier=None):
        """Process negotiator Accounting ads into globalview."""
        if not ads:
            return

        groups = {}   # {tier: {group_name: {ConfigQuota, ...}}}
        users = {}    # {tier: [user_dicts]}

        for ad in ads:
            neg_name = ad.get("NegotiatorName", "")
            tier = _negotiator_tier(neg_name)
            name = ad.get("Name", "")

            if ad.get("IsAccountingGroup"):
                if name == "<none>":
                    continue
                groups.setdefault(tier, {})[name] = {
                    "ConfigQuota": ad.get("ConfigQuota", 0),
                    "EffectiveQuota": ad.get("EffectiveQuota", 0),
                    "SurplusPolicy": ad.get("SurplusPolicy", ""),
                    # Group-level fair-share priority — drives surplus
                    # distribution between groups. Lower Priority means
                    # the negotiator favours this group on the next
                    # cycle. PriorityFactor is the admin-set multiplier
                    # applied to the raw decayed-usage EMA. Requested
                    # is the group's total weighted demand (idle+
                    # running). AccumulatedUsage is lifetime weighted
                    # consumption since BeginUsageTime — useful for
                    # context but does NOT drive priority.
                    "Priority": ad.get("Priority", 0),
                    "PriorityFactor": ad.get("PriorityFactor", 1),
                    "Requested": ad.get("Requested", 0),
                    "AccumulatedUsage": ad.get("AccumulatedUsage", 0),
                }
            else:
                acct_group = ad.get("AccountingGroup", "")
                users.setdefault(tier, []).append({
                    "name": name,
                    "group": acct_group,
                    "PriorityFactor": ad.get("PriorityFactor", 0),
                    "Priority": ad.get("Priority", 0),
                    "ResourcesUsed": ad.get("ResourcesUsed", 0),
                    "WeightedResourcesUsed": ad.get(
                        "WeightedResourcesUsed", 0),
                    "AccumulatedUsage": ad.get("AccumulatedUsage", 0),
                    "SubmitterLimit": ad.get("SubmitterLimit", 0),
                })

        snap["globalview"]["accounting"] = {
            "groups": groups,
            "users": users,
            "unclaimed_by_tier": unclaimed_by_tier or {},
        }

    # --- Step 9: Exit code collection ---

    def update_exit_codes(self, history_jobs):
        """Aggregate exit codes from recently completed jobs.

        Routes each completed job to the appropriate view(s) using the
        same logic as live job routing. Accumulates into minute-bucketed
        rolling window.

        Exit code selection per view:
        - prodview: Chirp_WMCore_cmsRun_ExitCode (fallback ExitCode)
        - analysisview: Chirp_CRAB3_Job_ExitCode (fallback ExitCode)
        - globalview: best Chirp code, then ExitBySignal as SIG:N, then ExitCode
        """
        count = 0
        errors = 0
        for job in history_jobs:
          try:
            # Only vanilla-universe payload jobs (mirrors the live-job
            # gate at universe != 5). Scheduler-universe (7) DAGMan
            # bootstraps and local-universe (12) CRAB service jobs run
            # on the schedd, not at a site; their data belongs only to
            # the schedd view, never to site/efficiency calculations.
            if job.get("JobUniverse") != 5:
                continue
            # Require a real CMS site. Jobs with no resolved
            # MATCH_GLIDEIN_CMSSite (None/""/"Unknown"/"unknown") are
            # not attributable to any site and must not enter exit-code
            # or efficiency aggregation — that "Unknown" bucket once
            # held mostly schedd-side service jobs and crushed the
            # wall-weighted CPU-efficiency headline.
            site = job.get("MATCH_GLIDEIN_CMSSite")
            if not site or site in ("Unknown", "unknown"):
                continue
            raw_exit = job.get("ExitCode")
            if raw_exit is None:
                continue
            completion = job.get("CompletionDate")
            if not completion:
                continue
            minute = int(completion) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
            schedd_type = job.get("_schedd_type", "unknown")
            schedd_name = job.get("_schedd", "unknown")
            count += 1

            # prodview: jobs with WMAgent_RequestName
            request = job.get("WMAgent_RequestName")
            if request:
                chirp_prod = job.get("Chirp_WMCore_cmsRun_ExitCode")
                code_str = str(chirp_prod if chirp_prod is not None
                               else raw_exit)
                bucket = _ensure(self.exit_codes, "prodview", request)
                bucket.setdefault(minute, {})
                bucket[minute].setdefault(code_str, 0)
                bucket[minute][code_str] += 1
                self._dirty_wfs["prodview"].add(request)
                # Per-subtask breakdown for the Subtasks table.
                subtask = job.get("WMAgent_SubTaskName") or request
                st_bucket = _ensure(self.exit_codes_by_subtask,
                                    request, subtask)
                st_bucket.setdefault(minute, {})
                st_bucket[minute].setdefault(code_str, 0)
                st_bucket[minute][code_str] += 1
                site_bucket = _ensure(self.exit_codes_by_site,
                                      "prodview", request, site)
                site_bucket.setdefault(minute, {})
                site_bucket[minute].setdefault(code_str, 0)
                site_bucket[minute][code_str] += 1
                self._add_exit_detail("prodview", code_str, minute,
                                      request, site, "cmst1")
                self._add_efficiency("prodview", request, site,
                                     minute, code_str, job)
                # Track failed job records for log links
                if code_str != "0" and job.get("WMAgent_JobID"):
                    rec_list = (_ensure(self.failed_job_records,
                                        "prodview", site)
                                .setdefault(request, []))
                    if len(rec_list) < 5000:
                        starts = job.get("NumJobStarts", 1)
                        # Extract short host from LastRemoteHost
                        # Format: slot1_1@glidein_...@hostname.domain
                        lrh = job.get("LastRemoteHost", "")
                        host = lrh.rsplit("@", 1)[-1] if lrh else ""
                        rec_list.append({
                            "code": code_str,
                            "task": job.get("WMAgent_SubTaskName", ""),
                            "schedd": schedd_name,
                            "jobid": job["WMAgent_JobID"],
                            "retry": max(0, starts - 1),
                            "ts": int(completion),
                            "wall": round(job.get(
                                "RemoteWallClockTime", 0)),
                            "rss_mb": round(job.get(
                                "ResidentSetSize", 0) / 1024),
                            "disk_mb": round(job.get(
                                "DiskUsage", 0) / 1024),
                            "req_mem": job.get("RequestMemory", 0),
                            "req_disk_mb": round(job.get(
                                "RequestDisk", 0) / 1024),
                            "cpus": job.get("CpusProvisioned", 0),
                            "host": host,
                            "cmssw_time": round(job.get(
                                "ChirpCMSSWElapsed", 0)),
                            "cmssw_events": job.get(
                                "ChirpCMSSWEvents", 0),
                            "cmssw_done": bool(job.get(
                                "ChirpCMSSWDone", False)),
                        })

            # analysisview: jobs from crabschedd
            if schedd_type == "crabschedd":
                user = job.get("CRAB_UserHN")
                crab_req = job.get("CRAB_ReqName")
                if user and crab_req:
                    chirp_crab = job.get("Chirp_CRAB3_Job_ExitCode")
                    code_str = str(chirp_crab if chirp_crab is not None
                                   else raw_exit)
                    wf_key = "{}/{}".format(user, crab_req)
                    bucket = _ensure(self.exit_codes, "analysisview", wf_key)
                    bucket.setdefault(minute, {})
                    bucket[minute].setdefault(code_str, 0)
                    bucket[minute][code_str] += 1
                    self._dirty_wfs["analysisview"].add(wf_key)
                    site_bucket = _ensure(self.exit_codes_by_site,
                                          "analysisview", wf_key, site)
                    site_bucket.setdefault(minute, {})
                    site_bucket[minute].setdefault(code_str, 0)
                    site_bucket[minute][code_str] += 1
                    self._add_exit_detail("analysisview", code_str, minute,
                                          wf_key, site, user)
                    self._add_efficiency("analysisview", wf_key, site,
                                         minute, code_str, job)

            # globalview: all jobs — best Chirp, then signal, then raw
            chirp_gv = job.get("Chirp_WMCore_cmsRun_ExitCode")
            if chirp_gv is None:
                chirp_gv = job.get("Chirp_CRAB3_Job_ExitCode")
            if chirp_gv is not None:
                gv_code = str(chirp_gv)
            elif job.get("ExitBySignal"):
                gv_code = "SIG:{}".format(raw_exit)
            else:
                gv_code = str(raw_exit)

            owner = job.get("Owner", "unknown")
            dagman_id = job.get("DAGManJobId")
            condora_req = job.get("CONDORA_RequestName")
            task = (job.get("CRAB_ReqName")
                    or job.get("WMAgent_RequestName")
                    or condora_req
                    or ("{}#{}".format(schedd_name, dagman_id)
                        if dagman_id else None)
                    or job.get("SubmitFile")
                    or "unknown")
            condora_round = job.get("CONDORA_Round")
            if condora_req and condora_round is not None:
                task = "{}/{}".format(task, condora_round)
            gv_key = "{}/{}".format(owner, task)
            bucket = _ensure(self.exit_codes, "globalview", gv_key)
            bucket.setdefault(minute, {})
            bucket[minute].setdefault(gv_code, 0)
            bucket[minute][gv_code] += 1
            self._dirty_wfs["globalview"].add(gv_key)
            site_bucket = _ensure(self.exit_codes_by_site,
                                  "globalview", gv_key, site)
            site_bucket.setdefault(minute, {})
            site_bucket[minute].setdefault(gv_code, 0)
            site_bucket[minute][gv_code] += 1
            self._add_exit_detail("globalview", gv_code, minute,
                                  gv_key, site, owner)
            self._add_efficiency("globalview", gv_key, site,
                                 minute, gv_code, job)
          except Exception:
            errors += 1
            if errors <= 3:
                log.warning("bad history job record", exc_info=True)

        self._prune_exit_code_window()
        self._prune_exit_detail_window()
        if errors:
            log.warning("skipped %d bad exit code records", errors)
        log.info("processed %d exit code records", count)

    def _add_exit_detail(self, view, code, minute, workflow, site, user):
        """Track per-code breakdown by workflow, site, and user."""
        detail = _ensure(self.exit_code_detail, view, code)
        detail.setdefault(minute, {})
        mb = detail[minute]
        mb.setdefault("workflows", {})
        mb["workflows"].setdefault(workflow, 0)
        mb["workflows"][workflow] += 1
        mb.setdefault("sites", {})
        mb["sites"].setdefault(site, 0)
        mb["sites"][site] += 1
        mb.setdefault("users", {})
        mb["users"].setdefault(user, 0)
        mb["users"][user] += 1

    def _add_efficiency(self, view, workflow, site, minute,
                        code_str, job):
        """Accumulate efficiency metrics for a completed job."""
        cpu = (job.get("RemoteUserCpu", 0) or 0) + \
              (job.get("RemoteSysCpu", 0) or 0)
        cpus = job.get("CpusProvisioned") or job.get("RequestCpus") or 1
        if not isinstance(cpus, (int, float)):
            cpus = 1
        wall = job.get("RemoteWallClockTime", 0) or 0
        slot = job.get("CommittedSlotTime", 0) or (wall * cpus)
        wall_cpus = wall * cpus
        if wall_cpus <= 0:
            return
        is_ok = code_str == "0"

        # Per-workflow
        eb = _ensure(self.efficiency, view, workflow)
        eb.setdefault(minute, {"cpu": 0, "wall_cpus": 0,
                               "slot_ok": 0, "slot_all": 0})
        b = eb[minute]
        b["cpu"] += cpu
        b["wall_cpus"] += wall_cpus
        b["slot_all"] += slot
        if is_ok:
            b["slot_ok"] += slot

        # Per-workflow-per-site
        sb = _ensure(self.efficiency_by_site, view, workflow, site)
        sb.setdefault(minute, {"cpu": 0, "wall_cpus": 0,
                               "slot_ok": 0, "slot_all": 0})
        s = sb[minute]
        s["cpu"] += cpu
        s["wall_cpus"] += wall_cpus
        s["slot_all"] += slot
        if is_ok:
            s["slot_ok"] += slot

        # Lifetime (prodview only, not time-bucketed)
        if view == "prodview":
            lt = self.efficiency_lifetime.setdefault(
                workflow, {"cpu": 0, "wall_cpus": 0,
                           "slot_ok": 0, "slot_all": 0})
            lt["cpu"] += cpu
            lt["wall_cpus"] += wall_cpus
            lt["slot_all"] += slot
            if is_ok:
                lt["slot_ok"] += slot

    def _prune_exit_detail_window(self):
        """Remove exit code detail buckets older than the rolling window."""
        cutoff = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET - EXIT_CODE_WINDOW
        for view, codes in self.exit_code_detail.items():
            dead_codes = []
            for code, buckets in codes.items():
                dead = [ts for ts in buckets if ts < cutoff]
                for ts in dead:
                    del buckets[ts]
                if not buckets:
                    dead_codes.append(code)
            for code in dead_codes:
                del codes[code]

    def _prune_exit_code_window(self):
        """Remove exit code buckets older than the rolling window."""
        cutoff = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET - EXIT_CODE_WINDOW
        for view, workflows in self.exit_codes.items():
            dead_wfs = []
            for wf, buckets in workflows.items():
                dead = [ts for ts in buckets if ts < cutoff]
                for ts in dead:
                    del buckets[ts]
                if not buckets:
                    dead_wfs.append(wf)
            for wf in dead_wfs:
                del workflows[wf]
        # Prune per-subtask exit code buckets (prodview)
        dead_reqs = []
        for req, subtasks in self.exit_codes_by_subtask.items():
            dead_sts = []
            for st, buckets in subtasks.items():
                dead = [ts for ts in buckets if ts < cutoff]
                for ts in dead:
                    del buckets[ts]
                if not buckets:
                    dead_sts.append(st)
            for st in dead_sts:
                del subtasks[st]
            if not subtasks:
                dead_reqs.append(req)
        for req in dead_reqs:
            del self.exit_codes_by_subtask[req]
        # Prune per-site exit code buckets
        for view, workflows in self.exit_codes_by_site.items():
            dead_wfs = []
            for wf, sites in workflows.items():
                dead_sites = []
                for site, buckets in sites.items():
                    dead = [ts for ts in buckets if ts < cutoff]
                    for ts in dead:
                        del buckets[ts]
                    if not buckets:
                        dead_sites.append(site)
                for site in dead_sites:
                    del sites[site]
                if not sites:
                    dead_wfs.append(wf)
            for wf in dead_wfs:
                del workflows[wf]
        # Prune efficiency buckets (same 7d cutoff)
        for view, workflows in self.efficiency.items():
            dead_wfs = []
            for wf, buckets in workflows.items():
                dead = [ts for ts in buckets if ts < cutoff]
                for ts in dead:
                    del buckets[ts]
                if not buckets:
                    dead_wfs.append(wf)
            for wf in dead_wfs:
                del workflows[wf]
        for view, workflows in self.efficiency_by_site.items():
            dead_wfs = []
            for wf, sites in workflows.items():
                dead_sites = []
                for site, buckets in sites.items():
                    dead = [ts for ts in buckets if ts < cutoff]
                    for ts in dead:
                        del buckets[ts]
                    if not buckets:
                        dead_sites.append(site)
                for site in dead_sites:
                    del sites[site]
                if not sites:
                    dead_wfs.append(wf)
            for wf in dead_wfs:
                del workflows[wf]
        # Prune failed job records older than 7d
        cutoff_7d = int(time.time()) - EXIT_CODE_WINDOWS["7d"]
        for view, sites in self.failed_job_records.items():
            dead_sites = []
            for site, wfs in sites.items():
                dead_wfs = []
                for wf, records in wfs.items():
                    wfs[wf] = [r for r in records if r["ts"] >= cutoff_7d]
                    if not wfs[wf]:
                        dead_wfs.append(wf)
                for wf in dead_wfs:
                    del wfs[wf]
                if not wfs:
                    dead_sites.append(site)
            for site in dead_sites:
                del sites[site]

    def _flatten_exit_codes(self, view):
        """Flatten minute buckets into total counts per workflow."""
        result = {}
        for wf, buckets in self.exit_codes.get(view, {}).items():
            totals = {}
            for codes in buckets.values():
                for code, count in codes.items():
                    totals.setdefault(code, 0)
                    totals[code] += count
            result[wf] = totals
        return result

    def _flatten_exit_codes_windowed(self, view, window_seconds):
        """Flatten buckets within window_seconds into total counts per code."""
        cutoff = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET - window_seconds
        overall = {}
        for wf, buckets in self.exit_codes.get(view, {}).items():
            for ts, codes in buckets.items():
                if ts < cutoff:
                    continue
                for code, count in codes.items():
                    overall.setdefault(code, 0)
                    overall[code] += count
        return overall

    @staticmethod
    def _compute_efficiency(buckets, cutoff):
        """Compute efficiency from time-bucketed data within window."""
        cpu = wall_cpus = slot_ok = slot_all = 0
        for ts, b in buckets.items():
            if ts < cutoff:
                continue
            cpu += b.get("cpu", 0)
            wall_cpus += b.get("wall_cpus", 0)
            slot_ok += b.get("slot_ok", 0)
            slot_all += b.get("slot_all", 0)
        return {
            "running_eff": round(cpu / wall_cpus, 4) if wall_cpus else 0,
            "processing_eff": round(slot_ok / slot_all, 4) if slot_all else 0,
            "cpu_hours": round(cpu / 3600, 1),
            "wall_cpu_hours": round(wall_cpus / 3600, 1),
        }

    def _build_completion_histogram(self, view):
        """Build per-bucket success/failure counts for histogram chart."""
        bucket_totals = {}  # {timestamp: {"success": N, "failure": N}}
        for wf, buckets in self.exit_codes.get(view, {}).items():
            for ts, codes in buckets.items():
                if ts not in bucket_totals:
                    bucket_totals[ts] = {"success": 0, "failure": 0}
                for code, count in codes.items():
                    if code == "0":
                        bucket_totals[ts]["success"] += count
                    else:
                        bucket_totals[ts]["failure"] += count
        timestamps = sorted(bucket_totals.keys())
        return {
            "bucket_size": EXIT_CODE_BUCKET,
            "timestamps": timestamps,
            "success": [bucket_totals[t]["success"] for t in timestamps],
            "failure": [bucket_totals[t]["failure"] for t in timestamps],
        }

    def _flush_globalview_owner_rollup(self, basedir, flat, now_bucket):
        """Aggregate exit-code/efficiency stats per owner across all
        their tasks and write basedir/<owner>/exit_codes.json plus
        basedir/<owner>/completion_histogram.json. Globalview only —
        the URL /globalview/request/<owner> wants a single roll-up,
        not a per-task drill-down.
        """
        view = "globalview"
        owners = sorted({wf.split("/", 1)[0] for wf in flat if "/" in wf})
        for owner in owners:
            if not _safe_name(owner):
                continue
            owner_wfs = [wf for wf in flat if wf.startswith(owner + "/")]
            if not owner_wfs:
                continue

            ec_view = self.exit_codes.get(view, {})
            ec_site_view = self.exit_codes_by_site.get(view, {})
            eff_view = self.efficiency.get(view, {})
            eff_site_view = self.efficiency_by_site.get(view, {})

            # Window-level exit code rollup — single pass over each wf's
            # buckets, accumulating into all 3 windows at once.
            cutoffs, oldest = _window_cutoffs(now_bucket)
            owner_window_codes = {wl: {} for wl, _ in cutoffs}
            for wf in owner_wfs:
                for ts, tcodes in ec_view.get(wf, {}).items():
                    if ts < oldest:
                        continue
                    for wl, cutoff in cutoffs:
                        if ts >= cutoff:
                            wcodes = owner_window_codes[wl]
                            for code, cnt in tcodes.items():
                                wcodes[code] = wcodes.get(code, 0) + cnt
            owner_windows = {}
            for wlabel, wcodes in owner_window_codes.items():
                wtotal = sum(wcodes.values())
                wfail = wtotal - wcodes.get("0", 0)
                owner_windows[wlabel] = {
                    "total": wtotal,
                    "failures": wfail,
                    "failure_rate": (round(wfail / wtotal, 4)
                                     if wtotal else 0),
                    "codes": wcodes,
                }

            # Per-site rollup with summed efficiency — single pass per
            # (wf, site) over both ec and eff buckets.
            sites_for_owner = set()
            for wf in owner_wfs:
                sites_for_owner.update(ec_site_view.get(wf, {}).keys())
            owner_sites_ec = {}
            for site in sites_for_owner:
                # Per-window accumulators
                ec_acc = {wl: [0, 0] for wl, _ in cutoffs}
                eff_acc = {wl: [0, 0, 0, 0] for wl, _ in cutoffs}
                for wf in owner_wfs:
                    for ts, scodes in (ec_site_view.get(wf, {})
                                       .get(site, {}).items()):
                        if ts < oldest:
                            continue
                        tot = sum(scodes.values())
                        fail = tot - scodes.get("0", 0)
                        for wl, cutoff in cutoffs:
                            if ts >= cutoff:
                                pair = ec_acc[wl]
                                pair[0] += tot
                                pair[1] += fail
                    for ts, b in (eff_site_view.get(wf, {})
                                  .get(site, {}).items()):
                        if ts < oldest:
                            continue
                        for wl, cutoff in cutoffs:
                            if ts >= cutoff:
                                v = eff_acc[wl]
                                v[0] += b.get("cpu", 0)
                                v[1] += b.get("wall_cpus", 0)
                                v[2] += b.get("slot_ok", 0)
                                v[3] += b.get("slot_all", 0)
                site_windows = {}
                for wlabel, (total, failures) in ec_acc.items():
                    if not total:
                        continue
                    cpu, wall_cpus, slot_ok, slot_all = eff_acc[wlabel]
                    site_windows[wlabel] = {
                        "total": total,
                        "failures": failures,
                        "failure_rate": round(failures / total, 4),
                        "running_eff": (round(cpu / wall_cpus, 4)
                                        if wall_cpus else 0),
                        "processing_eff": (round(slot_ok / slot_all, 4)
                                           if slot_all else 0),
                    }
                if site_windows:
                    owner_sites_ec[site] = site_windows

            # Window-level efficiency rollup — single pass per wf
            owner_eff_acc = {wl: [0, 0, 0, 0] for wl, _ in cutoffs}
            for wf in owner_wfs:
                for ts, b in eff_view.get(wf, {}).items():
                    if ts < oldest:
                        continue
                    for wl, cutoff in cutoffs:
                        if ts >= cutoff:
                            v = owner_eff_acc[wl]
                            v[0] += b.get("cpu", 0)
                            v[1] += b.get("wall_cpus", 0)
                            v[2] += b.get("slot_ok", 0)
                            v[3] += b.get("slot_all", 0)
            owner_eff = {}
            for wlabel, (cpu, wall_cpus, slot_ok, slot_all) in (
                    owner_eff_acc.items()):
                owner_eff[wlabel] = {
                    "running_eff": (round(cpu / wall_cpus, 4)
                                    if wall_cpus else 0),
                    "processing_eff": (round(slot_ok / slot_all, 4)
                                       if slot_all else 0),
                    "cpu_hours": round(cpu / 3600, 1),
                    "wall_cpu_hours": round(wall_cpus / 3600, 1),
                }

            # Lifetime efficiency rollup
            lt_cpu = lt_wall = lt_slot_ok = lt_slot_all = 0
            for wf in owner_wfs:
                lt = self.efficiency_lifetime.get(wf)
                if lt:
                    lt_cpu += lt.get("cpu", 0)
                    lt_wall += lt.get("wall_cpus", 0)
                    lt_slot_ok += lt.get("slot_ok", 0)
                    lt_slot_all += lt.get("slot_all", 0)
            owner_lt_eff = None
            if lt_wall:
                owner_lt_eff = {
                    "running_eff": round(lt_cpu / lt_wall, 4),
                    "processing_eff": (round(lt_slot_ok / lt_slot_all, 4)
                                       if lt_slot_all else 0),
                    "cpu_hours": round(lt_cpu / 3600, 1),
                    "wall_cpu_hours": round(lt_wall / 3600, 1),
                }

            # Backward-compat top-level mirrors 1h window
            ow_1h = owner_windows.get("1h", {})
            owner_dir = os.path.join(basedir,
                                     owner.replace("/", os.sep))
            os.makedirs(owner_dir, exist_ok=True)
            _atomic_json(os.path.join(owner_dir, "exit_codes.json"), {
                "updated": self.updated,
                "codes": ow_1h.get("codes", {}),
                "total": ow_1h.get("total", 0),
                "failures": ow_1h.get("failures", 0),
                "failure_rate": ow_1h.get("failure_rate", 0),
                "windows": owner_windows,
                "sites": owner_sites_ec,
                "efficiency": owner_eff,
                "lifetime_efficiency": owner_lt_eff,
            })

            # Owner completion histogram: sum success/failure per bucket
            owner_hist = {}
            for wf in owner_wfs:
                for ts, tcodes in ec_view.get(wf, {}).items():
                    if ts not in owner_hist:
                        owner_hist[ts] = {"success": 0, "failure": 0}
                    for code, cnt in tcodes.items():
                        if code == "0":
                            owner_hist[ts]["success"] += cnt
                        else:
                            owner_hist[ts]["failure"] += cnt
            hist_ts = sorted(owner_hist.keys())
            _atomic_json(
                os.path.join(owner_dir, "completion_histogram.json"), {
                "updated": self.updated,
                "bucket_size": EXIT_CODE_BUCKET,
                "timestamps": hist_ts,
                "success": [owner_hist[t]["success"] for t in hist_ts],
                "failure": [owner_hist[t]["failure"] for t in hist_ts],
            })

    def flush_exit_codes(self, cfg):
        """Write exit code JSON files for each view, in parallel.

        Each view's writes are independent (disjoint disk paths, no
        shared state mutation), so we fork one child per view and join.
        Globalview alone runs ~70s; sequential total is ~115s. Forking
        cuts wall time to the slowest view (~70s) and saves ~45s/cycle.

        Children inherit a COW copy of state, mutate nothing the parent
        cares about, and just write JSON. After all children exit the
        parent clears the dirty-workflow tracker and snapshots now_site.
        """
        import multiprocessing as mp
        views_to_flush = []
        for view in ("prodview", "analysisview", "globalview"):
            basedir = cfg.get(view, "basedir")
            if os.path.isdir(basedir):
                views_to_flush.append(view)

        ctx = mp.get_context("fork")
        procs = []
        for view in views_to_flush:
            p = ctx.Process(target=self._flush_one_view, args=(cfg, view))
            p.start()
            procs.append((view, p))
        failed = []
        for view, p in procs:
            p.join()
            if p.exitcode != 0:
                failed.append((view, p.exitcode))
        if failed:
            log.error("flush_exit_codes workers failed: %s", failed)

        # All views flushed: snapshot now_site and clear dirty tracker
        # in the parent. Children's clears (if any) didn't propagate.
        self._last_flush_now_site = (
            int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET)
        for v in self._dirty_wfs:
            self._dirty_wfs[v].clear()

    def _flush_one_view(self, cfg, target_view):
        """Per-view body of flush_exit_codes. Called either directly or
        via a forked subprocess. Iterates the existing per-view block
        but only acts on `target_view`, so we can keep the original
        body's indentation."""
        _reset_worker_signals()
        import time as _t
        for view in ("prodview", "analysisview", "globalview"):
            if view != target_view:
                continue
            basedir = cfg.get(view, "basedir")
            if not os.path.isdir(basedir):
                continue

            _ts = {}
            _t0 = _t.perf_counter()
            flat = self._flatten_exit_codes(view)
            _ts["flatten"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # Multi-window stats
            windows = {}
            for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                wcodes = self._flatten_exit_codes_windowed(view, wsec)
                wtotal = sum(wcodes.values())
                wfail = sum(v for k, v in wcodes.items() if k != "0")
                windows[wlabel] = {
                    "total": wtotal,
                    "failures": wfail,
                    "failure_rate": (round(wfail / wtotal, 4)
                                     if wtotal else 0),
                    "codes": wcodes,
                }

            # View-level efficiency (all workflows combined)
            now_bucket = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
            view_eff = {}
            for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                cutoff = now_bucket - wsec
                cpu = wall_cpus = slot_ok = slot_all = 0
                for wf_b in self.efficiency.get(view, {}).values():
                    for ts, b in wf_b.items():
                        if ts < cutoff:
                            continue
                        cpu += b.get("cpu", 0)
                        wall_cpus += b.get("wall_cpus", 0)
                        slot_ok += b.get("slot_ok", 0)
                        slot_all += b.get("slot_all", 0)
                view_eff[wlabel] = {
                    "running_eff": round(cpu / wall_cpus, 4) if wall_cpus else 0,
                    "processing_eff": round(slot_ok / slot_all, 4) if slot_all else 0,
                    "cpu_hours": round(cpu / 3600, 1),
                    "wall_cpu_hours": round(wall_cpus / 3600, 1),
                }

            _ts["windows_view_eff"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # Backward-compat top-level from 1h window
            w1h = windows.get("1h", {})
            _atomic_json(os.path.join(basedir, "exit_codes.json"), {
                "updated": self.updated,
                "window": EXIT_CODE_WINDOWS["1h"],
                "total": w1h.get("total", 0),
                "failures": w1h.get("failures", 0),
                "failure_rate": w1h.get("failure_rate", 0),
                "codes": w1h.get("codes", {}),
                "windows": windows,
                "efficiency": view_eff,
            })

            # Completion histogram
            histogram = self._build_completion_histogram(view)
            histogram["updated"] = self.updated
            _atomic_json(os.path.join(basedir, "completion_histogram.json"),
                         histogram)

            now_site = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET

            _ts["topfile_hist"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # Per-workflow exit code files. Skip writes when neither the
            # workflow nor its window cutoffs changed since last flush —
            # the on-disk file is still accurate.
            do_full = (now_site != self._last_flush_now_site)
            dirty_set = self._dirty_wfs.get(view, set())
            wf_n = 0
            wf_written = 0
            wf_completion = {}  # {wf: {total, failures, failure_rate}} for 1h
            for wf, codes in flat.items():
                if not _safe_name(wf):
                    continue
                wf_n += 1
                wf_dir = os.path.join(basedir, wf.replace("/", os.sep))
                needs_write = do_full or (wf in dirty_set)
                if needs_write:
                    os.makedirs(wf_dir, exist_ok=True)
                # Per-site completion stats for this workflow
                site_data = self.exit_codes_by_site.get(view, {}).get(wf, {})
                wf_sites_ec = {}
                for site, buckets in site_data.items():
                    site_totals = _window_totals(buckets, now_site)
                    site_windows = {}
                    for wlabel, (total, failures) in site_totals.items():
                        if total:
                            eff_b = self.efficiency_by_site.get(
                                view, {}).get(wf, {}).get(site, {})
                            eff = self._compute_efficiency(
                                eff_b, now_site - EXIT_CODE_WINDOWS[wlabel])
                            site_windows[wlabel] = {
                                "total": total,
                                "failures": failures,
                                "failure_rate": round(failures / total, 4),
                                "running_eff": eff["running_eff"],
                                "processing_eff": eff["processing_eff"],
                            }
                    if site_windows:
                        wf_sites_ec[site] = site_windows
                # Per-request windowed stats
                wf_buckets = self.exit_codes.get(view, {}).get(wf, {})
                wf_window_codes = _window_codes(wf_buckets, now_site)
                wf_windows = {}
                for wlabel, wcodes in wf_window_codes.items():
                    wtotal = sum(wcodes.values())
                    wfail = wtotal - wcodes.get("0", 0)
                    wf_windows[wlabel] = {
                        "total": wtotal, "failures": wfail,
                        "failure_rate": (round(wfail / wtotal, 4)
                                         if wtotal else 0),
                        "codes": wcodes,
                    }
                w7d = wf_windows.get("7d", {})
                # Per-workflow efficiency (windowed)
                wf_eff_buckets = self.efficiency.get(view, {}).get(wf, {})
                wf_eff = {}
                for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                    wf_eff[wlabel] = self._compute_efficiency(
                        wf_eff_buckets, now_site - wsec)
                # Lifetime efficiency
                lt_eff = self.efficiency_lifetime.get(wf)
                lt_eff_out = None
                if lt_eff and lt_eff["wall_cpus"] > 0:
                    lt_eff_out = {
                        "running_eff": round(
                            lt_eff["cpu"] / lt_eff["wall_cpus"], 4),
                        "processing_eff": round(
                            lt_eff["slot_ok"] / lt_eff["slot_all"], 4)
                        if lt_eff["slot_all"] else 0,
                        "cpu_hours": round(lt_eff["cpu"] / 3600, 1),
                        "wall_cpu_hours": round(
                            lt_eff["wall_cpus"] / 3600, 1),
                    }
                if w7d.get("total", 0):
                    eff7d = wf_eff.get("7d", {})
                    lt = self.efficiency_lifetime.get(wf)
                    lt_re = (round(lt["cpu"] / lt["wall_cpus"], 4)
                             if lt and lt.get("wall_cpus") else 0)
                    lt_pe = (round(lt["slot_ok"] / lt["slot_all"], 4)
                             if lt and lt.get("slot_all") else 0)
                    wf_completion[wf] = {
                        "total": w7d["total"],
                        "failures": w7d["failures"],
                        "failure_rate": w7d["failure_rate"],
                        "running_eff": eff7d.get("running_eff", 0),
                        "processing_eff": eff7d.get("processing_eff", 0),
                        "lt_running_eff": lt_re,
                        "lt_processing_eff": lt_pe,
                    }
                wf_w1h = wf_windows.get("1h", {})
                if not needs_write:
                    continue
                _atomic_json(os.path.join(wf_dir, "exit_codes.json"), {
                    "updated": self.updated,
                    "codes": wf_w1h.get("codes", {}),
                    "total": wf_w1h.get("total", 0),
                    "failures": wf_w1h.get("failures", 0),
                    "failure_rate": wf_w1h.get("failure_rate", 0),
                    "windows": wf_windows,
                    "sites": wf_sites_ec,
                    "efficiency": wf_eff,
                    "lifetime_efficiency": lt_eff_out,
                })
                # Per-request completion histogram
                wf_buckets = self.exit_codes.get(view, {}).get(wf, {})
                wf_hist = {}
                for ts, tcodes in wf_buckets.items():
                    if ts not in wf_hist:
                        wf_hist[ts] = {"success": 0, "failure": 0}
                    for code, cnt in tcodes.items():
                        if code == "0":
                            wf_hist[ts]["success"] += cnt
                        else:
                            wf_hist[ts]["failure"] += cnt
                hist_ts = sorted(wf_hist.keys())
                _atomic_json(
                    os.path.join(wf_dir, "completion_histogram.json"), {
                    "updated": self.updated,
                    "bucket_size": EXIT_CODE_BUCKET,
                    "timestamps": hist_ts,
                    "success": [wf_hist[t]["success"] for t in hist_ts],
                    "failure": [wf_hist[t]["failure"] for t in hist_ts],
                })
                wf_written += 1

            _ts["per_wf_loop"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()
            log.info("per_wf %s: n=%d written=%d full=%s",
                     view, wf_n, wf_written, do_full)

            # globalview-only: per-owner roll-up. Exit codes are tracked
            # per "<owner>/<task>" key, but the URL /globalview/request/<owner>
            # expects a roll-up across all of that owner's tasks. Sum
            # window/site/efficiency raw state across each owner's tasks
            # and write basedir/<owner>/exit_codes.json + completion_histogram.
            if view == "globalview":
                self._flush_globalview_owner_rollup(
                    basedir, flat, now_site)
            _ts["owner_rollup"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            _atomic_json(os.path.join(basedir, "wf_completion.json"), {
                "updated": self.updated,
                "workflows": wf_completion,
            })

            # Per-workflow totals (all codes) for failure rate context
            wf_totals = {}  # {wf: total_completed}
            wf_failures_map = {}  # {wf: total_failures}
            for wf, codes in flat.items():
                wf_totals[wf] = sum(codes.values())
                wf_failures_map[wf] = sum(v for k, v in codes.items()
                                          if k != "0")

            # Per-code detail files (workflows, sites, users breakdown)
            ec_dir = os.path.join(basedir, "_exitcodes")
            os.makedirs(ec_dir, exist_ok=True)
            now_bucket = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
            view_detail = self.exit_code_detail.get(view, {})
            for code, buckets in view_detail.items():
                workflows_agg = {}
                sites_agg = {}
                users_agg = {}
                code_total = 0
                # Per-window counts for this code
                code_windows = {}
                for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                    code_windows[wlabel] = 0
                for ts, mb in buckets.items():
                    for wf, n in mb.get("workflows", {}).items():
                        workflows_agg.setdefault(wf, 0)
                        workflows_agg[wf] += n
                        code_total += n
                    for s, n in mb.get("sites", {}).items():
                        sites_agg.setdefault(s, 0)
                        sites_agg[s] += n
                    for u, n in mb.get("users", {}).items():
                        users_agg.setdefault(u, 0)
                        users_agg[u] += n
                    bucket_count = sum(mb.get("workflows", {}).values())
                    for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                        if ts >= now_bucket - wsec:
                            code_windows[wlabel] += bucket_count
                safe_code = code.replace(":", "_")
                _atomic_json(os.path.join(ec_dir,
                                          "{}.json".format(safe_code)), {
                    "updated": self.updated,
                    "code": code,
                    "total": code_total,
                    "windows": code_windows,
                    "workflows": workflows_agg,
                    "wf_totals": {wf: wf_totals.get(wf, 0)
                                  for wf in workflows_agg},
                    "sites": sites_agg,
                    "users": users_agg,
                })

            _ts["per_code_detail"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # _all.json — all completed jobs (full 7d window)
            all_workflows = {}
            all_sites = {}
            all_users = {}
            for code, buckets in view_detail.items():
                for mb in buckets.values():
                    for wf, n in mb.get("workflows", {}).items():
                        all_workflows.setdefault(wf, 0)
                        all_workflows[wf] += n
                    for s, n in mb.get("sites", {}).items():
                        all_sites.setdefault(s, 0)
                        all_sites[s] += n
                    for u, n in mb.get("users", {}).items():
                        all_users.setdefault(u, 0)
                        all_users[u] += n
            all_total = sum(all_workflows.values())
            all_fail = sum(wf_failures_map.values())
            _atomic_json(os.path.join(ec_dir, "_all.json"), {
                "updated": self.updated,
                "total": all_total,
                "failures": all_fail,
                "windows": windows,
                "workflows": all_workflows,
                "wf_totals": wf_totals,
                "wf_failures": wf_failures_map,
                "sites": all_sites,
                "users": all_users,
            })

            _ts["all_json"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # View-level per-site completion stats (aggregated across all workflows)
            # Also build completion cross-reference: {wf: {site: [done, fail]}}
            view_site_ec = {}
            site_codes_1h = {}  # {site: {code: count}} for 1h window
            site_codes_7d = {}  # {site: {code: count}} for 7d window
            completion_xref = {}  # {wf: {site: [done_1h, fail_1h]}}
            cutoff_1h = now_site - EXIT_CODE_WINDOWS["1h"]
            cutoff_7d = now_site - EXIT_CODE_WINDOWS["7d"]
            view_cutoffs, view_oldest = _window_cutoffs(now_site)
            for wf, site_data in self.exit_codes_by_site.get(view, {}).items():
                for site, buckets in site_data.items():
                    wf_site_done = 0
                    wf_site_fail = 0
                    sc7_dst = site_codes_7d.setdefault(site, {})
                    sc1_dst = site_codes_1h.setdefault(site, {})
                    site_view_dst = view_site_ec.setdefault(site, {})
                    # Single pass: per-window totals + per-code 1h/7d
                    for ts, codes in buckets.items():
                        if ts < view_oldest:
                            continue
                        tot = sum(codes.values())
                        fail = tot - codes.get("0", 0)
                        for wl, cutoff in view_cutoffs:
                            if ts >= cutoff:
                                sw = site_view_dst.setdefault(
                                    wl, {"total": 0, "failures": 0})
                                sw["total"] += tot
                                sw["failures"] += fail
                        if ts >= cutoff_7d:
                            for code, cnt in codes.items():
                                sc7_dst[code] = sc7_dst.get(code, 0) + cnt
                        if ts >= cutoff_1h:
                            for code, cnt in codes.items():
                                sc1_dst[code] = sc1_dst.get(code, 0) + cnt
                                wf_site_done += cnt
                                if code != "0":
                                    wf_site_fail += cnt
                    if wf_site_done:
                        # Per-wf-per-site efficiency raw values for 1h
                        eff_b = self.efficiency_by_site.get(
                            view, {}).get(wf, {}).get(site, {})
                        cpu = wall_cpus = slot_ok = slot_all = 0
                        for ts, b in eff_b.items():
                            if ts < cutoff_1h:
                                continue
                            cpu += b.get("cpu", 0)
                            wall_cpus += b.get("wall_cpus", 0)
                            slot_ok += b.get("slot_ok", 0)
                            slot_all += b.get("slot_all", 0)
                        completion_xref.setdefault(wf, {})[site] = [
                            wf_site_done, wf_site_fail,
                            round(cpu), round(wall_cpus),
                            round(slot_ok), round(slot_all)]
            for site, site_wins in view_site_ec.items():
                for w in site_wins.values():
                    w["failure_rate"] = (round(w["failures"] / w["total"], 4)
                                         if w["total"] else 0)
            # Merge per-code counts and efficiency into site data
            site_ec_out = {}
            for site, wins in view_site_ec.items():
                entry = dict(wins)
                entry["codes"] = site_codes_1h.get(site, {})
                entry["codes_7d"] = site_codes_7d.get(site, {})
                # Per-site efficiency (aggregate across workflows)
                site_eff = {}
                for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                    cutoff = now_site - wsec
                    cpu = wall_cpus = slot_ok = slot_all = 0
                    for wf_sites in self.efficiency_by_site.get(
                            view, {}).values():
                        sb = wf_sites.get(site, {})
                        for ts, b in sb.items():
                            if ts < cutoff:
                                continue
                            cpu += b.get("cpu", 0)
                            wall_cpus += b.get("wall_cpus", 0)
                            slot_ok += b.get("slot_ok", 0)
                            slot_all += b.get("slot_all", 0)
                    site_eff[wlabel] = {
                        "running_eff": round(cpu / wall_cpus, 4)
                        if wall_cpus else 0,
                        "processing_eff": round(slot_ok / slot_all, 4)
                        if slot_all else 0,
                    }
                entry["efficiency"] = site_eff
                site_ec_out[site] = entry
            _atomic_json(os.path.join(basedir, "site_exit_codes.json"), {
                "updated": self.updated,
                "sites": site_ec_out,
            })
            _atomic_json(os.path.join(basedir,
                                      "completion_cross_reference.json"),
                         completion_xref)

            _ts["view_site"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # Per-site per-request completion stats + efficiency
            site_req_ec = {}
            for wf, site_data in self.exit_codes_by_site.get(view, {}).items():
                for site, buckets in site_data.items():
                    site_totals = _window_totals(buckets, now_site)
                    eff_b = None
                    for wlabel, (total, failures) in site_totals.items():
                        if not total:
                            continue
                        if eff_b is None:
                            eff_b = self.efficiency_by_site.get(
                                view, {}).get(wf, {}).get(site, {})
                        eff = self._compute_efficiency(
                            eff_b, now_site - EXIT_CODE_WINDOWS[wlabel])
                        rw = (site_req_ec.setdefault(site, {})
                              .setdefault(wf, {}))
                        rw[wlabel] = {
                            "total": total,
                            "failures": failures,
                            "failure_rate": round(failures / total, 4),
                            "running_eff": eff["running_eff"],
                            "processing_eff": eff["processing_eff"],
                        }
            sites_dir = os.path.join(basedir, "_sites")
            os.makedirs(sites_dir, exist_ok=True)
            for site, reqs in site_req_ec.items():
                safe_site = site.replace("/", "_")
                _atomic_json(os.path.join(
                    sites_dir, f"{safe_site}_exit_codes.json"), {
                    "updated": self.updated,
                    "requests": reqs,
                })

            _ts["site_req"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # Per-site completion histograms
            site_hists = {}
            for wf, site_data in self.exit_codes_by_site.get(view, {}).items():
                for site, buckets in site_data.items():
                    for ts, codes in buckets.items():
                        h = (site_hists.setdefault(site, {})
                             .setdefault(ts, {"success": 0, "failure": 0}))
                        for code, cnt in codes.items():
                            if code == "0":
                                h["success"] += cnt
                            else:
                                h["failure"] += cnt
            for site, hist in site_hists.items():
                safe_site = site.replace("/", "_")
                hist_ts = sorted(hist.keys())
                _atomic_json(os.path.join(
                    sites_dir, f"{safe_site}_histogram.json"), {
                    "updated": self.updated,
                    "bucket_size": EXIT_CODE_BUCKET,
                    "timestamps": hist_ts,
                    "success": [hist[t]["success"] for t in hist_ts],
                    "failure": [hist[t]["failure"] for t in hist_ts],
                })

            _ts["site_hists"] = _t.perf_counter() - _t0; _t0 = _t.perf_counter()

            # Per-site failed job records + view-level combined file
            all_failed = []
            for site, wfs in self.failed_job_records.get(view, {}).items():
                safe_site = site.replace("/", "_")
                _atomic_json(os.path.join(
                    sites_dir, f"{safe_site}_failed_jobs.json"), {
                    "updated": self.updated,
                    "requests": wfs,
                })
                for wf, records in wfs.items():
                    eos_base = eos_log_dir(wf)
                    for r in records:
                        # Cache has_log + log_size on the record — only stat once
                        if "has_log" not in r or ("has_log" in r and r["has_log"] and "log_mb" not in r):
                            task_short = r.get("task", "").rsplit("/", 1)[-1]
                            schedd = r.get("schedd", "")
                            jobid = r.get("jobid", 0)
                            retry = r.get("retry", 0)
                            eos_path = (f"{eos_base}/{wf}/{task_short}/"
                                        f"{schedd}-{jobid}-{retry}-log.tar.gz")
                            try:
                                sz = os.path.getsize(eos_path)
                                r["has_log"] = True
                                r["log_mb"] = round(sz / 1024 / 1024, 1)
                            except OSError:
                                r["has_log"] = False
                        rec = dict(r)
                        rec["site"] = site
                        rec["request"] = wf
                        all_failed.append(rec)
            all_failed.sort(key=lambda x: -x.get("ts", 0))
            _atomic_json(os.path.join(basedir, "failed_jobs.json"), {
                "updated": self.updated,
                "jobs": all_failed,
            })
            _ts["failed_jobs"] = _t.perf_counter() - _t0
            log.info("flush_exit_codes %s: %s", view,
                     ", ".join(f"{k}={v:.1f}s" for k, v in _ts.items()))

    def flush_exit_code_state(self, cfg):
        """Persist exit code buckets and watermarks for restart recovery."""
        basedir = cfg.get("globalview", "basedir")
        if not os.path.isdir(basedir):
            return
        _atomic_json(os.path.join(basedir, "exit_code_state.json"), {
            "watermarks": self.history_watermarks,
            "exit_codes": {
                view: {
                    wf: {str(ts): codes for ts, codes in buckets.items()}
                    for wf, buckets in workflows.items()
                }
                for view, workflows in self.exit_codes.items()
            },
            "exit_code_detail": {
                view: {
                    code: {
                        str(ts): mb for ts, mb in buckets.items()
                    }
                    for code, buckets in codes.items()
                }
                for view, codes in self.exit_code_detail.items()
            },
            "exit_codes_by_site": {
                view: {
                    wf: {
                        site: {
                            str(ts): codes
                            for ts, codes in buckets.items()
                        }
                        for site, buckets in sites.items()
                    }
                    for wf, sites in workflows.items()
                }
                for view, workflows in self.exit_codes_by_site.items()
            },
            "failed_job_records": self.failed_job_records,
            "efficiency": {
                view: {
                    wf: {str(ts): b for ts, b in buckets.items()}
                    for wf, buckets in workflows.items()
                }
                for view, workflows in self.efficiency.items()
            },
            "efficiency_by_site": {
                view: {
                    wf: {
                        site: {str(ts): b for ts, b in buckets.items()}
                        for site, buckets in sites.items()
                    }
                    for wf, sites in workflows.items()
                }
                for view, workflows in self.efficiency_by_site.items()
            },
            "efficiency_lifetime": self.efficiency_lifetime,
        })

    def restore_exit_code_state(self, cfg):
        """Load exit code state from disk on startup."""
        basedir = cfg.get("globalview", "basedir")
        path = os.path.join(basedir, "exit_code_state.json")
        if not os.path.exists(path):
            return
        try:
            with open(path) as f:
                data = json.load(f)
            self.history_watermarks = data.get("watermarks", {})
            raw = data.get("exit_codes", {})
            for view, workflows in raw.items():
                self.exit_codes[view] = {}
                for wf, buckets in workflows.items():
                    # Re-bucket old 1-minute timestamps to EXIT_CODE_BUCKET
                    merged = {}
                    for ts, codes in buckets.items():
                        aligned = int(ts) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
                        if aligned not in merged:
                            merged[aligned] = {}
                        for code, count in codes.items():
                            merged[aligned].setdefault(code, 0)
                            merged[aligned][code] += count
                    self.exit_codes[view][wf] = merged
            self._prune_exit_code_window()

            # Restore exit_code_detail
            raw_detail = data.get("exit_code_detail", {})
            for view, codes in raw_detail.items():
                self.exit_code_detail[view] = {}
                for code, buckets in codes.items():
                    merged = {}
                    for ts, mb in buckets.items():
                        aligned = int(ts) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
                        if aligned not in merged:
                            merged[aligned] = {}
                        dest = merged[aligned]
                        for dim in ("workflows", "sites", "users"):
                            for k, n in mb.get(dim, {}).items():
                                dest.setdefault(dim, {})
                                dest[dim].setdefault(k, 0)
                                dest[dim][k] += n
                    self.exit_code_detail[view][code] = merged
            self._prune_exit_detail_window()

            # Restore exit_codes_by_site
            raw_by_site = data.get("exit_codes_by_site", {})
            for view, workflows in raw_by_site.items():
                self.exit_codes_by_site[view] = {}
                for wf, sites in workflows.items():
                    self.exit_codes_by_site[view][wf] = {}
                    for site, buckets in sites.items():
                        merged = {}
                        for ts, codes in buckets.items():
                            aligned = int(ts) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
                            if aligned not in merged:
                                merged[aligned] = {}
                            for code, count in codes.items():
                                merged[aligned].setdefault(code, 0)
                                merged[aligned][code] += count
                        if merged:
                            self.exit_codes_by_site[view][wf][site] = merged

            # Restore failed_job_records and prune old entries
            self.failed_job_records = data.get("failed_job_records", {})
            cutoff_1h = int(time.time()) - EXIT_CODE_WINDOWS["1h"]
            for view, sites in self.failed_job_records.items():
                for site, wfs in list(sites.items()):
                    for wf, records in list(wfs.items()):
                        wfs[wf] = [r for r in records
                                   if r.get("ts", 0) >= cutoff_1h]
                        if not wfs[wf]:
                            del wfs[wf]
                    if not wfs:
                        del sites[site]

            # Restore efficiency
            for raw_key, target in [
                ("efficiency", self.efficiency),
                ("efficiency_by_site", self.efficiency_by_site)]:
                raw = data.get(raw_key, {})
                for view, workflows in raw.items():
                    target[view] = {}
                    if raw_key == "efficiency":
                        for wf, buckets in workflows.items():
                            target[view][wf] = {
                                int(ts): b for ts, b in buckets.items()}
                    else:
                        for wf, sites in workflows.items():
                            target[view][wf] = {}
                            for site, buckets in sites.items():
                                target[view][wf][site] = {
                                    int(ts): b for ts, b in buckets.items()}
            self.efficiency_lifetime = data.get("efficiency_lifetime", {})

            total_wf = sum(len(wfs) for wfs in self.exit_codes.values())
            total_failed = sum(
                len(recs)
                for sites in self.failed_job_records.values()
                for wfs in sites.values()
                for recs in wfs.values())
            log.info("restored exit code state: %d watermarks, %d workflows, "
                     "%d failed job records",
                     len(self.history_watermarks), total_wf, total_failed)
        except (json.JSONDecodeError, OSError):
            log.warning("failed to restore exit code state", exc_info=True)

    # --- Step 6: Sparse time-series ---

    def _append_timeseries(self):
        """Append current snapshot values to time-series.

        Only entities with data get new points (sparse).
        """
        now = int(self.updated)
        snap = self.snapshot
        self._ts_views_updated.clear()

        # prodview
        self._ts_append("prodview", "_summary", snap["prodview"]["totals"],
                        now)
        for req, subtasks in snap["prodview"]["workflows"].items():
            req_totals = _zero_counts()
            for st_name, sites in subtasks.items():
                summary = sites.get("Summary", {})
                for k in req_totals:
                    req_totals[k] += summary.get(k, 0)
            self._ts_append("prodview", f"request:{req}", req_totals, now)
        # Per-site CPU breakdown by accounting category
        site_cpus_cat = snap.get("_site_cpus_cat", {})
        for site, cats in site_cpus_cat.items():
            self._ts_append("prodview", f"site:{site}", {
                "CpusTier0": cats.get("tier0", 0),
                "CpusProd": cats.get("production", 0),
                "CpusAna": cats.get("analysis", 0),
                "CpusOther": cats.get("other", 0),
            }, now)
        prod_sites = snap["prodview"].get("sites", {})
        for site, counts in prod_sites.items():
            self._ts_append("prodview", f"site:{site}", counts, now)
        # Per-site failure rate and efficiency (1h window)
        cutoff_1h = now - EXIT_CODE_WINDOWS["1h"]
        for site in snap["prodview"]["sites"]:
            total = failures = 0
            cpu = wall_cpus = slot_ok = slot_all = 0
            for wf_sites in self.exit_codes_by_site.get("prodview", {}).values():
                buckets = wf_sites.get(site, {})
                for ts, codes in buckets.items():
                    if ts < cutoff_1h:
                        continue
                    for code, cnt in codes.items():
                        total += cnt
                        if code != "0":
                            failures += cnt
            for wf_sites in self.efficiency_by_site.get("prodview", {}).values():
                buckets = wf_sites.get(site, {})
                for ts, b in buckets.items():
                    if ts < cutoff_1h:
                        continue
                    cpu += b.get("cpu", 0)
                    wall_cpus += b.get("wall_cpus", 0)
                    slot_ok += b.get("slot_ok", 0)
                    slot_all += b.get("slot_all", 0)
            self._ts_append("prodview", f"site:{site}", {
                "FailureRate": round(failures / total * 100, 1) if total else 0,
                "CPUEff": round(cpu / wall_cpus * 100, 1) if wall_cpus else 0,
                "ProcEff": round(slot_ok / slot_all * 100, 1) if slot_all else 0,
            }, now)
        for block, counts in snap["prodview"]["priorities"].items():
            self._ts_append("prodview", f"priority:{block}", counts, now)
        for site_name, blocks in snap["prodview"].get(
                "site_priorities", {}).items():
            for block, counts in blocks.items():
                self._ts_append("prodview",
                                f"site_priority:{site_name}:{block}",
                                counts, now)

        # analysisview
        self._ts_append("analysisview", "_summary",
                        snap["analysisview"]["totals"], now)
        for wf, sites in snap["analysisview"]["workflows"].items():
            summary = sites.get("Summary", {})
            self._ts_append("analysisview", f"request:{wf}", summary, now)
        for site, counts in snap["analysisview"]["sites"].items():
            self._ts_append("analysisview", f"site:{site}", counts, now)

        # globalview
        self._ts_append("globalview", "_summary",
                        snap["globalview"]["totals"], now)
        for user, tasks in snap["globalview"]["users"].items():
            user_totals = _zero_counts()
            for task_name, sites in tasks.items():
                summary = sites.get("Summary", {})
                for k in user_totals:
                    user_totals[k] += summary.get(k, 0)
            self._ts_append("globalview", f"request:{user}",
                            user_totals, now)
        for site, counts in snap["globalview"]["sites"].items():
            self._ts_append("globalview", f"site:{site}", counts, now)

        # globalview fairshare
        for cat, counts in snap["globalview"].get("fairshare", {}).items():
            self._ts_append("globalview", f"fairshare:{cat}", counts, now)

        # poolview — totals computed from schedds (not yet in snapshot)
        pv_running = 0
        pv_idle = 0
        pv_held = 0
        for sd in snap["poolview"].get("schedds", {}).values():
            pv_running += sd.get("TotalRunningJobs", 0)
            pv_idle += sd.get("TotalIdleJobs", 0)
            pv_held += sd.get("TotalHeldJobs", 0)
        if pv_running or pv_idle or pv_held:
            self._ts_append("poolview", "_summary", {
                "TotalRunning": pv_running,
                "TotalIdle": pv_idle,
                "TotalHeld": pv_held,
            }, now)
        for schedd_name, sd in snap["poolview"].get("schedds", {}).items():
            self._ts_append("poolview", "schedd:" + schedd_name, {
                "TotalRunningJobs": sd.get("TotalRunningJobs", 0),
                "TotalIdleJobs": sd.get("TotalIdleJobs", 0),
                "TotalHeldJobs": sd.get("TotalHeldJobs", 0),
            }, now)
        for cat, counts in snap["poolview"].get("fairshare", {}).items():
            self._ts_append("poolview", f"fairshare:{cat}", counts, now)

        # factoryview
        fv_totals = snap["factoryview"].get("totals", {})
        if fv_totals:
            self._ts_append("factoryview", "_summary", {
                "Running": fv_totals.get("Running", 0),
                "Idle": fv_totals.get("Idle", 0),
                "Held": fv_totals.get("Held", 0),
            }, now)
        for site_name, site_data in snap["factoryview"].get("sites", {}).items():
            self._ts_append("factoryview", "site:" + site_name, {
                "Running": site_data.get("Running", 0),
                "Idle": site_data.get("Idle", 0),
                "Held": site_data.get("Held", 0),
            }, now)

        self._ts_close_silent(now)

    def _ts_close_silent(self, now):
        """Closing zeros: a key sampled last cycle but not this one went
        to zero (storage is sparse — zeros are never appended, and
        finished entities vanish from the snapshot entirely). Record
        one explicit 0 so charts don't extrapolate the last non-zero
        value across the gap, then let the key go silent. Views with
        no appends this cycle are skipped — absence there means a
        collection hiccup, not a real drop to zero."""
        prev_now = self._ts_prev_now
        if prev_now and prev_now < now:
            for view in self._ts_views_updated:
                dirty = self._dirty_ts.setdefault(view, set())
                for entity, series in self.timeseries.get(view, {}).items():
                    for pts in series.values():
                        if (pts["t"] and pts["v"][-1]
                                and prev_now <= pts["t"][-1] < now):
                            pts["t"].append(now)
                            pts["v"].append(0)
                            dirty.add(entity)
        self._ts_prev_now = now

    def _ts_append(self, view, entity, counts, now):
        """Append a data point for each non-zero counter.

        Storage is sparse — zero values are skipped — but when a key
        that was silent at the previous cycle becomes active again, an
        opening 0 is inserted at the previous cycle's timestamp so the
        plotted line ramps up over one cycle instead of climbing
        slowly across the whole silent gap. (The matching closing 0
        when a key goes silent is appended by the sweep at the end of
        _append_timeseries.)
        """
        if not any(counts.values()):
            return  # sparse: skip entirely inactive entities
        ts = _ensure(self.timeseries, view, entity)
        prev_now = self._ts_prev_now
        for key, val in counts.items():
            if val:
                pts = ts.get(key)
                if pts is None:
                    pts = ts[key] = {"t": [], "v": []}
                elif (prev_now and pts["t"]
                        and pts["t"][-1] < prev_now < now):
                    pts["t"].append(prev_now)
                    pts["v"].append(0)
                pts["t"].append(now)
                pts["v"].append(val)
        # Mark for incremental flush.
        self._dirty_ts.setdefault(view, set()).add(entity)
        self._ts_views_updated.add(view)

    # --- Step 6: Time-series maintenance ---

    def maintenance(self):
        """Downsample, prune old points, clean up inactive entities."""
        # Maintenance rewrites series in place; force full flush next
        # cycle so all the changes hit disk.
        self._ts_full_flush_needed = True
        now = time.time()
        cutoff_full = now - FULL_RES_SECONDS
        cutoff_hourly = now - HOURLY_RES_SECONDS - RETENTION_MARGIN_SECONDS

        for view, entities in self.timeseries.items():
            dead_entities = []
            for entity, series in entities.items():
                for key, pts in series.items():
                    t_arr = pts["t"]
                    v_arr = pts["v"]
                    # Separate into: keep full-res, downsample, drop
                    full_t, full_v = [], []
                    ds_t, ds_v = [], []
                    for i in range(len(t_arr)):
                        t = t_arr[i]
                        if t >= cutoff_full:
                            full_t.append(t)
                            full_v.append(v_arr[i])
                        elif t >= cutoff_hourly:
                            ds_t.append(t)
                            ds_v.append(v_arr[i])

                    # Downsample to hourly buckets
                    ht, hv = _downsample_hourly(ds_t, ds_v)

                    pts["t"] = ht + full_t
                    pts["v"] = hv + full_v

                # Prune empty series
                if all(len(pts["t"]) == 0 for pts in series.values()):
                    dead_entities.append(entity)

            for e in dead_entities:
                del entities[e]

        # Prune efficiency_lifetime for workflows no longer in snapshot
        active_wfs = set(self.snapshot.get("prodview", {}).get(
            "workflows", {}).keys())
        if active_wfs:
            dead = [wf for wf in self.efficiency_lifetime
                    if wf not in active_wfs]
            for wf in dead:
                del self.efficiency_lifetime[wf]
            if dead:
                log.info("pruned %d inactive workflows from "
                         "efficiency_lifetime", len(dead))

    # --- Step 7: Persistence ---

    def flush_snapshot(self, cfg):
        """Write snapshot JSON files to each view's basedir."""
        snap = self.snapshot
        if not snap:
            return

        for view in ("prodview", "analysisview", "globalview"):
            basedir = cfg.get(view, "basedir")
            if not os.path.isdir(basedir):
                continue

            view_data = snap[view]

            # Compute 7d averages from timeseries
            cutoff_7d = time.time() - 7 * 86400
            ts_summary = self.timeseries.get(view, {}).get(
                "_summary", {})
            avgs_7d = {}
            for key in ("Running", "MatchingIdle",
                        "CpusInUse", "CpusPending"):
                pts = ts_summary.get(key, {"t": [], "v": []})
                vals = [v for t, v in zip(pts["t"], pts["v"])
                        if t >= cutoff_7d]
                avgs_7d[key] = round(sum(vals) / len(vals)) if vals else 0

            summary_out = {
                "updated": self.updated,
                "schedds": view_data.get("schedds", {}),
                "totals": view_data.get("totals", {}),
                "averages_7d": avgs_7d,
            }
            if view == "prodview":
                summary_out["priorities"] = view_data.get("priorities", {})
            _atomic_json(os.path.join(basedir, "summary.json"), summary_out)

            # Fairshare JSON for globalview
            if view == "globalview":
                _atomic_json(os.path.join(basedir, "fairshare.json"), {
                    "updated": self.updated,
                    "categories": view_data.get("fairshare", {}),
                })
                acct = view_data.get("accounting", {})
                if acct:
                    _atomic_json(os.path.join(basedir, "accounting.json"), {
                        "updated": self.updated,
                        "groups": acct.get("groups", {}),
                        "users": acct.get("users", {}),
                        "unclaimed_by_tier": acct.get(
                            "unclaimed_by_tier", {}),
                    })

            if view == "analysisview":
                wf_out = {
                    req: {"Summary": subtasks.get("Summary", {})}
                    for req, subtasks in view_data.get("workflows", {}).items()
                }
            else:
                wf_out = {}
                for req, subtasks in view_data.get(
                        "workflows",
                        view_data.get("users", {})).items():
                    req_out = {}
                    for st, data in subtasks.items():
                        if st.startswith("_"):
                            continue
                        st_out = dict(data.get("Summary", {}))
                        st_prio = data.get("_priority", {})
                        if st_prio:
                            st_out["_prio"] = st_prio.get("_min", 0)
                        req_out[st] = st_out
                    wf_out[req] = req_out

            # Enrich with per-user accounting groups (globalview)
            if view == "globalview":
                for user, tasks in view_data.get("users", {}).items():
                    if user not in wf_out:
                        continue
                    user_acct = tasks.get("_acct", {})
                    if user_acct:
                        wf_out[user]["_groups"] = sorted(
                            user_acct.keys(),
                            key=user_acct.get, reverse=True)
                    group_stats = tasks.get("_group_stats", {})
                    if group_stats:
                        wf_out[user]["_group_stats"] = dict(group_stats)

            # Enrich with per-request priority data
            if view == "prodview":
                for req, subtasks_raw in view_data.get("workflows",
                                                       {}).items():
                    prio_data = subtasks_raw.get("_priority", {})
                    if prio_data and req in wf_out:
                        jobs_by_block = prio_data.get("_jobs", {})
                        min_prio = prio_data.get("_min", 0)
                        wf_out[req]["_priority"] = {
                            "block": _prio_block(min_prio),
                            "prio": min_prio,
                            "blocks": jobs_by_block,
                        }

            # Enrich with per-subtask completion windows (prodview)
            if view == "prodview":
                now_b = (int(time.time()) // EXIT_CODE_BUCKET
                         * EXIT_CODE_BUCKET)
                for req, st_data in self.exit_codes_by_subtask.items():
                    if req not in wf_out:
                        continue
                    for st_name, buckets in st_data.items():
                        if st_name not in wf_out[req]:
                            continue
                        wins = {}
                        for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                            cutoff = now_b - wsec
                            wcodes = {}
                            for ts, tcodes in buckets.items():
                                if ts < cutoff:
                                    continue
                                for code, cnt in tcodes.items():
                                    wcodes[code] = wcodes.get(code, 0) + cnt
                            wtotal = sum(wcodes.values())
                            wfail = sum(v for k, v in wcodes.items()
                                        if k != "0")
                            if wtotal:
                                wins[wlabel] = {
                                    "total": wtotal,
                                    "failures": wfail,
                                    "failure_rate": (round(wfail / wtotal, 4)
                                                     if wtotal else 0),
                                }
                        if wins:
                            wf_out[req][st_name]["_completion"] = wins

            _atomic_json(os.path.join(basedir, "totals.json"), {
                "updated": self.updated,
                "totals": view_data.get("totals", {}),
                "workflows": wf_out,
            })

            # Per-request detail files + per-site reverse index
            wf_source = (view_data.get("workflows", {})
                         if view != "globalview"
                         else view_data.get("users", {}))
            site_index = {}  # site → {req: counts}
            for req, subtasks in wf_source.items():
                if not _safe_name(req):
                    continue
                req_dir = os.path.join(basedir, req.replace("/", os.sep))
                os.makedirs(req_dir, exist_ok=True)
                req_sites = {}
                for st_name, sites_data in subtasks.items():
                    if st_name == "Summary" or st_name.startswith("_"):
                        continue
                    if not isinstance(sites_data, dict):
                        continue
                    for site_name, counts in sites_data.items():
                        if site_name == "Summary" or site_name.startswith("_"):
                            continue
                        if not isinstance(counts, dict):
                            continue
                        s = req_sites.setdefault(site_name, _zero_counts())
                        for k in s:
                            s[k] += counts.get(k, 0)
                _atomic_json(os.path.join(req_dir, "detail.json"), {
                    "updated": self.updated,
                    "subtasks": {
                        st: data
                        for st, data in subtasks.items()
                    },
                    "sites": req_sites,
                })
                # Build reverse index
                for site_name, counts in req_sites.items():
                    if any(counts.values()):
                        si = site_index.setdefault(site_name, {})
                        si[req] = dict(counts)

            # Write per-site detail files
            site_dir = os.path.join(basedir, "_sites")
            if site_index:
                os.makedirs(site_dir, exist_ok=True)
            for site_name, reqs in site_index.items():
                safe = site_name.replace("/", "_")
                _atomic_json(os.path.join(site_dir, f"{safe}.json"), {
                    "updated": self.updated,
                    "requests": reqs,
                })

            # Site summary with RequestCount
            site_data = view_data.get("sites", {})
            site_summary_out = {}
            for site_name, counts in site_data.items():
                entry = dict(counts)
                entry["RequestCount"] = len(site_index.get(site_name, {}))
                site_summary_out[site_name] = entry
            _atomic_json(os.path.join(basedir, "site_summary.json"),
                         site_summary_out)

            # Cross-reference: request → {site: [R, I, C, P]}
            cross_ref = {}
            for site_name, reqs in site_index.items():
                for req, counts in reqs.items():
                    cr = cross_ref.setdefault(req, {})
                    cr[site_name] = [
                        counts.get("Running", 0),
                        counts.get("MatchingIdle", 0),
                        counts.get("CpusInUse", 0),
                        counts.get("CpusPending", 0),
                    ]
            _atomic_json(os.path.join(basedir, "cross_reference.json"),
                         cross_ref)

        # poolview
        poolview_dir = cfg.get("poolview", "basedir")
        gv = snap.get("globalview", {})
        pv = snap.get("poolview", {})
        if os.path.isdir(poolview_dir):
            # Compute pool totals from schedd health data
            pv_running = 0
            pv_idle = 0
            pv_held = 0
            for sd in pv.get("schedds", {}).values():
                pv_running += sd.get("TotalRunningJobs", 0)
                pv_idle += sd.get("TotalIdleJobs", 0)
                pv_held += sd.get("TotalHeldJobs", 0)
            pv_totals = {
                "TotalRunning": pv_running,
                "TotalIdle": pv_idle,
                "TotalHeld": pv_held,
                "ScheddCount": len(pv.get("schedds", {})),
            }
            pv["totals"] = pv_totals

            # Aggregate globalview users into flat per-user summary
            user_agg = {}
            for owner, tasks in gv.get("users", {}).items():
                u = user_agg.setdefault(owner, {
                    "Running": 0, "MatchingIdle": 0, "Held": 0,
                    "CpusInUse": 0, "CpusPending": 0,
                })
                user_tools = tasks.get("_tools", {})
                if user_tools:
                    u["Tool"] = ", ".join(sorted(user_tools.keys()))
                user_stypes = tasks.get("_schedd_types", {})
                if user_stypes:
                    u["ScheddTypes"] = ", ".join(sorted(user_stypes.keys()))
                for task, task_data in tasks.items():
                    if task.startswith("_"):
                        continue
                    s = task_data.get("Summary", {})
                    u["Running"] += s.get("Running", 0)
                    u["MatchingIdle"] += s.get("MatchingIdle", 0)
                    u["Held"] += s.get("Held", 0)
                    u["CpusInUse"] += s.get("CpusInUse", 0)
                    u["CpusPending"] += s.get("CpusPending", 0)

            _atomic_json(os.path.join(poolview_dir, "summary.json"), {
                "updated": self.updated,
                "schedds": pv.get("schedds", {}),
                "negotiator": gv.get("negotiator", {}),
                "user_summary": user_agg,
                "fairshare": pv.get("fairshare", {}),
                "totals": pv_totals,
            })

        # factoryview
        fv_dir = cfg.get("factoryview", "basedir")
        fv = snap.get("factoryview", {})
        if os.path.isdir(fv_dir):
            _atomic_json(os.path.join(fv_dir, "summary.json"), {
                "updated": self.updated,
                "totals": fv.get("totals", {}),
                "errors": fv.get("errors", []),
            })

            # Per-site summaries (without nested entry detail)
            site_summaries = {}
            for site_name, site_data in fv.get("sites", {}).items():
                site_summaries[site_name] = {
                    k: v for k, v in site_data.items() if k != "entries"
                }
            _atomic_json(os.path.join(fv_dir, "totals.json"), {
                "updated": self.updated,
                "totals": fv.get("totals", {}),
                "sites": site_summaries,
            })

            # Per-site detail files
            for site_name, site_data in fv.get("sites", {}).items():
                site_dir = os.path.join(fv_dir, site_name)
                os.makedirs(site_dir, exist_ok=True)
                _atomic_json(os.path.join(site_dir, "summary.json"), {
                    "updated": self.updated,
                    "site": site_name,
                    "Running": site_data.get("Running", 0),
                    "Idle": site_data.get("Idle", 0),
                    "Held": site_data.get("Held", 0),
                    "Entries": site_data.get("Entries", 0),
                    "entries": site_data.get("entries", {}),
                })

    def flush_timeseries(self, cfg):
        """Write time-series JSON files to disk.

        Incremental by default: writes only the entities that gained a
        new sample since the last flush (tracked in self._dirty_ts). On
        the first call after restart, and after maintenance() rewrites
        series in place, does a full flush of every entity. Clears the
        dirty set on success."""
        full = self._ts_full_flush_needed
        n_total = n_written = 0
        for view in ("prodview", "analysisview", "globalview",
                      "poolview", "factoryview"):
            basedir = cfg.get(view, "basedir")
            if not os.path.isdir(basedir):
                continue
            ts_dir = os.path.join(basedir, "timeseries")
            os.makedirs(ts_dir, exist_ok=True)

            entities = self.timeseries.get(view, {})
            n_total += len(entities)
            if full:
                to_write = list(entities)
            else:
                to_write = list(self._dirty_ts.get(view, set())
                                & set(entities))
            for entity in to_write:
                series = entities.get(entity)
                if series is None:
                    continue
                safe_name = entity.replace("/", "_").replace(":", "_")
                path = os.path.join(ts_dir, f"{safe_name}.json")
                _atomic_json(path, {
                    "updated": self.updated,
                    "entity": entity,
                    "series": series,
                })
                n_written += 1
        log.info("flush_timeseries: wrote %d/%d entities (%s)",
                 n_written, n_total, "full" if full else "incremental")
        # Reset state after successful flush.
        self._ts_full_flush_needed = False
        for v in self._dirty_ts:
            self._dirty_ts[v].clear()

    def restore(self, cfg):
        """Load time-series from JSON files on startup."""
        for view in ("prodview", "analysisview", "globalview",
                      "poolview", "factoryview"):
            basedir = cfg.get(view, "basedir")
            ts_dir = os.path.join(basedir, "timeseries")
            if not os.path.isdir(ts_dir):
                continue

            self.timeseries.setdefault(view, {})
            for fname in os.listdir(ts_dir):
                if not fname.endswith(".json"):
                    continue
                path = os.path.join(ts_dir, fname)
                try:
                    with open(path) as f:
                        data = json.load(f)
                    # Use stored entity name; fall back to filename
                    entity = data.get("entity", fname[:-5])
                    raw_series = data.get("series", {})
                    # Migrate old [{t,v},...] format to {t:[],v:[]}
                    converted = {}
                    for key, val in raw_series.items():
                        if isinstance(val, list):
                            converted[key] = {
                                "t": [p["t"] for p in val],
                                "v": [p["v"] for p in val],
                            }
                        else:
                            converted[key] = val
                    # Merge if entity already loaded (e.g. duplicate
                    # files from old mangled-name bug)
                    if entity in self.timeseries[view]:
                        existing = self.timeseries[view][entity]
                        for key, pts in converted.items():
                            if key in existing:
                                existing[key]["t"].extend(pts["t"])
                                existing[key]["v"].extend(pts["v"])
                            else:
                                existing[key] = pts
                    else:
                        self.timeseries[view][entity] = converted
                except (json.JSONDecodeError, OSError):
                    log.warning("failed to restore %s", path, exc_info=True)

        total = sum(len(e) for v in self.timeseries.values()
                    for e in v.values())
        log.info("restored %d time-series entities", total)

        self.restore_exit_code_state(cfg)

    def prune_dirs(self, cfg):
        """Remove workflow/request directories inactive for 30+ days."""
        cutoff = time.time() - PRUNE_INACTIVE_DAYS * 86400
        for view in ("prodview", "analysisview", "globalview"):
            basedir = cfg.get(view, "basedir")
            if not os.path.isdir(basedir):
                continue
            for entry in os.scandir(basedir):
                if not entry.is_dir():
                    continue
                if entry.name in ("timeseries",):
                    continue
                # Check modification time
                if entry.stat().st_mtime < cutoff:
                    log.info("pruning inactive dir: %s", entry.path)
                    import shutil
                    shutil.rmtree(entry.path, ignore_errors=True)


def _downsample_hourly(t_arr, v_arr):
    """Average points into hourly buckets. Returns (out_t, out_v) lists."""
    if not t_arr:
        return [], []
    buckets = {}
    for i in range(len(t_arr)):
        hour = t_arr[i] // 3600 * 3600
        if hour not in buckets:
            buckets[hour] = []
        buckets[hour].append(v_arr[i])
    out_t, out_v = [], []
    for hour in sorted(buckets):
        vals = buckets[hour]
        out_t.append(hour)
        out_v.append(round(sum(vals) / len(vals), 1))
    return out_t, out_v


def _atomic_json(path, data):
    """Write JSON atomically (tmp + rename)."""
    dirpath = os.path.dirname(path)
    os.makedirs(dirpath, exist_ok=True, mode=0o755)
    fd, tmp = tempfile.mkstemp(dir=dirpath, suffix=".tmp")
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        os.rename(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
