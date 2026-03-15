"""In-memory state: snapshot aggregation and sparse time-series.

The State object lives for the lifetime of the collection process.
Each cycle, update() rebuilds the snapshot from fresh job data and
appends to time-series for active entities.
"""

import json
import logging
import os
import tempfile
import time

log = logging.getLogger(__name__)

# Retention: full resolution for 3.5 days, hourly for 365 days
FULL_RES_SECONDS = int(3.5 * 86400)  # 302400
HOURLY_RES_SECONDS = 365 * 86400     # 31536000
PRUNE_INACTIVE_DAYS = 30
EXIT_CODE_WINDOW = 7 * 86400       # retain 7 days of buckets
EXIT_CODE_BUCKET = 600              # 10-minute bucket resolution
EXIT_CODE_WINDOWS = {"1h": 3600, "24h": 86400, "7d": 7 * 86400}


def _ensure(d, *keys):
    """Ensure nested dict path exists, return innermost dict."""
    for k in keys:
        if k not in d:
            d[k] = {}
        d = d[k]
    return d


def _zero_counts():
    return {
        "Running": 0,
        "MatchingIdle": 0,
        "CpusInUse": 0,
        "CpusPending": 0,
    }


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
    ("B0", 210000),
    ("B1", 200000),
    ("B2", 190000),
    ("B3", 180000),
    ("B4", 170000),
    ("B5", 160000),
    ("B6", 150000),
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


class State:
    def __init__(self):
        self.snapshot = {}
        self.timeseries = {}
        self.updated = 0
        self.exit_codes = {}         # {view: {workflow: {minute_ts: {code: count}}}}
        self.exit_code_detail = {}   # {view: {code: {workflow: count, site: count, ...}}}
        self.history_watermarks = {} # {schedd_name: timestamp}

    def update(self, jobs, summary_ads, factory_data):
        """Rebuild snapshot from fresh data.

        One pass through all jobs, routing each to the relevant views
        based on job attributes and source schedd type.
        """
        t0 = time.time()
        snap = self._empty_snapshot()

        for job in jobs:
            status = job.get("JobStatus")
            cpus = job.get("RequestCpus", 1) or 1
            schedd_type = job.get("_schedd_type", "unknown")
            schedd_name = job.get("_schedd", "unknown")

            # --- globalview: all jobs, all statuses ---
            self._aggregate_globalview(snap["globalview"], job, status, cpus)

            # Only Idle (1) and Running (2) for prodview/analysisview
            if status not in (1, 2):
                continue

            # --- prodview: jobs with WMAgent_RequestName ---
            request = job.get("WMAgent_RequestName")
            if request:
                self._aggregate_prodview(snap["prodview"], job, status,
                                         cpus, request, schedd_name)

            # --- analysisview: jobs from crabschedd ---
            if schedd_type == "crabschedd":
                user = job.get("CRAB_UserHN")
                if user:
                    self._aggregate_analysisview(snap["analysisview"], job,
                                                 status, cpus, user,
                                                 schedd_name)

        # --- Summary ads → globalview pool-wide, poolview ---
        self._process_summary_ads(snap, summary_ads)

        # --- Factory data → factoryview, globalview ---
        self._process_factory_data(snap, factory_data)

        self.snapshot = snap
        self.updated = time.time()

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
                "schedds": {},
            },
            "analysisview": {
                "workflows": {},
                "sites": {},
                "totals": _zero_counts(),
                "schedds": {},
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
            },
            "poolview": {
                "schedds": {},
                "negotiator": {},
                "user_summary": {},
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
        desired = job.get("DESIRED_Sites")
        prio = _prio_block(job.get("JobPrio"))

        # workflows[request][subtask]["Summary"]
        st = _ensure(view["workflows"], request, subtask, "Summary")
        for k, v in _zero_counts().items():
            st.setdefault(k, 0)
        _add_counts(st, status, cpus)

        # workflows[request][subtask][site] (if running at a site)
        if site and status == 2:
            ss = _ensure(view["workflows"], request, subtask, site)
            for k, v in _zero_counts().items():
                ss.setdefault(k, 0)
            _add_counts(ss, status, cpus)

        # Per-site idle pressure (if idle with desired sites)
        if status == 1 and desired:
            sites = [s.strip() for s in desired.split(",")]
            unique = len(sites) == 1
            for s in sites:
                ss = _ensure(view["workflows"], request, subtask, s)
                for k, v in _zero_counts().items():
                    ss.setdefault(k, 0)
                _add_counts(ss, status, cpus)
                if unique:
                    ss.setdefault("UniquePressure", 0)
                    ss["UniquePressure"] += 1

        # view totals
        _add_counts(view["totals"], status, cpus)

        # per-site totals
        target_site = site if status == 2 else None
        if target_site:
            s = _ensure(view["sites"], target_site)
            for k, v in _zero_counts().items():
                s.setdefault(k, 0)
            _add_counts(s, status, cpus)

        # per-schedd totals
        sd = _ensure(view["schedds"], schedd_name)
        for k, v in _zero_counts().items():
            sd.setdefault(k, 0)
        _add_counts(sd, status, cpus)

        # priority block
        p = _ensure(view["priorities"], prio)
        for k, v in _zero_counts().items():
            p.setdefault(k, 0)
        _add_counts(p, status, cpus)

    def _aggregate_analysisview(self, view, job, status, cpus, user,
                                schedd_name):
        request = job.get("CRAB_ReqName", "unknown")
        site = job.get("MATCH_GLIDEIN_CMSSite")
        desired = job.get("DESIRED_Sites")

        # workflows[user/request]["Summary"]
        wf_key = f"{user}/{request}"
        st = _ensure(view["workflows"], wf_key, "Summary")
        for k, v in _zero_counts().items():
            st.setdefault(k, 0)
        _add_counts(st, status, cpus)

        # Per-site
        if site and status == 2:
            ss = _ensure(view["workflows"], wf_key, site)
            for k, v in _zero_counts().items():
                ss.setdefault(k, 0)
            _add_counts(ss, status, cpus)

        if status == 1 and desired:
            sites = [s.strip() for s in desired.split(",")]
            unique = len(sites) == 1
            for s in sites:
                ss = _ensure(view["workflows"], wf_key, s)
                for k, v in _zero_counts().items():
                    ss.setdefault(k, 0)
                _add_counts(ss, status, cpus)
                if unique:
                    ss.setdefault("UniquePressure", 0)
                    ss["UniquePressure"] += 1

        # view totals
        _add_counts(view["totals"], status, cpus)

        # per-site
        target_site = site if status == 2 else None
        if target_site:
            s = _ensure(view["sites"], target_site)
            for k, v in _zero_counts().items():
                s.setdefault(k, 0)
            _add_counts(s, status, cpus)

        # per-schedd
        sd = _ensure(view["schedds"], schedd_name)
        for k, v in _zero_counts().items():
            sd.setdefault(k, 0)
        _add_counts(sd, status, cpus)

    def _aggregate_globalview(self, view, job, status, cpus):
        owner = job.get("Owner", "unknown")
        schedd_name = job.get("_schedd", "unknown")
        site = job.get("MATCH_GLIDEIN_CMSSite")

        # Task identifier fallback chain
        dagman_id = job.get("DAGManJobId")
        task = (job.get("CRAB_ReqName")
                or job.get("WMAgent_RequestName")
                or (f"{schedd_name}#{dagman_id}" if dagman_id else None)
                or job.get("SubmitFile")
                or "unknown")

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
            for k, v in _zero_counts().items():
                ss.setdefault(k, 0)
            _add_counts(ss, status, cpus)

        # view totals (idle + running only, matching other views)
        if status in (1, 2):
            _add_counts(view["totals"], status, cpus)

        # per-site totals
        if site and status == 2:
            s = _ensure(view["sites"], site)
            for k, v in _zero_counts().items():
                s.setdefault(k, 0)
            _add_counts(s, status, cpus)

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

        # Schedd health ads → poolview
        for name, health in summary_ads.get("schedd_health", {}).items():
            sd = _ensure(snap["poolview"], "schedds", name)
            sd["TotalRunningJobs"] = health.get("TotalRunningJobs", 0)
            sd["TotalIdleJobs"] = health.get("TotalIdleJobs", 0)
            sd["TotalHeldJobs"] = health.get("TotalHeldJobs", 0)
            sd["MaxJobsRunning"] = health.get("MaxJobsRunning", 0)
            sd["CMSGWMS_Type"] = health.get("CMSGWMS_Type", "unknown")
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
        for job in history_jobs:
            raw_exit = job.get("ExitCode")
            if raw_exit is None:
                continue
            completion = job.get("CompletionDate")
            if not completion:
                continue
            minute = int(completion) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET
            schedd_type = job.get("_schedd_type", "unknown")
            schedd_name = job.get("_schedd", "unknown")
            site = job.get("MATCH_GLIDEIN_CMSSite", "unknown")
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
                self._add_exit_detail("prodview", code_str, minute,
                                      request, site, "cmst1")

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
                    self._add_exit_detail("analysisview", code_str, minute,
                                          wf_key, site, user)

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
            task = (job.get("CRAB_ReqName")
                    or job.get("WMAgent_RequestName")
                    or ("{}#{}".format(schedd_name, dagman_id)
                        if dagman_id else None)
                    or job.get("SubmitFile")
                    or "unknown")
            gv_key = "{}/{}".format(owner, task)
            bucket = _ensure(self.exit_codes, "globalview", gv_key)
            bucket.setdefault(minute, {})
            bucket[minute].setdefault(gv_code, 0)
            bucket[minute][gv_code] += 1
            self._add_exit_detail("globalview", gv_code, minute,
                                  gv_key, site, owner)

        self._prune_exit_code_window()
        self._prune_exit_detail_window()
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

    def flush_exit_codes(self, cfg):
        """Write exit code JSON files for each view."""
        for view in ("prodview", "analysisview", "globalview"):
            basedir = cfg.get(view, "basedir")
            if not os.path.isdir(basedir):
                continue

            flat = self._flatten_exit_codes(view)

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
            })

            # Completion histogram
            histogram = self._build_completion_histogram(view)
            histogram["updated"] = self.updated
            _atomic_json(os.path.join(basedir, "completion_histogram.json"),
                         histogram)

            total_jobs = w1h.get("total", 0)
            total_failures = w1h.get("failures", 0)

            # Per-workflow exit code files
            for wf, codes in flat.items():
                wf_total = sum(codes.values())
                wf_failures = sum(v for k, v in codes.items() if k != "0")
                wf_dir = os.path.join(basedir, wf.replace("/", os.sep))
                os.makedirs(wf_dir, exist_ok=True)
                _atomic_json(os.path.join(wf_dir, "exit_codes.json"), {
                    "updated": self.updated,
                    "codes": codes,
                    "total": wf_total,
                    "failures": wf_failures,
                    "failure_rate": (round(wf_failures / wf_total, 4)
                                     if wf_total else 0),
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
            total_wf = sum(len(wfs) for wfs in self.exit_codes.values())
            log.info("restored exit code state: %d watermarks, %d workflows",
                     len(self.history_watermarks), total_wf)
        except (json.JSONDecodeError, OSError):
            log.warning("failed to restore exit code state", exc_info=True)

    # --- Step 6: Sparse time-series ---

    def _append_timeseries(self):
        """Append current snapshot values to time-series.

        Only entities with data get new points (sparse).
        """
        now = int(self.updated)
        snap = self.snapshot

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
        for site, counts in snap["prodview"]["sites"].items():
            self._ts_append("prodview", f"site:{site}", counts, now)

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

        # globalview fairshare (placeholder — needs summary ad data)
        fs = snap["globalview"].get("fairshare", {})
        if fs:
            self._ts_append("globalview", "_fairshare", fs, now)

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

    def _ts_append(self, view, entity, counts, now):
        """Append a data point for each non-zero counter."""
        if not any(counts.values()):
            return  # sparse: skip entirely inactive entities
        ts = _ensure(self.timeseries, view, entity)
        for key, val in counts.items():
            if val:
                ts.setdefault(key, []).append({"t": now, "v": val})

    # --- Step 6: Time-series maintenance ---

    def maintenance(self):
        """Downsample, prune old points, clean up inactive entities."""
        now = time.time()
        cutoff_full = now - FULL_RES_SECONDS
        cutoff_hourly = now - HOURLY_RES_SECONDS

        for view, entities in self.timeseries.items():
            dead_entities = []
            for entity, series in entities.items():
                for key, points in series.items():
                    # Separate into: keep full-res, downsample, drop
                    full_res = []
                    to_downsample = []
                    for p in points:
                        if p["t"] >= cutoff_full:
                            full_res.append(p)
                        elif p["t"] >= cutoff_hourly:
                            to_downsample.append(p)
                        # else: older than 365 days — drop

                    # Downsample to hourly buckets
                    hourly = _downsample_hourly(to_downsample)

                    series[key] = hourly + full_res

                # Prune empty series
                if all(len(pts) == 0 for pts in series.values()):
                    dead_entities.append(entity)

            for e in dead_entities:
                del entities[e]

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

            _atomic_json(os.path.join(basedir, "summary.json"), {
                "updated": self.updated,
                "schedds": view_data.get("schedds", {}),
                "totals": view_data.get("totals", {}),
            })

            if view == "analysisview":
                wf_out = {
                    req: {"Summary": subtasks.get("Summary", {})}
                    for req, subtasks in view_data.get("workflows", {}).items()
                }
            else:
                wf_out = {
                    req: {
                        st: data.get("Summary", {})
                        for st, data in subtasks.items()
                    }
                    for req, subtasks in view_data.get("workflows",
                                                       view_data.get("users",
                                                                     {})).items()
                }

            _atomic_json(os.path.join(basedir, "totals.json"), {
                "updated": self.updated,
                "totals": view_data.get("totals", {}),
                "workflows": wf_out,
            })

            _atomic_json(os.path.join(basedir, "site_summary.json"), {
                "updated": self.updated,
                "sites": view_data.get("sites", {}),
            })

            # Per-request detail files (full subtask × site breakdown)
            wf_source = (view_data.get("workflows", {})
                         if view != "globalview"
                         else view_data.get("users", {}))
            for req, subtasks in wf_source.items():
                req_dir = os.path.join(basedir, req.replace("/", os.sep))
                os.makedirs(req_dir, exist_ok=True)
                # Aggregate site counts across subtasks
                req_sites = {}
                for st_name, sites_data in subtasks.items():
                    if st_name == "Summary":
                        continue
                    if not isinstance(sites_data, dict):
                        continue
                    for site_name, counts in sites_data.items():
                        if site_name == "Summary":
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

            _atomic_json(os.path.join(poolview_dir, "summary.json"), {
                "updated": self.updated,
                "schedds": pv.get("schedds", {}),
                "negotiator": gv.get("negotiator", {}),
                "user_summary": gv.get("user_summary", {}),
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
        """Write time-series JSON files to disk."""
        for view in ("prodview", "analysisview", "globalview",
                      "poolview", "factoryview"):
            basedir = cfg.get(view, "basedir")
            if not os.path.isdir(basedir):
                continue
            ts_dir = os.path.join(basedir, "timeseries")
            os.makedirs(ts_dir, exist_ok=True)

            entities = self.timeseries.get(view, {})
            for entity, series in entities.items():
                safe_name = entity.replace("/", "_").replace(":", "_")
                path = os.path.join(ts_dir, f"{safe_name}.json")
                _atomic_json(path, {
                    "updated": self.updated,
                    "series": series,
                })

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
                    entity = fname[:-5]  # strip .json
                    self.timeseries[view][entity] = data.get("series", {})
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


def _downsample_hourly(points):
    """Average points into hourly buckets."""
    if not points:
        return []
    buckets = {}
    for p in points:
        hour = p["t"] // 3600 * 3600
        if hour not in buckets:
            buckets[hour] = []
        buckets[hour].append(p["v"])
    result = []
    for hour in sorted(buckets):
        vals = buckets[hour]
        avg = sum(vals) / len(vals)
        result.append({"t": hour, "v": round(avg, 1)})
    return result


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
