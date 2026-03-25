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

# Retention: full resolution for 3.5 days, hourly for 365 days
FULL_RES_SECONDS = int(3.5 * 86400)  # 302400
HOURLY_RES_SECONDS = 30 * 86400      # 2592000
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


class State:
    def __init__(self):
        self.snapshot = {}
        self.timeseries = {}
        self.updated = 0
        self.exit_codes = {}         # {view: {workflow: {minute_ts: {code: count}}}}
        self.exit_codes_by_site = {} # {view: {workflow: {site: {minute_ts: {code: count}}}}}
        self.exit_code_detail = {}   # {view: {code: {workflow: count, site: count, ...}}}
        self.failed_job_records = {} # {view: {site: {workflow: [record, ...]}}}
        self.efficiency = {}         # {view: {wf: {ts: {cpu, wall_cpus, slot_ok, slot_all}}}}
        self.efficiency_by_site = {} # {view: {wf: {site: {ts: {cpu, wall_cpus, slot_ok, slot_all}}}}}
        self.efficiency_lifetime = {} # {wf: {cpu, wall_cpus, slot_ok, slot_all}}
        self.history_watermarks = {} # {schedd_name: timestamp}

    def update(self, jobs, summary_ads, factory_data, accounting_ads=None):
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

        # Copy fairshare from globalview → poolview (single source of truth)
        snap["poolview"]["fairshare"] = snap["globalview"]["fairshare"]

        # --- Summary ads → globalview pool-wide, poolview ---
        self._process_summary_ads(snap, summary_ads)

        # --- Factory data → factoryview, globalview ---
        self._process_factory_data(snap, factory_data)

        # --- Accounting ads → globalview ---
        self._process_accounting_ads(snap, accounting_ads or [])

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
                "accounting": {},
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
        desired = job.get("DESIRED_Sites")
        prio = _prio_block(job.get("JobPrio"))

        # workflows[request][subtask]["Summary"]
        st = _ensure(view["workflows"], request, subtask, "Summary")
        for k, v in _zero_counts().items():
            st.setdefault(k, 0)
        _add_counts(st, status, cpus)

        # per-request metadata (capture once from first job)
        req_meta = _ensure(view["workflows"], request, "_metadata")
        if not req_meta:
            for attr in ("CMS_JobType", "CMS_RequestType",
                         "CMS_CampaignName", "CMS_Type",
                         "CMSSW_Versions", "OriginalMaxWallTimeMins",
                         "OriginalMemory", "RequestDisk", "Owner",
                         "DESIRED_Sites"):
                val = job.get(attr)
                if val is not None:
                    req_meta[attr] = val

        # per-subtask priority
        st_prio = _ensure(view["workflows"], request, subtask, "_priority")
        st_prio.setdefault(prio, 0)
        st_prio[prio] += 1
        st_prio.setdefault("_sum", 0)
        st_prio.setdefault("_count", 0)
        st_prio["_sum"] += job.get("JobPrio") or 0
        st_prio["_count"] += 1

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

        # per-site totals (running)
        if site and status == 2:
            s = _ensure(view["sites"], site)
            for k in _zero_counts():
                s.setdefault(k, 0)
            _add_counts(s, status, cpus)

        # per-site idle pressure (matching idle per desired site)
        if status == 1 and desired:
            sites_list = [s.strip() for s in desired.split(",")]
            unique = len(sites_list) == 1
            for s in sites_list:
                sv = _ensure(view["sites"], s)
                for k in _zero_counts():
                    sv.setdefault(k, 0)
                sv["MatchingIdle"] += 1
                sv["CpusPending"] += cpus
                sv.setdefault("UniquePressure", 0)
                if unique:
                    sv["UniquePressure"] += 1

        # per-schedd totals
        sd = _ensure(view["schedds"], schedd_name)
        for k in _zero_counts():
            sd.setdefault(k, 0)
        _add_counts(sd, status, cpus)

        # priority block
        p = _ensure(view["priorities"], prio)
        for k, v in _zero_counts().items():
            p.setdefault(k, 0)
        _add_counts(p, status, cpus)

        # per-request priority tracking
        req_prio = _ensure(view["workflows"], request, "_priority")
        req_prio.setdefault("_jobs", {})
        req_prio["_jobs"].setdefault(prio, 0)
        req_prio["_jobs"][prio] += 1
        req_prio.setdefault("_sum", 0)
        req_prio.setdefault("_count", 0)
        job_prio_val = job.get("JobPrio") or 0
        req_prio["_sum"] += job_prio_val
        req_prio["_count"] += 1

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

        # per-site totals (running)
        if site and status == 2:
            s = _ensure(view["sites"], site)
            for k in _zero_counts():
                s.setdefault(k, 0)
            _add_counts(s, status, cpus)

        # per-site idle pressure
        if status == 1 and desired:
            sites_list = [s.strip() for s in desired.split(",")]
            unique_site = len(sites_list) == 1
            for s in sites_list:
                sv = _ensure(view["sites"], s)
                for k in _zero_counts():
                    sv.setdefault(k, 0)
                sv["MatchingIdle"] += 1
                sv["CpusPending"] += cpus
                sv.setdefault("UniquePressure", 0)
                if unique_site:
                    sv["UniquePressure"] += 1

        # per-schedd
        sd = _ensure(view["schedds"], schedd_name)
        for k, v in _zero_counts().items():
            sd.setdefault(k, 0)
        _add_counts(sd, status, cpus)

    def _aggregate_globalview(self, view, job, status, cpus):
        owner = job.get("Owner", "unknown")
        schedd_name = job.get("_schedd", "unknown")
        site = job.get("MATCH_GLIDEIN_CMSSite")
        desired = job.get("DESIRED_Sites")

        # Task identifier fallback chain
        dagman_id = job.get("DAGManJobId")
        condora_req = job.get("CONDORA_RequestName")
        task = (job.get("CRAB_ReqName")
                or job.get("WMAgent_RequestName")
                or condora_req
                or (f"{schedd_name}#{dagman_id}" if dagman_id else None)
                or job.get("SubmitFile")
                or "unknown")
        # CONDORA subtask: append round to task name
        condora_round = job.get("CONDORA_Round")
        if condora_req and condora_round is not None:
            task = f"{task}/{condora_round}"

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

        # per-site totals (running)
        if site and status == 2:
            s = _ensure(view["sites"], site)
            for k in _zero_counts():
                s.setdefault(k, 0)
            _add_counts(s, status, cpus)

        # per-site idle pressure
        if status == 1 and desired:
            sites_list = [s.strip() for s in desired.split(",")]
            unique_site = len(sites_list) == 1
            for s in sites_list:
                sv = _ensure(view["sites"], s)
                for k in _zero_counts():
                    sv.setdefault(k, 0)
                sv["MatchingIdle"] += 1
                sv["CpusPending"] += cpus
                sv.setdefault("UniquePressure", 0)
                if unique_site:
                    sv["UniquePressure"] += 1

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
            for k in _zero_counts():
                fs.setdefault(k, 0)
            _add_counts(fs, status, cpus)

        # per-user accounting groups (count jobs per category)
        user_acct = _ensure(view["users"], owner, "_acct")
        user_acct.setdefault(category, 0)
        user_acct[category] += 1

        # per-user per-group resource counts (idle + running)
        if status in (1, 2):
            gs = _ensure(view["users"], owner, "_group_stats", category)
            for k in _zero_counts():
                gs.setdefault(k, 0)
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

    def _process_accounting_ads(self, snap, ads):
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
                    if len(rec_list) < 200:
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
            site_bucket = _ensure(self.exit_codes_by_site,
                                  "globalview", gv_key, site)
            site_bucket.setdefault(minute, {})
            site_bucket[minute].setdefault(gv_code, 0)
            site_bucket[minute][gv_code] += 1
            self._add_exit_detail("globalview", gv_code, minute,
                                  gv_key, site, owner)
            self._add_efficiency("globalview", gv_key, site,
                                 minute, gv_code, job)

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
        # Prune failed job records older than 1h
        cutoff_1h = int(time.time()) - EXIT_CODE_WINDOWS["1h"]
        for view, sites in self.failed_job_records.items():
            dead_sites = []
            for site, wfs in sites.items():
                dead_wfs = []
                for wf, records in wfs.items():
                    wfs[wf] = [r for r in records if r["ts"] >= cutoff_1h]
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

            total_jobs = w1h.get("total", 0)
            total_failures = w1h.get("failures", 0)

            now_site = int(time.time()) // EXIT_CODE_BUCKET * EXIT_CODE_BUCKET

            # Per-workflow exit code files
            wf_completion = {}  # {wf: {total, failures, failure_rate}} for 1h
            for wf, codes in flat.items():
                if not _safe_name(wf):
                    continue
                wf_total = sum(codes.values())
                wf_failures = sum(v for k, v in codes.items() if k != "0")
                wf_dir = os.path.join(basedir, wf.replace("/", os.sep))
                os.makedirs(wf_dir, exist_ok=True)
                # Per-site completion stats for this workflow
                site_data = self.exit_codes_by_site.get(view, {}).get(wf, {})
                wf_sites_ec = {}
                for site, buckets in site_data.items():
                    site_windows = {}
                    for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                        cutoff = now_site - wsec
                        total = failures = 0
                        for ts, scodes in buckets.items():
                            if ts < cutoff:
                                continue
                            for code, cnt in scodes.items():
                                total += cnt
                                if code != "0":
                                    failures += cnt
                        if total:
                            eff_b = self.efficiency_by_site.get(
                                view, {}).get(wf, {}).get(site, {})
                            eff = self._compute_efficiency(eff_b, cutoff)
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
                wf_windows = {}
                for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                    cutoff = now_site - wsec
                    wcodes = {}
                    for ts, tcodes in wf_buckets.items():
                        if ts < cutoff:
                            continue
                        for code, cnt in tcodes.items():
                            wcodes[code] = wcodes.get(code, 0) + cnt
                    wtotal = sum(wcodes.values())
                    wfail = sum(v for k, v in wcodes.items() if k != "0")
                    wf_windows[wlabel] = {
                        "total": wtotal, "failures": wfail,
                        "failure_rate": (round(wfail / wtotal, 4)
                                         if wtotal else 0),
                        "codes": wcodes,
                    }
                w1h = wf_windows.get("1h", {})
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
                if w1h.get("total", 0):
                    eff1h = wf_eff.get("1h", {})
                    lt = self.efficiency_lifetime.get(wf)
                    lt_re = (round(lt["cpu"] / lt["wall_cpus"], 4)
                             if lt and lt.get("wall_cpus") else 0)
                    lt_pe = (round(lt["slot_ok"] / lt["slot_all"], 4)
                             if lt and lt.get("slot_all") else 0)
                    wf_completion[wf] = {
                        "total": w1h["total"],
                        "failures": w1h["failures"],
                        "failure_rate": w1h["failure_rate"],
                        "running_eff": eff1h.get("running_eff", 0),
                        "processing_eff": eff1h.get("processing_eff", 0),
                        "lt_running_eff": lt_re,
                        "lt_processing_eff": lt_pe,
                    }
                _atomic_json(os.path.join(wf_dir, "exit_codes.json"), {
                    "updated": self.updated,
                    "codes": w1h.get("codes", {}),
                    "total": w1h.get("total", 0),
                    "failures": w1h.get("failures", 0),
                    "failure_rate": w1h.get("failure_rate", 0),
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

            # View-level per-site completion stats (aggregated across all workflows)
            # Also build completion cross-reference: {wf: {site: [done, fail]}}
            view_site_ec = {}
            site_codes_1h = {}  # {site: {code: count}} for 1h window
            completion_xref = {}  # {wf: {site: [done_1h, fail_1h]}}
            cutoff_1h = now_site - EXIT_CODE_WINDOWS["1h"]
            for wf, site_data in self.exit_codes_by_site.get(view, {}).items():
                for site, buckets in site_data.items():
                    for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                        cutoff = now_site - wsec
                        for ts, codes in buckets.items():
                            if ts < cutoff:
                                continue
                            sw = (view_site_ec.setdefault(site, {})
                                  .setdefault(wlabel,
                                              {"total": 0, "failures": 0}))
                            for code, cnt in codes.items():
                                sw["total"] += cnt
                                if code != "0":
                                    sw["failures"] += cnt
                    # Per-code counts for 1h window + completion cross-ref
                    wf_site_done = 0
                    wf_site_fail = 0
                    for ts, codes in buckets.items():
                        if ts < cutoff_1h:
                            continue
                        sc = site_codes_1h.setdefault(site, {})
                        for code, cnt in codes.items():
                            sc.setdefault(code, 0)
                            sc[code] += cnt
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

            # Per-site per-request completion stats + efficiency
            site_req_ec = {}
            for wf, site_data in self.exit_codes_by_site.get(view, {}).items():
                for site, buckets in site_data.items():
                    for wlabel, wsec in EXIT_CODE_WINDOWS.items():
                        cutoff = now_site - wsec
                        total = failures = 0
                        for ts, codes in buckets.items():
                            if ts < cutoff:
                                continue
                            for code, cnt in codes.items():
                                total += cnt
                                if code != "0":
                                    failures += cnt
                        if total:
                            rw = (site_req_ec.setdefault(site, {})
                                  .setdefault(wf, {}))
                            eff_b = self.efficiency_by_site.get(
                                view, {}).get(wf, {}).get(site, {})
                            eff = self._compute_efficiency(
                                eff_b, cutoff)
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
                    for r in records:
                        rec = dict(r)
                        rec["site"] = site
                        rec["request"] = wf
                        all_failed.append(rec)
            all_failed.sort(key=lambda x: -x.get("ts", 0))
            _atomic_json(os.path.join(basedir, "failed_jobs.json"), {
                "updated": self.updated,
                "jobs": all_failed[:5000],
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
        for block, counts in snap["prodview"]["priorities"].items():
            self._ts_append("prodview", f"priority:{block}", counts, now)

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

    def _ts_append(self, view, entity, counts, now):
        """Append a data point for each non-zero counter."""
        if not any(counts.values()):
            return  # sparse: skip entirely inactive entities
        ts = _ensure(self.timeseries, view, entity)
        for key, val in counts.items():
            if val:
                if key not in ts:
                    ts[key] = {"t": [], "v": []}
                ts[key]["t"].append(now)
                ts[key]["v"].append(val)

    # --- Step 6: Time-series maintenance ---

    def maintenance(self):
        """Downsample, prune old points, clean up inactive entities."""
        now = time.time()
        cutoff_full = now - FULL_RES_SECONDS
        cutoff_hourly = now - HOURLY_RES_SECONDS

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

            summary_out = {
                "updated": self.updated,
                "schedds": view_data.get("schedds", {}),
                "totals": view_data.get("totals", {}),
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
                            st_count = st_prio.get("_count", 0)
                            st_out["_prio"] = (st_prio["_sum"] // st_count
                                               if st_count else 0)
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
                        total_jobs = prio_data.get("_count", 0)
                        avg_prio = (prio_data["_sum"] // total_jobs
                                    if total_jobs else 0)
                        dominant = (max(jobs_by_block,
                                        key=jobs_by_block.get)
                                    if jobs_by_block else "B7")
                        wf_out[req]["_priority"] = {
                            "block": dominant,
                            "prio": avg_prio,
                            "blocks": jobs_by_block,
                        }

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

            _atomic_json(os.path.join(poolview_dir, "summary.json"), {
                "updated": self.updated,
                "schedds": pv.get("schedds", {}),
                "negotiator": gv.get("negotiator", {}),
                "user_summary": gv.get("user_summary", {}),
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
                    "entity": entity,
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
