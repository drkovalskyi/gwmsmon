"""HTCondor query layer.

Three query types that together collect everything all views need
in a single collection round:

  4a. Job-level queries — parallel schedd queries for individual jobs
  4b. Summary ad queries — collector queries for pool-wide data
  4c. Factory XML feeds — HTTP fetch of factory status

All classad values are converted to plain Python at the boundary.
"""

import gc
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import htcondor

from gwmsmon.convert import classad_to_python, convert_ad

log = logging.getLogger(__name__)

# Union of all attributes needed across all views (SPEC 4.4)
JOB_PROJECTION = [
    "JobStatus",
    "JobUniverse",
    "RequestCpus",
    "RequestMemory",
    "DESIRED_Sites",
    "MATCH_GLIDEIN_CMSSite",
    "Owner",
    "EnteredCurrentStatus",
    "AccountingGroup",
    "WMAgent_RequestName",
    "WMAgent_SubTaskName",
    "JobPrio",
    "CRAB_UserHN",
    "CRAB_ReqName",
    "QDate",
    "CRAB_UserWebDir",
    "DAGManJobId",
    "SubmitFile",
    "DAG_NodesTotal",
    "DAG_NodesDone",
    "DAG_NodesFailed",
    "DAG_NodesQueued",
    "DAG_NodesReady",
]

# Projection for history queries (exit code collection, SPEC 4.5)
HISTORY_PROJECTION = [
    "ExitCode",
    "CompletionDate",
    "WMAgent_RequestName",
    "CRAB_UserHN",
    "CRAB_ReqName",
    "Owner",
    "DAGManJobId",
    "SubmitFile",
]


def get_schedds(pool):
    """Query collector for schedd ads.

    Returns a list of (schedd_ad, schedd_name, schedd_type) tuples.
    """
    collector = htcondor.Collector(pool)
    ads = collector.query(
        htcondor.AdTypes.Schedd,
        projection=["Name", "MyAddress", "CMSGWMS_Type",
                     "ScheddIpAddr", "Machine"],
    )
    result = []
    for ad in ads:
        name = classad_to_python(ad.get("Name", "unknown"))
        stype = classad_to_python(ad.get("CMSGWMS_Type", "unknown"))
        result.append((ad, name, stype))
    return result


def query_schedd(schedd_ad, projection=None):
    """Query a single schedd for all jobs. No constraint — fetch all
    jobs regardless of status. Each view filters in aggregation.

    Returns a list of plain Python dicts.

    Critical: classad objects pin C++ memory. Convert all ads to plain
    Python immediately and delete the raw result before returning.
    """
    if projection is None:
        projection = JOB_PROJECTION
    schedd = htcondor.Schedd(schedd_ad)
    raw = schedd.query(projection=projection)
    jobs = []
    for ad in raw:
        jobs.append(convert_ad(ad, projection))
    del raw
    gc.collect()
    return jobs


def query_schedds_parallel(schedd_info, projection=None, max_workers=12):
    """Query all schedds in parallel for job classads.

    schedd_info: list of (schedd_ad, schedd_name, schedd_type) from
                 get_schedds()

    Returns a list of plain Python dicts, each tagged with _schedd
    (name) and _schedd_type.

    Failed schedd queries are skipped with a warning — one bad schedd
    never blocks the entire collection cycle.
    """
    if projection is None:
        projection = JOB_PROJECTION

    all_jobs = []
    failed = []

    def _query_one(ad, name, stype):
        jobs = query_schedd(ad, projection)
        for job in jobs:
            job["_schedd"] = name
            job["_schedd_type"] = stype
        return jobs

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for ad, name, stype in schedd_info:
            f = pool.submit(_query_one, ad, name, stype)
            futures[f] = name

        for f in as_completed(futures):
            name = futures[f]
            try:
                result = f.result()
                all_jobs.extend(result)
                del result
            except Exception:
                log.warning("failed to query schedd %s, skipping", name,
                            exc_info=True)
                failed.append(name)

    # Force GC to release any lingering classad C++ memory
    gc.collect()

    if failed:
        log.warning("skipped %d failed schedds: %s", len(failed),
                    ", ".join(failed))
    return all_jobs


# --- 4a-hist. Exit code collection via schedd.history() ---

def query_schedd_history(schedd_ad, since_time, projection=None):
    """Query a single schedd for recently completed jobs.

    Uses the since parameter as a stop condition — history is scanned
    most-recent-first and stops when CompletionDate drops below
    since_time.
    """
    if projection is None:
        projection = HISTORY_PROJECTION
    schedd = htcondor.Schedd(schedd_ad)
    since_expr = "CompletionDate < {}".format(int(since_time))
    jobs = []
    for ad in schedd.history(
        constraint="true",
        projection=projection,
        match=-1,
        since=since_expr,
    ):
        jobs.append(convert_ad(ad, projection))
    gc.collect()
    return jobs


def query_history_parallel(schedd_info, watermarks, default_since=300,
                           projection=None, max_workers=12):
    """Query all schedds history in parallel for recently completed jobs.

    schedd_info: list of (schedd_ad, schedd_name, schedd_type)
    watermarks: dict of {schedd_name: last_query_timestamp}
    default_since: seconds back from now for schedds with no watermark

    Returns (history_jobs, new_watermarks).
    Failed schedd queries keep their old watermark.
    """
    if projection is None:
        projection = HISTORY_PROJECTION

    now = time.time()
    all_jobs = []
    failed = []
    new_watermarks = {}

    def _query_one(ad, name, stype):
        since_time = watermarks.get(name, now - default_since)
        jobs = query_schedd_history(ad, since_time, projection)
        for job in jobs:
            job["_schedd"] = name
            job["_schedd_type"] = stype
        return jobs

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for ad, name, stype in schedd_info:
            f = pool.submit(_query_one, ad, name, stype)
            futures[f] = name

        for f in as_completed(futures):
            name = futures[f]
            try:
                result = f.result()
                all_jobs.extend(result)
                del result
                new_watermarks[name] = now
            except Exception:
                log.warning("failed to query history for schedd %s, skipping",
                            name, exc_info=True)
                failed.append(name)
                if name in watermarks:
                    new_watermarks[name] = watermarks[name]

    gc.collect()

    if failed:
        log.warning("skipped %d failed history queries: %s",
                    len(failed), ", ".join(failed))

    return all_jobs, new_watermarks


# --- 4b. Summary ad queries ---

def query_schedd_summary_ads(pool):
    """Query collector for schedd summary ads (Submitter type).

    Returns a list of plain Python dicts with per-schedd totals.
    """
    collector = htcondor.Collector(pool)
    projection = ["Name", "ScheddName", "RunningJobs", "IdleJobs",
                   "HeldJobs", "CMSGWMS_Type"]
    ads = collector.query(htcondor.AdTypes.Submitter, projection=projection)
    return [convert_ad(ad, projection) for ad in ads]


def query_slot_ads(pool):
    """Query collector for pilot/slot ads.

    Returns a list of plain Python dicts with slot-level information.
    """
    collector = htcondor.Collector(pool)
    projection = ["Name", "Machine", "SlotType", "Cpus", "Memory",
                   "GLIDEIN_CMSSite", "GLIDEIN_ToRetire", "State",
                   "Activity", "TotalSlotCpus", "TotalSlotMemory"]
    ads = collector.query(htcondor.AdTypes.Startd, projection=projection)
    return [convert_ad(ad, projection) for ad in ads]


def query_negotiator_ads(pool):
    """Query collector for negotiator ads.

    Returns a list of plain Python dicts with negotiation metrics.
    """
    collector = htcondor.Collector(pool)
    projection = ["Name", "LastNegotiationCycleDuration0"]
    ads = collector.query(htcondor.AdTypes.Negotiator, projection=projection)
    return [convert_ad(ad, projection) for ad in ads]


# --- 4c. Factory XML feeds ---

def fetch_factory_xml(factory_urls):
    """Fetch factory XML status feeds.

    factory_urls: list of base URLs for factory status pages

    Returns a dict with parsed factory entry data.
    Placeholder — XML parsing to be implemented.
    """
    # TODO: fetch and parse descript.xml and
    # rrd_Status_Attributes.xml from each factory URL
    log.info("factory XML fetch not yet implemented")
    return {}


# --- Orchestration ---

def query_all(cfg):
    """Execute all query types for a single collection round.

    Returns (jobs, summary_ads, factory_data, schedd_info).
    """
    pool = cfg.get("htcondor", "pool")

    t0 = time.time()

    # 4a: job-level queries
    schedd_info = get_schedds(pool)
    log.info("found %d schedds", len(schedd_info))
    jobs = query_schedds_parallel(schedd_info)
    log.info("collected %d jobs in %.1fs", len(jobs), time.time() - t0)

    # 4b: summary ads (can overlap with job queries in the future)
    t1 = time.time()
    summary_ads = {
        "submitters": query_schedd_summary_ads(pool),
        "slots": query_slot_ads(pool),
        "negotiator": query_negotiator_ads(pool),
    }
    log.info("collected summary ads in %.1fs", time.time() - t1)

    # 4c: factory XML
    factory_data = fetch_factory_xml(
        cfg.get("factoryview", "factory_urls", fallback="")
    )

    return jobs, summary_ads, factory_data, schedd_info
