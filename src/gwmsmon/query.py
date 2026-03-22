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
from urllib.request import urlopen
from urllib.error import URLError
from xml.etree import ElementTree

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
    "CONDORA_RequestName",
    "CONDORA_Round",
    "DAGManJobId",
    "SubmitFile",
    "DAG_NodesTotal",
    "DAG_NodesDone",
    "DAG_NodesFailed",
    "DAG_NodesQueued",
    "DAG_NodesReady",
    "CMS_JobType",
    "CMS_RequestType",
    "CMS_CampaignName",
    "CMS_Type",
    "CMSSW_Versions",
    "OriginalMaxWallTimeMins",
    "OriginalMemory",
    "RequestDisk",
]

# Projection for history queries (exit code collection, SPEC 4.5)
HISTORY_PROJECTION = [
    "ExitCode",
    "ExitBySignal",
    "Chirp_WMCore_cmsRun_ExitCode",
    "Chirp_CRAB3_Job_ExitCode",
    "CompletionDate",
    "MATCH_GLIDEIN_CMSSite",
    "WMAgent_RequestName",
    "CRAB_UserHN",
    "CRAB_ReqName",
    "CONDORA_RequestName",
    "CONDORA_Round",
    "Owner",
    "DAGManJobId",
    "SubmitFile",
]


def get_schedds(pool):
    """Query collector for schedd ads.

    Returns a list of (schedd_ad, schedd_name, schedd_type, health) tuples.
    health is a dict with TotalRunningJobs, TotalIdleJobs, TotalHeldJobs,
    MaxJobsRunning from the schedd ad itself.
    """
    collector = htcondor.Collector(pool)
    ads = collector.query(
        htcondor.AdTypes.Schedd,
        projection=["Name", "MyAddress", "CMSGWMS_Type",
                     "ScheddIpAddr", "Machine",
                     "TotalRunningJobs", "TotalIdleJobs",
                     "TotalHeldJobs", "MaxJobsRunning"],
    )
    result = []
    for ad in ads:
        name = classad_to_python(ad.get("Name", "unknown"))
        stype = classad_to_python(ad.get("CMSGWMS_Type", "unknown"))
        health = {
            "TotalRunningJobs": classad_to_python(ad.get("TotalRunningJobs", 0)) or 0,
            "TotalIdleJobs": classad_to_python(ad.get("TotalIdleJobs", 0)) or 0,
            "TotalHeldJobs": classad_to_python(ad.get("TotalHeldJobs", 0)) or 0,
            "MaxJobsRunning": classad_to_python(ad.get("MaxJobsRunning", 0)) or 0,
        }
        result.append((ad, name, stype, health))
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
        for ad, name, stype, _health in schedd_info:
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
        for ad, name, stype, _health in schedd_info:
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


# --- 4b-2. Negotiator accounting ads ---

# Map negotiator Name prefix → site tier label
_NEGOTIATOR_TIERS = {
    "vocms0824": "CERN",
    "NEGOTIATORT1": "T1",
    "NEGOTIATORUS": "US_T2",
}


def negotiator_tier(neg_name):
    """Map a NegotiatorName to a site tier label."""
    for prefix, tier in _NEGOTIATOR_TIERS.items():
        if neg_name.startswith(prefix):
            return tier
    return "nonUS_T2_T3"


def query_accounting_ads(negotiator_collectors):
    """Query Accounting ads from negotiator collector hosts.

    Args:
        negotiator_collectors: comma-separated collector hostnames

    Returns list of plain Python dicts with accounting attributes.
    """
    hosts = [h.strip() for h in negotiator_collectors.split(",") if h.strip()]
    all_ads = []
    for host in hosts:
        try:
            collector = htcondor.Collector(host)
            ads = collector.query(htcondor.AdTypes.Accounting)
            all_ads.extend(convert_ad(ad) for ad in ads)
            log.info("accounting ads from %s: %d", host, len(ads))
        except Exception:
            log.warning("failed to query accounting ads from %s", host,
                        exc_info=True)
    return all_ads


# --- 4c. Factory XML feeds ---

def _parse_factory_urls(factory_urls_str):
    """Parse 'NAME=URL,NAME=URL' into list of (name, url) tuples."""
    result = []
    for part in factory_urls_str.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            name, url = part.split("=", 1)
            result.append((name.strip(), url.strip()))
        else:
            result.append((part, part))
    return result


def _fetch_xml(url, timeout=30):
    """Fetch and parse an XML document from a URL.

    Note: xml.etree.ElementTree does not expand external entities,
    so it is safe against XXE by default (unlike lxml).
    """
    import ssl
    ctx = ssl.create_default_context() if url.startswith("https") else None
    resp = urlopen(url, timeout=timeout, context=ctx)
    return ElementTree.parse(resp)


def _parse_descript_xml(tree):
    """Parse descript.xml into {entry_name: config_dict}.

    Attributes are XML attributes on the <attributes> child element
    (e.g., GLIDEIN_CMSSite="T1_DE_KIT"). Limits come from the
    <descript> child element (PerEntryMaxHeld, PerEntryMaxIdle, etc.).
    """
    entries = {}
    for entry_el in tree.iter("entry"):
        name = entry_el.get("name")
        if not name:
            continue
        cfg = {}
        # <attributes GLIDEIN_CMSSite="..." GLIDEIN_CPUS="..." .../>
        attrs_el = entry_el.find("attributes")
        if attrs_el is not None:
            cfg.update(attrs_el.attrib)
        # <descript PerEntryMaxHeld="80" PerEntryMaxIdle="80" .../>
        descript_el = entry_el.find("descript")
        if descript_el is not None:
            cfg.update(descript_el.attrib)
        entries[name] = cfg
    return entries


def _parse_status_xml(tree):
    """Parse rrd_Status_Attributes.xml into per-entry status.

    Returns {entry_name: {running, idle, held}} using the period=7200
    bucket and summing across frontends containing 'cmspilot'.
    Status values are XML attributes on <period> elements
    (e.g., StatusRunning="1.35", StatusIdle="9.97").
    """
    entries = {}
    for entry_el in tree.iter("entry"):
        name = entry_el.get("name")
        if not name:
            continue
        running = 0.0
        idle = 0.0
        held = 0.0
        for frontend_el in entry_el.iter("frontend"):
            fname = frontend_el.get("name", "")
            if "cmspilot" not in fname:
                continue
            for period_el in frontend_el.iter("period"):
                if period_el.get("name") != "7200":
                    continue
                running += float(period_el.get("StatusRunning", "0"))
                idle += float(period_el.get("StatusIdle", "0"))
                held += float(period_el.get("StatusHeld", "0"))
        entries[name] = {
            "running": int(round(running)),
            "idle": int(round(idle)),
            "held": int(round(held)),
        }
    return entries


def fetch_factory_xml(factory_urls_str, timeout=30):
    """Fetch factory XML status feeds.

    factory_urls_str: 'NAME=URL,NAME=URL' format
    timeout: per-URL fetch timeout in seconds

    Returns {site: {entry: {factory: {running, idle, held, ...}}}}.
    Dead URLs produce warnings but don't block.
    """
    urls = _parse_factory_urls(factory_urls_str)
    if not urls:
        return {}

    result = {}  # {site: {entry: {factory: {...}}}}
    errors = []

    for factory_name, base_url in urls:
        base_url = base_url.rstrip("/")
        try:
            descript_tree = _fetch_xml(
                base_url + "/descript.xml", timeout)
            status_tree = _fetch_xml(
                base_url + "/rrd_Status_Attributes.xml", timeout)
        except (URLError, ElementTree.ParseError, OSError) as exc:
            log.warning("factory %s fetch failed: %s", factory_name, exc)
            errors.append({"factory": factory_name, "error": str(exc)})
            continue

        descript = _parse_descript_xml(descript_tree)
        status = _parse_status_xml(status_tree)

        for entry_name, cfg in descript.items():
            site = cfg.get("GLIDEIN_CMSSite", "Unknown")
            entry_status = status.get(entry_name, {})

            entry_data = {
                "running": entry_status.get("running", 0),
                "idle": entry_status.get("idle", 0),
                "held": entry_status.get("held", 0),
                "maxIdle": _int_or(cfg.get("PerEntryMaxIdle"), 0),
                "maxHeld": _int_or(cfg.get("PerEntryMaxHeld"), 0),
                "maxMem": _int_or(cfg.get("GLIDEIN_MaxMemMBs"), 0),
                "maxWalltime": _int_or(cfg.get("GLIDEIN_Max_Walltime"), 0),
                "maxCpus": _int_or(cfg.get("GLIDEIN_CPUS"), 0),
            }

            site_entries = result.setdefault(site, {})
            entry_factories = site_entries.setdefault(entry_name, {})
            entry_factories[factory_name] = entry_data

        log.info("factory %s: %d entries parsed", factory_name,
                 len(descript))

    return {"sites": result, "errors": errors}


def _int_or(val, default=0):
    """Convert to int, return default on failure."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


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
        "schedd_health": {
            name: {**health, "CMSGWMS_Type": stype}
            for (_, name, stype, health) in schedd_info
        },
    }
    log.info("collected summary ads in %.1fs", time.time() - t1)

    # 4c: factory XML
    fetch_timeout = int(cfg.get("factoryview", "fetch_timeout",
                                fallback="30"))
    factory_data = fetch_factory_xml(
        cfg.get("factoryview", "factory_urls", fallback=""),
        timeout=fetch_timeout,
    )

    return jobs, summary_ads, factory_data, schedd_info
