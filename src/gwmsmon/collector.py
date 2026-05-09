"""Long-running collection process for HTCondor pool data."""

import argparse
import ctypes
import fcntl
import gc
import logging
import os
import resource
import signal
import sys
import time
import tracemalloc

# Try to bind malloc_trim(0) — releases freed heap pages back to OS.
# pymalloc/glibc retain pages in arenas; the parent process's RSS
# climbs over many ProcessPool cycles even though Python objects
# have been freed. malloc_trim forces a release; if RSS drops after
# the call, the growth is allocator fragmentation, not a true leak.
try:
    _libc = ctypes.CDLL("libc.so.6", use_errno=True)
    _libc.malloc_trim.argtypes = [ctypes.c_size_t]
    _libc.malloc_trim.restype = ctypes.c_int
except (OSError, AttributeError):
    _libc = None


def _malloc_trim():
    if _libc is not None:
        try:
            _libc.malloc_trim(0)
        except Exception:
            pass


def _rss_mb():
    """Current RSS in MB (live, not peak — getrusage gives peak only)."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

from gwmsmon import config
from gwmsmon.query import query_all, query_history_parallel, query_accounting_ads
from gwmsmon.state import State
from gwmsmon.status_history import StatusHistory

log = logging.getLogger("gwmsmon")

COOLDOWN = 60  # seconds between cycles (fixed initially)
TS_FLUSH_INTERVAL = 5  # flush time-series every N cycles
MAINTENANCE_INTERVAL = 60  # run maintenance every N cycles

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("received signal %d, shutting down after current cycle", signum)
    _shutdown = True


def main():
    parser = argparse.ArgumentParser(
        description="Collect HTCondor pool data for gwmsmon views"
    )
    parser.add_argument(
        "--config", default="/etc/gwmsmon.conf",
        help="path to configuration file"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="run a single collection cycle and exit"
    )
    parser.add_argument(
        "--check", action="store_true",
        help=("dry-run one cycle (no lock, no persistence) for use as "
              "a deploy canary; safe to run alongside the live collector")
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="enable verbose logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # --check: data-pipeline smoke test, no lock, no flush.
    # Exits non-zero on any pipeline failure; deploy.sh aborts.
    if args.check:
        cfg = config.load(args.config)
        log.info("--- canary check ---")
        t0 = time.time()
        try:
            state = State()
            jobs, summary_ads, factory_data, schedd_info = query_all(cfg)
            neg_hosts = cfg.get("htcondor", "negotiator_collectors",
                                fallback="")
            accounting_ads = query_accounting_ads(neg_hosts) if neg_hosts else []
            state.update(jobs, summary_ads, factory_data, accounting_ads)
            del jobs, summary_ads, factory_data, accounting_ads
            gc.collect()
            history_jobs, _ = query_history_parallel(schedd_info, {})
            state.update_exit_codes(history_jobs)
        except Exception:
            log.error("canary failed", exc_info=True)
            sys.exit(1)
        log.info("canary OK in %.1fs", time.time() - t0)
        sys.exit(0)

    # Ensure files are world-readable for Apache
    os.umask(0o022)

    # Exclusive lock to prevent concurrent instances
    lockfile = "/var/lib/gwmsmon/.collector.lock"
    lock_fd = open(lockfile, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.error("another collector instance is running (lock: %s)", lockfile)
        sys.exit(1)

    cfg = config.load(args.config)
    state = State()
    state.restore(cfg)
    status_history = StatusHistory()
    status_history.restore(cfg.get("prodview", "basedir"))

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Memory leak diagnostic: snapshot allocations between cycles
    # to identify what's growing. Runs continuously; small overhead.
    tracemalloc.start(10)
    tm_prev = None

    cycle = 0
    while not _shutdown:
        cycle += 1
        t0 = time.time()
        log.info("--- cycle %d ---", cycle)

        rss_phase = {"start": _rss_mb()}
        try:
            jobs, summary_ads, factory_data, schedd_info = query_all(cfg)
            rss_phase["after_query_all"] = _rss_mb()
            neg_hosts = cfg.get("htcondor", "negotiator_collectors",
                                fallback="")
            accounting_ads = query_accounting_ads(neg_hosts) if neg_hosts else []
            rss_phase["after_acct_ads"] = _rss_mb()
            state.update(jobs, summary_ads, factory_data, accounting_ads)
            rss_phase["after_state_update"] = _rss_mb()
            del jobs, summary_ads, factory_data, accounting_ads
            for _ in range(3):
                gc.collect()
            rss_phase["after_del_jobs_gc"] = _rss_mb()
            _malloc_trim()
            rss_phase["after_malloc_trim_1"] = _rss_mb()

            # Exit code collection via schedd.history()
            t_hist = time.time()
            history_jobs, new_watermarks = query_history_parallel(
                schedd_info, state.history_watermarks
            )
            state.history_watermarks = new_watermarks
            hist_count = len(history_jobs)
            state.update_exit_codes(history_jobs)
            del history_jobs, schedd_info
            for _ in range(3):
                gc.collect()
            rss_phase["after_history"] = _rss_mb()
            log.info("history query: %d jobs in %.1fs",
                     hist_count, time.time() - t_hist)

            state._append_timeseries()
            state.flush_snapshot(cfg)
            state.flush_exit_codes(cfg)
            rss_phase["after_flush"] = _rss_mb()

            if cycle % TS_FLUSH_INTERVAL == 0:
                state.flush_timeseries(cfg)
                state.flush_exit_code_state(cfg)

            if cycle % MAINTENANCE_INTERVAL == 0:
                state.maintenance()
                state.prune_dirs(cfg)

            _malloc_trim()
            rss_phase["after_malloc_trim_2"] = _rss_mb()

        except Exception:
            log.error("cycle %d failed", cycle, exc_info=True)
            if args.once:
                sys.exit(1)

        elapsed = time.time() - t0
        rss_mb = _rss_mb()
        ts_entities = sum(len(e) for v in state.timeseries.values()
                          for e in v.values())
        ts_points = sum(len(pts["t"]) for v in state.timeseries.values()
                        for e in v.values() for pts in e.values())
        log.info("cycle %d completed in %.1fs | RSS=%.0fMB | "
                 "ts_entities=%d ts_points=%d",
                 cycle, elapsed, rss_mb, ts_entities, ts_points)
        # Per-phase RSS — diagnose where memory accumulates and whether
        # malloc_trim() releases pages back to the OS.
        log.info("rss_per_phase: " + ", ".join(
            f"{k}={int(v)}MB" for k, v in rss_phase.items()))

        # --- memory leak diagnostic ---
        # Per-state-attribute counts (proxy for size; flat structures
        # are small, nested ones are the suspects).
        ec_minutes = sum(len(b) for view in state.exit_codes.values()
                         for b in view.values())
        ec_site_minutes = sum(len(b)
                              for view in state.exit_codes_by_site.values()
                              for sites in view.values()
                              for b in sites.values())
        ec_subtask_minutes = sum(len(b)
                                 for reqs in state.exit_codes_by_subtask.values()
                                 for b in reqs.values())
        ec_detail_minutes = sum(len(b)
                                for view in state.exit_code_detail.values()
                                for b in view.values())
        eff_buckets = sum(len(b) for view in state.efficiency.values()
                          for b in view.values())
        eff_site_buckets = sum(len(b)
                               for view in state.efficiency_by_site.values()
                               for sites in view.values()
                               for b in sites.values())
        failed_recs = sum(len(recs)
                          for view in state.failed_job_records.values()
                          for sites in view.values()
                          for recs in sites.values())
        log.info("state buckets: exit_codes=%d ec_by_site=%d "
                 "ec_by_subtask=%d ec_detail=%d eff=%d eff_by_site=%d "
                 "failed_recs=%d eff_lifetime=%d",
                 ec_minutes, ec_site_minutes, ec_subtask_minutes,
                 ec_detail_minutes, eff_buckets, eff_site_buckets,
                 failed_recs, len(state.efficiency_lifetime))

        # tracemalloc: top allocators + diff vs prev cycle
        try:
            snap = tracemalloc.take_snapshot()
            top = snap.statistics('lineno')[:8]
            log.info("tracemalloc top 8 (cycle %d):", cycle)
            for stat in top:
                log.info("  %.1fMB (%d blocks) %s",
                         stat.size / 1024 / 1024,
                         stat.count, stat.traceback[0])
            if tm_prev is not None:
                diffs = snap.compare_to(tm_prev, 'lineno')
                grew = [d for d in diffs if d.size_diff > 1024 * 1024][:8]
                if grew:
                    log.info("tracemalloc grew >1MB vs prev cycle:")
                    for stat in grew:
                        log.info("  +%.1fMB (+%d blocks) %s",
                                 stat.size_diff / 1024 / 1024,
                                 stat.count_diff, stat.traceback[0])
            tm_prev = snap
        except Exception:
            log.warning("tracemalloc snapshot failed", exc_info=True)
        # --- end memory diagnostic ---

        # Write service status
        ec_wfs = sum(len(wfs) for wfs in state.exit_codes.values())
        failed_count = sum(
            len(recs)
            for sites in state.failed_job_records.values()
            for wfs in sites.values()
            for recs in wfs.values())
        import json as _json
        status_path = os.path.join(
            cfg.get("prodview", "basedir"), "service_status.json")
        try:
            with open(status_path, "w") as f:
                _json.dump({
                    "cycle": cycle,
                    "cycle_time": round(elapsed, 1),
                    "rss_mb": round(rss_mb),
                    "ts_entities": ts_entities,
                    "ts_points": ts_points,
                    "exit_code_workflows": ec_wfs,
                    "failed_job_records": failed_count,
                    "efficiency_lifetime": len(state.efficiency_lifetime),
                    "updated": time.time(),
                }, f)
        except OSError:
            pass

        # Record status metrics for history charts
        state_path = os.path.join(
            cfg.get("globalview", "basedir"), "exit_code_state.json")
        try:
            state_size_mb = round(os.path.getsize(state_path) / 1024 / 1024, 1)
        except OSError:
            state_size_mb = 0
        status_history.record(round(elapsed, 1), round(rss_mb), state_size_mb)
        status_history.flush(cfg.get("prodview", "basedir"))

        if args.once:
            break

        sleep_time = max(0, COOLDOWN - elapsed)
        if sleep_time > 0 and not _shutdown:
            log.debug("sleeping %.0fs", sleep_time)
            # Sleep in small increments so we can respond to signals
            end = time.time() + sleep_time
            while time.time() < end and not _shutdown:
                time.sleep(min(1, end - time.time()))

    log.info("flushing state before shutdown")
    try:
        state.flush_timeseries(cfg)
        state.flush_exit_code_state(cfg)
    except Exception:
        log.error("failed to flush on shutdown", exc_info=True)

    log.info("shutdown complete")


if __name__ == "__main__":
    main()
