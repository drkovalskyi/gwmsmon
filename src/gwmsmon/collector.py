"""Long-running collection process for HTCondor pool data."""

import argparse
import gc
import logging
import os
import resource
import signal
import sys
import time

from gwmsmon import config
from gwmsmon.query import query_all, query_history_parallel, query_accounting_ads
from gwmsmon.state import State

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
        "--config", default="/etc/gwmsmon2.conf",
        help="path to configuration file"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="run a single collection cycle and exit"
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

    # Ensure files are world-readable for Apache
    os.umask(0o022)

    cfg = config.load(args.config)
    state = State()
    state.restore(cfg)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    cycle = 0
    while not _shutdown:
        cycle += 1
        t0 = time.time()
        log.info("--- cycle %d ---", cycle)

        try:
            jobs, summary_ads, factory_data, schedd_info = query_all(cfg)
            neg_hosts = cfg.get("htcondor", "negotiator_collectors",
                                fallback="")
            accounting_ads = query_accounting_ads(neg_hosts) if neg_hosts else []
            state.update(jobs, summary_ads, factory_data, accounting_ads)
            del jobs, summary_ads, factory_data, accounting_ads
            gc.collect()

            # Exit code collection via schedd.history()
            t_hist = time.time()
            history_jobs, new_watermarks = query_history_parallel(
                schedd_info, state.history_watermarks
            )
            state.history_watermarks = new_watermarks
            hist_count = len(history_jobs)
            state.update_exit_codes(history_jobs)
            del history_jobs, schedd_info
            gc.collect()
            log.info("history query: %d jobs in %.1fs",
                     hist_count, time.time() - t_hist)

            state._append_timeseries()
            state.flush_snapshot(cfg)
            state.flush_exit_codes(cfg)

            if cycle % TS_FLUSH_INTERVAL == 0:
                state.flush_timeseries(cfg)
                state.flush_exit_code_state(cfg)

            if cycle % MAINTENANCE_INTERVAL == 0:
                state.maintenance()
                state.prune_dirs(cfg)

        except Exception:
            log.error("cycle %d failed", cycle, exc_info=True)

        elapsed = time.time() - t0
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        ts_entities = sum(len(e) for v in state.timeseries.values()
                          for e in v.values())
        ts_points = sum(len(pts["t"]) for v in state.timeseries.values()
                        for e in v.values() for pts in e.values())
        log.info("cycle %d completed in %.1fs | RSS=%.0fMB | "
                 "ts_entities=%d ts_points=%d",
                 cycle, elapsed, rss_mb, ts_entities, ts_points)

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
