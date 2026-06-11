"""Multi-tier time-series for service status metrics.

Three retention tiers:
  24h  — 1-hour bins   (24 points)
  7d   — 4-hour bins   (42 points)
  1y   — 1-week bins   (52 points)

Each data point stores the average of all samples that fell into its bin.

Also keeps a rolling per-cycle RSS window for memory-leak detection
on /status. Linear regression of RSS over the last N cycles gives a
slope (MB/cycle); drift > a threshold means the parent's RSS isn't
stabilising — typical of a Python-allocator fragmentation regression
or a real reference leak.
"""

import collections
import json
import logging
import os
import tempfile
import time

log = logging.getLogger(__name__)

TIERS = [
    # (name, bin_seconds, retention_seconds)
    # Retention runs 2 bins past the chart window so the renderer
    # always has an out-of-window anchor point for left-edge
    # interpolation (instead of extrapolating the edge to 0).
    ("24h", 3600, 24 * 3600 + 2 * 3600),
    ("7d", 4 * 3600, 7 * 86400 + 8 * 3600),
    ("1y", 7 * 86400, 365 * 86400 + 14 * 86400),
]

METRICS = ("cycle_time", "rss_mb", "state_size_mb")

# Memory-leak detector window: last 100 cycles (~5h at current rate).
LEAK_WINDOW_CYCLES = 100
# Slope thresholds in MB/cycle for the verdict.
LEAK_DRIFT_THRESHOLD = 5.0
LEAK_HARD_THRESHOLD = 50.0


class StatusHistory:
    """Accumulates service metrics into multi-tier binned time-series."""

    def __init__(self):
        # {metric: {tier_name: {"t": [...], "v": [...]}}}
        self.series = {m: {t[0]: {"t": [], "v": []} for t in TIERS}
                       for m in METRICS}
        # Accumulator for current bin: {metric: {tier_name: [values]}}
        self._accum = {m: {t[0]: [] for t in TIERS} for m in METRICS}
        # Current bin start: {metric: {tier_name: timestamp}}
        self._bin_start = {m: {t[0]: 0 for t in TIERS} for m in METRICS}
        # Rolling per-cycle RSS window for leak detection.
        # Items: (cycle_num, rss_mb).
        self.rss_window = collections.deque(maxlen=LEAK_WINDOW_CYCLES)

    def record(self, cycle_time, rss_mb, state_size_mb, cycle=None):
        """Record a sample. Called once per collector cycle.

        cycle: cycle number (used for leak-detection regression). If
        omitted the rolling window uses 0 — leak detection becomes
        time-based instead of cycle-based.
        """
        now = time.time()
        values = {
            "cycle_time": cycle_time,
            "rss_mb": rss_mb,
            "state_size_mb": state_size_mb,
        }
        for name, bin_sec, _ in TIERS:
            bin_start = now // bin_sec * bin_sec
            for metric in METRICS:
                if self._bin_start[metric][name] != bin_start:
                    # Flush previous bin if it had data
                    self._flush_bin(metric, name)
                    self._bin_start[metric][name] = bin_start
                self._accum[metric][name].append(values[metric])

        # Per-cycle RSS sample for the leak detector.
        if cycle is None:
            cycle = (self.rss_window[-1][0] + 1) if self.rss_window else 0
        self.rss_window.append((cycle, rss_mb))

    def leak_report(self):
        """Linear regression of RSS over the rolling window.

        Returns dict: {n, slope_mb_per_cycle, first_rss, last_rss,
        verdict in ("stable", "drift", "leak")}.
        """
        n = len(self.rss_window)
        if n < 5:
            return {
                "n": n, "slope_mb_per_cycle": 0.0,
                "first_rss": 0, "last_rss": 0, "verdict": "warmup",
                "window": LEAK_WINDOW_CYCLES,
            }
        xs = [p[0] for p in self.rss_window]
        ys = [p[1] for p in self.rss_window]
        mx = sum(xs) / n
        my = sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = sum((x - mx) ** 2 for x in xs)
        slope = (num / den) if den else 0.0
        if abs(slope) < LEAK_DRIFT_THRESHOLD:
            verdict = "stable"
        elif slope < LEAK_HARD_THRESHOLD:
            verdict = "drift"
        else:
            verdict = "leak"
        return {
            "n": n,
            "slope_mb_per_cycle": round(slope, 2),
            "first_rss": round(ys[0]),
            "last_rss": round(ys[-1]),
            "verdict": verdict,
            "window": LEAK_WINDOW_CYCLES,
            "drift_threshold": LEAK_DRIFT_THRESHOLD,
            "leak_threshold": LEAK_HARD_THRESHOLD,
        }

    def _flush_bin(self, metric, tier_name):
        """Average the accumulator and append to the series."""
        acc = self._accum[metric][tier_name]
        if not acc:
            return
        avg = round(sum(acc) / len(acc), 2)
        ts = self._bin_start[metric][tier_name]
        self.series[metric][tier_name]["t"].append(ts)
        self.series[metric][tier_name]["v"].append(avg)
        self._accum[metric][tier_name] = []

    def prune(self):
        """Drop points outside the retention window."""
        now = time.time()
        for _, bin_sec, retention_sec in TIERS:
            name = _tier_name(bin_sec)
            cutoff = now - retention_sec
            for metric in METRICS:
                pts = self.series[metric][name]
                if not pts["t"]:
                    continue
                # Find first index within retention
                i = 0
                while i < len(pts["t"]) and pts["t"][i] < cutoff:
                    i += 1
                if i > 0:
                    pts["t"] = pts["t"][i:]
                    pts["v"] = pts["v"][i:]

    def flush(self, basedir):
        """Flush all pending accumulators and write to disk."""
        # Flush any partial bins so the latest data is visible
        for metric in METRICS:
            for name, _, _ in TIERS:
                self._flush_partial(metric, name)
        self.prune()
        path = os.path.join(basedir, "status_history.json")
        _atomic_json(path, self.series)

    def _flush_partial(self, metric, tier_name):
        """Flush accumulator without resetting bin_start (partial bin)."""
        acc = self._accum[metric][tier_name]
        if not acc:
            return
        avg = round(sum(acc) / len(acc), 2)
        ts = self._bin_start[metric][tier_name]
        pts = self.series[metric][tier_name]
        # Update in-place if last point is same bin, else append
        if pts["t"] and pts["t"][-1] == ts:
            pts["v"][-1] = avg
        else:
            pts["t"].append(ts)
            pts["v"].append(avg)
        # Don't clear accumulator — record() still adding to this bin

    def restore(self, basedir):
        """Load previously saved history from disk."""
        path = os.path.join(basedir, "status_history.json")
        if not os.path.exists(path):
            return
        try:
            with open(path) as f:
                data = json.load(f)
            for metric in METRICS:
                if metric not in data:
                    continue
                for name, _, _ in TIERS:
                    if name in data[metric]:
                        stored = data[metric][name]
                        if isinstance(stored, dict) and "t" in stored:
                            self.series[metric][name] = stored
            log.info("restored status history from %s", path)
        except (OSError, json.JSONDecodeError, KeyError):
            log.warning("failed to restore status history", exc_info=True)


def _tier_name(bin_sec):
    """Map bin_seconds to tier name."""
    for name, bs, _ in TIERS:
        if bs == bin_sec:
            return name
    return "unknown"


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
