"""Matplotlib graph rendering for gwmsmon2.

Generates PNG graphs from pre-computed time-series JSON files.
All rendering uses the Agg backend (non-interactive, no GUI).
"""

import io
import json
import logging
import os
import threading
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from datetime import datetime

log = logging.getLogger(__name__)

SERIES_COLORS = {
    "Running": "#2196F3",
    "MatchingIdle": "#FF9800",
    "CpusInUse": "#4CAF50",
    "CpusPending": "#F44336",
    "Held": "#9C27B0",
    "UniquePressure": "#00BCD4",
}

INTERVALS = {
    "hourly": 3 * 3600,
    "daily": 24 * 3600,
    "weekly": 7 * 86400,
    "monthly": 30 * 86400,
}

# Two subplot groups: running (top) and pending (bottom)
# Each panel shows CPUs on left y-axis and cores/job on right y-axis
CPUS_COLOR = "#0055D4"       # punchy blue — CPUs
RATIO_COLOR = "#222222"      # near-black — cores/job (dashed, always readable)
PANELS = [
    # (label, cpus_key, jobs_key)
    ("Running", "CpusInUse", "Running"),
    ("Pending", "CpusPending", "MatchingIdle"),
]
# Padding fraction above max values so lines don't touch the frame
YLIM_PAD = 0.10

GRAPH_WIDTH = 400   # CSS pixels
GRAPH_HEIGHT = 240  # CSS pixels (two subplots need more vertical space)
GRAPH_DPI = 200     # 2x for Retina displays


def _fmt_count(x, pos):
    """Format large numbers as K/M for y-axis ticks."""
    if abs(x) >= 1e6:
        s = "{:.1f}".format(x / 1e6).rstrip("0").rstrip(".")
        return s + "M"
    if abs(x) >= 1e3:
        s = "{:.1f}".format(x / 1e3).rstrip("0").rstrip(".")
        return s + "K"
    return "{:.0f}".format(x)


# In-memory PNG cache: {(ts_path, interval_name): (mtime, png_bytes)}
_cache = {}
_cache_lock = threading.Lock()


def render_graph(basedir, spec):
    """Render a graph from a spec string.

    Spec examples:
        summary/daily
        request/SomeWorkflow/hourly
        site/T2_US_MIT/weekly

    Returns PNG bytes or None if data not found.
    """
    parts = spec.strip("/").split("/")
    if len(parts) < 2:
        return None

    interval_name = parts[-1]
    if interval_name not in INTERVALS:
        return None
    interval = INTERVALS[interval_name]

    graph_type = parts[0]
    if graph_type == "summary":
        entity = "_summary"
        title = interval_name.capitalize()
    elif graph_type in ("request", "site"):
        if len(parts) < 3:
            return None
        entity_name = "/".join(parts[1:-1])
        entity = "{}:{}".format(graph_type, entity_name)
        title = "{} ({})".format(entity_name, interval_name)
        if len(title) > 60:
            title = title[:57] + "..."
    else:
        return None

    # Resolve timeseries file path
    safe_name = entity.replace("/", "_").replace(":", "_")
    ts_dir = os.path.join(basedir, "timeseries")
    ts_path = os.path.realpath(os.path.join(ts_dir, safe_name + ".json"))
    if not ts_path.startswith(os.path.realpath(ts_dir) + os.sep):
        return None

    # Check file mtime for cache validity
    try:
        mtime = os.path.getmtime(ts_path)
    except OSError:
        return None

    cache_key = (ts_path, interval_name)
    with _cache_lock:
        cached = _cache.get(cache_key)
        if cached and cached[0] == mtime:
            return cached[1]

    # Cache miss — load and render
    try:
        with open(ts_path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    series = data.get("series", {})
    if not series:
        return None

    png = _render_timeseries(series, interval, title)

    if png is not None:
        with _cache_lock:
            _cache[cache_key] = (mtime, png)

    return png


def _render_timeseries(series, interval, title):
    """Render a two-panel time-series graph as PNG bytes.

    Top panel: Running — CpusInUse (left) + cores/job (right)
    Bottom panel: Pending — CpusPending (left) + cores/job (right)

    Design rules:
    - Zero is always visible on both y-axes. Suppressing zero distorts
      perception of magnitude and hides whether values are large or small.
    - The cores/job right y-axis uses a single shared scale across both
      panels AND across all interval plots on the same page.  To achieve
      this the max is computed from ALL data in the file (not filtered
      by interval), so every render of the same entity gets the same limit.
    - Values at max/min never touch the frame (YLIM_PAD headroom).
    """
    now = time.time()
    cutoff = now - interval

    # Pre-compute global max cores/job from ALL data (unfiltered).
    # Because every interval render loads the same JSON file, this
    # gives a consistent right y-axis across hourly/daily/weekly plots.
    global_max_ratio = 0
    for _, cpus_key, jobs_key in PANELS:
        cpus_pts = series.get(cpus_key, {"t": [], "v": []})
        jobs_pts = series.get(jobs_key, {"t": [], "v": []})
        cpus_by_t = {cpus_pts["t"][i]: cpus_pts["v"][i]
                     for i in range(len(cpus_pts["t"]))}
        for i in range(len(jobs_pts["t"])):
            jobs = jobs_pts["v"][i]
            cpus = cpus_by_t.get(jobs_pts["t"][i])
            if cpus and jobs and jobs > 0:
                global_max_ratio = max(global_max_ratio, cpus / jobs)
    ratio_ymax = max(global_max_ratio, 1) * (1 + YLIM_PAD)

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1,
        figsize=(GRAPH_WIDTH / 100, GRAPH_HEIGHT / 100),
        dpi=GRAPH_DPI,
        sharex=True,
    )

    fig.suptitle(title, fontsize=9, y=0.99)

    has_data = False
    axes = [ax_top, ax_bot]
    for ax, (label, cpus_key, jobs_key) in zip(axes, PANELS):
        # Filter CPU series
        cpus_raw = series.get(cpus_key, {"t": [], "v": []})
        cpus_filtered = [(cpus_raw["t"][i], cpus_raw["v"][i])
                         for i in range(len(cpus_raw["t"]))
                         if cpus_raw["t"][i] >= cutoff]

        # Filter jobs series (for cores/job ratio)
        jobs_raw = series.get(jobs_key, {"t": [], "v": []})
        jobs_filtered = {jobs_raw["t"][i]: jobs_raw["v"][i]
                         for i in range(len(jobs_raw["t"]))
                         if jobs_raw["t"][i] >= cutoff}

        if cpus_filtered:
            has_data = True
            times = [datetime.fromtimestamp(t) for t, _ in cpus_filtered]
            values = [v for _, v in cpus_filtered]
            ax.plot(times, values, color=CPUS_COLOR, label=cpus_key,
                    linewidth=1.5)

            # Compute cores/job where we have both series at same timestamp
            ratio_times = []
            ratio_values = []
            for t, cpus in cpus_filtered:
                jobs = jobs_filtered.get(t)
                if jobs and jobs > 0:
                    ratio_times.append(datetime.fromtimestamp(t))
                    ratio_values.append(cpus / jobs)

            if ratio_times:
                ax2 = ax.twinx()
                ax2.plot(ratio_times, ratio_values, color=RATIO_COLOR,
                         label="cores/job", linewidth=1.2,
                         linestyle="--")
                ax2.set_ylim(0, ratio_ymax)
                ax2.set_ylabel("cores/job", fontsize=6, labelpad=2,
                               color=RATIO_COLOR)
                ax2.tick_params(labelsize=6, colors=RATIO_COLOR)
                ax2.yaxis.set_major_formatter(
                    mticker.FuncFormatter(lambda x, p: "{:g}".format(x)))

        # Left y-axis: always show zero, pad above max
        cpus_max = max(values) if cpus_filtered else 0
        ax.set_ylim(0, max(cpus_max, 1) * (1 + YLIM_PAD))
        ax.set_facecolor("#fafafa")
        ax.grid(True, alpha=0.3, linewidth=0.5)
        ax.set_ylabel(label, fontsize=7, labelpad=3, color=CPUS_COLOR)
        ax.tick_params(labelsize=6, axis="y", colors=CPUS_COLOR)
        ax.tick_params(labelsize=6, axis="x")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_count))

    if not has_data:
        plt.close(fig)
        return None

    # Fixed x-axis window so each interval always looks distinct
    t_end = datetime.fromtimestamp(now)
    t_start = datetime.fromtimestamp(cutoff)
    ax_bot.set_xlim(t_start, t_end)

    # Time axis formatting (only on bottom since sharex)
    if interval <= 4 * 3600:
        ax_bot.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    elif interval <= 2 * 86400:
        ax_bot.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    else:
        ax_bot.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout(pad=0.5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=GRAPH_DPI,
                facecolor="white", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
