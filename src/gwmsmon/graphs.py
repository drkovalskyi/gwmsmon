"""Matplotlib graph rendering for gwmsmon2.

Generates PNG graphs from pre-computed time-series JSON files.
All rendering uses the Agg backend (non-interactive, no GUI).
"""

import io
import json
import logging
import os
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

# Which series to plot for different graph types
SUMMARY_SERIES = ["Running", "MatchingIdle", "CpusInUse", "CpusPending"]
ENTITY_SERIES = ["Running", "MatchingIdle", "CpusInUse", "CpusPending"]

GRAPH_WIDTH = 520
GRAPH_HEIGHT = 200
GRAPH_DPI = 100


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
        series_keys = SUMMARY_SERIES
        title = interval_name.capitalize()
    elif graph_type in ("request", "site"):
        if len(parts) < 3:
            return None
        entity_name = "/".join(parts[1:-1])
        entity = "{}:{}".format(graph_type, entity_name)
        series_keys = ENTITY_SERIES
        title = "{} ({})".format(entity_name, interval_name)
        # Truncate long titles
        if len(title) > 60:
            title = title[:57] + "..."
    else:
        return None

    # Load time-series data
    safe_name = entity.replace("/", "_").replace(":", "_")
    ts_path = os.path.join(basedir, "timeseries", safe_name + ".json")
    try:
        with open(ts_path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None

    series = data.get("series", {})
    if not series:
        return None

    return _render_timeseries(series, series_keys, interval, title)


def _render_timeseries(series, series_keys, interval, title):
    """Render a time-series graph as PNG bytes."""
    now = time.time()
    cutoff = now - interval

    fig, ax = plt.subplots(
        figsize=(GRAPH_WIDTH / GRAPH_DPI, GRAPH_HEIGHT / GRAPH_DPI),
        dpi=GRAPH_DPI,
    )

    has_data = False
    for key in series_keys:
        points = series.get(key, [])
        if not points:
            continue

        # Filter to interval
        filtered = [(p["t"], p["v"]) for p in points if p["t"] >= cutoff]
        if not filtered:
            continue

        has_data = True
        times = [datetime.fromtimestamp(t) for t, _ in filtered]
        values = [v for _, v in filtered]
        color = SERIES_COLORS.get(key, "#757575")
        ax.plot(times, values, color=color, label=key,
                linewidth=1.2, alpha=0.85)

    if not has_data:
        plt.close(fig)
        return None

    # Style
    ax.set_facecolor("#fafafa")
    ax.grid(True, alpha=0.3, linewidth=0.5)
    ax.set_title(title, fontsize=9, pad=4)
    ax.legend(fontsize=7, loc="upper left", framealpha=0.7,
              ncol=min(len(series_keys), 4))
    ax.tick_params(labelsize=7)

    # Time axis formatting
    if interval <= 4 * 3600:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    elif interval <= 2 * 86400:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout(pad=0.5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=GRAPH_DPI,
                facecolor="white", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read()
