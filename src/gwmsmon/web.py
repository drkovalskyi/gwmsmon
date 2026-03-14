"""Flask web application for gwmsmon2.

Serves pre-computed JSON data and renders HTML overview pages
with Jinja2 templates. Graphs are generated on-demand with
Matplotlib.
"""

import argparse
import json
import logging
import os
import time

from flask import (Flask, abort, render_template, redirect,
                   send_from_directory, url_for)

from gwmsmon import config

log = logging.getLogger(__name__)

# View registry — all views share the same page structure,
# differing only in data and display options.
VIEWS = {
    "prodview": {
        "title": "Production",
        "entity_label": "Workflow",
        "show_priorities": True,
    },
    "analysisview": {
        "title": "Analysis",
        "entity_label": "User",
    },
    "globalview": {
        "title": "Global",
        "entity_label": "User/Task",
        "show_pilots": True,
    },
    "poolview": {
        "title": "Pool",
        "entity_label": "Scheduler",
        "overview_only": True,
    },
    "factoryview": {
        "title": "Factory",
        "entity_label": "Entry",
        "overview_only": True,
    },
}


def _load_json(basedir, filename):
    """Load a JSON file from basedir. Returns empty dict on error."""
    path = os.path.join(basedir, filename)
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _freshness(updated):
    """Return human-readable age string from a timestamp."""
    if not updated:
        return "unknown"
    age = time.time() - updated
    if age < 60:
        return "{}s ago".format(int(age))
    if age < 3600:
        return "{}m ago".format(int(age / 60))
    if age < 86400:
        return "{}h ago".format(int(age / 3600))
    return "{}d ago".format(int(age / 86400))


def _format_number(n):
    """Format a number with thousand separators."""
    if n is None:
        return "0"
    if isinstance(n, float):
        return "{:,.1f}".format(n)
    return "{:,}".format(n)


def create_app(config_path="/etc/gwmsmon2.conf"):
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )
    cfg = config.load(config_path)
    app.config["gwmsmon_cfg"] = cfg

    app.jinja_env.filters["fmt"] = _format_number
    app.jinja_env.globals["views"] = VIEWS

    # --- Routes ---

    @app.route("/")
    def index():
        return redirect(url_for("overview", view="prodview"))

    @app.route("/<view>/")
    def overview(view):
        if view not in VIEWS:
            abort(404)
        view_cfg = VIEWS[view]
        basedir = cfg.get(view, "basedir")

        summary = _load_json(basedir, "summary.json")
        totals_data = _load_json(basedir, "totals.json")
        site_summary = _load_json(basedir, "site_summary.json")
        exit_codes = _load_json(basedir, "exit_codes.json")

        # Sort workflows by running desc, then idle desc
        workflows = totals_data.get("workflows", {})

        # For analysisview, group by user (part before first '/')
        if view == "analysisview":
            users = {}
            for wf_name, subtasks in workflows.items():
                user = wf_name.split("/", 1)[0]
                if user not in users:
                    users[user] = {"Running": 0, "MatchingIdle": 0,
                                   "CpusInUse": 0, "CpusPending": 0}
                for st_data in subtasks.values():
                    for k in users[user]:
                        users[user][k] += st_data.get(k, 0)
            sorted_wf = sorted(
                users.items(),
                key=lambda x: (-x[1]["Running"], -x[1]["MatchingIdle"]),
            )
            entity_url_prefix = "user"
        else:
            sorted_wf = sorted(
                workflows.items(),
                key=lambda x: (
                    -_subtask_total(x[1], "Running"),
                    -_subtask_total(x[1], "MatchingIdle"),
                ),
            )
            entity_url_prefix = "request"

        # Sort sites by running desc
        sites = site_summary.get("sites", {})
        sorted_sites = sorted(
            sites.items(),
            key=lambda x: -x[1].get("Running", 0),
        )

        updated = summary.get("updated", 0)

        return render_template(
            "overview.html",
            view=view,
            view_cfg=view_cfg,
            totals=summary.get("totals", {}),
            workflows=sorted_wf,
            sites=sorted_sites,
            exit_codes=exit_codes,
            schedds=summary.get("schedds", {}),
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
            entity_url_prefix=entity_url_prefix,
        )

    @app.route("/<view>/request/<path:name>")
    def request_detail(view, name):
        if view not in VIEWS or VIEWS[view].get("overview_only"):
            abort(404)
        basedir = cfg.get(view, "basedir")

        totals_data = _load_json(basedir, "totals.json")
        workflows = totals_data.get("workflows", {})
        if name not in workflows:
            abort(404)

        subtasks = workflows[name]
        exit_codes = _load_json(
            os.path.join(basedir, name.replace("/", os.sep)),
            "exit_codes.json",
        )

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)

        # Compute request-level totals from subtasks
        req_totals = {"Running": 0, "MatchingIdle": 0,
                      "CpusInUse": 0, "CpusPending": 0}
        for st_data in subtasks.values():
            for k in req_totals:
                req_totals[k] += st_data.get(k, 0)

        return render_template(
            "request.html",
            view=view,
            view_cfg=VIEWS[view],
            name=name,
            subtasks=subtasks,
            req_totals=req_totals,
            exit_codes=exit_codes,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    @app.route("/<view>/user/<name>")
    def user_detail(view, name):
        if view != "analysisview":
            abort(404)
        basedir = cfg.get(view, "basedir")

        totals_data = _load_json(basedir, "totals.json")
        workflows = totals_data.get("workflows", {})

        # Filter workflows belonging to this user
        prefix = name + "/"
        user_wf = {k: v for k, v in workflows.items()
                   if k.startswith(prefix) or k == name}
        if not user_wf:
            abort(404)

        # Compute user-level totals
        req_totals = {"Running": 0, "MatchingIdle": 0,
                      "CpusInUse": 0, "CpusPending": 0}
        for subtasks in user_wf.values():
            for st_data in subtasks.values():
                for k in req_totals:
                    req_totals[k] += st_data.get(k, 0)

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)

        return render_template(
            "user.html",
            view=view,
            view_cfg=VIEWS[view],
            name=name,
            workflows=user_wf,
            req_totals=req_totals,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    @app.route("/<view>/site/<name>")
    def site_detail(view, name):
        if view not in VIEWS or VIEWS[view].get("overview_only"):
            abort(404)
        basedir = cfg.get(view, "basedir")

        site_summary = _load_json(basedir, "site_summary.json")
        sites = site_summary.get("sites", {})
        if name not in sites:
            abort(404)

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)

        return render_template(
            "site.html",
            view=view,
            view_cfg=VIEWS[view],
            name=name,
            site_data=sites[name],
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    # --- Timeseries JSON endpoint ---

    @app.route("/<view>/tsdata/<path:entity>")
    def tsdata(view, entity):
        if view not in VIEWS:
            abort(404)
        basedir = cfg.get(view, "basedir")

        # Map URL path to timeseries filename
        parts = entity.strip("/").split("/", 1)
        kind = parts[0]
        if kind == "summary":
            filename = "_summary.json"
        elif kind in ("request", "site") and len(parts) == 2:
            safe = parts[1].replace("/", "_")
            filename = "{}_{}.json".format(kind, safe)
        else:
            abort(404)

        ts_dir = os.path.join(basedir, "timeseries")
        resp = send_from_directory(
            ts_dir, filename, mimetype="application/json",
        )
        resp.headers["Cache-Control"] = "max-age=120, public"
        return resp

    # --- JSON endpoints ---

    @app.route("/<view>/json/<path:filename>")
    def json_file(view, filename):
        if view not in VIEWS:
            abort(404)
        basedir = cfg.get(view, "basedir")
        if not filename.endswith(".json"):
            filename += ".json"
        return send_from_directory(
            basedir, filename, mimetype="application/json",
        )

    # --- Graph endpoints ---

    @app.route("/<view>/graphs/<path:spec>")
    def graph(view, spec):
        if view not in VIEWS:
            abort(404)
        basedir = cfg.get(view, "basedir")

        from gwmsmon.graphs import render_graph
        png = render_graph(basedir, spec)
        if png is None:
            abort(404)

        from flask import Response
        return Response(png, mimetype="image/png",
                        headers={"Cache-Control": "max-age=120, public"})

    return app


def _subtask_total(subtasks, key):
    """Sum a key across all subtasks in a workflow."""
    total = 0
    for st_data in subtasks.values():
        total += st_data.get(key, 0)
    return total


def main():
    parser = argparse.ArgumentParser(
        description="gwmsmon2 web application"
    )
    parser.add_argument("--config", default="/etc/gwmsmon2.conf")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    app = create_app(args.config)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
