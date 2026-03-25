"""Flask web application for gwmsmon.

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
from gwmsmon.exitcodes import describe as _describe_exit_code

log = logging.getLogger(__name__)


def _safe_path(basedir, untrusted):
    """Resolve untrusted path and verify it stays within basedir.

    Returns the resolved path, or calls abort(404) on traversal.
    """
    resolved = os.path.realpath(os.path.join(basedir, untrusted))
    if not resolved.startswith(os.path.realpath(basedir) + os.sep) and \
       resolved != os.path.realpath(basedir):
        abort(404)
    return resolved

# View registry — all views share the same page structure,
# differing only in data and display options.
VIEWS = {
    "prodview": {
        "title": "Production",
        "entity_label": "Request",
        "show_priorities": True,
    },
    "analysisview": {
        "title": "Analysis",
        "entity_label": "User",
    },
    "globalview": {
        "title": "Global",
        "entity_label": "User",
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


def _annotate_exit_codes(exit_codes):
    """Add descriptions to exit code dicts.

    Transforms exit_codes.codes from {"code": count} to a list of
    {"code": str, "count": int, "desc": str} sorted by count desc.
    """
    if not exit_codes or "codes" not in exit_codes:
        return exit_codes
    codes = exit_codes["codes"]
    annotated = []
    for code, count in codes.items():
        annotated.append({
            "code": code,
            "count": count,
            "desc": _describe_exit_code(code),
        })
    annotated.sort(key=lambda x: -x["count"])
    exit_codes["annotated"] = annotated
    return exit_codes


def create_app(config_path="/etc/gwmsmon.conf"):
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
    )
    cfg = config.load(config_path)
    app.config["gwmsmon_cfg"] = cfg

    app.jinja_env.filters["fmt"] = _format_number
    app.jinja_env.globals["views"] = VIEWS

    @app.after_request
    def _security_headers(resp):
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("X-Frame-Options", "DENY")
        resp.headers.setdefault("Referrer-Policy", "same-origin")
        resp.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "img-src 'self' data:; "
            "frame-ancestors 'none'"
        )
        return resp

    # --- Routes ---

    @app.route("/debug/token")
    def debug_token():
        from flask import request as req, jsonify
        import urllib.request
        import urllib.parse
        import ssl
        token = req.headers.get("X-Oidc-Access-Token", "")
        user = req.headers.get("X-Oidc-User", "")
        ctx = ssl.create_default_context()

        # Try token exchange for EOS audience
        exchanged = ""
        exchange_err = ""
        if token:
            try:
                data = urllib.parse.urlencode({
                    "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token": token,
                    "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
                    "audience": "eos-service",
                    "client_id": "cms-gwmsmon",
                    "client_secret": "QMxiaPIj4DZx06upvOkXZPFUowR41rpe",
                }).encode()
                r = urllib.request.Request(
                    "https://auth.cern.ch/auth/realms/cern/protocol/openid-connect/token",
                    data=data, method="POST")
                resp = urllib.request.urlopen(r, timeout=10, context=ctx)
                import json as _json
                result = _json.loads(resp.read())
                exchanged = result.get("access_token", "")[:30] + "..."
            except urllib.error.HTTPError as e:
                exchange_err = f"HTTP {e.code}: {e.read().decode()[:200]}"
            except Exception as e:
                exchange_err = str(e)

        # Test EOS with exchanged token (or original)
        eos_token = exchanged.rstrip(".") if exchanged else token
        test_url = ("https://eoscms.cern.ch/eos/cms/store/logs/prod/"
                    "recent/PRODUCTION/")
        eos_result = "not tested"
        if eos_token and len(eos_token) > 30:
            try:
                r = urllib.request.Request(test_url, method="HEAD")
                r.add_header("Authorization", "Bearer " + eos_token)
                resp = urllib.request.urlopen(r, timeout=10, context=ctx)
                eos_result = f"HTTP {resp.status}"
            except urllib.error.HTTPError as e:
                eos_result = f"HTTP {e.code}"
            except Exception as e:
                eos_result = str(e)

        return jsonify({
            "user": user,
            "token_length": len(token),
            "exchange": exchanged or exchange_err,
            "eos_test": eos_result,
        })

    @app.route("/")
    def index():
        return redirect(url_for("overview", view="prodview"))

    @app.route("/<view>/")
    def overview(view):
        if view not in VIEWS:
            abort(404)

        if view == "poolview":
            return _poolview_overview(cfg)
        if view == "factoryview":
            return _factoryview_overview(cfg)

        view_cfg = VIEWS[view]
        basedir = cfg.get(view, "basedir")

        summary = _load_json(basedir, "summary.json")
        totals_data = _load_json(basedir, "totals.json")
        site_summary = _load_json(basedir, "site_summary.json")
        exit_codes = _annotate_exit_codes(
            _load_json(basedir, "exit_codes.json"))
        site_exit_codes = _load_json(basedir,
                                     "site_exit_codes.json").get("sites", {})
        wf_completion = _load_json(basedir,
                                   "wf_completion.json").get("workflows", {})

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
        elif view == "globalview":
            # Expand into (user, group) rows from _group_stats
            rows = []
            for user, data in workflows.items():
                group_stats = data.get("_group_stats", {})
                if group_stats:
                    for group, stats in group_stats.items():
                        if not any(stats.get(k, 0)
                                   for k in ("Running", "MatchingIdle")):
                            continue
                        row = dict(stats)
                        row["_group"] = group
                        rows.append((user, row))
                else:
                    row = {"Running": 0, "MatchingIdle": 0,
                           "CpusInUse": 0, "CpusPending": 0, "_group": ""}
                    for st, st_data in data.items():
                        if st.startswith("_") or not isinstance(st_data, dict):
                            continue
                        for k in ("Running", "MatchingIdle",
                                  "CpusInUse", "CpusPending"):
                            row[k] += st_data.get(k, 0)
                    if row["Running"] or row["MatchingIdle"]:
                        rows.append((user, row))
            sorted_wf = sorted(
                rows,
                key=lambda x: (-x[1].get("CpusInUse", 0),
                               -x[1].get("MatchingIdle", 0)),
            )
            entity_url_prefix = "request"
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
        sites = site_summary.get("sites") or site_summary
        sorted_sites = sorted(
            sites.items(),
            key=lambda x: -x[1].get("Running", 0),
        )

        updated = summary.get("updated", 0)

        priorities = summary.get("priorities", {}) if view == "prodview" else {}

        fairshare = {}
        if view == "globalview":
            fairshare = _load_json(basedir, "fairshare.json").get(
                "categories", {})

        return render_template(
            "overview.html",
            view=view,
            view_cfg=view_cfg,
            totals=summary.get("totals", {}),
            workflows=sorted_wf,
            sites=sorted_sites,
            exit_codes=exit_codes,
            site_exit_codes=site_exit_codes,
            wf_completion=wf_completion,
            schedds=summary.get("schedds", {}),
            priorities=priorities,
            fairshare=fairshare,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
            entity_url_prefix=entity_url_prefix,
        )

    @app.route("/globalview/group/<name>")
    def group_detail(name):
        basedir = cfg.get("globalview", "basedir")
        totals_data = _load_json(basedir, "totals.json")
        fairshare = _load_json(basedir, "fairshare.json").get(
            "categories", {})
        accounting = _load_json(basedir, "accounting.json")

        if name not in fairshare:
            abort(404)

        group_totals = fairshare[name]

        # Filter users belonging to this group
        workflows = totals_data.get("workflows", {})
        users = []
        for user, data in workflows.items():
            if name in (data.get("_groups") or []):
                user_totals = {"Running": 0, "MatchingIdle": 0,
                               "CpusInUse": 0, "CpusPending": 0}
                for st, st_data in data.items():
                    if st.startswith("_"):
                        continue
                    for k in user_totals:
                        user_totals[k] += st_data.get(k, 0)
                users.append((user, user_totals))
        users.sort(key=lambda x: -x[1]["CpusInUse"])

        # Fair share by tier from accounting ads
        acct_groups = accounting.get("groups", {})
        tier_order = ["CERN", "T1", "US_T2", "nonUS_T2_T3"]
        tier_rows = []
        for tier in tier_order:
            tier_groups = acct_groups.get(tier, {})
            info = tier_groups.get(name, {})
            tier_rows.append({
                "tier": tier,
                "ConfigQuota": info.get("ConfigQuota", 0),
                "EffectiveQuota": info.get("EffectiveQuota", 0),
                "SurplusPolicy": info.get("SurplusPolicy", ""),
            })

        # Per-tier user priority data
        acct_users = accounting.get("users", {})
        tier_users = {}
        for tier in tier_order:
            tier_user_list = [
                u for u in acct_users.get(tier, [])
                if u.get("group") == name
            ]
            tier_user_list.sort(key=lambda u: -u.get("ResourcesUsed", 0))
            if tier_user_list:
                tier_users[tier] = tier_user_list

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)

        return render_template(
            "group.html",
            view="globalview",
            view_cfg=VIEWS["globalview"],
            group_name=name,
            group_totals=group_totals,
            users=users,
            tier_rows=tier_rows,
            tier_users=tier_users,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
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
        req_dir = _safe_path(basedir, name.replace("/", os.sep))
        exit_codes = _annotate_exit_codes(
            _load_json(req_dir, "exit_codes.json"))
        site_exit_codes = exit_codes.get("sites", {}) if exit_codes else {}
        detail = _load_json(req_dir, "detail.json")

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)

        prio_info = subtasks.get("_priority", {})
        metadata = detail.get("subtasks", {}).get("_metadata", {})

        # Compute request-level totals from subtasks
        req_totals = {"Running": 0, "MatchingIdle": 0,
                      "CpusInUse": 0, "CpusPending": 0}
        for st_name, st_data in subtasks.items():
            if st_name.startswith("_"):
                continue
            for k in req_totals:
                req_totals[k] += st_data.get(k, 0)

        # Site breakdown from detail.json
        sites = detail.get("sites", {})
        sorted_sites = sorted(
            sites.items(),
            key=lambda x: -x[1].get("Running", 0),
        )

        return render_template(
            "request.html",
            view=view,
            view_cfg=VIEWS[view],
            name=name,
            subtasks=subtasks,
            req_totals=req_totals,
            prio_info=prio_info,
            metadata=metadata,
            exit_codes=exit_codes,
            site_exit_codes=site_exit_codes,
            sites=sorted_sites,
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

    @app.route("/poolview/schedd/<name>")
    def schedd_detail(name):
        basedir = cfg.get("poolview", "basedir")
        summary = _load_json(basedir, "summary.json")
        schedds = summary.get("schedds", {})
        if name not in schedds:
            abort(404)
        schedd_data = schedds[name]
        updated = summary.get("updated", 0)
        return render_template(
            "schedd.html",
            view="poolview",
            view_cfg=VIEWS["poolview"],
            name=name,
            schedd_data=schedd_data,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    @app.route("/<view>/site/<name>")
    def site_detail(view, name):
        if view == "factoryview":
            return _factoryview_site_detail(cfg, name)
        if view not in VIEWS or VIEWS[view].get("overview_only"):
            abort(404)
        basedir = cfg.get(view, "basedir")

        site_summary = _load_json(basedir, "site_summary.json")
        sites = site_summary.get("sites") or site_summary
        if name not in sites:
            abort(404)

        # Per-site request breakdown
        safe_site = name.replace("/", "_")
        site_detail_data = _load_json(
            os.path.join(basedir, "_sites"), f"{safe_site}.json")
        site_requests = site_detail_data.get("requests", {})
        sorted_requests = sorted(
            site_requests.items(),
            key=lambda x: -x[1].get("CpusInUse", 0),
        )

        site_ec = _load_json(
            os.path.join(basedir, "_sites"),
            f"{safe_site}_exit_codes.json")
        site_req_ec = site_ec.get("requests", {})

        # Per-site completion summary (windowed stats + exit codes)
        all_site_ec = _load_json(basedir, "site_exit_codes.json")
        site_completion = all_site_ec.get("sites", {}).get(name, {})
        site_exit_codes = _annotate_exit_codes(
            {"codes": site_completion.pop("codes", {})})

        # Failed job records (for linking Fail count)
        failed_jobs_data = _load_json(
            os.path.join(basedir, "_sites"),
            f"{safe_site}_failed_jobs.json")
        failed_requests = set(failed_jobs_data.get("requests", {}).keys())

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)

        return render_template(
            "site.html",
            view=view,
            view_cfg=VIEWS[view],
            name=name,
            site_data=sites[name],
            requests=sorted_requests,
            site_req_ec=site_req_ec,
            site_completion=site_completion,
            site_exit_codes=site_exit_codes,
            failed_requests=failed_requests,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    EOS_LOG_PATH = "/eos/cms/store/logs/prod/recent/PRODUCTION"

    def _eos_tarball_path(request, task, schedd, jobid, retry):
        """Construct EOS FUSE path for a job log tarball."""
        task_short = task.rsplit("/", 1)[-1] if task else ""
        filename = f"{schedd}-{jobid}-{retry}-log.tar.gz"
        return (f"{EOS_LOG_PATH}/{request}/{task_short}/{filename}",
                task_short)

    @app.route("/<view>/failed")
    def failed_jobs_unified(view):
        """Unified failed jobs view with optional filters."""
        if view not in VIEWS or VIEWS[view].get("overview_only"):
            abort(404)
        from flask import request as req
        basedir = cfg.get(view, "basedir")
        data = _load_json(basedir, "failed_jobs.json")
        jobs = data.get("jobs", [])

        # Apply filters from query params
        f_request = req.args.get("request", "")
        f_site = req.args.get("site", "")
        f_code = req.args.get("code", "")
        if f_request:
            jobs = [j for j in jobs
                    if f_request.lower() in j.get("request", "").lower()]
        if f_site:
            jobs = [j for j in jobs
                    if f_site.lower() in j.get("site", "").lower()]
        if f_code:
            jobs = [j for j in jobs if j.get("code") == f_code]

        # Check EOS log existence and add descriptions
        for job in jobs:
            eos_path, _ = _eos_tarball_path(
                job.get("request", ""), job.get("task", ""),
                job.get("schedd", ""), job.get("jobid", 0),
                job.get("retry", 0))
            job["has_log"] = os.path.exists(eos_path)
            job["desc"] = _describe_exit_code(str(job.get("code", "")))

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)
        return render_template(
            "failed_jobs.html",
            view=view,
            view_cfg=VIEWS[view],
            site_name=f_site,
            request_name=f_request,
            filter_code=f_code,
            jobs=jobs,
            unified=True,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    @app.route("/<view>/site/<site_name>/failed/<path:request>")
    def failed_jobs(view, site_name, request):
        if view not in VIEWS or VIEWS[view].get("overview_only"):
            abort(404)
        basedir = cfg.get(view, "basedir")
        safe_site = site_name.replace("/", "_")
        data = _load_json(
            os.path.join(basedir, "_sites"),
            f"{safe_site}_failed_jobs.json")
        jobs = data.get("requests", {}).get(request, [])
        for job in jobs:
            eos_path, _ = _eos_tarball_path(
                request, job.get("task", ""),
                job.get("schedd", ""), job.get("jobid", 0),
                job.get("retry", 0))
            job["has_log"] = os.path.exists(eos_path)
            job["desc"] = _describe_exit_code(str(job.get("code", "")))

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)
        return render_template(
            "failed_jobs.html",
            view=view,
            view_cfg=VIEWS[view],
            site_name=site_name,
            request_name=request,
            jobs=jobs,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    def _eos_log_path(request, task, schedd, jobid, retry):
        """Construct EOS FUSE path for a log tarball from URL components."""
        return (f"{EOS_LOG_PATH}/{request}/{task}/"
                f"{schedd}-{jobid}-{retry}-log.tar.gz")

    @app.route("/<view>/log/<path:request>/<task>/"
                "<schedd>/<int:jobid>/<int:retry>")
    def log_list(view, request, task, schedd, jobid, retry):
        """List files inside a job's log tarball."""
        if view not in VIEWS:
            abort(404)
        eos_path = _eos_log_path(request, task, schedd, jobid, retry)
        if not os.path.exists(eos_path):
            abort(404)
        import tarfile
        try:
            with tarfile.open(eos_path, "r:gz") as tar:
                members = []
                for m in tar.getmembers():
                    if m.isfile():
                        members.append({
                            "name": m.name,
                            "size": m.size,
                        })
        except (tarfile.TarError, OSError):
            abort(404)
        members.sort(key=lambda x: x["name"])
        basedir = cfg.get(view, "basedir")
        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)
        return render_template(
            "log_viewer.html",
            view=view,
            view_cfg=VIEWS[view],
            request_name=request,
            task=task,
            schedd=schedd,
            jobid=jobid,
            retry=retry,
            members=members,
            updated=updated,
            freshness=_freshness(updated),
            updated_ts=updated,
        )

    @app.route("/<view>/log/<path:request>/<task>/"
                "<schedd>/<int:jobid>/<int:retry>/<path:filepath>")
    def log_file(view, request, task, schedd, jobid, retry, filepath):
        """Serve a single file from a job's log tarball."""
        if view not in VIEWS:
            abort(404)
        if ".." in filepath:
            abort(400)
        eos_path = _eos_log_path(request, task, schedd, jobid, retry)
        if not os.path.exists(eos_path):
            abort(404)
        import tarfile
        from flask import Response
        try:
            with tarfile.open(eos_path, "r:gz") as tar:
                member = tar.getmember(filepath)
                if not member.isfile():
                    abort(404)
                f = tar.extractfile(member)
                content = f.read()
        except (tarfile.TarError, KeyError, OSError):
            abort(404)
        ext = filepath.rsplit(".", 1)[-1].lower() if "." in filepath else ""
        ct = {"xml": "text/xml", "html": "text/html",
              "json": "application/json"}.get(ext, "text/plain")
        return Response(content, mimetype=ct,
                        headers={"Content-Disposition": "inline"})

    @app.route("/<view>/completed")
    def completed_overview(view):
        """All completed jobs — same as exitcode detail with no filter."""
        return _exitcode_view(view, None)

    @app.route("/<view>/exitcode/<code>")
    def exitcode_detail(view, code):
        """Completed jobs filtered to a specific exit code."""
        return _exitcode_view(view, code)

    def _exitcode_view(view, code):
        if view not in VIEWS or VIEWS[view].get("overview_only"):
            abort(404)
        basedir = cfg.get(view, "basedir")
        ec_dir = os.path.join(basedir, "_exitcodes")

        if code is None:
            detail = _load_json(ec_dir, "_all.json")
            title = "Completed Jobs"
            desc = ""
        else:
            safe_code = code.replace(":", "_")
            _safe_path(ec_dir, "{}.json".format(safe_code))
            detail = _load_json(ec_dir, "{}.json".format(safe_code))
            title = "Exit Code {}".format(code)
            desc = _describe_exit_code(code)

        if not detail:
            abort(404)

        total_count = detail.get("total", 0)
        wf_totals = detail.get("wf_totals", {})
        wf_failures = detail.get("wf_failures", {})

        workflows = []
        for wf, count in detail.get("workflows", {}).items():
            wf_total = wf_totals.get(wf, 0)
            wf_fail = wf_failures.get(wf, 0) if code is None else count
            workflows.append({
                "name": wf,
                "count": count,
                "total": wf_total,
                "failures": wf_fail,
                "rate": (round(wf_fail / wf_total * 100, 1)
                         if wf_total else 0),
            })
        workflows.sort(key=lambda x: -x["count"])

        sites = sorted(
            [{"name": k, "count": v}
             for k, v in detail.get("sites", {}).items()],
            key=lambda x: -x["count"],
        )
        users = sorted(
            [{"name": k, "count": v}
             for k, v in detail.get("users", {}).items()],
            key=lambda x: -x["count"],
        )

        summary = _load_json(basedir, "summary.json")
        updated = summary.get("updated", 0)
        overall_failures = detail.get("failures", 0)
        detail_windows = detail.get("windows", {})

        return render_template(
            "exitcode.html",
            view=view,
            view_cfg=VIEWS[view],
            code=code,
            title=title,
            desc=desc,
            total_count=total_count,
            total_failures=overall_failures,
            detail_windows=detail_windows,
            workflows=workflows,
            sites=sites,
            users=users,
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
        elif kind == "priorities" and len(parts) == 1:
            # Combined endpoint: merge all priority block timeseries
            ts_dir = os.path.join(basedir, "timeseries")
            blocks = ["B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7"]
            combined = {}
            for block in blocks:
                fname = f"priority_{block}.json"
                data = _load_json(ts_dir, fname)
                if data.get("series"):
                    combined[block] = data["series"]
            from flask import jsonify
            resp = jsonify(combined)
            resp.headers["Cache-Control"] = "max-age=120, public"
            return resp
        elif kind in ("request", "site", "schedd", "fairshare",
                      "priority") and len(parts) == 2:
            safe = parts[1].replace("/", "_")
            filename = "{}_{}.json".format(kind, safe)
        else:
            abort(404)

        ts_dir = os.path.join(basedir, "timeseries")
        _safe_path(ts_dir, filename)
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
        _safe_path(basedir, filename)
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


def _poolview_overview(cfg):
    basedir = cfg.get("poolview", "basedir")
    summary = _load_json(basedir, "summary.json")
    updated = summary.get("updated", 0)

    schedds = summary.get("schedds", {})
    sorted_schedds = sorted(
        schedds.items(),
        key=lambda x: -x[1].get("TotalRunningJobs", 0),
    )

    user_summary = summary.get("user_summary", {})
    sorted_users = sorted(
        user_summary.items(),
        key=lambda x: -x[1].get("Running", 0),
    )

    fairshare = summary.get("fairshare", {})
    sorted_fairshare = sorted(
        fairshare.items(),
        key=lambda x: -x[1].get("CpusInUse", 0),
    )

    return render_template(
        "poolview.html",
        view="poolview",
        view_cfg=VIEWS["poolview"],
        totals=summary.get("totals", {}),
        schedds=sorted_schedds,
        user_summary=sorted_users,
        fairshare=sorted_fairshare,
        updated=updated,
        freshness=_freshness(updated),
        updated_ts=updated,
    )


def _factoryview_overview(cfg):
    basedir = cfg.get("factoryview", "basedir")
    summary = _load_json(basedir, "summary.json")
    totals_data = _load_json(basedir, "totals.json")
    updated = summary.get("updated", 0)

    sites = totals_data.get("sites", {})
    sorted_sites = sorted(
        sites.items(),
        key=lambda x: -x[1].get("Running", 0),
    )

    return render_template(
        "factoryview.html",
        view="factoryview",
        view_cfg=VIEWS["factoryview"],
        totals=summary.get("totals", totals_data.get("totals", {})),
        sites=sorted_sites,
        errors=summary.get("errors", []),
        updated=updated,
        freshness=_freshness(updated),
        updated_ts=updated,
    )


def _factoryview_site_detail(cfg, name):
    basedir = cfg.get("factoryview", "basedir")
    site_dir = _safe_path(basedir, name)
    site_data = _load_json(site_dir, "summary.json")
    if not site_data:
        abort(404)

    entries = site_data.get("entries", {})
    sorted_entries = sorted(
        entries.items(),
        key=lambda x: -sum(
            fd.get("running", 0) for fd in x[1].values()
        ),
    )

    summary = _load_json(basedir, "summary.json")
    updated = summary.get("updated", 0)

    return render_template(
        "factoryview_site.html",
        view="factoryview",
        view_cfg=VIEWS["factoryview"],
        site_name=name,
        site_data=site_data,
        entries=sorted_entries,
        updated=updated,
        freshness=_freshness(updated),
        updated_ts=updated,
    )


def _subtask_total(subtasks, key):
    """Sum a key across all subtasks in a workflow."""
    total = 0
    for st_name, st_data in subtasks.items():
        if st_name.startswith("_") or not isinstance(st_data, dict):
            continue
        total += st_data.get(key, 0)
    return total


def main():
    parser = argparse.ArgumentParser(
        description="gwmsmon web application"
    )
    parser.add_argument("--config", default="/etc/gwmsmon.conf")
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
