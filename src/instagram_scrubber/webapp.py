from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from flask import Flask, abort, render_template_string, request, send_from_directory, url_for

from .config import build_settings
from .exporters import write_csv
from .instagram_api import InstagramGraphClient
from .pipeline import build_leads
from .storage import (
    create_run,
    db_path,
    finish_run_failure,
    finish_run_success,
    get_active_profile,
    init_db,
    list_runs,
    upsert_active_profile,
)

if os.getenv("VERCEL") == "1":
    OUTPUT_DIR = Path("/tmp/outputs")
else:
    OUTPUT_DIR = Path.cwd() / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>InstaScrapper | Podcast Lead Intelligence</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=Sora:wght@600;700;800&display=swap');

    :root {
      --bg-a: #eff7ff;
      --bg-b: #eef9f3;
      --panel: #ffffff;
      --line: #d7e4ef;
      --text: #10253a;
      --sub: #4a647a;
      --accent: #0b7285;
      --accent-2: #0f766e;
      --accent-soft: #e0f4f5;
      --good-bg: #dcfce7;
      --good-line: #86efac;
      --bad-bg: #fff1f2;
      --bad-line: #fecdd3;
      --shadow: 0 14px 32px rgba(8, 39, 68, 0.08);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: "Manrope", "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 10% 0%, #d7ecff 0%, rgba(215, 236, 255, 0) 40%),
        radial-gradient(circle at 90% 5%, #dff8f0 0%, rgba(223, 248, 240, 0) 30%),
        linear-gradient(180deg, var(--bg-a), var(--bg-b));
      padding: 24px 14px 48px;
    }

    .shell {
      max-width: 1100px;
      margin: 0 auto;
      display: grid;
      gap: 14px;
      animation: rise 460ms ease both;
    }

    @keyframes rise {
      from { opacity: 0; transform: translateY(8px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: var(--shadow);
    }

    .hero {
      position: relative;
      overflow: hidden;
      background:
        linear-gradient(125deg, rgba(11, 114, 133, 0.07), rgba(15, 118, 110, 0.09) 55%, rgba(255, 255, 255, 0.9)),
        #fff;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid #b8d8de;
      background: #f2fcfd;
      color: #0b5666;
      border-radius: 999px;
      padding: 5px 11px;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.2px;
      text-transform: uppercase;
    }

    h1 {
      margin: 10px 0 4px;
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 1.72rem;
      line-height: 1.22;
      letter-spacing: 0.2px;
    }

    .sub {
      margin: 0;
      color: var(--sub);
      font-size: 0.94rem;
    }

    .flow {
      margin-top: 11px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      color: #255a6d;
      font-size: 0.82rem;
      font-weight: 600;
    }

    .flow span {
      border: 1px solid #c8e4e9;
      background: #f2fcfd;
      border-radius: 999px;
      padding: 4px 10px;
    }

    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }

    .stat {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fbfdff;
      padding: 12px;
    }

    .stat .label {
      font-size: 0.77rem;
      color: var(--sub);
      text-transform: uppercase;
      letter-spacing: 0.55px;
      font-weight: 700;
    }

    .stat .value {
      margin-top: 4px;
      font-size: 1.25rem;
      font-weight: 800;
      color: #16344f;
      word-break: break-word;
    }

    .notice,
    .error {
      border: 1px solid;
      border-radius: 11px;
      padding: 10px 12px;
      font-size: 0.92rem;
    }

    .notice {
      background: var(--good-bg);
      border-color: var(--good-line);
      color: #14532d;
    }

    .error {
      background: var(--bad-bg);
      border-color: var(--bad-line);
      color: #881337;
    }

    h2 {
      margin: 0 0 8px;
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 1.04rem;
      letter-spacing: 0.1px;
    }

    .muted {
      color: var(--sub);
      font-size: 0.88rem;
    }

    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      margin-top: 10px;
    }

    button,
    .btn {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      font-size: 0.88rem;
      font-weight: 700;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(140deg, var(--accent), var(--accent-2));
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      transition: transform 120ms ease;
    }

    button:hover,
    .btn:hover { transform: translateY(-1px); }

    .btn.alt {
      background: #fff;
      color: #1f3f60;
      border: 1px solid #bfd7ea;
    }

    .form-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
    }

    label {
      display: block;
      margin-bottom: 5px;
      color: var(--sub);
      font-size: 0.82rem;
      font-weight: 600;
    }

    input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font-size: 0.91rem;
      color: #0f172a;
      background: #fff;
    }

    details {
      margin-top: 10px;
      border-top: 1px dashed #dbe5ee;
      padding-top: 10px;
    }

    summary {
      cursor: pointer;
      color: #135f72;
      font-weight: 700;
      font-size: 0.9rem;
      list-style: none;
    }

    summary::-webkit-details-marker { display: none; }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
    }

    th,
    td {
      text-align: left;
      border-bottom: 1px solid var(--line);
      padding: 8px 6px;
      vertical-align: top;
    }

    th {
      background: #f8fbff;
      color: var(--sub);
      font-size: 0.79rem;
      letter-spacing: 0.24px;
      text-transform: uppercase;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 0.74rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.35px;
      border: 1px solid;
    }

    .badge.success {
      background: #ecfdf3;
      color: #166534;
      border-color: #a7f3d0;
    }

    .badge.failed {
      background: #fff1f2;
      color: #9f1239;
      border-color: #fecdd3;
    }

    .badge.running {
      background: #eff6ff;
      color: #1d4ed8;
      border-color: #bfdbfe;
    }

    a { color: #0f4f8a; text-decoration: none; }
    a:hover { text-decoration: underline; }

    @media (max-width: 980px) {
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }

    @media (max-width: 760px) {
      body { padding: 16px 10px 34px; }
      .stats { grid-template-columns: 1fr; }
      .actions { flex-direction: column; align-items: stretch; }
      button,
      .btn { width: 100%; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="card hero">
      <div class="brand">InstaScrapper Pro</div>
      <h1>Podcast Lead Intelligence Dashboard</h1>
      <p class="sub">One-time setup. One-click report runs. Clean history for every previous export.</p>
      <div class="flow">
        <span>1. Setup</span>
        <span>2. Run Report</span>
        <span>3. Download from History</span>
      </div>
    </section>

    <section class="stats">
      <div class="stat">
        <div class="label">Workspace</div>
        <div class="value">{{ profile.name if profile else 'Not configured' }}</div>
      </div>
      <div class="stat">
        <div class="label">Total Runs</div>
        <div class="value">{{ stats.total_runs }}</div>
      </div>
      <div class="stat">
        <div class="label">Successful</div>
        <div class="value">{{ stats.success_runs }}</div>
      </div>
      <div class="stat">
        <div class="label">Last Report</div>
        <div class="value">{{ stats.last_report_at }}</div>
      </div>
    </section>

    {% if message %}
      <section class="card"><div class="notice">{{ message|safe }}</div></section>
    {% endif %}
    {% if error %}
      <section class="card"><div class="error">{{ error }}</div></section>
    {% endif %}

    <section class="card">
      <h2>Setup</h2>
      {% if profile and not show_setup_form %}
        <p class="muted">Connected account ID: <strong>{{ profile.business_account_id }}</strong>. Stored defaults will be used for each run.</p>
        <div class="actions">
          <form method="post" action="{{ url_for('run_scrub') }}">
            <button type="submit">Run Report Now</button>
          </form>
          <a class="btn alt" href="{{ url_for('index') }}?edit_setup=1">Edit Setup</a>
        </div>
      {% endif %}

      {% if show_setup_form %}
      <form method="post" action="{{ url_for('setup') }}">
        <div class="form-grid">
          <div>
            <label for="name">Workspace Name</label>
            <input id="name" name="name" value="{{ setup_form.name }}" required />
          </div>
          <div>
            <label for="business_account_id">Instagram Business Account ID (numeric)</label>
            <input id="business_account_id" name="business_account_id" value="{{ setup_form.business_account_id }}" placeholder="1784..." required />
          </div>
          <div>
            <label for="access_token">Instagram Access Token</label>
            <input id="access_token" name="access_token" value="{{ setup_form.access_token }}" placeholder="Paste raw token only" required />
          </div>
        </div>

        <details>
          <summary>Advanced Defaults</summary>
          <div class="form-grid">
            <div>
              <label for="graph_version">Graph Version</label>
              <input id="graph_version" name="graph_version" value="{{ setup_form.graph_version }}" />
            </div>
            <div>
              <label for="timeout_seconds">Timeout Seconds</label>
              <input id="timeout_seconds" name="timeout_seconds" type="number" min="1" value="{{ setup_form.timeout_seconds }}" />
            </div>
            <div>
              <label for="retry_count">Retry Count</label>
              <input id="retry_count" name="retry_count" type="number" min="0" value="{{ setup_form.retry_count }}" />
            </div>
            <div>
              <label for="retry_backoff_seconds">Retry Backoff Seconds</label>
              <input id="retry_backoff_seconds" name="retry_backoff_seconds" type="number" min="0" step="0.1" value="{{ setup_form.retry_backoff_seconds }}" />
            </div>
            <div>
              <label for="default_media_limit">Default Media Limit</label>
              <input id="default_media_limit" name="default_media_limit" type="number" min="1" value="{{ setup_form.default_media_limit }}" />
            </div>
            <div>
              <label for="default_comments_per_media">Default Comments/Media</label>
              <input id="default_comments_per_media" name="default_comments_per_media" type="number" min="1" value="{{ setup_form.default_comments_per_media }}" />
            </div>
            <div>
              <label for="default_lookback_days">Default Lookback Days</label>
              <input id="default_lookback_days" name="default_lookback_days" type="number" min="1" value="{{ setup_form.default_lookback_days }}" />
            </div>
            <div>
              <label for="default_max_profiles">Default Max Profiles (optional)</label>
              <input id="default_max_profiles" name="default_max_profiles" type="number" min="1" value="{{ setup_form.default_max_profiles }}" />
            </div>
          </div>
        </details>

        <div class="actions">
          <button type="submit">Save Setup</button>
          {% if profile %}
            <a class="btn alt" href="{{ url_for('index') }}">Cancel</a>
          {% endif %}
        </div>
        <p class="muted">Storage: {{ storage_mode }}</p>
      </form>
      {% endif %}
    </section>

    <section class="card">
      <h2>Report History</h2>
      {% if runs %}
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Started</th>
            <th>Status</th>
            <th>Leads</th>
            <th>Report</th>
          </tr>
        </thead>
        <tbody>
          {% for run in runs %}
          <tr>
            <td>#{{ run.id }}</td>
            <td>{{ run.started_at_display }}</td>
            <td>
              <span class="badge {{ run.status }}">{{ run.status }}</span>
            </td>
            <td>{{ run.lead_count if run.lead_count is not none else '-' }}</td>
            <td>
              {% if run.output_filename %}
                <a href="{{ url_for('download_output', filename=run.output_filename) }}">Download CSV</a>
              {% elif run.error_message %}
                {{ run.error_message }}
              {% else %}
                -
              {% endif %}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <p class="muted">No reports yet. Save setup, then click "Run Report Now".</p>
      {% endif %}
    </section>

    {% if preview %}
    <section class="card">
      <h2>Latest Results Preview</h2>
      <table>
        <thead>
          <tr>
            <th>Instagram</th>
            <th>Podcast URL(s)</th>
            <th>Monthly Listeners (est.)</th>
            <th>Email</th>
          </tr>
        </thead>
        <tbody>
          {% for row in preview %}
          <tr>
            <td>{{ row.instagram_handle }}</td>
            <td>{{ '; '.join(row.podcast_urls) }}</td>
            <td>{{ row.estimated_monthly_listeners }}</td>
            <td>{{ row.email or '' }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </section>
    {% endif %}
  </div>
</body>
</html>
"""


def _slug(value: str) -> str:
    keep = []
    for char in value.lower().strip():
        if char.isalnum():
            keep.append(char)
        elif char in (" ", "-", "_"):
            keep.append("-")
    slug = "".join(keep).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "report"


def _to_int(raw: str, label: str, minimum: int = 1) -> int:
    try:
        value = int(raw)
    except ValueError as err:
        raise ValueError(f"{label} must be a whole number") from err
    if value < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    return value


def _to_optional_int(raw: str, label: str, minimum: int = 1) -> int | None:
    cleaned = raw.strip()
    if not cleaned:
        return None
    return _to_int(cleaned, label=label, minimum=minimum)


def _to_float(raw: str, label: str, minimum: float = 0.0) -> float:
    try:
        value = float(raw)
    except ValueError as err:
        raise ValueError(f"{label} must be a number") from err
    if value < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    return value


def _format_iso(value: str | None) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y %I:%M %p")
    except ValueError:
        return value


def _sanitize_access_token(raw: str) -> str:
    token = raw.strip().strip("\"").strip("'")
    if not token:
        return token

    if token.lower().startswith("bearer "):
        token = token.split(" ", 1)[1].strip()

    if token.startswith("http://") or token.startswith("https://"):
        parsed = urlparse(token)
        query_token = parse_qs(parsed.query).get("access_token", [None])[0]
        if query_token:
            token = query_token.strip()

    if "access_token=" in token:
        query_like = token.replace("?", "&")
        query_token = parse_qs(query_like).get("access_token", [None])[0]
        if query_token:
            token = query_token.strip()

    if token.startswith("access_token="):
        token = token.split("=", 1)[1].strip()

    return token


def _sanitize_business_account_id(raw: str) -> str:
    value = raw.strip().strip("\"").strip("'")
    if value.startswith("@"):
        value = value[1:]
    return value


def _extract_error_code(text: str) -> int | None:
    match = re.search(r"[\"']code[\"']\s*:\s*(\d+)", text)
    if match:
        return int(match.group(1))
    return None


def _friendly_api_error_text(text: str) -> str:
    lowered = text.lower()
    code = _extract_error_code(text)

    if code == 190 or "invalid oauth access token" in lowered:
        return (
            "Instagram rejected the access token. Paste only the raw token value "
            "(no 'Bearer', no quotes, no 'access_token=' prefix), then save setup again."
        )

    if code in {10, 200} or "permission" in lowered:
        return (
            "Token is missing required permissions. Regenerate it with Instagram "
            "Graph permissions for reading media/comments."
        )

    if "unsupported get request" in lowered or "unknown path components" in lowered:
        return (
            "Instagram Business Account ID appears invalid for this token. "
            "Use the numeric IG Business Account ID (example: 1784...)."
        )

    return text


def _validate_setup_credentials(
    *,
    access_token: str,
    business_account_id: str,
    graph_version: str,
) -> None:
    if not business_account_id.isdigit():
        raise ValueError(
            "Instagram Business Account ID must be numeric only (example: 1784...). "
            "Use Workspace Name for text labels like 'FamilyTeams'."
        )

    base_url = f"https://graph.facebook.com/{graph_version}"
    timeout_seconds = 15

    try:
        token_check = requests.get(
            f"{base_url}/me",
            params={"access_token": access_token},
            timeout=timeout_seconds,
        )
        token_payload = token_check.json()
    except requests.RequestException as err:
        raise ValueError(f"Could not reach Instagram API to validate token: {err}") from err

    if "error" in token_payload:
        raise ValueError(_friendly_api_error_text(str(token_payload["error"])))

    try:
        account_check = requests.get(
            f"{base_url}/{business_account_id}",
            params={"fields": "id", "access_token": access_token},
            timeout=timeout_seconds,
        )
        account_payload = account_check.json()
    except requests.RequestException as err:
        raise ValueError(f"Could not validate account ID against Instagram API: {err}") from err

    if "error" in account_payload:
        raise ValueError(_friendly_api_error_text(str(account_payload["error"])))


def _default_setup_form(profile: dict | None = None) -> dict[str, str]:
    if profile is None:
        return {
            "name": "My Workspace",
            "business_account_id": "",
            "access_token": "",
            "graph_version": "v21.0",
            "timeout_seconds": "25",
            "retry_count": "3",
            "retry_backoff_seconds": "1.5",
            "default_media_limit": "25",
            "default_comments_per_media": "200",
            "default_lookback_days": "90",
            "default_max_profiles": "",
        }

    return {
        "name": str(profile["name"]),
        "business_account_id": str(profile["business_account_id"]),
        "access_token": str(profile["access_token"]),
        "graph_version": str(profile["graph_version"]),
        "timeout_seconds": str(profile["timeout_seconds"]),
        "retry_count": str(profile["retry_count"]),
        "retry_backoff_seconds": str(profile["retry_backoff_seconds"]),
        "default_media_limit": str(profile["default_media_limit"]),
        "default_comments_per_media": str(profile["default_comments_per_media"]),
        "default_lookback_days": str(profile["default_lookback_days"]),
        "default_max_profiles": "" if profile["default_max_profiles"] is None else str(profile["default_max_profiles"]),
    }


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _bootstrap_profile_from_env_if_missing() -> None:
    if get_active_profile() is not None:
        return

    access_token = _sanitize_access_token(os.getenv("IG_ACCESS_TOKEN", ""))
    business_account_id = _sanitize_business_account_id(os.getenv("IG_BUSINESS_ACCOUNT_ID", ""))
    if not access_token or not business_account_id:
        return

    upsert_active_profile(
        name=os.getenv("IG_PROFILE_NAME", "Default Workspace").strip() or "Default Workspace",
        business_account_id=business_account_id,
        access_token=access_token,
        graph_version=os.getenv("IG_GRAPH_VERSION", "v21.0").strip() or "v21.0",
        timeout_seconds=_env_int("REQUEST_TIMEOUT_SECONDS", 25),
        retry_count=_env_int("REQUEST_RETRY_COUNT", 3),
        retry_backoff_seconds=_env_float("REQUEST_RETRY_BACKOFF_SECONDS", 1.5),
        default_media_limit=_env_int("DEFAULT_MEDIA_LIMIT", 25),
        default_comments_per_media=_env_int("DEFAULT_COMMENTS_PER_MEDIA", 200),
        default_lookback_days=_env_int("DEFAULT_LOOKBACK_DAYS", 90),
        default_max_profiles=_env_int("DEFAULT_MAX_PROFILES", 0) or None,
    )


def _render_page(
    *,
    message: str | None = None,
    error: str | None = None,
    preview: list | None = None,
    setup_form: dict[str, str] | None = None,
    show_setup_form: bool | None = None,
):
    profile = get_active_profile()
    runs = list_runs(limit=50)
    runs_view = []
    for run in runs:
        item = dict(run)
        item["started_at_display"] = _format_iso(run.get("started_at"))
        if run.get("status") not in {"success", "failed", "running"}:
            item["status"] = "running"
        runs_view.append(item)

    total_runs = len(runs)
    success_runs = len([row for row in runs if row.get("status") == "success"])
    last_success = next((row for row in runs if row.get("status") == "success"), None)
    last_report_at = _format_iso(last_success.get("completed_at")) if last_success else "-"

    effective_show_setup = profile is None if show_setup_form is None else show_setup_form
    storage_mode = f"ephemeral ({db_path()})" if os.getenv("VERCEL") == "1" else str(db_path())

    return render_template_string(
        INDEX_HTML,
        profile=profile,
        runs=runs_view,
        message=message,
        error=error,
        preview=preview,
        setup_form=setup_form or _default_setup_form(profile),
        show_setup_form=effective_show_setup,
        storage_mode=storage_mode,
        stats={
            "total_runs": total_runs,
            "success_runs": success_runs,
            "last_report_at": last_report_at,
        },
    )


def create_app() -> Flask:
    app = Flask(__name__)
    init_db()
    _bootstrap_profile_from_env_if_missing()

    @app.get("/")
    def index():
        edit_setup = request.args.get("edit_setup") == "1"
        return _render_page(show_setup_form=edit_setup)

    @app.post("/setup")
    def setup():
        form = {
            "name": request.form.get("name", "").strip(),
            "business_account_id": _sanitize_business_account_id(request.form.get("business_account_id", "")),
            "access_token": _sanitize_access_token(request.form.get("access_token", "")),
            "graph_version": request.form.get("graph_version", "v21.0").strip() or "v21.0",
            "timeout_seconds": request.form.get("timeout_seconds", "25").strip() or "25",
            "retry_count": request.form.get("retry_count", "3").strip() or "3",
            "retry_backoff_seconds": request.form.get("retry_backoff_seconds", "1.5").strip() or "1.5",
            "default_media_limit": request.form.get("default_media_limit", "25").strip() or "25",
            "default_comments_per_media": request.form.get("default_comments_per_media", "200").strip() or "200",
            "default_lookback_days": request.form.get("default_lookback_days", "90").strip() or "90",
            "default_max_profiles": request.form.get("default_max_profiles", "").strip(),
        }

        try:
            if not form["name"]:
                raise ValueError("Workspace name is required")
            if not form["business_account_id"]:
                raise ValueError("Instagram Business Account ID is required")
            if not form["access_token"]:
                raise ValueError("Instagram access token is required")
            if not form["business_account_id"].isdigit():
                raise ValueError(
                    "Instagram Business Account ID must be numeric only (example: 1784...). "
                    "Use Workspace Name for labels like 'FamilyTeams'."
                )

            _validate_setup_credentials(
                access_token=form["access_token"],
                business_account_id=form["business_account_id"],
                graph_version=form["graph_version"],
            )

            upsert_active_profile(
                name=form["name"],
                business_account_id=form["business_account_id"],
                access_token=form["access_token"],
                graph_version=form["graph_version"],
                timeout_seconds=_to_int(form["timeout_seconds"], "Timeout seconds", minimum=1),
                retry_count=_to_int(form["retry_count"], "Retry count", minimum=0),
                retry_backoff_seconds=_to_float(form["retry_backoff_seconds"], "Retry backoff seconds", minimum=0.0),
                default_media_limit=_to_int(form["default_media_limit"], "Default media limit", minimum=1),
                default_comments_per_media=_to_int(
                    form["default_comments_per_media"],
                    "Default comments per media",
                    minimum=1,
                ),
                default_lookback_days=_to_int(form["default_lookback_days"], "Default lookback days", minimum=1),
                default_max_profiles=_to_optional_int(form["default_max_profiles"], "Default max profiles", minimum=1),
            )
            return _render_page(message="Setup saved. You can run reports now.", show_setup_form=False)
        except Exception as err:  # noqa: BLE001
            return _render_page(error=str(err), setup_form=form, show_setup_form=True)

    @app.post("/run")
    def run_scrub():
        profile = get_active_profile()
        if profile is None:
            return _render_page(error="Complete one-time setup first.", show_setup_form=True)

        media_limit = int(profile["default_media_limit"])
        comments_per_media = int(profile["default_comments_per_media"])
        lookback_days = int(profile["default_lookback_days"])
        max_profiles = profile["default_max_profiles"]

        run_id = create_run(
            profile_id=int(profile["id"]),
            media_limit=media_limit,
            comments_per_media=comments_per_media,
            lookback_days=lookback_days,
            max_profiles=max_profiles,
        )

        try:
            settings = build_settings(
                access_token=str(profile["access_token"]),
                business_account_id=str(profile["business_account_id"]),
                graph_version=str(profile["graph_version"]),
                timeout_seconds=int(profile["timeout_seconds"]),
                retry_count=int(profile["retry_count"]),
                retry_backoff_seconds=float(profile["retry_backoff_seconds"]),
            )
            client = InstagramGraphClient(settings)
            records = build_leads(
                client=client,
                media_limit=media_limit,
                comments_per_media=comments_per_media,
                lookback_days=lookback_days,
                max_profiles=max_profiles,
            )
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"report_{_slug(str(profile['name']))}_{timestamp}.csv"
            output_path = write_csv(records, str(OUTPUT_DIR / filename))
            finish_run_success(run_id, lead_count=len(records), output_filename=output_path.name)

            message = (
                f"Report complete. Found {len(records)} leads. "
                f"<a href='{url_for('download_output', filename=output_path.name)}'>Download CSV</a>."
            )
            return _render_page(message=message, preview=records[:25], show_setup_form=False)
        except Exception as err:  # noqa: BLE001
            finish_run_failure(run_id, str(err))
            friendly = _friendly_api_error_text(str(err))
            return _render_page(error=f"Run failed: {friendly}", show_setup_form=False)

    @app.get("/download/<path:filename>")
    def download_output(filename: str):
        output_root = OUTPUT_DIR.resolve()
        candidate = (output_root / filename).resolve()
        if output_root not in candidate.parents and candidate != output_root:
            abort(404)
        if not candidate.exists() or not candidate.is_file():
            abort(404)
        return send_from_directory(str(output_root), filename, as_attachment=True)

    return app


def main() -> int:
    app = create_app()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
