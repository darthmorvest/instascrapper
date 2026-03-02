from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

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
  <title>Instagram Lead Scrubber</title>
  <style>
    :root {
      --bg-1: #f5f9ff;
      --bg-2: #eaf7f2;
      --panel: #ffffff;
      --line: #d8e3ef;
      --text: #14263b;
      --sub: #4f6479;
      --accent: #0f766e;
      --accent-2: #0e7490;
      --ok-bg: #dcfce7;
      --ok-line: #86efac;
      --warn-bg: #fff1f2;
      --warn-line: #fecdd3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      font-family: "Avenir Next", "Nunito Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 12% 0%, #d8efff 0%, rgba(216, 239, 255, 0) 40%),
        linear-gradient(180deg, var(--bg-1), var(--bg-2));
      min-height: 100vh;
      padding: 20px 14px 42px;
    }
    .wrap {
      max-width: 1050px;
      margin: 0 auto;
      display: grid;
      gap: 14px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 15px;
      box-shadow: 0 10px 24px rgba(4, 33, 68, 0.06);
    }
    h1, h2 {
      margin: 0;
    }
    h1 {
      font-size: 1.75rem;
      letter-spacing: 0.15px;
    }
    h2 {
      font-size: 1.08rem;
      margin-bottom: 8px;
    }
    .sub {
      margin-top: 6px;
      color: var(--sub);
      font-size: 0.95rem;
    }
    .steps {
      margin-top: 8px;
      color: var(--sub);
      font-size: 0.9rem;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .step {
      border: 1px solid #cde8db;
      background: #f1fdf7;
      border-radius: 999px;
      padding: 4px 10px;
    }
    .notice,
    .error {
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid;
      font-size: 0.92rem;
    }
    .notice {
      background: var(--ok-bg);
      border-color: var(--ok-line);
      color: #065f46;
    }
    .error {
      background: var(--warn-bg);
      border-color: var(--warn-line);
      color: #881337;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 10px;
    }
    label {
      display: block;
      margin-bottom: 5px;
      color: var(--sub);
      font-size: 0.84rem;
    }
    input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font-size: 0.92rem;
      color: #0f172a;
      background: #fff;
    }
    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      margin-top: 10px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      color: #fff;
      cursor: pointer;
      font-size: 0.9rem;
      font-weight: 600;
      background: linear-gradient(145deg, var(--accent), var(--accent-2));
    }
    .secondary {
      background: #fff;
      border: 1px solid #c3d7ea;
      color: #1e3a5f;
    }
    .muted {
      color: var(--sub);
      font-size: 0.86rem;
    }
    details {
      margin-top: 10px;
      border-top: 1px dashed #dbe5ef;
      padding-top: 10px;
    }
    summary {
      cursor: pointer;
      color: #164e63;
      font-size: 0.9rem;
      font-weight: 600;
    }
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
      color: var(--sub);
      font-weight: 600;
      background: #f8fbff;
    }
    a {
      color: #0f4f8a;
      text-decoration: none;
    }
    a:hover { text-decoration: underline; }
    @media (max-width: 760px) {
      .actions {
        flex-direction: column;
        align-items: stretch;
      }
      button {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card">
      <h1>Instagram Lead Scrubber</h1>
      <div class="sub">Simple workflow: setup once, then run reports whenever you want.</div>
      <div class="steps">
        <span class="step">1. Setup once</span>
        <span class="step">2. Click Run Report</span>
        <span class="step">3. View old reports</span>
      </div>
    </section>

    {% if message %}
      <section class="card"><div class="notice">{{ message|safe }}</div></section>
    {% endif %}
    {% if error %}
      <section class="card"><div class="error">{{ error }}</div></section>
    {% endif %}

    <section class="card">
      {% if profile %}
        <h2>Setup Complete</h2>
        <div class="sub">Connected to account ID <strong>{{ profile.business_account_id }}</strong> as "{{ profile.name }}".</div>
        <div class="actions">
          <form method="post" action="{{ url_for('run_scrub') }}">
            <button type="submit">Run Report Now</button>
          </form>
          <form method="get" action="{{ url_for('index') }}">
            <input type="hidden" name="edit_setup" value="1" />
            <button type="submit" class="secondary">Edit Setup</button>
          </form>
        </div>
        <div class="muted">Uses defaults: media {{ profile.default_media_limit }}, comments {{ profile.default_comments_per_media }}, lookback {{ profile.default_lookback_days }} days.</div>
      {% else %}
        <h2>One-Time Setup</h2>
        <div class="sub">Add your Instagram API details once, then you can run reports from one button.</div>
      {% endif %}

      {% if show_setup_form %}
      <form method="post" action="{{ url_for('setup') }}" style="margin-top: 12px;">
        <div class="grid">
          <div>
            <label for="name">Workspace name</label>
            <input id="name" name="name" value="{{ setup_form.name }}" placeholder="My Lead Scrubber" required />
          </div>
          <div>
            <label for="business_account_id">Instagram Business Account ID</label>
            <input id="business_account_id" name="business_account_id" value="{{ setup_form.business_account_id }}" required />
          </div>
          <div>
            <label for="access_token">Instagram Access Token</label>
            <input id="access_token" name="access_token" value="{{ setup_form.access_token }}" required />
          </div>
        </div>

        <details>
          <summary>Advanced defaults (optional)</summary>
          <div class="grid" style="margin-top: 10px;">
            <div>
              <label for="graph_version">Graph version</label>
              <input id="graph_version" name="graph_version" value="{{ setup_form.graph_version }}" />
            </div>
            <div>
              <label for="timeout_seconds">Timeout seconds</label>
              <input id="timeout_seconds" name="timeout_seconds" type="number" min="1" value="{{ setup_form.timeout_seconds }}" />
            </div>
            <div>
              <label for="retry_count">Retry count</label>
              <input id="retry_count" name="retry_count" type="number" min="0" value="{{ setup_form.retry_count }}" />
            </div>
            <div>
              <label for="retry_backoff_seconds">Retry backoff seconds</label>
              <input id="retry_backoff_seconds" name="retry_backoff_seconds" type="number" min="0" step="0.1" value="{{ setup_form.retry_backoff_seconds }}" />
            </div>
            <div>
              <label for="default_media_limit">Default media limit</label>
              <input id="default_media_limit" name="default_media_limit" type="number" min="1" value="{{ setup_form.default_media_limit }}" />
            </div>
            <div>
              <label for="default_comments_per_media">Default comments/media</label>
              <input id="default_comments_per_media" name="default_comments_per_media" type="number" min="1" value="{{ setup_form.default_comments_per_media }}" />
            </div>
            <div>
              <label for="default_lookback_days">Default lookback days</label>
              <input id="default_lookback_days" name="default_lookback_days" type="number" min="1" value="{{ setup_form.default_lookback_days }}" />
            </div>
            <div>
              <label for="default_max_profiles">Default max profiles (optional)</label>
              <input id="default_max_profiles" name="default_max_profiles" type="number" min="1" value="{{ setup_form.default_max_profiles }}" />
            </div>
          </div>
        </details>

        <div class="actions">
          <button type="submit">Save Setup</button>
          {% if profile %}
            <a href="{{ url_for('index') }}" class="secondary" style="display:inline-block;padding:10px 14px;border-radius:10px;border:1px solid #c3d7ea;color:#1e3a5f;">Cancel</a>
          {% endif %}
        </div>
        <div class="muted">Storage path: {{ storage_mode }}</div>
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
            <td>{{ run.started_at }}</td>
            <td>{{ run.status }}</td>
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
      <div class="sub">No reports yet. Click "Run Report Now" after setup.</div>
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
              <th>Monthly Listeners (Est.)</th>
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


def _default_setup_form(profile: dict | None = None) -> dict[str, str]:
    if profile is None:
        return {
            "name": "My Lead Scrubber",
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

    access_token = os.getenv("IG_ACCESS_TOKEN", "").strip()
    business_account_id = os.getenv("IG_BUSINESS_ACCOUNT_ID", "").strip()
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
    effective_show_setup = profile is None if show_setup_form is None else show_setup_form
    storage_mode = f"ephemeral ({db_path()})" if os.getenv("VERCEL") == "1" else str(db_path())

    return render_template_string(
        INDEX_HTML,
        profile=profile,
        runs=runs,
        message=message,
        error=error,
        preview=preview,
        setup_form=setup_form or _default_setup_form(profile),
        show_setup_form=effective_show_setup,
        storage_mode=storage_mode,
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
            "business_account_id": request.form.get("business_account_id", "").strip(),
            "access_token": request.form.get("access_token", "").strip(),
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
            return _render_page(message="Setup saved. You can now run reports.", show_setup_form=False)
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
            return _render_page(error=f"Run failed: {err}", show_setup_form=False)

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
