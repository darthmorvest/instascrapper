from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

from flask import Flask, abort, render_template_string, request, send_from_directory, url_for

from .config import build_settings
from .exporters import write_csv
from .instagram_api import InstagramGraphClient
from .pipeline import build_leads
from .storage import (
    create_profile,
    create_run,
    db_path,
    delete_profile,
    finish_run_failure,
    finish_run_success,
    get_profile,
    init_db,
    list_profiles,
    list_runs,
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
  <title>Lead Scrubber Console</title>
  <style>
    :root {
      --bg-1: #f0f8fb;
      --bg-2: #e9f5ff;
      --panel: #ffffff;
      --text: #122236;
      --sub: #4a6077;
      --line: #d6e3ed;
      --accent: #0e7490;
      --accent-2: #0369a1;
      --warn: #b91c1c;
      --ok-bg: #ddfbef;
      --ok-line: #79e4b1;
      --warn-bg: #ffecec;
      --warn-line: #f8b2b2;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: "Avenir Next", "Nunito Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 15% 0%, #dcf4ff 0%, rgba(220, 244, 255, 0) 40%),
        linear-gradient(180deg, var(--bg-1) 0%, var(--bg-2) 40%, #eef9f4 100%);
      padding: 22px 14px 40px;
    }
    .shell {
      max-width: 1180px;
      margin: 0 auto;
      display: grid;
      gap: 14px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 15px;
      box-shadow: 0 12px 26px rgba(2, 32, 71, 0.06);
    }
    h1, h2 { margin: 0; }
    h1 {
      font-size: 1.7rem;
      letter-spacing: 0.2px;
    }
    h2 {
      font-size: 1.05rem;
      margin-bottom: 10px;
    }
    .sub {
      margin-top: 6px;
      color: var(--sub);
      font-size: 0.95rem;
    }
    .meta {
      margin-top: 8px;
      font-size: 0.83rem;
      color: var(--sub);
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .pill {
      background: #eff8ff;
      border: 1px solid #cbe8ff;
      border-radius: 999px;
      padding: 4px 9px;
    }
    .notice,
    .error {
      border-radius: 11px;
      padding: 10px 12px;
      font-size: 0.93rem;
      border: 1px solid;
    }
    .notice {
      background: var(--ok-bg);
      border-color: var(--ok-line);
      color: #065f46;
    }
    .error {
      background: var(--warn-bg);
      border-color: var(--warn-line);
      color: #7f1d1d;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 10px;
    }
    label {
      display: block;
      font-size: 0.83rem;
      color: var(--sub);
      margin-bottom: 5px;
    }
    input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 11px;
      font-size: 0.92rem;
      color: #0f172a;
      background: #fff;
    }
    .controls {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    button {
      border: 0;
      border-radius: 10px;
      padding: 10px 13px;
      font-size: 0.9rem;
      font-weight: 600;
      cursor: pointer;
      background: linear-gradient(145deg, var(--accent), var(--accent-2));
      color: #fff;
    }
    button:hover { filter: brightness(0.96); }
    .danger {
      background: #fff;
      color: var(--warn);
      border: 1px solid #f6b5b5;
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
    .inline {
      display: inline-grid;
      grid-template-columns: repeat(4, minmax(95px, 1fr));
      gap: 6px;
      min-width: 420px;
    }
    .inline input {
      padding: 7px 8px;
      font-size: 0.84rem;
    }
    .actions {
      display: flex;
      gap: 6px;
      align-items: center;
      flex-wrap: wrap;
    }
    a { color: #0a4f84; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .hint {
      margin-top: 8px;
      color: var(--sub);
      font-size: 0.84rem;
    }
    @media (max-width: 760px) {
      .inline {
        grid-template-columns: 1fr 1fr;
        min-width: 0;
      }
      .actions {
        flex-direction: column;
        align-items: stretch;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="panel">
      <h1>Lead Scrubber Console</h1>
      <div class="sub">Create reusable scrub profiles and trigger runs with one click.</div>
      <div class="meta">
        <span class="pill">Profiles: {{ profiles|length }}</span>
        <span class="pill">Runs logged: {{ runs|length }}</span>
        <span class="pill">Storage: {{ storage_mode }}</span>
      </div>
    </section>

    {% if message %}
      <section class="panel"><div class="notice">{{ message|safe }}</div></section>
    {% endif %}
    {% if error %}
      <section class="panel"><div class="error">{{ error }}</div></section>
    {% endif %}

    <section class="panel">
      <h2>Create Profile</h2>
      <form method="post" action="{{ url_for('create_profile_route') }}">
        <div class="grid">
          <div>
            <label for="name">Profile name</label>
            <input id="name" name="name" required value="{{ profile_form.name }}" placeholder="Client A - Main IG" />
          </div>
          <div>
            <label for="business_account_id">Instagram Business Account ID</label>
            <input id="business_account_id" name="business_account_id" required value="{{ profile_form.business_account_id }}" />
          </div>
          <div>
            <label for="access_token">Access token</label>
            <input id="access_token" name="access_token" required value="{{ profile_form.access_token }}" />
          </div>
          <div>
            <label for="graph_version">Graph version</label>
            <input id="graph_version" name="graph_version" value="{{ profile_form.graph_version }}" />
          </div>
          <div>
            <label for="timeout_seconds">Timeout seconds</label>
            <input id="timeout_seconds" name="timeout_seconds" type="number" min="1" value="{{ profile_form.timeout_seconds }}" />
          </div>
          <div>
            <label for="retry_count">Retry count</label>
            <input id="retry_count" name="retry_count" type="number" min="0" value="{{ profile_form.retry_count }}" />
          </div>
          <div>
            <label for="retry_backoff_seconds">Retry backoff seconds</label>
            <input id="retry_backoff_seconds" name="retry_backoff_seconds" type="number" step="0.1" min="0" value="{{ profile_form.retry_backoff_seconds }}" />
          </div>
          <div>
            <label for="default_media_limit">Default media limit</label>
            <input id="default_media_limit" name="default_media_limit" type="number" min="1" value="{{ profile_form.default_media_limit }}" />
          </div>
          <div>
            <label for="default_comments_per_media">Default comments/media</label>
            <input id="default_comments_per_media" name="default_comments_per_media" type="number" min="1" value="{{ profile_form.default_comments_per_media }}" />
          </div>
          <div>
            <label for="default_lookback_days">Default lookback days</label>
            <input id="default_lookback_days" name="default_lookback_days" type="number" min="1" value="{{ profile_form.default_lookback_days }}" />
          </div>
          <div>
            <label for="default_max_profiles">Default max profiles (optional)</label>
            <input id="default_max_profiles" name="default_max_profiles" type="number" min="1" value="{{ profile_form.default_max_profiles }}" />
          </div>
        </div>
        <div class="controls"><button type="submit">Save Profile</button></div>
      </form>
      <div class="hint">Access tokens are stored in app storage for reusable runs. Use separate profiles for each client account.</div>
    </section>

    <section class="panel">
      <h2>Profiles</h2>
      {% if profiles %}
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Account</th>
            <th>Defaults</th>
            <th>Run Now</th>
          </tr>
        </thead>
        <tbody>
          {% for p in profiles %}
          <tr>
            <td>
              <strong>{{ p.name }}</strong><br />
              <span class="hint">Graph {{ p.graph_version }} | timeout {{ p.timeout_seconds }}s</span>
            </td>
            <td>{{ p.business_account_id }}</td>
            <td>
              media {{ p.default_media_limit }}, comments {{ p.default_comments_per_media }}, lookback {{ p.default_lookback_days }}, max {{ p.default_max_profiles if p.default_max_profiles is not none else "all" }}
            </td>
            <td>
              <form method="post" action="{{ url_for('run_scrub') }}" class="actions">
                <input type="hidden" name="profile_id" value="{{ p.id }}" />
                <div class="inline">
                  <input name="media_limit" type="number" min="1" value="{{ p.default_media_limit }}" title="Media limit" />
                  <input name="comments_per_media" type="number" min="1" value="{{ p.default_comments_per_media }}" title="Comments per media" />
                  <input name="lookback_days" type="number" min="1" value="{{ p.default_lookback_days }}" title="Lookback days" />
                  <input name="max_profiles" type="number" min="1" value="{{ p.default_max_profiles if p.default_max_profiles is not none else '' }}" placeholder="max" title="Max profiles" />
                </div>
                <button type="submit">Run</button>
              </form>
              <form method="post" action="{{ url_for('delete_profile_route', profile_id=p.id) }}" onsubmit="return confirm('Delete this profile and its run history?')">
                <button class="danger" type="submit">Delete</button>
              </form>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div class="hint">No profiles yet. Create one above to start running scrubs.</div>
      {% endif %}
    </section>

    <section class="panel">
      <h2>Run History</h2>
      {% if runs %}
      <table>
        <thead>
          <tr>
            <th>Run ID</th>
            <th>Profile</th>
            <th>Status</th>
            <th>Started</th>
            <th>Params</th>
            <th>Result</th>
          </tr>
        </thead>
        <tbody>
          {% for run in runs %}
          <tr>
            <td>{{ run.id }}</td>
            <td>{{ run.profile_name }}</td>
            <td>{{ run.status }}</td>
            <td>{{ run.started_at }}</td>
            <td>
              media {{ run.media_limit }}, comments {{ run.comments_per_media }}, lookback {{ run.lookback_days }}, max {{ run.max_profiles if run.max_profiles is not none else "all" }}
            </td>
            <td>
              {% if run.status == 'success' %}
                {{ run.lead_count }} leads
                {% if run.output_filename %}
                  | <a href="{{ url_for('download_output', filename=run.output_filename) }}">CSV</a>
                {% endif %}
              {% else %}
                {{ run.error_message or 'Failed' }}
              {% endif %}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <div class="hint">No runs yet.</div>
      {% endif %}
    </section>

    {% if preview %}
    <section class="panel">
      <h2>Latest Preview (Top {{ preview|length }})</h2>
      <table>
        <thead>
          <tr>
            <th>Handle</th>
            <th>Podcast URLs</th>
            <th>Est. Listeners</th>
            <th>Confidence</th>
            <th>Email</th>
            <th>Website</th>
          </tr>
        </thead>
        <tbody>
          {% for row in preview %}
          <tr>
            <td>{{ row.instagram_handle }}</td>
            <td>{{ '; '.join(row.podcast_urls) }}</td>
            <td>{{ row.estimated_monthly_listeners }}</td>
            <td>{{ row.estimate_confidence }}</td>
            <td>{{ row.email or '' }}</td>
            <td>{{ row.website or '' }}</td>
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
    return slug or "profile"


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


def _default_profile_form() -> dict[str, str]:
    return {
        "name": "",
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


def _render_page(
    *,
    message: str | None = None,
    error: str | None = None,
    preview: list | None = None,
    profile_form: dict[str, str] | None = None,
):
    profiles = list_profiles()
    runs = list_runs(limit=40)
    storage_mode = f"ephemeral ({db_path()})" if os.getenv("VERCEL") == "1" else str(db_path())
    return render_template_string(
        INDEX_HTML,
        profiles=profiles,
        runs=runs,
        message=message,
        error=error,
        preview=preview,
        profile_form=profile_form or _default_profile_form(),
        storage_mode=storage_mode,
    )


def create_app() -> Flask:
    app = Flask(__name__)
    init_db()

    @app.get("/")
    def index():
        return _render_page()

    @app.post("/profiles")
    def create_profile_route():
        form = {k: request.form.get(k, "").strip() for k in _default_profile_form()}
        try:
            if not form["name"]:
                raise ValueError("Profile name is required")
            if not form["access_token"]:
                raise ValueError("Access token is required")
            if not form["business_account_id"]:
                raise ValueError("Instagram Business Account ID is required")

            create_profile(
                name=form["name"],
                access_token=form["access_token"],
                business_account_id=form["business_account_id"],
                graph_version=form["graph_version"] or "v21.0",
                timeout_seconds=_to_int(form["timeout_seconds"], "Timeout seconds", minimum=1),
                retry_count=_to_int(form["retry_count"], "Retry count", minimum=0),
                retry_backoff_seconds=_to_float(
                    form["retry_backoff_seconds"],
                    "Retry backoff seconds",
                    minimum=0.0,
                ),
                default_media_limit=_to_int(form["default_media_limit"], "Default media limit", minimum=1),
                default_comments_per_media=_to_int(
                    form["default_comments_per_media"],
                    "Default comments per media",
                    minimum=1,
                ),
                default_lookback_days=_to_int(
                    form["default_lookback_days"],
                    "Default lookback days",
                    minimum=1,
                ),
                default_max_profiles=_to_optional_int(
                    form["default_max_profiles"],
                    "Default max profiles",
                    minimum=1,
                ),
            )
            return _render_page(
                message=f"Profile <strong>{form['name']}</strong> saved.",
                profile_form=_default_profile_form(),
            )
        except sqlite3.IntegrityError:
            return _render_page(error="A profile with that name already exists.", profile_form=form)
        except Exception as err:  # noqa: BLE001
            return _render_page(error=str(err), profile_form=form)

    @app.post("/profiles/<int:profile_id>/delete")
    def delete_profile_route(profile_id: int):
        delete_profile(profile_id)
        return _render_page(message="Profile deleted.")

    @app.post("/run")
    def run_scrub():
        try:
            profile_id_raw = request.form.get("profile_id", "").strip()
            if not profile_id_raw:
                raise ValueError("Profile is required")
            profile_id = _to_int(profile_id_raw, "Profile id", minimum=1)

            profile = get_profile(profile_id)
            if profile is None:
                raise ValueError("Profile not found")

            media_limit = _to_int(
                request.form.get("media_limit", str(profile["default_media_limit"])).strip() or str(profile["default_media_limit"]),
                "Media limit",
                minimum=1,
            )
            comments_per_media = _to_int(
                request.form.get("comments_per_media", str(profile["default_comments_per_media"])).strip()
                or str(profile["default_comments_per_media"]),
                "Comments per media",
                minimum=1,
            )
            lookback_days = _to_int(
                request.form.get("lookback_days", str(profile["default_lookback_days"])).strip()
                or str(profile["default_lookback_days"]),
                "Lookback days",
                minimum=1,
            )
            max_profiles_input = request.form.get("max_profiles", "").strip()
            if max_profiles_input:
                max_profiles = _to_int(max_profiles_input, "Max profiles", minimum=1)
            else:
                max_profiles = profile["default_max_profiles"]

            run_id = create_run(
                profile_id=profile_id,
                media_limit=media_limit,
                comments_per_media=comments_per_media,
                lookback_days=lookback_days,
                max_profiles=max_profiles,
            )

            try:
                settings = build_settings(
                    access_token=profile["access_token"],
                    business_account_id=profile["business_account_id"],
                    graph_version=profile["graph_version"],
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
                filename = f"leads_{_slug(profile['name'])}_{timestamp}.csv"
                output_path = write_csv(records, str(OUTPUT_DIR / filename))
                finish_run_success(run_id, lead_count=len(records), output_filename=output_path.name)

                message = (
                    f"Run complete for <strong>{profile['name']}</strong>. Found {len(records)} leads. "
                    f"<a href='{url_for('download_output', filename=output_path.name)}'>Download CSV</a>."
                )
                return _render_page(message=message, preview=records[:25])
            except Exception as err:  # noqa: BLE001
                finish_run_failure(run_id, str(err))
                return _render_page(error=f"Run failed: {err}")
        except Exception as err:  # noqa: BLE001
            return _render_page(error=str(err))

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
