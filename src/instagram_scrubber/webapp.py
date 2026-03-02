from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from flask import Flask, abort, render_template_string, request, send_from_directory, url_for

from .config import load_settings
from .exporters import write_csv
from .instagram_api import InstagramGraphClient
from .pipeline import build_leads

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
  <title>Instagram Podcast Lead Scrubber</title>
  <style>
    :root {
      --bg: #f4f5f7;
      --card: #ffffff;
      --text: #1b2430;
      --muted: #4d5c6d;
      --accent: #0f766e;
      --accent-soft: #d6f5f1;
      --border: #d7dde5;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", Tahoma, sans-serif;
      background: linear-gradient(180deg, #eff4f9 0%, #f7f8fa 45%, #eef8f6 100%);
      color: var(--text);
      min-height: 100vh;
      padding: 32px 16px;
    }
    .wrap {
      max-width: 1080px;
      margin: 0 auto;
      display: grid;
      gap: 16px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 8px 20px rgba(17, 24, 39, 0.05);
    }
    h1 {
      margin: 0;
      font-size: 1.8rem;
      letter-spacing: 0.2px;
    }
    .sub {
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.97rem;
    }
    form {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      align-items: end;
    }
    label {
      font-size: 0.85rem;
      color: var(--muted);
      margin-bottom: 6px;
      display: block;
    }
    input {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 0.95rem;
      background: #fff;
    }
    button {
      border: none;
      border-radius: 10px;
      padding: 11px 14px;
      background: var(--accent);
      color: white;
      font-weight: 600;
      cursor: pointer;
      width: 100%;
    }
    button:hover {
      filter: brightness(0.96);
    }
    .notice {
      background: var(--accent-soft);
      border: 1px solid #9de4d9;
      color: #0f5132;
      padding: 10px 12px;
      border-radius: 10px;
      font-size: 0.92rem;
    }
    .error {
      background: #ffefef;
      border: 1px solid #ffb3b3;
      color: #7f1d1d;
      padding: 10px 12px;
      border-radius: 10px;
      font-size: 0.92rem;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      font-size: 0.91rem;
    }
    th, td {
      text-align: left;
      border-bottom: 1px solid var(--border);
      padding: 8px 6px;
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 600; }
    .list {
      margin: 8px 0 0;
      padding-left: 16px;
      color: var(--muted);
      font-size: 0.92rem;
    }
    .list a {
      color: #155e75;
      text-decoration: none;
    }
    .list a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card">
      <h1>Instagram Podcast Lead Scrubber</h1>
      <div class="sub">Runs your verified podcast lead pipeline and generates downloadable CSV files.</div>
    </section>

    <section class="card">
      <form method="post" action="{{ url_for('run_scrub') }}">
        <div>
          <label for="media_limit">Media limit</label>
          <input id="media_limit" name="media_limit" type="number" min="1" value="{{ form.media_limit }}" required />
        </div>
        <div>
          <label for="comments_per_media">Comments per media</label>
          <input id="comments_per_media" name="comments_per_media" type="number" min="1" value="{{ form.comments_per_media }}" required />
        </div>
        <div>
          <label for="lookback_days">Lookback days</label>
          <input id="lookback_days" name="lookback_days" type="number" min="1" value="{{ form.lookback_days }}" required />
        </div>
        <div>
          <label for="max_profiles">Max profiles (optional)</label>
          <input id="max_profiles" name="max_profiles" type="number" min="1" value="{{ form.max_profiles }}" />
        </div>
        <div>
          <button type="submit">Run Scrub</button>
        </div>
      </form>
    </section>

    {% if message %}
      <section class="card">
        <div class="notice">{{ message|safe }}</div>
      </section>
    {% endif %}

    {% if error %}
      <section class="card">
        <div class="error">{{ error }}</div>
      </section>
    {% endif %}

    {% if preview %}
      <section class="card">
        <h2>Latest Preview</h2>
        <table>
          <thead>
            <tr>
              <th>Handle</th>
              <th>Podcast URLs</th>
              <th>Est. Monthly Listeners</th>
              <th>Confidence</th>
              <th>Email</th>
              <th>Website</th>
            </tr>
          </thead>
          <tbody>
          {% for row in preview %}
            <tr>
              <td>{{ row.instagram_handle }}</td>
              <td>{{ "; ".join(row.podcast_urls) }}</td>
              <td>{{ row.estimated_monthly_listeners }}</td>
              <td>{{ row.estimate_confidence }}</td>
              <td>{{ row.email or "" }}</td>
              <td>{{ row.website or "" }}</td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      </section>
    {% endif %}

    {% if recent_outputs %}
      <section class="card">
        <h2>Recent CSV Files</h2>
        <ul class="list">
          {% for item in recent_outputs %}
            <li>
              <a href="{{ url_for('download_output', filename=item.name) }}">{{ item.name }}</a>
              ({{ item.size_kb }} KB, {{ item.modified }})
            </li>
          {% endfor %}
        </ul>
      </section>
    {% endif %}
  </div>
</body>
</html>
"""


def _to_int(form_key: str, default: int) -> int:
    raw = request.form.get(form_key, str(default)).strip()
    return int(raw) if raw else default


def _to_optional_int(form_key: str) -> int | None:
    raw = request.form.get(form_key, "").strip()
    return int(raw) if raw else None


def _recent_outputs(limit: int = 10) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(OUTPUT_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
        stat = path.stat()
        rows.append(
            {
                "name": path.name,
                "size_kb": f"{stat.st_size / 1024:.1f}",
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return rows


def _default_form() -> dict[str, str]:
    return {
        "media_limit": "25",
        "comments_per_media": "200",
        "lookback_days": "90",
        "max_profiles": "",
    }


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return render_template_string(
            INDEX_HTML,
            form=_default_form(),
            message=None,
            error=None,
            preview=None,
            recent_outputs=_recent_outputs(),
        )

    @app.post("/run")
    def run_scrub():
        form_state = {
            "media_limit": request.form.get("media_limit", "25"),
            "comments_per_media": request.form.get("comments_per_media", "200"),
            "lookback_days": request.form.get("lookback_days", "90"),
            "max_profiles": request.form.get("max_profiles", ""),
        }

        try:
            media_limit = _to_int("media_limit", 25)
            comments_per_media = _to_int("comments_per_media", 200)
            lookback_days = _to_int("lookback_days", 90)
            max_profiles = _to_optional_int("max_profiles")

            settings = load_settings()
            client = InstagramGraphClient(settings)
            records = build_leads(
                client=client,
                media_limit=media_limit,
                comments_per_media=comments_per_media,
                lookback_days=lookback_days,
                max_profiles=max_profiles,
            )
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"leads_{timestamp}.csv"
            output_path = write_csv(records, str(OUTPUT_DIR / filename))
            message = (
                f"Run complete. Found {len(records)} leads. "
                f"<a href='{url_for('download_output', filename=output_path.name)}'>Download latest CSV</a>."
            )
            return render_template_string(
                INDEX_HTML,
                form=form_state,
                message=message,
                error=None,
                preview=records[:25],
                recent_outputs=_recent_outputs(),
            )
        except Exception as err:  # noqa: BLE001
            return render_template_string(
                INDEX_HTML,
                form=form_state,
                message=None,
                error=str(err),
                preview=None,
                recent_outputs=_recent_outputs(),
            )

    @app.get("/download/<path:filename>")
    def download_output(filename: str):
        output_root = OUTPUT_DIR.resolve()
        candidate = (output_root / filename).resolve()
        if output_root not in candidate.parents and candidate != output_root:
            abort(404)
        if not candidate.exists() or not candidate.is_file():
            abort(404)
        return send_from_directory(output_root, filename, as_attachment=True)

    return app


def main() -> int:
    app = create_app()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
