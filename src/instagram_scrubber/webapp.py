from __future__ import annotations

import os
import re
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from flask import Flask, Response, abort, jsonify, render_template_string, request, send_from_directory, url_for

from .ai_enrichment import ai_enabled
from .config import build_settings
from .instagram_api import InstagramGraphClient
from .run_engine import process_run_step
from .storage import (
    create_profile,
    create_run,
    delete_profile,
    get_profile,
    get_report_file,
    get_run,
    init_db,
    is_ephemeral_storage,
    list_profiles,
    list_runs,
    storage_mode_label,
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
      max-width: 1120px;
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

    h2 {
      margin: 0 0 8px;
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 1.04rem;
      letter-spacing: 0.1px;
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

    .flag {
      display: inline-flex;
      margin-top: 10px;
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 0.78rem;
      font-weight: 700;
      border: 1px solid;
    }

    .flag.ok {
      color: #166534;
      border-color: #86efac;
      background: #ecfdf3;
    }

    .flag.off {
      color: #92400e;
      border-color: #fcd34d;
      background: #fffbeb;
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
      font-size: 1.1rem;
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

    .warn {
      background: #fffbeb;
      border-color: #fcd34d;
      color: #92400e;
    }

    .error {
      background: var(--bad-bg);
      border-color: var(--bad-line);
      color: #881337;
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
    }

    .btn.alt,
    button.alt {
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

    input,
    select {
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

    .badge.queued {
      background: #f8fafc;
      color: #334155;
      border-color: #cbd5e1;
    }

    .progress-wrap {
      margin-top: 8px;
      border: 1px solid #cfe0ea;
      border-radius: 999px;
      overflow: hidden;
      background: #f8fbff;
      height: 12px;
    }

    .progress-fill {
      height: 100%;
      background: linear-gradient(140deg, var(--accent), var(--accent-2));
      width: 0%;
      transition: width 240ms ease;
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
      <p class="sub">Add one or more Instagram accounts, choose one, set lookback, and run your report.</p>
      <div class="flag {{ 'ok' if ai_enabled else 'off' }}">
        AI Enrichment: {{ 'Enabled' if ai_enabled else 'Disabled (set OPENAI_API_KEY)' }}
      </div>
      <div class="flow">
        <span>1. Add Account</span>
        <span>2. Choose Account + Lookback</span>
        <span>3. Download from History</span>
      </div>
    </section>

    <section class="stats">
      <div class="stat">
        <div class="label">Accounts</div>
        <div class="value">{{ stats.account_count }}</div>
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
    {% if is_ephemeral %}
      <section class="card"><div class="notice warn">Demo mode on Vercel uses temporary storage. Saved accounts and old reports can disappear after cold starts or redeploys. For production persistence, move storage to a managed database.</div></section>
    {% endif %}

    {% if active_run and active_run.status in ['queued', 'running'] %}
      <section class="card" id="run-status">
        <h2>Run In Progress</h2>
        <p class="muted" id="run-progress-message">
          Run #{{ active_run.id }} for <strong>{{ active_run.profile_name }}</strong>
          {% if active_run.progress_message %}- {{ active_run.progress_message }}{% endif %}
        </p>
        <div class="progress-wrap">
          {% set pct = ((active_run.progress_current / active_run.progress_total) * 100) if active_run.progress_total else 8 %}
          <div id="run-progress-fill" class="progress-fill" style="width: {{ pct|round(0, 'floor') }}%;"></div>
        </div>
        <div class="actions">
          <a class="btn alt" href="{{ url_for('index', active_run_id=active_run.id, profile_id=run_form.profile_id) }}">Refresh Progress</a>
          <span class="muted" id="run-progress-count">{{ active_run.progress_current }} / {{ active_run.progress_total if active_run.progress_total else '?' }}</span>
          <span class="muted">Scope: {{ active_run.posts_scope_display }}, {{ active_run.comments_scope_display }}</span>
        </div>
      </section>
    {% endif %}

    <section class="card" id="accounts">
      <h2>Add Instagram Account</h2>
      <p class="muted">Create a reusable account profile for running reports. You can add multiple clients.</p>
      <form method="post" action="{{ url_for('create_account') }}">
        <div class="form-grid">
          <div>
            <label for="name">Account label</label>
            <input id="name" name="name" value="{{ account_form.name }}" placeholder="Family Teams" required />
          </div>
          <div>
            <label for="business_account_id">Instagram Business Account ID (numeric)</label>
            <input id="business_account_id" name="business_account_id" value="{{ account_form.business_account_id }}" placeholder="1784..." required />
          </div>
          <div>
            <label for="access_token">Instagram Access Token</label>
            <input id="access_token" name="access_token" value="{{ account_form.access_token }}" placeholder="Paste raw token only" required />
          </div>
        </div>

        <details>
          <summary>Advanced Defaults</summary>
          <div class="form-grid">
            <div>
              <label for="graph_version">Graph Version</label>
              <input id="graph_version" name="graph_version" value="{{ account_form.graph_version }}" />
            </div>
            <div>
              <label for="timeout_seconds">Timeout Seconds</label>
              <input id="timeout_seconds" name="timeout_seconds" type="number" min="1" value="{{ account_form.timeout_seconds }}" />
            </div>
            <div>
              <label for="retry_count">Retry Count</label>
              <input id="retry_count" name="retry_count" type="number" min="0" value="{{ account_form.retry_count }}" />
            </div>
            <div>
              <label for="retry_backoff_seconds">Retry Backoff Seconds</label>
              <input id="retry_backoff_seconds" name="retry_backoff_seconds" type="number" min="0" step="0.1" value="{{ account_form.retry_backoff_seconds }}" />
            </div>
            <div>
              <label for="default_media_limit">Default Media Limit</label>
              <input id="default_media_limit" name="default_media_limit" type="number" min="1" value="{{ account_form.default_media_limit }}" />
            </div>
            <div>
              <label for="default_comments_per_media">Default Comments/Media</label>
              <input id="default_comments_per_media" name="default_comments_per_media" type="number" min="1" value="{{ account_form.default_comments_per_media }}" />
            </div>
            <div>
              <label for="default_lookback_days">Default Lookback Days</label>
              <input id="default_lookback_days" name="default_lookback_days" type="number" min="1" value="{{ account_form.default_lookback_days }}" />
            </div>
            <div>
              <label for="default_max_profiles">Default Max Profiles (optional)</label>
              <input id="default_max_profiles" name="default_max_profiles" type="number" min="1" value="{{ account_form.default_max_profiles }}" />
            </div>
          </div>
        </details>

        <div class="actions">
          <button type="submit">Save Account</button>
        </div>
        <p class="muted">Storage: {{ storage_mode }}</p>
      </form>
    </section>

    <section class="card" id="run">
      <h2>Run Report</h2>
      {% if profiles %}
        <form method="post" action="{{ url_for('run_scrub') }}">
          <div class="form-grid">
            <div>
              <label for="profile_id">Account</label>
              <select id="profile_id" name="profile_id" required>
                {% for p in profiles %}
                  <option value="{{ p.id }}" {% if (p.id|string) == (run_form.profile_id|string) %}selected{% endif %}>{{ p.name }} ({{ p.business_account_id }})</option>
                {% endfor %}
              </select>
            </div>
            <div>
              <label for="lookback_days">Lookback Days</label>
              <select id="lookback_days" name="lookback_days" required>
                <option value="7" {% if run_form.lookback_days == '7' %}selected{% endif %}>7 days</option>
                <option value="14" {% if run_form.lookback_days == '14' %}selected{% endif %}>14 days</option>
                <option value="30" {% if run_form.lookback_days == '30' %}selected{% endif %}>30 days</option>
                <option value="60" {% if run_form.lookback_days == '60' %}selected{% endif %}>60 days</option>
                <option value="90" {% if run_form.lookback_days == '90' %}selected{% endif %}>90 days</option>
                <option value="180" {% if run_form.lookback_days == '180' %}selected{% endif %}>180 days</option>
              </select>
            </div>
            <div>
              <label for="media_limit">Posts to Scan</label>
              <select id="media_limit" name="media_limit">
                <option value="5" {% if run_form.media_limit == '5' %}selected{% endif %}>5 posts</option>
                <option value="10" {% if run_form.media_limit == '10' %}selected{% endif %}>10 posts</option>
                <option value="25" {% if run_form.media_limit == '25' %}selected{% endif %}>25 posts</option>
                <option value="50" {% if run_form.media_limit == '50' %}selected{% endif %}>50 posts</option>
                <option value="100" {% if run_form.media_limit == '100' %}selected{% endif %}>100 posts</option>
              </select>
              <div class="muted">Used when no specific posts are selected below.</div>
            </div>
          </div>

          <div class="form-grid">
            <div>
              <label for="selected_media_ids">Choose Specific Posts (optional)</label>
              <select id="selected_media_ids" name="selected_media_ids" multiple size="8">
                {% if media_options %}
                  {% for m in media_options %}
                    <option value="{{ m.id }}" {% if m.id in run_form.selected_media_ids %}selected{% endif %}>{{ m.label }}</option>
                  {% endfor %}
                {% else %}
                  <option value="" disabled>No recent posts loaded. Select account and refresh.</option>
                {% endif %}
              </select>
              <div class="muted">Select one or more posts to scan all comments on those exact posts.</div>
            </div>
            <div>
              <label for="comments_per_media">Comments per Post</label>
              <select id="comments_per_media" name="comments_per_media">
                <option value="all" {% if run_form.comments_per_media in ['all', '0'] %}selected{% endif %}>All comments</option>
                <option value="50" {% if run_form.comments_per_media == '50' %}selected{% endif %}>First 50 comments</option>
                <option value="100" {% if run_form.comments_per_media == '100' %}selected{% endif %}>First 100 comments</option>
                <option value="200" {% if run_form.comments_per_media == '200' %}selected{% endif %}>First 200 comments</option>
                <option value="500" {% if run_form.comments_per_media == '500' %}selected{% endif %}>First 500 comments</option>
                <option value="1000" {% if run_form.comments_per_media == '1000' %}selected{% endif %}>First 1000 comments</option>
              </select>
              <div class="muted">Ignored when specific posts are selected: selected posts always run with all comments.</div>
            </div>
            <div>
              <label for="max_profiles">Max Leads (optional)</label>
              <select id="max_profiles" name="max_profiles">
                <option value="" {% if run_form.max_profiles == '' %}selected{% endif %}>No cap</option>
                <option value="25" {% if run_form.max_profiles == '25' %}selected{% endif %}>25</option>
                <option value="50" {% if run_form.max_profiles == '50' %}selected{% endif %}>50</option>
                <option value="100" {% if run_form.max_profiles == '100' %}selected{% endif %}>100</option>
                <option value="250" {% if run_form.max_profiles == '250' %}selected{% endif %}>250</option>
                <option value="500" {% if run_form.max_profiles == '500' %}selected{% endif %}>500</option>
              </select>
            </div>
          </div>

          <div class="actions">
            <button type="submit">Run Report Now</button>
          </div>
        </form>
      {% else %}
        <p class="muted">Add at least one account above before running reports.</p>
      {% endif %}
    </section>

    <section class="card">
      <h2>Saved Accounts</h2>
      {% if profiles %}
      <table>
        <thead>
          <tr>
            <th>Label</th>
            <th>Business ID</th>
            <th>Defaults</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {% for p in profiles %}
          <tr>
            <td>{{ p.name }}</td>
            <td>{{ p.business_account_id }}</td>
            <td>lookback {{ p.default_lookback_days }}d, media {{ p.default_media_limit }}, comments {{ p.default_comments_per_media }}</td>
            <td>
              <form method="post" action="{{ url_for('delete_account', profile_id=p.id) }}" onsubmit="return confirm('Delete this account and related runs?');">
                <button class="alt" type="submit">Delete</button>
              </form>
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
      <p class="muted">No accounts saved yet.</p>
      {% endif %}
    </section>

    <section class="card">
      <h2>Report History</h2>
      {% if runs %}
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Account</th>
            <th>Started</th>
            <th>Lookback</th>
            <th>Posts</th>
            <th>Comments</th>
            <th>Status</th>
            <th>Progress</th>
            <th>Report</th>
          </tr>
        </thead>
        <tbody>
          {% for run in runs %}
          <tr>
            <td>#{{ run.id }}</td>
            <td>{{ run.profile_name }}</td>
            <td>{{ run.started_at_display }}</td>
            <td>{{ run.lookback_days }} days</td>
            <td>{{ run.posts_scope_display }}</td>
            <td>{{ run.comments_scope_display }}</td>
            <td><span class="badge {{ run.status }}">{{ run.status }}</span></td>
            <td>
              {% if run.status in ['running', 'queued'] %}
                {{ run.progress_current }} / {{ run.progress_total if run.progress_total else '?' }}
              {% elif run.progress_message %}
                {{ run.progress_message }}
              {% else %}
                -
              {% endif %}
            </td>
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
      <p class="muted">No reports yet.</p>
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
            <th>Lead Score</th>
            <th>AI Fit</th>
            <th>AI Summary</th>
            <th>Email</th>
          </tr>
        </thead>
        <tbody>
          {% for row in preview %}
          <tr>
            <td>{{ row.instagram_handle }}</td>
            <td>{{ '; '.join(row.podcast_urls) }}</td>
            <td>{{ row.estimated_monthly_listeners }}</td>
            <td>{{ row.lead_score or '' }}</td>
            <td>{{ row.ai_fit_score or '' }}</td>
            <td>{{ row.ai_summary or '' }}</td>
            <td>{{ row.email or '' }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </section>
    {% endif %}
  </div>
  {% if auto_continue and active_run and active_run.status in ['queued', 'running'] %}
  <script>
    (function () {
      const endpoint = "{{ url_for('run_status', run_id=active_run.id) }}";
      const finishUrl = "{{ url_for('index', active_run_id=active_run.id, profile_id=run_form.profile_id) }}";
      const progressMessage = document.getElementById("run-progress-message");
      const progressFill = document.getElementById("run-progress-fill");
      const progressCount = document.getElementById("run-progress-count");

      async function tick() {
        try {
          const res = await fetch(endpoint + "?advance=1&t=" + Date.now(), {
            cache: "no-store",
            headers: { "Accept": "application/json" }
          });
          if (!res.ok) {
            setTimeout(tick, 1600);
            return;
          }
          const data = await res.json();
          if (progressMessage) {
            progressMessage.innerHTML = "Run #" + data.id + " for <strong>" + data.profile_name + "</strong> - " + (data.progress_message || "Running");
          }
          if (progressCount) {
            progressCount.textContent = (data.progress_current || 0) + " / " + (data.progress_total || "?");
          }
          if (progressFill) {
            progressFill.style.width = (data.progress_percent || 8) + "%";
          }
          if (data.status === "success" || data.status === "failed") {
            window.location.href = finishUrl;
            return;
          }
        } catch (err) {}
        setTimeout(tick, 1400);
      }

      setTimeout(tick, 900);
    })();
  </script>
  {% endif %}
  <script>
    (function () {
      const profileSelect = document.getElementById("profile_id");
      if (!profileSelect) return;
      profileSelect.addEventListener("change", function () {
        const selected = profileSelect.value;
        if (!selected) return;
        const target = "{{ url_for('index') }}" + "?profile_id=" + encodeURIComponent(selected);
        window.location.href = target;
      });
    })();
  </script>
</body>
</html>
"""


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


def _parse_comments_limit(raw: str) -> int:
    cleaned = raw.strip().lower()
    if cleaned in {"all", "0"}:
        return 0
    return _to_int(cleaned, label="Comments per post", minimum=1)


def _to_float(raw: str, label: str, minimum: float = 0.0) -> float:
    try:
        value = float(raw)
    except ValueError as err:
        raise ValueError(f"{label} must be a number") from err
    if value < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    return value


def _parse_json_string_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in parsed:
        candidate = str(item).strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)
    return out


def _load_media_options(profile: dict | None) -> list[dict[str, str]]:
    if profile is None:
        return []
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
        items = client.list_media(media_limit=50, lookback_days=None)
    except Exception:
        return []

    options: list[dict[str, str]] = []
    for item in items:
        dt = item.timestamp.strftime("%Y-%m-%d") if item.timestamp else "Unknown date"
        permalink = item.permalink or ""
        short_ref = permalink.rstrip("/").split("/")[-1] if permalink else item.media_id[-8:]
        label = f"{dt} - {short_ref} - {item.media_id}"
        options.append({"id": str(item.media_id), "label": label})
    return options


def _format_iso(value: str | None) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y %I:%M %p")
    except ValueError:
        return value


def _sanitize_access_token(raw: str) -> str:
    token = raw.strip().strip('"').strip("'")
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
    value = raw.strip().strip('"').strip("'")
    if value.startswith("@"):
        value = value[1:]
    return value


def _is_placeholder_credential(value: str) -> bool:
    cleaned = value.strip().strip('"').strip("'").lower()
    return cleaned in {
        "",
        "replace_me",
        "changeme",
        "your_token_here",
        "your_business_account_id",
    }


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
            "(no 'Bearer', no quotes, no 'access_token=' prefix), then save account again."
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
            "Use Account Label for text names like 'Family Teams'."
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


def _default_account_form() -> dict[str, str]:
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
    if list_profiles():
        return

    access_token = _sanitize_access_token(os.getenv("IG_ACCESS_TOKEN", ""))
    business_account_id = _sanitize_business_account_id(os.getenv("IG_BUSINESS_ACCOUNT_ID", ""))
    if _is_placeholder_credential(access_token) or _is_placeholder_credential(business_account_id):
        return

    try:
        create_profile(
            name=os.getenv("IG_PROFILE_NAME", "Default Account").strip() or "Default Account",
            access_token=access_token,
            business_account_id=business_account_id,
            graph_version=os.getenv("IG_GRAPH_VERSION", "v21.0").strip() or "v21.0",
            timeout_seconds=_env_int("REQUEST_TIMEOUT_SECONDS", 25),
            retry_count=_env_int("REQUEST_RETRY_COUNT", 3),
            retry_backoff_seconds=_env_float("REQUEST_RETRY_BACKOFF_SECONDS", 1.5),
            default_media_limit=_env_int("DEFAULT_MEDIA_LIMIT", 25),
            default_comments_per_media=_env_int("DEFAULT_COMMENTS_PER_MEDIA", 200),
            default_lookback_days=_env_int("DEFAULT_LOOKBACK_DAYS", 90),
            default_max_profiles=_env_int("DEFAULT_MAX_PROFILES", 0) or None,
        )
    except Exception:
        return


def _default_run_form(selected_profile: dict | None) -> dict[str, str]:
    allowed_lookback = {"7", "14", "30", "60", "90", "180"}
    allowed_media = {"5", "10", "25", "50", "100"}
    allowed_comments = {"all", "50", "100", "200", "500", "1000"}
    allowed_max_profiles = {"", "25", "50", "100", "250", "500"}

    def _pick(raw: str, allowed: set[str], fallback: str) -> str:
        return raw if raw in allowed else fallback

    if selected_profile is None:
        return {
            "profile_id": "",
            "lookback_days": "90",
            "media_limit": "25",
            "comments_per_media": "200",
            "max_profiles": "",
            "selected_media_ids": [],
        }

    comments_default = str(selected_profile["default_comments_per_media"])
    if comments_default == "0":
        comments_default = "all"

    return {
        "profile_id": str(selected_profile["id"]),
        "lookback_days": _pick(str(selected_profile["default_lookback_days"]), allowed_lookback, "90"),
        "media_limit": _pick(str(selected_profile["default_media_limit"]), allowed_media, "25"),
        "comments_per_media": _pick(comments_default, allowed_comments, "200"),
        "max_profiles": _pick(
            "" if selected_profile["default_max_profiles"] is None else str(selected_profile["default_max_profiles"]),
            allowed_max_profiles,
            "",
        ),
        "selected_media_ids": [],
    }


def _preview_from_run(run: dict | None) -> list[dict] | None:
    if run is None:
        return None
    raw = run.get("preview_json")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, list):
        return parsed
    return None


def _apply_run_scope_fields(item: dict) -> dict:
    selected_media_ids = _parse_json_string_list(item.get("selected_media_ids_json"))
    selected_count = len(selected_media_ids)
    comments_per_media = int(item.get("comments_per_media") or 0)

    if selected_count > 0:
        item["posts_scope_display"] = f"{selected_count} selected"
    else:
        item["posts_scope_display"] = f"{int(item.get('media_limit') or 0)} recent"

    if comments_per_media <= 0:
        item["comments_scope_display"] = "all comments"
    else:
        item["comments_scope_display"] = f"up to {comments_per_media} comments/post"

    item["selected_media_count"] = selected_count
    return item


def _render_page(
    *,
    message: str | None = None,
    error: str | None = None,
    preview: list | None = None,
    account_form: dict[str, str] | None = None,
    run_form: dict[str, str] | None = None,
    selected_profile_id: int | None = None,
    active_run: dict | None = None,
    auto_continue: bool = False,
):
    profiles = list_profiles()
    profile_by_id = {int(p["id"]): p for p in profiles}

    selected_profile = None
    if selected_profile_id is not None and selected_profile_id in profile_by_id:
        selected_profile = profile_by_id[selected_profile_id]
    elif profiles:
        selected_profile = profiles[0]

    runs = list_runs(limit=50)
    runs_view = []
    for run in runs:
        item = dict(run)
        item["started_at_display"] = _format_iso(item.get("started_at"))
        status = item.get("status")
        if status not in {"success", "failed", "running", "queued"}:
            item["status"] = "running"
        item = _apply_run_scope_fields(item)
        runs_view.append(item)

    total_runs = len(runs)
    success_runs = len([row for row in runs if row.get("status") == "success"])
    last_success = next((row for row in runs if row.get("status") == "success"), None)
    last_report_at = _format_iso(last_success.get("completed_at")) if last_success else "-"

    if run_form is None:
        run_form = _default_run_form(selected_profile)
    else:
        run_form = dict(run_form)
        selected_media_ids = run_form.get("selected_media_ids")
        if not isinstance(selected_media_ids, list):
            run_form["selected_media_ids"] = []

    if account_form is None:
        account_form = _default_account_form()

    active_run_view = None
    if active_run is not None:
        active_run_view = _apply_run_scope_fields(dict(active_run))

    selected_profile_for_media = None
    if selected_profile is not None:
        selected_profile_for_media = get_profile(int(selected_profile["id"]))
    media_options = _load_media_options(selected_profile_for_media)

    return render_template_string(
        INDEX_HTML,
        profiles=profiles,
        runs=runs_view,
        message=message,
        error=error,
        preview=preview,
        active_run=active_run_view,
        auto_continue=auto_continue,
        is_ephemeral=is_ephemeral_storage(),
        ai_enabled=ai_enabled(),
        account_form=account_form,
        run_form=run_form,
        media_options=media_options,
        storage_mode=storage_mode_label(),
        stats={
            "account_count": len(profiles),
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
        selected_raw = request.args.get("profile_id", "").strip()
        selected_profile_id = int(selected_raw) if selected_raw.isdigit() else None
        active_run_raw = request.args.get("active_run_id", "").strip()
        active_run_id = int(active_run_raw) if active_run_raw.isdigit() else None

        active_run = None
        message = None
        error = None
        preview = None
        auto_continue = False

        if active_run_id is not None:
            try:
                active_run = get_run(active_run_id)
            except Exception as err:  # noqa: BLE001
                error = f"Run failed: {_friendly_api_error_text(str(err))}"
                active_run = get_run(active_run_id)
        else:
            running = next(
                (row for row in list_runs(limit=10) if row.get("status") in {"queued", "running"}),
                None,
            )
            if running is not None:
                active_run = running

        if active_run is not None:
            selected_profile_id = selected_profile_id or int(active_run["profile_id"])
            status = str(active_run.get("status", "running"))
            if status in {"queued", "running"}:
                auto_continue = True
            elif status == "success":
                preview = _preview_from_run(active_run)
                if active_run.get("output_filename"):
                    message = (
                        f"Report complete for <strong>{active_run.get('profile_name')}</strong>. "
                        f"Found {active_run.get('lead_count', 0)} leads. "
                        f"<a href='{url_for('download_output', filename=active_run['output_filename'])}'>Download CSV</a>."
                    )
            elif status == "failed":
                error_text = str(active_run.get("error_message") or "Unknown error")
                error = f"Run failed: {_friendly_api_error_text(error_text)}"

        return _render_page(
            selected_profile_id=selected_profile_id,
            message=message,
            error=error,
            preview=preview,
            active_run=active_run,
            auto_continue=auto_continue,
        )

    @app.post("/accounts")
    def create_account():
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
                raise ValueError("Account label is required")
            if not form["business_account_id"]:
                raise ValueError("Instagram Business Account ID is required")
            if not form["access_token"]:
                raise ValueError("Instagram access token is required")
            if _is_placeholder_credential(form["access_token"]):
                raise ValueError("Access token is still a placeholder. Paste a real token.")
            if _is_placeholder_credential(form["business_account_id"]):
                raise ValueError("Business Account ID is still a placeholder. Paste the numeric ID.")
            if not form["business_account_id"].isdigit():
                raise ValueError("Instagram Business Account ID must be numeric only (example: 1784...).")

            _validate_setup_credentials(
                access_token=form["access_token"],
                business_account_id=form["business_account_id"],
                graph_version=form["graph_version"],
            )

            profile_id = create_profile(
                name=form["name"],
                access_token=form["access_token"],
                business_account_id=form["business_account_id"],
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

            return _render_page(
                message=f"Account <strong>{form['name']}</strong> saved.",
                account_form=_default_account_form(),
                selected_profile_id=profile_id,
            )
        except sqlite3.IntegrityError:
            return _render_page(
                error="An account with this label already exists. Use a different label.",
                account_form=form,
            )
        except Exception as err:  # noqa: BLE001
            err_text = str(err).lower()
            if "unique" in err_text and "name" in err_text:
                return _render_page(
                    error="An account with this label already exists. Use a different label.",
                    account_form=form,
                )
            return _render_page(error=str(err), account_form=form)

    @app.post("/accounts/<int:profile_id>/delete")
    def delete_account(profile_id: int):
        delete_profile(profile_id)
        return _render_page(message="Account deleted.")

    @app.post("/run")
    def run_scrub():
        selected_media_ids_raw = request.form.getlist("selected_media_ids")
        selected_media_ids: list[str] = []
        seen_media: set[str] = set()
        for raw_id in selected_media_ids_raw:
            media_id = raw_id.strip()
            if not media_id or media_id in seen_media:
                continue
            seen_media.add(media_id)
            selected_media_ids.append(media_id)

        form = {
            "profile_id": request.form.get("profile_id", "").strip(),
            "lookback_days": request.form.get("lookback_days", "").strip(),
            "media_limit": request.form.get("media_limit", "").strip(),
            "comments_per_media": request.form.get("comments_per_media", "").strip(),
            "max_profiles": request.form.get("max_profiles", "").strip(),
            "selected_media_ids": selected_media_ids,
        }

        try:
            if not form["profile_id"].isdigit():
                raise ValueError("Select an account before running the report.")
            profile_id = int(form["profile_id"])
            profile = get_profile(profile_id)
            if profile is None:
                raise ValueError("Selected account was not found. Please pick another account.")

            lookback_days = _to_optional_int(form["lookback_days"], "Lookback days", minimum=1)
            if lookback_days is None:
                lookback_days = int(profile["default_lookback_days"])

            media_limit = _to_optional_int(form["media_limit"], "Media limit", minimum=1)
            if media_limit is None:
                media_limit = int(profile["default_media_limit"])

            if form["comments_per_media"]:
                comments_per_media = _parse_comments_limit(form["comments_per_media"])
            else:
                comments_per_media = int(profile["default_comments_per_media"])

            if selected_media_ids:
                media_limit = len(selected_media_ids)
                comments_per_media = 0

            max_profiles = _to_optional_int(form["max_profiles"], "Max profiles", minimum=1)
            if max_profiles is None:
                max_profiles = profile["default_max_profiles"]

            run_id = create_run(
                profile_id=profile_id,
                media_limit=media_limit,
                comments_per_media=comments_per_media,
                lookback_days=lookback_days,
                max_profiles=max_profiles,
                selected_media_ids=selected_media_ids,
            )

            run = process_run_step(run_id=run_id, output_dir=OUTPUT_DIR)

            form["lookback_days"] = str(lookback_days)
            form["media_limit"] = str(media_limit)
            form["comments_per_media"] = "all" if comments_per_media <= 0 else str(comments_per_media)
            form["max_profiles"] = "" if max_profiles is None else str(max_profiles)
            form["selected_media_ids"] = selected_media_ids

            status = str(run.get("status", "running"))
            if status == "success":
                preview = _preview_from_run(run)
                message = (
                    f"Report complete for <strong>{profile['name']}</strong>. Found {run.get('lead_count', 0)} leads. "
                    f"<a href='{url_for('download_output', filename=run['output_filename'])}'>Download CSV</a>."
                )
                return _render_page(
                    message=message,
                    preview=preview,
                    run_form=form,
                    selected_profile_id=profile_id,
                    active_run=run,
                    auto_continue=False,
                )

            if status == "failed":
                friendly = _friendly_api_error_text(str(run.get("error_message") or "Unknown error"))
                return _render_page(
                    error=f"Run failed: {friendly}",
                    run_form=form,
                    selected_profile_id=profile_id,
                    active_run=run,
                    auto_continue=False,
                )

            return _render_page(
                message=f"Run queued for <strong>{profile['name']}</strong>. Processing in the background...",
                run_form=form,
                selected_profile_id=profile_id,
                active_run=run,
                auto_continue=True,
            )
        except Exception as err:  # noqa: BLE001
            return _render_page(error=str(err), run_form=form)

    @app.get("/runs/<int:run_id>/status")
    def run_status(run_id: int):
        advance = request.args.get("advance", "").strip() == "1"
        try:
            run = process_run_step(run_id=run_id, output_dir=OUTPUT_DIR) if advance else get_run(run_id)
        except Exception as err:  # noqa: BLE001
            fallback = get_run(run_id)
            if fallback is None:
                return jsonify({"error": str(err)}), 500
            run = fallback

        if run is None:
            abort(404)

        progress_total = int(run.get("progress_total") or 0)
        progress_current = int(run.get("progress_current") or 0)
        selected_media_count = len(_parse_json_string_list(run.get("selected_media_ids_json")))
        if progress_total > 0:
            pct = max(1, min(100, int((progress_current / progress_total) * 100)))
        else:
            pct = 8 if run.get("status") in {"queued", "running"} else 100

        return jsonify(
            {
                "id": int(run["id"]),
                "profile_id": int(run["profile_id"]),
                "profile_name": str(run.get("profile_name") or ""),
                "status": str(run.get("status") or "running"),
                "phase": str(run.get("phase") or ""),
                "progress_message": str(run.get("progress_message") or ""),
                "progress_current": progress_current,
                "progress_total": progress_total,
                "progress_percent": pct,
                "media_limit": int(run.get("media_limit") or 0),
                "comments_per_media": int(run.get("comments_per_media") or 0),
                "lookback_days": int(run.get("lookback_days") or 0),
                "selected_media_count": selected_media_count,
                "lead_count": int(run.get("lead_count") or 0),
                "output_filename": run.get("output_filename"),
                "error_message": run.get("error_message"),
            }
        )

    @app.get("/download/<path:filename>")
    def download_output(filename: str):
        safe_filename = Path(filename).name
        if safe_filename != filename:
            abort(404)

        output_root = OUTPUT_DIR.resolve()
        candidate = (output_root / safe_filename).resolve()
        if output_root not in candidate.parents and candidate != output_root:
            abort(404)

        if candidate.exists() and candidate.is_file():
            return send_from_directory(str(output_root), safe_filename, as_attachment=True)

        report = get_report_file(safe_filename)
        if report is None:
            abort(404)

        response = Response(str(report.get("csv_content") or ""), mimetype="text/csv")
        response.headers["Content-Disposition"] = f'attachment; filename="{safe_filename}"'
        return response

    return app


def main() -> int:
    app = create_app()
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
