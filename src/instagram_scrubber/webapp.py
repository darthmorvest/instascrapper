from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from flask import Flask, Response, abort, jsonify, redirect, render_template_string, request, send_from_directory, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from .ai_enrichment import ai_enabled
from .config import build_settings
from .instagram_api import InstagramGraphClient
from .run_engine import process_run_step
from .storage import (
    accept_workspace_invite,
    add_workspace_member,
    count_users,
    create_profile,
    create_run,
    create_user,
    create_workspace,
    create_workspace_invite,
    delete_profile,
    get_profile,
    get_report_file,
    get_run,
    get_run_by_output_filename,
    get_user,
    get_user_by_email,
    get_workspace_invite,
    get_workspace_membership,
    init_db,
    is_ephemeral_storage,
    list_profiles,
    list_runs,
    list_user_workspaces,
    list_workspace_invites,
    list_workspace_members,
    storage_mode_label,
)

if os.getenv("VERCEL") == "1":
    OUTPUT_DIR = Path("/tmp/outputs")
else:
    OUTPUT_DIR = Path.cwd() / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AUTH_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>InstaScrapper Pro | Sign In</title>
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
      padding: 22px 12px 34px;
    }

    .shell {
      max-width: 980px;
      margin: 0 auto;
      display: grid;
      gap: 12px;
    }

    .card {
      background: #fff;
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

    h1 {
      margin: 10px 0 6px;
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 1.68rem;
      line-height: 1.24;
      letter-spacing: 0.2px;
    }

    h2 {
      margin: 0 0 8px;
      font-family: "Sora", "Manrope", sans-serif;
      font-size: 1.04rem;
    }

    .brand {
      display: inline-flex;
      align-items: center;
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

    .sub { margin: 0; color: var(--sub); font-size: 0.95rem; }

    .grid {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
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
      font-size: 0.92rem;
      color: #0f172a;
      background: #fff;
    }

    .form-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }

    .actions {
      display: flex;
      gap: 8px;
      margin-top: 10px;
      flex-wrap: wrap;
    }

    button,
    .btn {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      font-size: 0.9rem;
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

    .muted { color: var(--sub); font-size: 0.87rem; }

    @media (max-width: 860px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="card hero">
      <div class="brand">InstaScrapper Pro</div>
      <h1>Secure Access</h1>
      <p class="sub">Sign in to manage client accounts, run reports, and download report history.</p>
    </section>

    {% if message %}
      <section class="card"><div class="notice">{{ message }}</div></section>
    {% endif %}
    {% if error %}
      <section class="card"><div class="error">{{ error }}</div></section>
    {% endif %}

    {% if pending_invite %}
      <section class="card">
        <div class="notice">You were invited to join workspace <strong>{{ pending_invite.workspace_name }}</strong> as <strong>{{ pending_invite.role }}</strong> with email <strong>{{ pending_invite.email }}</strong>.</div>
      </section>
    {% endif %}

    <section class="grid">
      <article class="card">
        <h2>Sign In</h2>
        <form method="post" action="{{ url_for('login_post') }}">
          <div class="form-grid">
            <div>
              <label for="login_email">Email</label>
              <input id="login_email" name="email" type="email" required />
            </div>
            <div>
              <label for="login_password">Password</label>
              <input id="login_password" name="password" type="password" required />
            </div>
          </div>
          <div class="actions">
            <button type="submit">Sign In</button>
            <a class="btn alt" href="{{ url_for('register_get') }}">Create Account</a>
          </div>
        </form>
      </article>

      <article class="card">
        <h2>Create Account</h2>
        {% if allow_registration %}
          <form method="post" action="{{ url_for('register_post') }}">
            <div class="form-grid">
              <div>
                <label for="register_name">Full Name</label>
                <input id="register_name" name="full_name" required />
              </div>
              <div>
                <label for="register_email">Email</label>
                <input id="register_email" name="email" type="email" required />
              </div>
              <div>
                <label for="register_password">Password</label>
                <input id="register_password" name="password" type="password" minlength="8" required />
              </div>
            </div>
            <div class="actions">
              <button type="submit">Create Account</button>
            </div>
          </form>
          <p class="muted">Recommended: first account is owner. Additional users should join via workspace invite links.</p>
        {% else %}
          <p class="muted">Self-signup is disabled. Ask your workspace owner for an invite link.</p>
        {% endif %}
      </article>
    </section>
    <section class="card">
      <p class="muted">
        Legal:
        <a href="{{ url_for('privacy_policy') }}" target="_blank" rel="noopener">Privacy Policy</a>
        |
        <a href="{{ url_for('terms_of_service') }}" target="_blank" rel="noopener">Terms of Service</a>
      </p>
    </section>
  </div>
</body>
</html>
"""

INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>InstaScrapper | Podcast Lead Intelligence</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Space+Grotesk:wght@600;700&display=swap');

    :root {
      --bg-a: #f3f8ff;
      --bg-b: #eaf4ff;
      --panel: rgba(255, 255, 255, 0.82);
      --line: rgba(148, 163, 184, 0.26);
      --text: #0f2037;
      --sub: #4f657f;
      --accent: #0f6fa9;
      --accent-2: #2192bf;
      --good-bg: #dcfce7;
      --good-line: #86efac;
      --bad-bg: #fff1f2;
      --bad-line: #fecdd3;
      --warn-bg: #f8fafc;
      --warn-line: #cbd5e1;
      --shadow: 0 14px 40px rgba(15, 23, 42, 0.11);
      --ring: rgba(15, 111, 169, 0.22);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: "Plus Jakarta Sans", "Avenir Next", sans-serif;
      background:
        radial-gradient(circle at 7% -5%, #dbeafe 0%, rgba(219, 234, 254, 0) 45%),
        radial-gradient(circle at 95% 8%, #cffafe 0%, rgba(207, 250, 254, 0) 36%),
        linear-gradient(180deg, var(--bg-a), var(--bg-b));
      padding: 18px 12px 34px;
    }

    .shell {
      max-width: 1240px;
      margin: 0 auto;
      display: grid;
      gap: 14px;
      animation: rise 460ms ease both;
    }

    @keyframes rise {
      from { opacity: 0; transform: translateY(7px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 18px;
      backdrop-filter: blur(16px);
      box-shadow: var(--shadow);
    }

    .hero {
      background:
        linear-gradient(120deg, rgba(15, 111, 169, 0.11), rgba(33, 146, 191, 0.12) 55%, rgba(255, 255, 255, 0.92)),
        #fff;
    }

    .topbar {
      display: flex;
      gap: 10px;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 6px;
    }

    .brand {
      display: inline-flex;
      align-items: center;
      border: 1px solid rgba(33, 146, 191, 0.32);
      background: rgba(255, 255, 255, 0.82);
      color: #0f4d7a;
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.42px;
      text-transform: uppercase;
    }

    h1 {
      margin: 10px 0 4px;
      font-family: "Space Grotesk", "Plus Jakarta Sans", sans-serif;
      font-size: 2rem;
      line-height: 1.18;
      letter-spacing: -0.02em;
    }

    h2 {
      margin: 0 0 8px;
      font-family: "Space Grotesk", "Plus Jakarta Sans", sans-serif;
      font-size: 1.06rem;
      letter-spacing: -0.01em;
    }

    .sub { margin: 0; color: var(--sub); font-size: 1rem; }
    .muted { color: var(--sub); font-size: 0.88rem; }

    .flag {
      display: inline-flex;
      margin-top: 10px;
      border-radius: 999px;
      padding: 6px 11px;
      font-size: 0.78rem;
      font-weight: 700;
      border: 1px solid;
    }

    .flag.ok { color: #166534; border-color: #86efac; background: #ecfdf3; }
    .flag.off { color: #92400e; border-color: #fcd34d; background: #fffbeb; }

    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }

    .stat {
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255, 255, 255, 0.75);
      padding: 12px;
    }

    .stat .label {
      font-size: 0.76rem;
      color: var(--sub);
      text-transform: uppercase;
      letter-spacing: 0.55px;
      font-weight: 700;
    }

    .stat .value {
      margin-top: 4px;
      font-size: 1.06rem;
      font-weight: 800;
      color: #16344f;
      word-break: break-word;
    }

    .notice,
    .error,
    .warn {
      border: 1px solid;
      border-radius: 11px;
      padding: 10px 12px;
      font-size: 0.92rem;
    }

    .notice { background: var(--good-bg); border-color: var(--good-line); color: #14532d; }
    .error { background: var(--bad-bg); border-color: var(--bad-line); color: #881337; }
    .warn { background: var(--warn-bg); border-color: var(--warn-line); color: #92400e; }

    .form-grid {
      margin-top: 10px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 9px;
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
      border-radius: 12px;
      padding: 10px 11px;
      font-size: 0.9rem;
      color: #0f172a;
      background: rgba(255, 255, 255, 0.96);
      transition: border-color 160ms ease, box-shadow 160ms ease;
    }

    input:focus,
    select:focus {
      outline: none;
      border-color: rgba(15, 111, 169, 0.5);
      box-shadow: 0 0 0 3px var(--ring);
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
      border-radius: 12px;
      padding: 10px 14px;
      font-size: 0.88rem;
      font-weight: 700;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(145deg, var(--accent), var(--accent-2));
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      transition: transform 120ms ease, box-shadow 160ms ease;
      box-shadow: 0 8px 22px rgba(15, 111, 169, 0.23);
    }

    button:hover,
    .btn:hover {
      transform: translateY(-1px);
    }

    button:disabled {
      opacity: 0.62;
      cursor: not-allowed;
      transform: none;
      box-shadow: none;
    }

    .btn.alt,
    button.alt {
      background: rgba(255, 255, 255, 0.96);
      color: #1f3f60;
      border: 1px solid rgba(148, 163, 184, 0.35);
      box-shadow: none;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.88rem;
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
      font-size: 0.76rem;
      letter-spacing: 0.24px;
      text-transform: uppercase;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.35px;
      border: 1px solid;
    }

    .badge.success { background: #ecfdf3; color: #166534; border-color: #a7f3d0; }
    .badge.failed { background: #fff1f2; color: #9f1239; border-color: #fecdd3; }
    .badge.running { background: #eff6ff; color: #1d4ed8; border-color: #bfdbfe; }
    .badge.queued { background: #f8fafc; color: #334155; border-color: #cbd5e1; }

    .progress-wrap {
      margin-top: 8px;
      border: 1px solid #cfe0ea;
      border-radius: 999px;
      overflow: hidden;
      background: #f8fbff;
      height: 11px;
    }

    .progress-fill {
      height: 100%;
      background: linear-gradient(140deg, var(--accent), var(--accent-2));
      width: 0%;
      transition: width 220ms ease;
    }

    .split {
      display: grid;
      gap: 10px;
      grid-template-columns: 1.2fr 0.8fr;
    }

    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 0.8rem;
      word-break: break-all;
    }

    .manual-setup {
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.66);
      padding: 10px 12px;
    }

    .manual-setup summary {
      cursor: pointer;
      font-weight: 700;
      color: #254a6f;
      list-style: none;
    }

    .manual-setup summary::-webkit-details-marker { display: none; }

    .oauth-tip {
      margin-top: 8px;
      border: 1px solid rgba(148, 163, 184, 0.32);
      border-radius: 12px;
      padding: 10px 11px;
      background: rgba(248, 250, 252, 0.88);
      color: #334155;
      font-size: 0.84rem;
      line-height: 1.4;
    }

    a { color: #0f4f8a; text-decoration: none; }
    a:hover { text-decoration: underline; }

    @media (max-width: 980px) {
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .split { grid-template-columns: 1fr; }
    }

    @media (max-width: 760px) {
      body { padding: 14px 10px 28px; }
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
      <div class="topbar">
        <div class="brand">InstaScrapper Pro</div>
        <form method="post" action="{{ url_for('logout') }}">
          <button class="alt" type="submit">Logout</button>
        </form>
      </div>
      <h1>Podcast Lead Intelligence Dashboard</h1>
      <p class="sub">One-time account setup. Select exact posts or a time window. Run and download reports from history.</p>
      <div class="actions">
        <form method="post" action="{{ url_for('switch_workspace') }}" style="display:flex; gap:8px; width:100%; max-width:460px;">
          <div style="flex:1;">
            <label for="workspace_id">Workspace</label>
            <select id="workspace_id" name="workspace_id">
              {% for ws in workspaces %}
                <option value="{{ ws.id }}" {% if ws.id == workspace_id %}selected{% endif %}>{{ ws.name }} ({{ ws.role }})</option>
              {% endfor %}
            </select>
          </div>
          <div style="display:flex; align-items:flex-end;">
            <button class="alt" type="submit">Switch</button>
          </div>
        </form>
        <span class="muted">Signed in as {{ user.full_name }} ({{ user.email }})</span>
      </div>
      <div class="flag {{ 'ok' if ai_enabled else 'off' }}">AI Enrichment: {{ 'Enabled' if ai_enabled else 'Disabled (set OPENAI_API_KEY)' }}</div>
      {% if meta_oauth_enabled %}
        <div class="oauth-tip">Meta Login is active. Team members can connect client Instagram accounts with one click and no manual token handling.</div>
      {% else %}
        <div class="oauth-tip">Meta Login is not enabled on this deployment yet. Add `META_APP_ID`, `META_APP_SECRET`, and `META_REDIRECT_URI` in Vercel to remove manual API setup.</div>
      {% endif %}
    </section>

    <section class="stats">
      <div class="stat"><div class="label">Accounts</div><div class="value">{{ stats.account_count }}</div></div>
      <div class="stat"><div class="label">Total Runs</div><div class="value">{{ stats.total_runs }}</div></div>
      <div class="stat"><div class="label">Successful</div><div class="value">{{ stats.success_runs }}</div></div>
      <div class="stat"><div class="label">Last Report</div><div class="value">{{ stats.last_report_at }}</div></div>
    </section>

    {% if message %}
      <section class="card"><div class="notice">{{ message }}</div></section>
    {% endif %}
    {% if error %}
      <section class="card"><div class="error">{{ error }}</div></section>
    {% endif %}
    {% if is_ephemeral %}
      <section class="card"><div class="warn">Demo mode on Vercel uses temporary storage. Set `DATABASE_URL` for durable account and report history.</div></section>
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
          <a class="btn alt" href="{{ url_for('index', active_run_id=active_run.id, profile_id=run_form.profile_id) }}">Refresh</a>
          <span class="muted" id="run-progress-count">{{ active_run.progress_current }} / {{ active_run.progress_total if active_run.progress_total else '?' }}</span>
          <span class="muted">Scope: {{ active_run.posts_scope_display }}, {{ active_run.comments_scope_display }}</span>
        </div>
      </section>
    {% endif %}

    {% if meta_connect_accounts %}
      <section class="card">
        <h2>Finish Meta Connection</h2>
        <p class="muted">Choose which connected Instagram Business account to save in this workspace.</p>
        <form method="post" action="{{ url_for('meta_connect_save') }}">
          <div class="form-grid">
            <div>
              <label for="meta_account_index">Instagram Account</label>
              <select id="meta_account_index" name="account_index" required>
                {% for acct in meta_connect_accounts %}
                  <option value="{{ loop.index0 }}">{{ acct.display_name }} ({{ acct.business_account_id }})</option>
                {% endfor %}
              </select>
            </div>
            <div>
              <label for="meta_label">Account label</label>
              <input id="meta_label" name="name" placeholder="Client account label" required />
            </div>
          </div>
          <div class="actions">
            <button type="submit">Save Connected Account</button>
            <button class="alt" type="submit" formaction="{{ url_for('meta_connect_cancel') }}">Cancel</button>
          </div>
        </form>
      </section>
    {% endif %}

    <section class="split">
      <article class="card">
        <h2>Account Setup</h2>
        <p class="muted">Recommended flow: connect with Meta Login, pick the Instagram account, and save. No token copy/paste needed.</p>
        <div class="actions">
          {% if meta_oauth_enabled %}
            <a class="btn" href="{{ url_for('meta_connect_start') }}">Connect Instagram via Meta</a>
          {% else %}
            <button class="alt" type="button" disabled>Meta Login Requires Environment Setup</button>
          {% endif %}
        </div>
        <details class="manual-setup">
          <summary>Manual API token setup (advanced)</summary>
          <form method="post" action="{{ url_for('create_account') }}">
            <div class="form-grid">
              <div>
                <label for="name">Account label</label>
                <input id="name" name="name" value="{{ account_form.name }}" required />
              </div>
              <div>
                <label for="business_account_id">Instagram Business Account ID</label>
                <input id="business_account_id" name="business_account_id" value="{{ account_form.business_account_id }}" placeholder="1784..." required />
              </div>
              <div>
                <label for="access_token">Access token</label>
                <input id="access_token" name="access_token" value="{{ account_form.access_token }}" placeholder="Raw token only" required />
              </div>
            </div>

            <div class="form-grid">
              <div>
                <label for="graph_version">Graph version</label>
                <input id="graph_version" name="graph_version" value="{{ account_form.graph_version }}" />
              </div>
              <div>
                <label for="timeout_seconds">Timeout seconds</label>
                <input id="timeout_seconds" name="timeout_seconds" type="number" min="1" value="{{ account_form.timeout_seconds }}" />
              </div>
              <div>
                <label for="retry_count">Retry count</label>
                <input id="retry_count" name="retry_count" type="number" min="0" value="{{ account_form.retry_count }}" />
              </div>
              <div>
                <label for="retry_backoff_seconds">Retry backoff seconds</label>
                <input id="retry_backoff_seconds" name="retry_backoff_seconds" type="number" min="0" step="0.1" value="{{ account_form.retry_backoff_seconds }}" />
              </div>
              <div>
                <label for="default_media_limit">Default posts</label>
                <input id="default_media_limit" name="default_media_limit" type="number" min="1" value="{{ account_form.default_media_limit }}" />
              </div>
              <div>
                <label for="default_comments_per_media">Default comments/post</label>
                <input id="default_comments_per_media" name="default_comments_per_media" type="number" min="1" value="{{ account_form.default_comments_per_media }}" />
              </div>
              <div>
                <label for="default_lookback_days">Default lookback days</label>
                <input id="default_lookback_days" name="default_lookback_days" type="number" min="1" value="{{ account_form.default_lookback_days }}" />
              </div>
              <div>
                <label for="default_max_profiles">Default max leads</label>
                <input id="default_max_profiles" name="default_max_profiles" type="number" min="1" value="{{ account_form.default_max_profiles }}" />
              </div>
              <div>
                <label for="team_member_user_ids">Team members for this account</label>
                <select id="team_member_user_ids" name="team_member_user_ids" multiple size="5">
                  {% for m in members %}
                    <option value="{{ m.user_id }}" {% if m.user_id in account_form.team_member_user_ids %}selected{% endif %}>{{ m.full_name }} ({{ m.role }})</option>
                  {% endfor %}
                </select>
              </div>
            </div>

            <div class="actions">
              <button type="submit">Save Account</button>
            </div>
            <p class="muted">Storage mode: {{ storage_mode }}</p>
          </form>
        </details>
      </article>

      <article class="card">
        <h2>Team</h2>
        <p class="muted">Invite teammates to this workspace. They can access accounts and run reports based on role.</p>
        <form method="post" action="{{ url_for('invite_workspace_member') }}">
          <div class="form-grid">
            <div>
              <label for="invite_email">Invite email</label>
              <input id="invite_email" name="email" type="email" required />
            </div>
            <div>
              <label for="invite_role">Role</label>
              <select id="invite_role" name="role">
                <option value="member">member</option>
                <option value="admin">admin</option>
              </select>
            </div>
          </div>
          <div class="actions">
            <button type="submit">Create Invite Link</button>
          </div>
        </form>

        <h2 style="margin-top:14px;">Members</h2>
        {% if members %}
          <table>
            <thead><tr><th>Name</th><th>Email</th><th>Role</th></tr></thead>
            <tbody>
              {% for m in members %}
                <tr>
                  <td>{{ m.full_name }}</td>
                  <td>{{ m.email }}</td>
                  <td>{{ m.role }}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        {% else %}
          <p class="muted">No members yet.</p>
        {% endif %}

        {% if pending_invites %}
          <h2 style="margin-top:14px;">Pending Invites</h2>
          {% for inv in pending_invites %}
            <div style="margin-bottom:8px; border:1px solid var(--line); border-radius:10px; padding:8px;">
              <div class="muted">{{ inv.email }} ({{ inv.role }}) expires {{ inv.expires_at_display }}</div>
              <div class="mono">{{ inv.link }}</div>
            </div>
          {% endfor %}
        {% endif %}
      </article>
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
              <label for="lookback_days">Lookback days</label>
              <select id="lookback_days" name="lookback_days" required>
                <option value="7" {% if run_form.lookback_days == '7' %}selected{% endif %}>7</option>
                <option value="14" {% if run_form.lookback_days == '14' %}selected{% endif %}>14</option>
                <option value="30" {% if run_form.lookback_days == '30' %}selected{% endif %}>30</option>
                <option value="60" {% if run_form.lookback_days == '60' %}selected{% endif %}>60</option>
                <option value="90" {% if run_form.lookback_days == '90' %}selected{% endif %}>90</option>
                <option value="180" {% if run_form.lookback_days == '180' %}selected{% endif %}>180</option>
              </select>
            </div>
            <div>
              <label for="media_limit">Posts to scan (if not manually selected)</label>
              <select id="media_limit" name="media_limit">
                <option value="5" {% if run_form.media_limit == '5' %}selected{% endif %}>5</option>
                <option value="10" {% if run_form.media_limit == '10' %}selected{% endif %}>10</option>
                <option value="25" {% if run_form.media_limit == '25' %}selected{% endif %}>25</option>
                <option value="50" {% if run_form.media_limit == '50' %}selected{% endif %}>50</option>
                <option value="100" {% if run_form.media_limit == '100' %}selected{% endif %}>100</option>
              </select>
            </div>
            <div>
              <label for="comments_per_media">Comments per post</label>
              <select id="comments_per_media" name="comments_per_media">
                <option value="all" {% if run_form.comments_per_media in ['all', '0'] %}selected{% endif %}>all comments</option>
                <option value="50" {% if run_form.comments_per_media == '50' %}selected{% endif %}>50</option>
                <option value="100" {% if run_form.comments_per_media == '100' %}selected{% endif %}>100</option>
                <option value="200" {% if run_form.comments_per_media == '200' %}selected{% endif %}>200</option>
                <option value="500" {% if run_form.comments_per_media == '500' %}selected{% endif %}>500</option>
                <option value="1000" {% if run_form.comments_per_media == '1000' %}selected{% endif %}>1000</option>
              </select>
            </div>
            <div>
              <label for="max_profiles">Max leads</label>
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

          <h2 style="margin-top:14px;">Manual Post Selection</h2>
          <p class="muted">Select exact posts/reels to scan. If any are selected, run uses all comments on selected posts.</p>
          {% if media_items %}
            <div class="actions" style="margin-top:0; margin-bottom:6px;">
              <button class="alt" type="button" id="select-all-media">Select all visible</button>
              <button class="alt" type="button" id="clear-all-media">Clear selection</button>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Pick</th>
                  <th>Date</th>
                  <th>Type</th>
                  <th>Comments</th>
                  <th>Likes</th>
                  <th>Permalink</th>
                </tr>
              </thead>
              <tbody>
                {% for item in media_items %}
                  <tr>
                    <td><input class="media-checkbox" type="checkbox" name="selected_media_ids" value="{{ item.id }}" {% if item.selected %}checked{% endif %} /></td>
                    <td>{{ item.timestamp_display }}</td>
                    <td>{{ item.media_type or '-' }}</td>
                    <td>{{ item.comments_count if item.comments_count is not none else '-' }}</td>
                    <td>{{ item.like_count if item.like_count is not none else '-' }}</td>
                    <td>{% if item.permalink %}<a href="{{ item.permalink }}" target="_blank" rel="noopener">Open</a>{% else %}-{% endif %}</td>
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          {% else %}
            <p class="muted">No posts found for the selected account/lookback window.</p>
          {% endif %}

          <div class="actions">
            <button type="submit">Run Report Now</button>
          </div>
        </form>
      {% else %}
        <p class="muted">Save at least one account first.</p>
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
              <th>Team</th>
              <th>Defaults</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {% for p in profiles %}
              <tr>
                <td>{{ p.name }}</td>
                <td>{{ p.business_account_id }}</td>
                <td>{{ p.team_member_names_display }}</td>
                <td>lookback {{ p.default_lookback_days }}d, posts {{ p.default_media_limit }}, comments {{ p.default_comments_per_media }}</td>
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
                  {% elif run.error_display %}
                    {{ run.error_display }}
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
              <th>Genre</th>
              <th>Monthly Listeners</th>
              <th>Lead Score</th>
              <th>AI Fit</th>
              <th>Email</th>
            </tr>
          </thead>
          <tbody>
            {% for row in preview %}
              <tr>
                <td>{{ row.instagram_handle }}</td>
                <td>{{ '; '.join(row.podcast_urls or []) }}</td>
                <td>{{ row.podcast_genre or '' }}</td>
                <td>{{ row.estimated_monthly_listeners }}</td>
                <td>{{ row.lead_score or '' }}</td>
                <td>{{ row.ai_fit_score or '' }}</td>
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
              setTimeout(tick, 1700);
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
          setTimeout(tick, 1500);
        }

        setTimeout(tick, 800);
      })();
    </script>
  {% endif %}

  <script>
    (function () {
      const profileSelect = document.getElementById("profile_id");
      const lookbackSelect = document.getElementById("lookback_days");
      if (profileSelect) {
        profileSelect.addEventListener("change", function () {
          const params = new URLSearchParams(window.location.search);
          params.set("profile_id", profileSelect.value || "");
          if (lookbackSelect && lookbackSelect.value) {
            params.set("lookback_days", lookbackSelect.value);
          }
          window.location.href = "{{ url_for('index') }}" + "?" + params.toString();
        });
      }
      if (lookbackSelect) {
        lookbackSelect.addEventListener("change", function () {
          const params = new URLSearchParams(window.location.search);
          if (profileSelect && profileSelect.value) {
            params.set("profile_id", profileSelect.value);
          }
          params.set("lookback_days", lookbackSelect.value || "90");
          window.location.href = "{{ url_for('index') }}" + "?" + params.toString();
        });
      }

      const selectAllBtn = document.getElementById("select-all-media");
      const clearAllBtn = document.getElementById("clear-all-media");
      const checkboxes = Array.from(document.querySelectorAll(".media-checkbox"));
      if (selectAllBtn) {
        selectAllBtn.addEventListener("click", function () {
          checkboxes.forEach((cb) => { cb.checked = true; });
        });
      }
      if (clearAllBtn) {
        clearAllBtn.addEventListener("click", function () {
          checkboxes.forEach((cb) => { cb.checked = false; });
        });
      }
    })();
  </script>
</body>
</html>
"""

LEGAL_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ title }}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Space+Grotesk:wght@600;700&display=swap');

    :root {
      --bg-a: #f3f8ff;
      --bg-b: #eaf4ff;
      --panel: rgba(255, 255, 255, 0.9);
      --line: rgba(148, 163, 184, 0.28);
      --text: #0f2037;
      --sub: #4f657f;
      --accent: #0f6fa9;
      --shadow: 0 14px 40px rgba(15, 23, 42, 0.1);
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: "Plus Jakarta Sans", "Avenir Next", sans-serif;
      background:
        radial-gradient(circle at 7% -5%, #dbeafe 0%, rgba(219, 234, 254, 0) 45%),
        radial-gradient(circle at 95% 8%, #cffafe 0%, rgba(207, 250, 254, 0) 36%),
        linear-gradient(180deg, var(--bg-a), var(--bg-b));
      padding: 24px 12px 36px;
    }

    .wrap { max-width: 920px; margin: 0 auto; display: grid; gap: 12px; }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
    }
    .badge {
      display: inline-flex;
      border: 1px solid rgba(33, 146, 191, 0.34);
      color: #0f4d7a;
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 0.74rem;
      font-weight: 700;
      letter-spacing: 0.42px;
      text-transform: uppercase;
      background: rgba(255, 255, 255, 0.84);
    }
    h1 {
      margin: 10px 0 4px;
      font-family: "Space Grotesk", "Plus Jakarta Sans", sans-serif;
      font-size: 2rem;
      letter-spacing: -0.02em;
    }
    h2 {
      margin: 16px 0 8px;
      font-family: "Space Grotesk", "Plus Jakarta Sans", sans-serif;
      font-size: 1.04rem;
    }
    p, li { color: var(--sub); line-height: 1.55; font-size: 0.95rem; }
    ul { margin: 8px 0; padding-left: 18px; }
    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .meta { color: var(--sub); font-size: 0.86rem; margin-top: 8px; }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="card">
      <span class="badge">InstaScrapper Pro Legal</span>
      <h1>{{ title }}</h1>
      <p class="meta">Effective date: {{ effective_date }} | Contact: {{ contact_email }}</p>
    </section>
    <section class="card">
      {{ body|safe }}
      <p class="meta">Questions: <a href="mailto:{{ contact_email }}">{{ contact_email }}</a></p>
    </section>
  </main>
</body>
</html>
"""

PRIVACY_BODY_HTML = """
<h2>Overview</h2>
<p>InstaScrapper Pro provides analytics and reporting software for Instagram business accounts. This policy explains what data we collect, why we collect it, and how we protect it.</p>

<h2>Information We Collect</h2>
<ul>
  <li>Account data you provide directly, including name, email, and workspace/team settings.</li>
  <li>Instagram account data authorized by you through Meta Login and Instagram Graph API, such as media metadata and comments needed for report generation.</li>
  <li>Usage and system logs required for security, troubleshooting, and service reliability.</li>
</ul>

<h2>How We Use Information</h2>
<ul>
  <li>Authenticate users and secure workspace access.</li>
  <li>Generate lead reports from selected Instagram media and comments.</li>
  <li>Store report history and account settings for ongoing use.</li>
  <li>Improve product performance, reliability, and fraud prevention.</li>
</ul>

<h2>Data Sharing</h2>
<p>We do not sell personal data. We share data only with service providers required to operate the platform (for example, hosting and database providers) and when legally required.</p>

<h2>Data Retention</h2>
<p>We retain data while your account is active or as needed for legal, security, and operational purposes. You may request deletion of your workspace data by contacting us.</p>

<h2>Security</h2>
<p>We use reasonable technical and organizational controls to protect data in transit and at rest. No online service can guarantee absolute security.</p>

<h2>Your Rights</h2>
<p>You may request access, correction, or deletion of your data, subject to applicable law and legitimate operational requirements.</p>

<h2>Policy Updates</h2>
<p>We may update this policy periodically. Material updates will be reflected by a revised effective date on this page.</p>
"""

TERMS_BODY_HTML = """
<h2>Acceptance of Terms</h2>
<p>By using InstaScrapper Pro, you agree to these terms and to comply with Meta and Instagram platform policies.</p>

<h2>Permitted Use</h2>
<ul>
  <li>You must have authority to connect and analyze each Instagram account you add.</li>
  <li>You are responsible for lawful use of exported report data.</li>
  <li>You may not use the service for spam, abuse, or policy-violating activity.</li>
</ul>

<h2>Accounts and Security</h2>
<p>You are responsible for maintaining the confidentiality of login credentials and for activity under your account.</p>

<h2>Third-Party Platforms</h2>
<p>The service depends on third-party APIs, including Meta and Instagram. Availability and data access may change based on platform rules and permissions.</p>

<h2>Disclaimer</h2>
<p>The service is provided on an \"as is\" and \"as available\" basis. We do not guarantee uninterrupted access or specific business outcomes.</p>

<h2>Limitation of Liability</h2>
<p>To the maximum extent permitted by law, InstaScrapper Pro is not liable for indirect, incidental, special, consequential, or punitive damages.</p>

<h2>Changes</h2>
<p>We may update these terms from time to time. Continued use after updates constitutes acceptance of the revised terms.</p>
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


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


def _parse_comments_limit(raw: str) -> int:
    cleaned = raw.strip().lower()
    if cleaned in {"all", "0"}:
        return 0
    return _to_int(cleaned, label="Comments per post", minimum=1)


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


def _extract_error_code(text: str, key: str) -> int | None:
    match = re.search(rf"[\"']{key}[\"']\s*:\s*(\d+)", text)
    if match:
        return int(match.group(1))
    return None


def _friendly_api_error_text(text: str) -> str:
    lowered = text.lower()
    code = _extract_error_code(text, "code")
    subcode = _extract_error_code(text, "error_subcode")

    if code == 190 and subcode == 463:
        return "Instagram token expired. Reconnect the account (Meta login) or paste a fresh long-lived access token."

    if code == 190 or "invalid oauth access token" in lowered:
        return (
            "Instagram rejected the access token. Paste only the raw token value "
            "(no 'Bearer', no quotes, no 'access_token=' prefix), then save account again."
        )

    if code in {10, 200} or "permission" in lowered:
        return (
            "Token is missing required permissions. Regenerate it with Instagram Graph "
            "permissions for reading media/comments."
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
            "Use account label for text names."
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


def _format_iso(value: str | None) -> str:
    if not value:
        return "-"
    dt = _parse_iso(value)
    if dt is None:
        return value
    return dt.strftime("%b %d, %Y %I:%M %p")


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


def _parse_json_int_list(raw: str | None) -> list[int]:
    out: list[int] = []
    for item in _parse_json_string_list(raw):
        if item.isdigit():
            out.append(int(item))
    return out


def _normalize_name(value: str) -> str:
    trimmed = value.strip()
    return trimmed if trimmed else "Unknown"


def _default_account_form() -> dict:
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
        "team_member_user_ids": [],
    }


def _default_run_form(selected_profile: dict | None, lookback_override: str | None = None) -> dict:
    if selected_profile is None:
        return {
            "profile_id": "",
            "lookback_days": lookback_override or "90",
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
        "lookback_days": lookback_override or str(selected_profile["default_lookback_days"]),
        "media_limit": str(selected_profile["default_media_limit"]),
        "comments_per_media": comments_default,
        "max_profiles": "" if selected_profile["default_max_profiles"] is None else str(selected_profile["default_max_profiles"]),
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
        item["comments_scope_display"] = f"up to {comments_per_media}/post"

    item["selected_media_count"] = selected_count
    return item


def _load_media_items(profile: dict | None, *, lookback_days: int | None) -> list[dict]:
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
        items = client.list_media(media_limit=100, lookback_days=lookback_days)
    except Exception:
        return []

    out: list[dict] = []
    for item in items:
        out.append(
            {
                "id": str(item.media_id),
                "timestamp_display": item.timestamp.strftime("%Y-%m-%d") if item.timestamp else "Unknown",
                "media_type": item.media_type,
                "comments_count": item.comments_count,
                "like_count": item.like_count,
                "caption": item.caption,
                "permalink": item.permalink,
            }
        )
    return out


def _is_placeholder_credential(value: str) -> bool:
    cleaned = value.strip().strip('"').strip("'").lower()
    return cleaned in {
        "",
        "replace_me",
        "changeme",
        "your_token_here",
        "your_business_account_id",
    }


def _meta_oauth_enabled() -> bool:
    return bool(_meta_client_id() and os.getenv("META_APP_SECRET", "").strip())


def _meta_client_id() -> str:
    raw = os.getenv("META_APP_ID", "").strip()
    # Keep only digits so accidental quotes/whitespace/non-digit chars do not break OAuth.
    return "".join(ch for ch in raw if ch.isdigit())


def _meta_graph_version() -> str:
    return os.getenv("META_GRAPH_VERSION", os.getenv("IG_GRAPH_VERSION", "v21.0")).strip() or "v21.0"


def _meta_redirect_uri() -> str:
    configured = os.getenv("META_REDIRECT_URI", "").strip()
    if configured:
        return configured
    return url_for("meta_connect_callback", _external=True)


def _meta_scopes() -> str:
    return os.getenv(
        "META_OAUTH_SCOPES",
        "public_profile,email,pages_show_list,pages_read_engagement,instagram_basic,instagram_manage_comments,business_management",
    ).strip()


def _invite_is_expired(invite: dict) -> bool:
    expires_at = _parse_iso(str(invite.get("expires_at") or ""))
    if expires_at is None:
        return False
    return expires_at < datetime.now(timezone.utc)


def _workspace_and_user() -> tuple[dict | None, int | None, list[dict], dict | None]:
    user_id = session.get("user_id")
    if user_id is None:
        return (None, None, [], None)
    try:
        user = get_user(int(user_id))
    except Exception:
        return (None, None, [], None)
    if user is None:
        return (None, None, [], None)

    workspaces = list_user_workspaces(int(user["id"]))
    if not workspaces:
        add_workspace_member(workspace_id=1, user_id=int(user["id"]), role="owner")
        workspaces = list_user_workspaces(int(user["id"]))

    if not workspaces:
        workspace_id = create_workspace(name=f"{_normalize_name(str(user['full_name']))} Workspace", owner_user_id=int(user["id"]))
        workspaces = list_user_workspaces(int(user["id"]))
    else:
        requested = session.get("workspace_id")
        workspace_ids = {int(ws["id"]) for ws in workspaces}
        if isinstance(requested, int) and requested in workspace_ids:
            workspace_id = requested
        elif isinstance(requested, str) and requested.isdigit() and int(requested) in workspace_ids:
            workspace_id = int(requested)
        else:
            workspace_id = int(workspaces[0]["id"])

    session["workspace_id"] = int(workspace_id)
    membership = get_workspace_membership(int(workspace_id), int(user["id"]))
    return (user, int(workspace_id), workspaces, membership)


def _require_auth(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user, workspace_id, workspaces, membership = _workspace_and_user()
        if user is None:
            return redirect(url_for("login_get"))
        request._ctx_user = user
        request._ctx_workspace_id = workspace_id
        request._ctx_workspaces = workspaces
        request._ctx_membership = membership
        return view(*args, **kwargs)

    return wrapped


def _render_auth_page(*, message: str | None = None, error: str | None = None):
    pending_invite_view = None
    token = session.get("pending_invite_token")
    if isinstance(token, str) and token:
        invite = get_workspace_invite(token)
        if invite is not None and not _invite_is_expired(invite):
            pending_invite_view = {
                "workspace_name": f"Workspace #{invite['workspace_id']}",
                "role": str(invite.get("role") or "member"),
                "email": str(invite.get("email") or ""),
            }

    allow_registration = count_users() == 0 or pending_invite_view is not None

    return render_template_string(
        AUTH_HTML,
        message=message,
        error=error,
        pending_invite=pending_invite_view,
        allow_registration=allow_registration,
    )


def _build_profile_view(profiles: list[dict], members: list[dict]) -> list[dict]:
    member_names = {int(m["user_id"]): _normalize_name(str(m.get("full_name") or m.get("email") or "")) for m in members}
    out: list[dict] = []
    for profile in profiles:
        item = dict(profile)
        team_ids = _parse_json_int_list(item.get("team_member_ids_json"))
        display_names = [member_names[user_id] for user_id in team_ids if user_id in member_names]
        item["team_member_ids"] = team_ids
        item["team_member_names_display"] = ", ".join(display_names) if display_names else "All members"
        out.append(item)
    return out


def _render_dashboard(
    *,
    user: dict,
    workspace_id: int,
    workspaces: list[dict],
    membership: dict | None,
    message: str | None = None,
    error: str | None = None,
    preview: list | None = None,
    account_form: dict | None = None,
    run_form: dict | None = None,
    selected_profile_id: int | None = None,
    active_run: dict | None = None,
    auto_continue: bool = False,
):
    members = list_workspace_members(workspace_id)
    raw_profiles = list_profiles(workspace_id=workspace_id)
    profiles = _build_profile_view(raw_profiles, members)
    profile_by_id = {int(p["id"]): p for p in profiles}

    selected_profile = None
    if selected_profile_id is not None and selected_profile_id in profile_by_id:
        selected_profile = profile_by_id[selected_profile_id]
    elif profiles:
        selected_profile = profiles[0]

    runs = list_runs(limit=50, workspace_id=workspace_id)
    runs_view = []
    for run in runs:
        item = dict(run)
        item["started_at_display"] = _format_iso(item.get("started_at"))
        status = str(item.get("status") or "running")
        if status not in {"success", "failed", "running", "queued"}:
            item["status"] = "running"
        item = _apply_run_scope_fields(item)
        if item.get("error_message"):
            item["error_display"] = _friendly_api_error_text(str(item["error_message"]))[:180]
        else:
            item["error_display"] = ""
        runs_view.append(item)

    total_runs = len(runs)
    success_runs = len([row for row in runs if row.get("status") == "success"])
    last_success = next((row for row in runs if row.get("status") == "success"), None)
    last_report_at = _format_iso(last_success.get("completed_at")) if last_success else "-"

    if run_form is None:
        run_form = _default_run_form(selected_profile)
    else:
        run_form = dict(run_form)
        if not isinstance(run_form.get("selected_media_ids"), list):
            run_form["selected_media_ids"] = []

    if account_form is None:
        account_form = _default_account_form()

    active_run_view = _apply_run_scope_fields(dict(active_run)) if active_run is not None else None

    selected_profile_for_media = None
    lookback_for_media = _to_optional_int(str(run_form.get("lookback_days") or ""), "Lookback days", minimum=1)
    if selected_profile is not None:
        selected_profile_for_media = get_profile(int(selected_profile["id"]), workspace_id=workspace_id)
    media_items = _load_media_items(selected_profile_for_media, lookback_days=lookback_for_media)
    selected_media_ids = set(str(item) for item in run_form.get("selected_media_ids", []))
    for item in media_items:
        item["selected"] = str(item.get("id")) in selected_media_ids

    pending_invites_raw = list_workspace_invites(workspace_id)
    pending_invites = []
    for invite in pending_invites_raw:
        if invite.get("accepted_at"):
            continue
        if _invite_is_expired(invite):
            continue
        pending_invites.append(
            {
                "email": str(invite.get("email") or ""),
                "role": str(invite.get("role") or "member"),
                "expires_at_display": _format_iso(invite.get("expires_at")),
                "link": url_for("accept_invite", token=str(invite.get("token")), _external=True),
            }
        )

    meta_connect = session.get("meta_connect")
    meta_connect_accounts = []
    if isinstance(meta_connect, dict):
        accounts = meta_connect.get("accounts")
        if isinstance(accounts, list):
            for account in accounts:
                if not isinstance(account, dict):
                    continue
                meta_connect_accounts.append(
                    {
                        "display_name": str(account.get("display_name") or "Connected Account"),
                        "business_account_id": str(account.get("business_account_id") or ""),
                    }
                )

    return render_template_string(
        INDEX_HTML,
        user=user,
        workspace_id=workspace_id,
        workspaces=workspaces,
        membership=membership,
        profiles=profiles,
        runs=runs_view,
        members=members,
        pending_invites=pending_invites,
        message=message,
        error=error,
        preview=preview,
        active_run=active_run_view,
        auto_continue=auto_continue,
        is_ephemeral=is_ephemeral_storage(),
        ai_enabled=ai_enabled(),
        meta_oauth_enabled=_meta_oauth_enabled(),
        meta_connect_accounts=meta_connect_accounts,
        account_form=account_form,
        run_form=run_form,
        media_items=media_items,
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
    secret_key = (
        os.getenv("APP_SECRET_KEY", "").strip()
        or os.getenv("FLASK_SECRET_KEY", "").strip()
        or "change-me-before-prod"
    )
    app.secret_key = secret_key
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=os.getenv("VERCEL") == "1",
    )

    init_db()
    legal_contact_email = os.getenv("LEGAL_CONTACT_EMAIL", "privacy@instascrapper.com").strip() or "privacy@instascrapper.com"
    legal_effective_date = os.getenv("LEGAL_EFFECTIVE_DATE", "March 3, 2026").strip() or "March 3, 2026"

    @app.get("/privacy")
    def privacy_policy():
        return render_template_string(
            LEGAL_HTML,
            title="Privacy Policy",
            effective_date=legal_effective_date,
            contact_email=legal_contact_email,
            body=PRIVACY_BODY_HTML,
        )

    @app.get("/terms")
    def terms_of_service():
        return render_template_string(
            LEGAL_HTML,
            title="Terms of Service",
            effective_date=legal_effective_date,
            contact_email=legal_contact_email,
            body=TERMS_BODY_HTML,
        )

    @app.get("/")
    @_require_auth
    def index():
        user = request._ctx_user
        workspace_id = int(request._ctx_workspace_id)
        workspaces = request._ctx_workspaces
        membership = request._ctx_membership

        selected_raw = request.args.get("profile_id", "").strip()
        selected_profile_id = int(selected_raw) if selected_raw.isdigit() else None
        lookback_raw = request.args.get("lookback_days", "").strip()
        active_run_raw = request.args.get("active_run_id", "").strip()
        active_run_id = int(active_run_raw) if active_run_raw.isdigit() else None

        active_run = None
        message = request.args.get("message", "").strip() or None
        error = request.args.get("error", "").strip() or None
        preview = None
        auto_continue = False

        if active_run_id is not None:
            try:
                active_run = get_run(active_run_id, workspace_id=workspace_id)
            except Exception as err:  # noqa: BLE001
                error = f"Run failed: {_friendly_api_error_text(str(err))}"
                active_run = get_run(active_run_id, workspace_id=workspace_id)
        else:
            running = next(
                (row for row in list_runs(limit=10, workspace_id=workspace_id) if row.get("status") in {"queued", "running"}),
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
                        f"Report complete for {active_run.get('profile_name')}. "
                        f"Found {active_run.get('lead_count', 0)} leads. "
                        "Use Report History to download CSV."
                    )
            elif status == "failed":
                error_text = str(active_run.get("error_message") or "Unknown error")
                error = f"Run failed: {_friendly_api_error_text(error_text)}"

        selected_profile = get_profile(selected_profile_id, workspace_id=workspace_id) if selected_profile_id else None
        run_form = _default_run_form(selected_profile, lookback_override=lookback_raw or None)

        return _render_dashboard(
            user=user,
            workspace_id=workspace_id,
            workspaces=workspaces,
            membership=membership,
            selected_profile_id=selected_profile_id,
            message=message,
            error=error,
            preview=preview,
            run_form=run_form,
            active_run=active_run,
            auto_continue=auto_continue,
        )

    @app.get("/login")
    def login_get():
        user, _, _, _ = _workspace_and_user()
        if user is not None:
            return redirect(url_for("index"))
        return _render_auth_page()

    @app.post("/login")
    def login_post():
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        if not email or not password:
            return _render_auth_page(error="Email and password are required.")

        user = get_user_by_email(email)
        if user is None or not check_password_hash(str(user.get("password_hash") or ""), password):
            return _render_auth_page(error="Invalid email or password.")

        session["user_id"] = int(user["id"])

        token = session.get("pending_invite_token")
        if isinstance(token, str) and token:
            invite = get_workspace_invite(token)
            if invite and not _invite_is_expired(invite):
                if str(invite.get("email", "")).strip().lower() != email:
                    session.clear()
                    return _render_auth_page(error="Invite email does not match this login.")
                accept_workspace_invite(token=token, user_id=int(user["id"]))
                session.pop("pending_invite_token", None)
                session["workspace_id"] = int(invite["workspace_id"])

        _, workspace_id, _, _ = _workspace_and_user()
        if workspace_id is not None:
            session["workspace_id"] = int(workspace_id)
        return redirect(url_for("index"))

    @app.get("/register")
    def register_get():
        user, _, _, _ = _workspace_and_user()
        if user is not None:
            return redirect(url_for("index"))
        return _render_auth_page()

    @app.post("/register")
    def register_post():
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not full_name:
            return _render_auth_page(error="Full name is required.")
        if not email:
            return _render_auth_page(error="Email is required.")
        if len(password) < 8:
            return _render_auth_page(error="Password must be at least 8 characters.")
        if get_user_by_email(email) is not None:
            return _render_auth_page(error="An account with this email already exists.")

        pending_token = session.get("pending_invite_token")
        invite = get_workspace_invite(pending_token) if isinstance(pending_token, str) and pending_token else None
        users_before = count_users()
        allow_registration = users_before == 0 or invite is not None
        if not allow_registration:
            return _render_auth_page(error="Self-signup is disabled. Use an invite link.")

        if invite is not None and _invite_is_expired(invite):
            session.pop("pending_invite_token", None)
            return _render_auth_page(error="Invite link expired. Ask for a new invite.")

        if invite is not None:
            invite_email = str(invite.get("email") or "").strip().lower()
            if invite_email != email:
                return _render_auth_page(error="Use the same email address the invite was sent to.")

        password_hash = generate_password_hash(password)
        user_id = create_user(email=email, password_hash=password_hash, full_name=full_name)

        if invite is not None:
            accept_workspace_invite(token=str(invite["token"]), user_id=user_id)
            session.pop("pending_invite_token", None)
            workspace_id = int(invite["workspace_id"])
        elif users_before == 0:
            add_workspace_member(workspace_id=1, user_id=user_id, role="owner")
            workspace_id = 1
        else:
            workspace_id = create_workspace(name=f"{_normalize_name(full_name)} Workspace", owner_user_id=user_id)

        session["user_id"] = int(user_id)
        session["workspace_id"] = int(workspace_id)
        return redirect(url_for("index"))

    @app.post("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login_get"))

    @app.get("/invite/<token>")
    def accept_invite(token: str):
        invite = get_workspace_invite(token)
        if invite is None:
            return _render_auth_page(error="Invite link is invalid.")
        if _invite_is_expired(invite):
            return _render_auth_page(error="Invite link expired. Ask for a new invite.")

        user, _, _, _ = _workspace_and_user()
        if user is None:
            session["pending_invite_token"] = token
            return _render_auth_page(message="Sign in or create account to accept this invite.")

        user_email = str(user.get("email") or "").strip().lower()
        invite_email = str(invite.get("email") or "").strip().lower()
        if user_email != invite_email:
            return _render_auth_page(error="Invite email does not match signed-in account.")

        accept_workspace_invite(token=token, user_id=int(user["id"]))
        session["workspace_id"] = int(invite["workspace_id"])
        return redirect(url_for("index", message="Invite accepted. Workspace added."))

    @app.post("/workspace/switch")
    @_require_auth
    def switch_workspace():
        user = request._ctx_user
        workspace_raw = request.form.get("workspace_id", "").strip()
        if not workspace_raw.isdigit():
            return redirect(url_for("index", error="Choose a valid workspace."))
        workspace_id = int(workspace_raw)
        membership = get_workspace_membership(workspace_id, int(user["id"]))
        if membership is None:
            return redirect(url_for("index", error="You do not have access to that workspace."))
        session["workspace_id"] = workspace_id
        return redirect(url_for("index"))

    @app.post("/workspace/invite")
    @_require_auth
    def invite_workspace_member():
        user = request._ctx_user
        workspace_id = int(request._ctx_workspace_id)
        membership = request._ctx_membership or {}

        role = str(membership.get("role") or "member").lower()
        if role not in {"owner", "admin"}:
            return redirect(url_for("index", error="Only owners/admins can invite members."))

        email = request.form.get("email", "").strip().lower()
        invite_role = request.form.get("role", "member").strip().lower()
        if not email:
            return redirect(url_for("index", error="Invite email is required."))
        if invite_role not in {"admin", "member"}:
            return redirect(url_for("index", error="Invite role must be admin or member."))

        expires_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        token = create_workspace_invite(
            workspace_id=workspace_id,
            email=email,
            role=invite_role,
            invited_by_user_id=int(user["id"]),
            expires_at=expires_at,
        )
        invite_link = url_for("accept_invite", token=token, _external=True)
        return redirect(url_for("index", message=f"Invite created for {email}: {invite_link}"))

    @app.get("/connect/meta/start")
    @_require_auth
    def meta_connect_start():
        if not _meta_oauth_enabled():
            return redirect(
                url_for(
                    "index",
                    message=(
                        "Meta Login is not configured yet. Add META_APP_ID, META_APP_SECRET, "
                        "and META_REDIRECT_URI in Vercel Project Settings -> Environment Variables."
                    ),
                )
            )

        client_id = _meta_client_id()
        if len(client_id) < 8:
            return redirect(
                url_for(
                    "index",
                    error=(
                        "Meta Login configuration error: META_APP_ID is invalid. "
                        "Set META_APP_ID to your numeric Meta App ID."
                    ),
                )
            )

        print(
            "[meta_oauth_start]",
            {
                "client_id": client_id,
                "redirect_uri": _meta_redirect_uri(),
            },
            flush=True,
        )

        state = secrets.token_urlsafe(24)
        session["meta_oauth_state"] = state

        params = {
            "client_id": client_id,
            "redirect_uri": _meta_redirect_uri(),
            "state": state,
            "response_type": "code",
            "scope": _meta_scopes(),
        }
        auth_url = f"https://www.facebook.com/dialog/oauth?{urlencode(params)}"
        return redirect(auth_url)

    @app.get("/connect/meta/debug")
    @_require_auth
    def meta_connect_debug():
        client_id = _meta_client_id()
        return jsonify(
            {
                "meta_oauth_enabled": _meta_oauth_enabled(),
                "client_id": client_id,
                "client_id_length": len(client_id),
                "redirect_uri": _meta_redirect_uri(),
                "graph_version": _meta_graph_version(),
            }
        )

    @app.get("/connect/meta/callback")
    @_require_auth
    def meta_connect_callback():
        if not _meta_oauth_enabled():
            return redirect(
                url_for(
                    "index",
                    message=(
                        "Meta Login is not configured yet. Add META_APP_ID, META_APP_SECRET, "
                        "and META_REDIRECT_URI in Vercel Project Settings -> Environment Variables."
                    ),
                )
            )

        expected_state = session.get("meta_oauth_state")
        returned_state = request.args.get("state", "")
        if not expected_state or returned_state != expected_state:
            return redirect(url_for("index", error="Meta OAuth state mismatch. Try connecting again."))

        code = request.args.get("code", "").strip()
        if not code:
            error_reason = request.args.get("error_description", request.args.get("error", "OAuth denied.")).strip()
            return redirect(url_for("index", error=f"Meta OAuth failed: {error_reason}"))

        graph_version = _meta_graph_version()
        app_id = os.getenv("META_APP_ID", "").strip()
        app_secret = os.getenv("META_APP_SECRET", "").strip()

        try:
            token_resp = requests.get(
                f"https://graph.facebook.com/{graph_version}/oauth/access_token",
                params={
                    "client_id": app_id,
                    "client_secret": app_secret,
                    "redirect_uri": _meta_redirect_uri(),
                    "code": code,
                },
                timeout=25,
            ).json()
            if "error" in token_resp:
                raise RuntimeError(_friendly_api_error_text(str(token_resp["error"])))

            short_token = str(token_resp.get("access_token") or "").strip()
            if not short_token:
                raise RuntimeError("Meta OAuth did not return an access token.")

            long_resp = requests.get(
                f"https://graph.facebook.com/{graph_version}/oauth/access_token",
                params={
                    "grant_type": "fb_exchange_token",
                    "client_id": app_id,
                    "client_secret": app_secret,
                    "fb_exchange_token": short_token,
                },
                timeout=25,
            ).json()
            long_token = str(long_resp.get("access_token") or short_token).strip()

            pages_resp = requests.get(
                f"https://graph.facebook.com/{graph_version}/me/accounts",
                params={
                    "fields": "id,name,access_token,instagram_business_account{id,username}",
                    "limit": 100,
                    "access_token": long_token,
                },
                timeout=25,
            ).json()
            if "error" in pages_resp:
                raise RuntimeError(_friendly_api_error_text(str(pages_resp["error"])))

            accounts: list[dict] = []
            for page in pages_resp.get("data", []):
                ig_data = page.get("instagram_business_account") or {}
                business_id = str(ig_data.get("id") or "").strip()
                if not business_id:
                    continue
                username = str(ig_data.get("username") or "").strip()
                page_name = str(page.get("name") or "").strip()
                page_token = str(page.get("access_token") or "").strip() or long_token
                display_name = f"{page_name} ({'@' + username if username else business_id})"
                accounts.append(
                    {
                        "business_account_id": business_id,
                        "display_name": display_name,
                        "page_access_token": page_token,
                    }
                )

            if not accounts:
                raise RuntimeError(
                    "No Instagram Business accounts found on this Meta login. Ensure the user has page/account access."
                )

            session["meta_connect"] = {
                "graph_version": graph_version,
                "long_token": long_token,
                "accounts": accounts,
            }
            session.pop("meta_oauth_state", None)
            return redirect(url_for("index", message="Meta connected. Choose an account to save."))
        except Exception as err:  # noqa: BLE001
            session.pop("meta_connect", None)
            session.pop("meta_oauth_state", None)
            return redirect(url_for("index", error=f"Meta OAuth error: {err}"))

    @app.post("/connect/meta/save")
    @_require_auth
    def meta_connect_save():
        workspace_id = int(request._ctx_workspace_id)
        members = list_workspace_members(workspace_id)
        member_ids = [int(m["user_id"]) for m in members]

        meta_connect = session.get("meta_connect")
        if not isinstance(meta_connect, dict):
            return redirect(url_for("index", error="No Meta connection session found. Connect again."))

        accounts = meta_connect.get("accounts")
        if not isinstance(accounts, list) or not accounts:
            return redirect(url_for("index", error="No Meta account options found. Connect again."))

        index_raw = request.form.get("account_index", "").strip()
        if not index_raw.isdigit():
            return redirect(url_for("index", error="Choose a Meta account to save."))
        selected_index = int(index_raw)
        if selected_index < 0 or selected_index >= len(accounts):
            return redirect(url_for("index", error="Selected Meta account is invalid."))

        account = accounts[selected_index]
        name = request.form.get("name", "").strip()
        if not name:
            return redirect(url_for("index", error="Account label is required."))

        graph_version = str(meta_connect.get("graph_version") or "v21.0")
        access_token = _sanitize_access_token(str(account.get("page_access_token") or meta_connect.get("long_token") or ""))
        business_account_id = _sanitize_business_account_id(str(account.get("business_account_id") or ""))

        if _is_placeholder_credential(access_token) or _is_placeholder_credential(business_account_id):
            return redirect(url_for("index", error="Connected Meta account returned invalid credentials."))

        try:
            _validate_setup_credentials(
                access_token=access_token,
                business_account_id=business_account_id,
                graph_version=graph_version,
            )

            profile_id = create_profile(
                workspace_id=workspace_id,
                name=name,
                team_member_user_ids=member_ids,
                access_token=access_token,
                business_account_id=business_account_id,
                graph_version=graph_version,
                timeout_seconds=25,
                retry_count=3,
                retry_backoff_seconds=1.5,
                default_media_limit=25,
                default_comments_per_media=200,
                default_lookback_days=90,
                default_max_profiles=None,
            )
            session.pop("meta_connect", None)
            return redirect(url_for("index", profile_id=profile_id, message=f"Connected account '{name}' saved."))
        except sqlite3.IntegrityError:
            return redirect(url_for("index", error="An account with that label already exists in this workspace."))
        except Exception as err:  # noqa: BLE001
            return redirect(url_for("index", error=f"Could not save Meta account: {err}"))

    @app.post("/connect/meta/cancel")
    @_require_auth
    def meta_connect_cancel():
        session.pop("meta_connect", None)
        return redirect(url_for("index", message="Meta connection canceled."))

    @app.post("/accounts")
    @_require_auth
    def create_account():
        workspace_id = int(request._ctx_workspace_id)
        member_ids = {int(m["user_id"]) for m in list_workspace_members(workspace_id)}

        selected_team_ids: list[int] = []
        for raw_id in request.form.getlist("team_member_user_ids"):
            cleaned = raw_id.strip()
            if cleaned.isdigit():
                user_id = int(cleaned)
                if user_id in member_ids and user_id not in selected_team_ids:
                    selected_team_ids.append(user_id)

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
            "team_member_user_ids": selected_team_ids,
        }

        try:
            if not form["name"]:
                raise ValueError("Account label is required.")
            if not form["business_account_id"]:
                raise ValueError("Instagram Business Account ID is required.")
            if not form["access_token"]:
                raise ValueError("Instagram access token is required.")
            if _is_placeholder_credential(form["access_token"]):
                raise ValueError("Access token is still a placeholder.")
            if _is_placeholder_credential(form["business_account_id"]):
                raise ValueError("Business Account ID is still a placeholder.")
            if not form["business_account_id"].isdigit():
                raise ValueError("Instagram Business Account ID must be numeric only (example: 1784...).")

            _validate_setup_credentials(
                access_token=form["access_token"],
                business_account_id=form["business_account_id"],
                graph_version=form["graph_version"],
            )

            profile_id = create_profile(
                workspace_id=workspace_id,
                name=form["name"],
                team_member_user_ids=form["team_member_user_ids"],
                access_token=form["access_token"],
                business_account_id=form["business_account_id"],
                graph_version=form["graph_version"],
                timeout_seconds=_to_int(form["timeout_seconds"], "Timeout seconds", minimum=1),
                retry_count=_to_int(form["retry_count"], "Retry count", minimum=0),
                retry_backoff_seconds=_to_float(form["retry_backoff_seconds"], "Retry backoff seconds", minimum=0.0),
                default_media_limit=_to_int(form["default_media_limit"], "Default posts", minimum=1),
                default_comments_per_media=_to_int(form["default_comments_per_media"], "Default comments", minimum=1),
                default_lookback_days=_to_int(form["default_lookback_days"], "Default lookback days", minimum=1),
                default_max_profiles=_to_optional_int(form["default_max_profiles"], "Default max leads", minimum=1),
            )

            return redirect(url_for("index", profile_id=profile_id, message=f"Account '{form['name']}' saved."))
        except sqlite3.IntegrityError:
            return redirect(url_for("index", error="An account with this label already exists."))
        except Exception as err:  # noqa: BLE001
            return redirect(url_for("index", error=str(err)))

    @app.post("/accounts/<int:profile_id>/delete")
    @_require_auth
    def delete_account(profile_id: int):
        workspace_id = int(request._ctx_workspace_id)
        profile = get_profile(profile_id, workspace_id=workspace_id)
        if profile is None:
            return redirect(url_for("index", error="Account not found."))
        delete_profile(profile_id, workspace_id=workspace_id)
        return redirect(url_for("index", message="Account deleted."))

    @app.post("/run")
    @_require_auth
    def run_scrub():
        workspace_id = int(request._ctx_workspace_id)
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
            profile = get_profile(profile_id, workspace_id=workspace_id)
            if profile is None:
                raise ValueError("Selected account was not found. Pick another account.")

            lookback_days = _to_optional_int(form["lookback_days"], "Lookback days", minimum=1)
            if lookback_days is None:
                lookback_days = int(profile["default_lookback_days"])

            media_limit = _to_optional_int(form["media_limit"], "Posts to scan", minimum=1)
            if media_limit is None:
                media_limit = int(profile["default_media_limit"])

            if form["comments_per_media"]:
                comments_per_media = _parse_comments_limit(form["comments_per_media"])
            else:
                comments_per_media = int(profile["default_comments_per_media"])

            if selected_media_ids:
                media_limit = len(selected_media_ids)
                comments_per_media = 0

            max_profiles = _to_optional_int(form["max_profiles"], "Max leads", minimum=1)
            if max_profiles is None:
                max_profiles = profile["default_max_profiles"]

            run_id = create_run(
                workspace_id=workspace_id,
                profile_id=profile_id,
                media_limit=media_limit,
                comments_per_media=comments_per_media,
                lookback_days=lookback_days,
                max_profiles=max_profiles,
                selected_media_ids=selected_media_ids,
            )

            run = process_run_step(run_id=run_id, output_dir=OUTPUT_DIR, workspace_id=workspace_id)

            form["lookback_days"] = str(lookback_days)
            form["media_limit"] = str(media_limit)
            form["comments_per_media"] = "all" if comments_per_media <= 0 else str(comments_per_media)
            form["max_profiles"] = "" if max_profiles is None else str(max_profiles)
            form["selected_media_ids"] = selected_media_ids

            status = str(run.get("status", "running"))
            user = request._ctx_user
            workspaces = request._ctx_workspaces
            membership = request._ctx_membership
            if status == "success":
                preview = _preview_from_run(run)
                message = (
                    f"Report complete for {profile['name']}. "
                    f"Found {run.get('lead_count', 0)} leads. "
                    "Use Report History to download CSV."
                )
                return _render_dashboard(
                    user=user,
                    workspace_id=workspace_id,
                    workspaces=workspaces,
                    membership=membership,
                    message=message,
                    preview=preview,
                    run_form=form,
                    selected_profile_id=profile_id,
                    active_run=run,
                    auto_continue=False,
                )

            if status == "failed":
                friendly = _friendly_api_error_text(str(run.get("error_message") or "Unknown error"))
                return _render_dashboard(
                    user=user,
                    workspace_id=workspace_id,
                    workspaces=workspaces,
                    membership=membership,
                    error=f"Run failed: {friendly}",
                    run_form=form,
                    selected_profile_id=profile_id,
                    active_run=run,
                    auto_continue=False,
                )

            return _render_dashboard(
                user=user,
                workspace_id=workspace_id,
                workspaces=workspaces,
                membership=membership,
                message=f"Run queued for {profile['name']}. Processing in the background...",
                run_form=form,
                selected_profile_id=profile_id,
                active_run=run,
                auto_continue=True,
            )
        except Exception as err:  # noqa: BLE001
            user = request._ctx_user
            workspaces = request._ctx_workspaces
            membership = request._ctx_membership
            return _render_dashboard(
                user=user,
                workspace_id=workspace_id,
                workspaces=workspaces,
                membership=membership,
                error=str(err),
                run_form=form,
            )

    @app.get("/runs/<int:run_id>/status")
    @_require_auth
    def run_status(run_id: int):
        workspace_id = int(request._ctx_workspace_id)
        advance = request.args.get("advance", "").strip() == "1"
        try:
            run = process_run_step(run_id=run_id, output_dir=OUTPUT_DIR, workspace_id=workspace_id) if advance else get_run(run_id, workspace_id=workspace_id)
        except Exception as err:  # noqa: BLE001
            fallback = get_run(run_id, workspace_id=workspace_id)
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
    @_require_auth
    def download_output(filename: str):
        workspace_id = int(request._ctx_workspace_id)
        safe_filename = Path(filename).name
        if safe_filename != filename:
            abort(404)

        run = get_run_by_output_filename(safe_filename, workspace_id=workspace_id)
        if run is None:
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
