# InstaScrapper Pro - Developer Handoff

Last updated: March 4, 2026
Repository: `https://github.com/darthmorvest/instascrapper`

This document is a technical handoff for another software engineer (or coding agent) to quickly understand:
- what the app does,
- how it is structured,
- what has been implemented,
- what is currently live,
- what is currently local-only and intentionally not pushed yet.

## 1) Product Summary

This is a multi-tenant web app for Instagram lead intelligence.

Primary workflow:
1. User logs into the app (email/password auth).
2. User connects an Instagram professional account via Meta OAuth (preferred) or manual token fallback.
3. User selects a time window and/or exact posts/reels.
4. App scrubs comments, enriches commenters, filters for qualified podcast leads, scores with heuristics + optional AI.
5. App stores run history and lets user download CSV reports.

Output includes:
- Instagram handle and profile URL
- Verification status
- Podcast URLs and podcast genre
- Estimated monthly listeners
- Lead score
- AI fit score and summary (when OpenAI configured)
- Email/website (if discoverable)
- Source post/comment metadata

## 2) Stack and Runtime

- Backend/UI: Flask + server-rendered HTML (single-app Python deployment)
- Data:
  - Preferred: Postgres via `DATABASE_URL` (Supabase tested)
  - Fallback: SQLite
- Deployment: Vercel (`api/index.py` -> `create_app()`)
- Long tasks: chunked state machine in DB (`run_engine.process_run_step`)
- AI enrichment: OpenAI API (optional)

Key entry points:
- Web app: `src/instagram_scrubber/webapp.py`
- Run processing engine: `src/instagram_scrubber/run_engine.py`
- DB/storage layer: `src/instagram_scrubber/storage.py`
- Instagram Graph client: `src/instagram_scrubber/instagram_api.py`

## 3) Current High-Level Architecture

### 3.1 Authentication and Multi-Tenancy

- Email/password auth with session cookies.
- Workspace model with members + invites.
- Accounts and runs are scoped by `workspace_id`.
- Team members can access runs/accounts per workspace.

Important routes:
- `/login`, `/register`, `/logout`
- workspace switching and invite acceptance flows in `webapp.py`

### 3.2 Account Onboarding

Two paths:
1. Recommended: Meta OAuth connect flow.
2. Advanced fallback: manual API token + business account ID form.

Meta OAuth routes:
- `GET /connect/meta/start`
- `GET /connect/meta/callback`
- `POST /connect/meta/save`
- `POST /connect/meta/cancel`
- `GET /connect/meta/debug` (diagnostics)

### 3.3 Run Lifecycle

Runs are persisted and processed in phases:
- `queued`
- `collect_media`
- `collect_interactions`
- `enrich_profiles`
- `ai_enrichment` (optional)
- `finalize`
- `completed` (status `success`) or `failed`

The engine (`process_run_step`) uses bounded time slices and stores state JSON between steps.

### 3.4 Report Storage and Download

- CSV content is persisted to `report_files` table.
- Optional filesystem copy is written to `outputs/` (or `/tmp/outputs` on Vercel).
- Downloads are served by filename lookup with workspace guard.

## 4) Database Model (Key Tables)

Defined/migrated in `storage.py`.

- `users`
- `workspaces`
- `workspace_members`
- `workspace_invites`
- `profiles` (connected IG account settings/tokens)
- `runs` (progress, state machine, history)
- `report_files` (CSV payload per run)

Notes:
- Both SQLite and Postgres are supported.
- For Supabase transaction pooler, psycopg `prepare_threshold=None` is used to avoid prepared statement issues.

## 5) Meta OAuth Integration: What Was Built

The integration was heavily hardened due real-world Meta configuration/permission issues.

Implemented improvements:
- Supports `META_CONFIG_ID` (Facebook Login for Business configuration flow).
- Falls back to `scope` only when config ID is not provided.
- Strips non-digit characters from app ID for OAuth safety.
- Re-consent forced via `auth_type=rerequest`.
- Handles both naming variants:
  - `instagram_basic` and `instagram_business_basic`
  - `instagram_manage_comments` and `instagram_business_manage_comments`
- Account discovery fallbacks:
  - `/me/accounts` with both `instagram_business_account` and `connected_instagram_account`
  - page-level fallback fetch (`/{page_id}`)
  - debug-token granular scopes + target IDs fallback
- Friendly error translation and more actionable red-banner diagnostics.

Known practical reality:
- Even with linked page + selected Instagram account in Meta dialog, granted token can still miss IG scopes unless app/use-case/config permissions are aligned.

## 6) Important Environment Variables

Core:
- `DATABASE_URL`
- `APP_SECRET_KEY`
- `OPENAI_API_KEY` (optional)
- `OPENAI_MODEL` (optional)

Meta OAuth:
- `META_APP_ID`
- `META_APP_SECRET`
- `META_REDIRECT_URI`
- `META_CONFIG_ID` (recommended)
- `META_GRAPH_VERSION`
- `META_OAUTH_SCOPES` (fallback mode)

Run tuning:
- `RUN_STEP_BUDGET_SECONDS`
- `RUN_MEDIA_BATCH_SIZE`
- `RUN_PROFILE_BATCH_SIZE`
- `RUN_AI_BATCH_SIZE`

## 7) Vercel Deployment Status and Recent Commits

Recent shipped commit sequence (most relevant):
- `ca7442a` fix secret key fallback + pg pooler behavior
- `ba6aa9e` privacy/terms pages
- `e09cb86` OAuth client ID hardening + debug endpoint
- `b708cd9` business `config_id` support
- `4fda958` better Meta account discovery diagnostics
- `9498b86` page-level IG discovery fallback
- `d1a327f` debug_token granular-scope fallback
- `6ec2c5b` force OAuth re-consent
- `8bc462b` scope-name compatibility handling
- `005dfb2` save-path fix: prefer long OAuth token over page token

## 8) IMPORTANT: Local-Only (Not Yet Pushed/Deployed) Work

Per user request, there is an in-progress background runner enhancement that has been implemented locally only.
Do not assume this is live.

Local changes currently present (uncommitted at handoff time):
- `src/instagram_scrubber/webapp.py`
- `src/instagram_scrubber/storage.py`
- `vercel.json`
- `.env.example`
- `README.md`

### 8.1 What the local background-runner change does

Goal:
- Allow runs to continue when user closes laptop/browser.

Added:
- Secure internal endpoint:
  - `GET/POST /internal/runner/tick`
- Secret-based auth for runner endpoint:
  - `CRON_SECRET` or `RUNNER_SECRET` required
- Feature flag:
  - `BACKGROUND_RUNNER_ENABLED=1` enables server-side runner mode
- Active run fetch helper in storage:
  - `list_active_runs(...)`
- Browser polling behavior change:
  - in background mode, UI polls status without advancing steps
- Vercel cron schedule in `vercel.json`:
  - every minute hitting `/internal/runner/tick`

### 8.2 Why it was not deployed yet

User explicitly requested no push/deploy while a current report is running.

## 9) Operational Behavior (Current Live vs Local)

Live today:
- Run advancement is primarily browser-poll driven (`/runs/<id>/status?advance=1`).
- If browser closes, runs usually pause and resume later when polled again.

Local (not yet shipped):
- Background cron tick can keep progressing queued/running runs without an open tab.

## 10) Common Troubleshooting Guide

### 10.1 Meta connect says no IG accounts found

Usually one of:
- Missing IG scopes on granted token
- Config does not include required permissions
- User lacks correct page/account role
- Stale prior grant (must remove Business Integration and reconnect)

Use:
- `/connect/meta/debug`
- red-banner message details
- Vercel logs for `/connect/meta/start` and `/connect/meta/callback`

### 10.2 Save connected account says token missing permissions

Previously caused by wrong token priority (page token). Fixed by preferring long OAuth token (`005dfb2`).

### 10.3 Data disappears on Vercel

If `DATABASE_URL` is unset, SQLite in `/tmp` is ephemeral.
Use Supabase/Postgres for durable storage.

## 11) Suggested Next Steps for Any New Developer

1. Confirm whether to ship the local background-runner changes now.
2. If yes:
   - commit local changes,
   - push,
   - deploy,
   - set env vars:
     - `BACKGROUND_RUNNER_ENABLED=1`
     - `CRON_SECRET=<strong-random>`
3. Validate by:
   - starting a long run,
   - closing browser,
   - checking run progress advances via periodic refresh.
4. Consider adding:
   - retry/backoff telemetry for runner endpoint,
   - run lock to avoid concurrent step processing races,
   - admin diagnostics page for active runner ticks.

## 12) Quick Command Reference

Local dev:
```bash
ig-scrubber-web
```

Compile sanity:
```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile src/instagram_scrubber/webapp.py src/instagram_scrubber/storage.py
```

Deploy production:
```bash
npx vercel --prod --yes
```

Read logs:
```bash
npx vercel logs --environment production --no-branch --no-follow --limit 200 --expand
```

---

If you are taking over this project, start with:
- `src/instagram_scrubber/webapp.py` (route and orchestration logic),
- `src/instagram_scrubber/run_engine.py` (state machine),
- `src/instagram_scrubber/storage.py` (data model and persistence behavior),
- and then compare git status to confirm whether background runner changes are still local-only.
