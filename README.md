# Instagram Podcast Lead Scrubber (MVP)

This project builds a lead list from Instagram comment interactions for accounts that:

1. Engage on your posts (comment-level)
2. Are verified (when resolvable via official API)
3. Appear to run a podcast

It outputs a CSV with:

- Instagram handle
- Podcast URLs
- Estimated monthly listeners (heuristic + confidence)
- Email
- Website
- Source interaction details (comment + media link + share metric if available)

## What this can and cannot do

### Supported

- Pull your account's media
- Pull comments per media
- Pull media-level share metric when Instagram Insights exposes it
- Enrich commenter profiles using business discovery where available
- Detect podcast links from biography/website
- Produce listener estimates with transparent assumptions

### Not supported (official API limits)

- User-level list of who shared/reposted content is generally unavailable
- Public listener counts are rarely exposed by podcast platforms, so estimates are modeled

## Quick start

1. Create a Meta app + Instagram Graph API credentials.
2. Copy `.env.example` to `.env` and fill values.
3. Install dependencies.
4. Run the CLI or web app.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
cp .env.example .env
ig-scrubber --output leads.csv --media-limit 30 --comments-per-media 200 --lookback-days 120
```

## Web app mode (secure workspace flow)

Start the web app:

```bash
source .venv/bin/activate
ig-scrubber-web
```

Open:

- `http://localhost:8080`

How it works:

1. Create the first owner login (email/password).
2. Add team members with invite links (owner/admin only).
3. Add one or more Instagram account profiles (`label + account ID + access token`) or connect via Meta OAuth.
4. In **Run Report**, choose account + lookback, then:
   - choose specific posts/reels (manual selection scans all comments on selected posts), or
   - run by dropdown defaults (posts/comments caps).
5. Click **Run Report Now**.
6. Runs process in resumable background-safe steps with live status polling.
7. Download current/old reports from **Report History**.

Defaults can be saved per account (media limit, comments per media, lookback days).

Access tokens are server-side only and scoped by workspace.

## Deploy on a subdomain

### Option A: VPS (Ubuntu + Nginx)

1. SSH into server and clone this project.
2. Create `.env` with your API values.
3. Install dependencies and package.
4. Run with Gunicorn on port `8080`.
5. Put Nginx in front and point `scrubber.yourdomain.com` to server IP.
6. Add TLS via Let's Encrypt.

Example Gunicorn command:

```bash
source .venv/bin/activate
gunicorn -w 2 -b 0.0.0.0:8080 "instagram_scrubber.webapp:create_app()"
```

### Option B: PaaS (Render/Railway/Fly)

1. Push repo to GitHub.
2. Create web service from repo.
3. Set build command: `pip install -r requirements.txt && pip install -e .`
4. Set start command: `gunicorn -w 2 -b 0.0.0.0:$PORT "instagram_scrubber.webapp:create_app()"`
5. Add env vars from `.env.example`.
6. Map your subdomain via platform custom domain settings.

### Option C: Vercel (recommended for fast setup)

This repo includes:

- `vercel.json` (routes all traffic to Python app)
- `api/index.py` (Vercel Python entrypoint)

#### 1) Push to GitHub

```bash
git init
git add .
git commit -m "Initial Instagram scrubber web app"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

#### 2) Import into Vercel

1. In Vercel dashboard, click **Add New Project**
2. Import your GitHub repo
3. Framework preset: **Other**
4. Build command: leave empty
5. Output directory: leave empty

#### 3) Set Environment Variables in Vercel

Optional for CLI-only workflows. Web profile mode stores credentials in app storage:

- `DATABASE_URL` (recommended for persistent Supabase/Postgres storage)
- `APP_SECRET_KEY` (required in production for secure session signing)
- `IG_ACCESS_TOKEN`
- `IG_BUSINESS_ACCOUNT_ID`
- `IG_GRAPH_VERSION` (optional, e.g. `v21.0`)
- `REQUEST_TIMEOUT_SECONDS` (optional)
- `REQUEST_RETRY_COUNT` (optional)
- `REQUEST_RETRY_BACKOFF_SECONDS` (optional)
- `OPENAI_API_KEY` (optional AI scoring/enrichment)
- `OPENAI_MODEL` (optional, default `gpt-4.1-mini`)

Optional Meta OAuth (recommended for marketer-friendly onboarding):

- `META_APP_ID`
- `META_APP_SECRET`
- `META_REDIRECT_URI` (optional; defaults to `/connect/meta/callback` absolute URL)
- `META_GRAPH_VERSION` (optional; default from `IG_GRAPH_VERSION`)
- `META_OAUTH_SCOPES` (optional override)

#### 3.1) Supabase managed database (recommended)

1. In Supabase: Project Settings -> Database -> Connection string -> Transaction pooler
2. Copy the full `postgresql://...` URI
3. In Vercel: Project Settings -> Environment Variables
4. Add `DATABASE_URL` with that Supabase URI
5. Redeploy

When `DATABASE_URL` is set, this app uses managed Postgres automatically and persists:

- Saved account profiles
- Run history
- Report metadata

#### 4) Deploy + attach subdomain

1. Click **Deploy**
2. Go to Project Settings -> Domains
3. Add `scrubber.yourdomain.com`
4. Create DNS CNAME record pointing your subdomain to Vercel target shown in UI

#### Important persistence note on Vercel

Without `DATABASE_URL`, this app uses SQLite:

- Local/VPS: persistent at `data/instagram_scrubber.db`
- Vercel: stored in `/tmp` (ephemeral), so profiles and run history are not guaranteed to persist

For durable one-time setup + old report history on Vercel, set `DATABASE_URL` to Supabase/Postgres.

#### Timeout-safe runner on Vercel

Report execution is chunked into resumable steps, so large runs do not need to finish in one function invocation.  
If a run is still in progress, keep the tab open; the dashboard updates progress in place and continues until complete.

## Required environment variables

Required for CLI mode:

- `IG_ACCESS_TOKEN`: Long-lived user/system token with required IG permissions
- `IG_BUSINESS_ACCOUNT_ID`: Instagram Business Account ID (numeric)

Optional:

- `IG_GRAPH_VERSION` (default: `v21.0`)
- `REQUEST_TIMEOUT_SECONDS` (default: `25`)
- `REQUEST_RETRY_COUNT` (default: `3`)
- `REQUEST_RETRY_BACKOFF_SECONDS` (default: `1.5`)
- `DATABASE_URL` (optional but recommended on Vercel): managed Postgres connection string
- `RUN_STEP_BUDGET_SECONDS` (default: `7`) - per-request processing budget for queued runs
- `RUN_MEDIA_BATCH_SIZE` (default: `2`) - media items processed per run step
- `RUN_PROFILE_BATCH_SIZE` (default: `3`) - profiles enriched per run step
- `RUN_AI_BATCH_SIZE` (default: `5`) - leads AI-scored per run step
- `OPENAI_API_KEY` (optional) - enables deeper AI lead scoring/summaries
- `OPENAI_MODEL` (optional, default `gpt-4.1-mini`)
- `OPENAI_BASE_URL` (optional, default `https://api.openai.com/v1`)
- `APP_SECRET_KEY` (required for secure web login sessions)
- `META_APP_ID` / `META_APP_SECRET` (optional; enables Meta OAuth account connection)
- `META_REDIRECT_URI` (optional callback URL override)
- `META_GRAPH_VERSION` (optional; defaults to IG graph version)
- `META_OAUTH_SCOPES` (optional override)

### Enable AI enrichment in Vercel

1. Vercel -> Project Settings -> Environment Variables
2. Add `OPENAI_API_KEY`
3. (Optional) Add `OPENAI_MODEL` (default `gpt-4.1-mini`)
4. Redeploy

The dashboard header shows whether AI enrichment is enabled.

## CSV columns

- `instagram_handle`
- `instagram_profile_url`
- `is_verified`
- `podcast_urls`
- `podcast_genre`
- `estimated_monthly_listeners`
- `estimate_confidence`
- `lead_score`
- `engagement_comment_count`
- `ai_fit_score`
- `ai_summary`
- `ai_outreach_angle`
- `email`
- `website`
- `source_media_permalink`
- `source_media_share_count`
- `source_comment_id`
- `source_comment_text`
- `source_comment_timestamp`
- `notes`

## Estimation model

The estimator is intentionally simple and explainable:

- Uses follower count as primary proxy
- Applies multipliers for verification and podcast-platform evidence
- Assigns confidence from available signal coverage

Tune logic in `src/instagram_scrubber/estimation.py`.
