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

## Web app mode

Start the web app:

```bash
source .venv/bin/activate
ig-scrubber-web
```

Then open:

- `http://localhost:8080`

The app will:

- Let you run a scrub from the browser
- Save CSV files in `outputs/`
- Let you download any recent CSV from the UI

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

Add these to Project Settings -> Environment Variables:

- `IG_ACCESS_TOKEN`
- `IG_BUSINESS_ACCOUNT_ID`
- `IG_GRAPH_VERSION` (optional, e.g. `v21.0`)
- `REQUEST_TIMEOUT_SECONDS` (optional)
- `REQUEST_RETRY_COUNT` (optional)
- `REQUEST_RETRY_BACKOFF_SECONDS` (optional)

#### 4) Deploy + attach subdomain

1. Click **Deploy**
2. Go to Project Settings -> Domains
3. Add `scrubber.yourdomain.com`
4. Create DNS CNAME record pointing your subdomain to Vercel target shown in UI

## Required environment variables

- `IG_ACCESS_TOKEN`: Long-lived user/system token with required IG permissions
- `IG_BUSINESS_ACCOUNT_ID`: Instagram Business Account ID (numeric)

Optional:

- `IG_GRAPH_VERSION` (default: `v21.0`)
- `REQUEST_TIMEOUT_SECONDS` (default: `25`)
- `REQUEST_RETRY_COUNT` (default: `3`)
- `REQUEST_RETRY_BACKOFF_SECONDS` (default: `1.5`)

## CSV columns

- `instagram_handle`
- `instagram_profile_url`
- `is_verified`
- `podcast_urls`
- `estimated_monthly_listeners`
- `estimate_confidence`
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
