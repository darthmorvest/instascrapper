from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .instagram_api import InstagramGraphClient
from .models import ProfileEnrichment

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
URL_RE = re.compile(r"(https?://[^\s]+)", flags=re.IGNORECASE)

PODCAST_HINT_KEYWORDS = (
    "podcast",
    "show",
    "episodes",
    "listen",
)

PODCAST_HOST_HINTS = (
    "spotify.com/show",
    "podcasts.apple.com",
    "youtube.com",
    "youtu.be",
    "rss.com",
    "buzzsprout.com",
    "libsyn.com",
    "transistor.fm",
    "podbean.com",
    "castos.com",
    "redcircle.com",
    "simplecast.com",
    "megaphone.fm",
)

GENRE_HINTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Business & Marketing", ("business", "entrepreneur", "founder", "marketing", "sales", "startup")),
    ("Health & Wellness", ("wellness", "fitness", "nutrition", "mental health", "mindset", "health")),
    ("Comedy", ("comedy", "funny", "humor", "stand up", "stand-up")),
    ("News & Politics", ("news", "politics", "policy", "current events", "journalist")),
    ("True Crime", ("true crime", "crime", "investigation", "mystery")),
    ("Technology", ("tech", "ai", "software", "developer", "engineering", "saas")),
    ("Education", ("education", "learning", "teacher", "study", "tutorial")),
    ("Sports", ("sports", "athlete", "football", "basketball", "soccer", "baseball", "training")),
    ("Faith & Spirituality", ("faith", "church", "christian", "spiritual", "bible", "meditation")),
    ("Entertainment", ("entertainment", "music", "film", "tv", "pop culture", "creator")),
)


def _http_get(url: str, timeout_seconds: int) -> requests.Response:
    return requests.get(
        url,
        timeout=timeout_seconds,
        headers={"User-Agent": "InstagramLeadScrubber/0.1"},
    )


def _normalize_url(url: str) -> str:
    candidate = url.strip().strip(".,)")
    if not candidate:
        return candidate
    parsed = urlparse(candidate)
    if parsed.scheme:
        return candidate
    return f"https://{candidate}"


def extract_emails(text: str | None) -> list[str]:
    if not text:
        return []
    return sorted(set(EMAIL_RE.findall(text)))


def extract_urls(text: str | None) -> list[str]:
    if not text:
        return []
    return sorted({_normalize_url(url) for url in URL_RE.findall(text)})


def looks_like_podcast_url(url: str) -> bool:
    lower = url.lower()
    if any(host in lower for host in PODCAST_HOST_HINTS):
        return True
    return any(word in lower for word in PODCAST_HINT_KEYWORDS)


def infer_podcast_genre(*, biography: str | None, podcast_urls: list[str]) -> str | None:
    haystack = " ".join([biography or "", " ".join(podcast_urls)]).lower()
    if not haystack.strip():
        return None

    best_genre = None
    best_score = 0
    for genre, hints in GENRE_HINTS:
        score = sum(1 for hint in hints if hint in haystack)
        if score > best_score:
            best_score = score
            best_genre = genre
    return best_genre


def crawl_website_for_hints(
    website: str | None,
    timeout_seconds: int = 12,
) -> tuple[list[str], list[str]]:
    if not website:
        return ([], [])
    normalized = _normalize_url(website)
    try:
        response = _http_get(normalized, timeout_seconds=timeout_seconds)
        response.raise_for_status()
    except Exception:  # noqa: BLE001
        return ([], [])

    soup = BeautifulSoup(response.text, "html.parser")
    discovered_podcast_links: set[str] = set()
    discovered_emails: set[str] = set()

    discovered_emails.update(extract_emails(soup.get_text(" ", strip=True)))

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "").strip()
        if not href:
            continue
        if href.lower().startswith("mailto:"):
            discovered_emails.add(href.split("mailto:", 1)[1].strip())
            continue
        absolute = urljoin(normalized, href)
        if looks_like_podcast_url(absolute):
            discovered_podcast_links.add(absolute)
    return (sorted(discovered_podcast_links), sorted(discovered_emails))


def enrich_profile(client: InstagramGraphClient, username: str) -> ProfileEnrichment:
    profile = ProfileEnrichment(username=username)
    discovery = client.business_discovery(username)
    if discovery is None:
        profile.notes.append("business_discovery_unavailable_for_username")
        return profile

    profile.ig_user_id = str(discovery.get("id")) if discovery.get("id") else None
    profile.is_verified = discovery.get("is_verified")
    profile.biography = discovery.get("biography")
    profile.website = discovery.get("website")
    profile.followers_count = discovery.get("followers_count")

    emails = extract_emails(profile.biography)
    if emails:
        profile.email = emails[0]

    candidate_urls: set[str] = set()
    candidate_urls.update(extract_urls(profile.biography))
    if profile.website:
        candidate_urls.add(_normalize_url(profile.website))

    podcast_urls = {url for url in candidate_urls if looks_like_podcast_url(url)}
    if profile.website:
        website_podcast_urls, website_emails = crawl_website_for_hints(profile.website)
        if website_emails and not profile.email:
            profile.email = website_emails[0]
            profile.notes.append("email_discovered_from_website")
        if not podcast_urls and website_podcast_urls:
            podcast_urls.update(website_podcast_urls)
            profile.notes.append("podcast_links_discovered_from_website")

    profile.podcast_urls = sorted(podcast_urls)
    profile.podcast_genre = infer_podcast_genre(
        biography=profile.biography,
        podcast_urls=profile.podcast_urls,
    )
    if profile.podcast_genre:
        profile.notes.append(f"podcast_genre_inferred={profile.podcast_genre}")
    return profile
