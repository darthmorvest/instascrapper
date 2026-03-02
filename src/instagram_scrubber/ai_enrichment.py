from __future__ import annotations

import json
import os
from typing import Any

import requests


def ai_enabled() -> bool:
    return bool(os.getenv("OPENAI_API_KEY", "").strip())


def enrich_leads_with_ai(
    *,
    leads: list[dict[str, Any]],
    timeout_seconds: int = 25,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or not leads:
        return ({}, [])

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    endpoint = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")

    compact_leads = []
    for lead in leads:
        compact_leads.append(
            {
                "instagram_handle": lead.get("instagram_handle"),
                "followers_count": lead.get("followers_count"),
                "estimated_monthly_listeners": lead.get("estimated_monthly_listeners"),
                "podcast_urls": lead.get("podcast_urls") or [],
                "bio_excerpt": (lead.get("biography") or "")[:360],
                "website": lead.get("website"),
                "engagement_comment_count": lead.get("engagement_comment_count"),
                "source_comment_text": (lead.get("source_comment_text") or "")[:220],
            }
        )

    prompt = {
        "task": (
            "For each lead, score partnership fit for podcast guest/influencer outreach. "
            "Return JSON only."
        ),
        "output_schema": {
            "analyses": [
                {
                    "instagram_handle": "string",
                    "ai_fit_score": "integer 0-100",
                    "ai_summary": "short sentence, <= 24 words",
                    "ai_outreach_angle": "short sentence, <= 20 words",
                }
            ]
        },
        "leads": compact_leads,
    }

    try:
        response = requests.post(
            f"{endpoint}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a B2B podcast lead analyst. "
                            "Return strict JSON only, no markdown."
                        ),
                    },
                    {
                        "role": "user",
                        "content": json.dumps(prompt, ensure_ascii=True),
                    },
                ],
            },
            timeout=timeout_seconds,
        )
        payload = response.json()
    except Exception as err:  # noqa: BLE001
        return ({}, [f"ai_enrichment_failed={err}"])

    if response.status_code >= 400:
        return ({}, [f"ai_enrichment_http_{response.status_code}"])

    content = (
        payload.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    if not content:
        return ({}, ["ai_enrichment_empty_response"])

    try:
        parsed = json.loads(content)
    except Exception:  # noqa: BLE001
        return ({}, ["ai_enrichment_invalid_json"])

    analyses = parsed.get("analyses", [])
    mapped: dict[str, dict[str, Any]] = {}
    for item in analyses:
        handle = str(item.get("instagram_handle", "")).strip().lower()
        if not handle:
            continue
        fit_score = item.get("ai_fit_score")
        if isinstance(fit_score, (int, float)):
            fit_score = int(max(0, min(int(fit_score), 100)))
        else:
            fit_score = None
        mapped[handle] = {
            "ai_fit_score": fit_score,
            "ai_summary": (str(item.get("ai_summary", "")).strip() or None),
            "ai_outreach_angle": (str(item.get("ai_outreach_angle", "")).strip() or None),
        }
    return (mapped, [])
