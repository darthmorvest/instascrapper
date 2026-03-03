from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .ai_enrichment import ai_enabled, enrich_leads_with_ai
from .config import build_settings
from .enrichment import enrich_profile
from .estimation import estimate_monthly_listeners
from .exporters import render_csv, write_csv_content
from .instagram_api import InstagramGraphClient
from .models import LeadRecord
from .storage import (
    finish_run_failure,
    finish_run_success,
    get_profile,
    get_run,
    save_report_file,
    update_run_progress,
)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _slug(value: str) -> str:
    keep = []
    for char in value.lower().strip():
        if char.isalnum():
            keep.append(char)
        elif char in {" ", "-", "_"}:
            keep.append("-")
    slug = "".join(keep).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "report"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _serialize_state(state: dict[str, Any]) -> str:
    return json.dumps(state, ensure_ascii=True, separators=(",", ":"))


def _load_state(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _normalize_state(state: dict[str, Any]) -> dict[str, Any]:
    state.setdefault("media", [])
    state.setdefault("media_cursor", 0)
    state.setdefault("user_best", {})
    state.setdefault("usernames", [])
    state.setdefault("user_cursor", 0)
    state.setdefault("records", [])
    state.setdefault("ai_cursor", 0)
    return state


def _share_sort_value(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return -1


def _is_better_candidate(candidate: dict[str, Any], existing: dict[str, Any]) -> bool:
    existing_share = _share_sort_value(existing.get("media_share_count"))
    candidate_share = _share_sort_value(candidate.get("media_share_count"))
    if candidate_share != existing_share:
        return candidate_share > existing_share

    existing_ts = _parse_iso(existing.get("comment_timestamp"))
    candidate_ts = _parse_iso(candidate.get("comment_timestamp"))
    if candidate_ts and existing_ts:
        return candidate_ts > existing_ts
    if candidate_ts and not existing_ts:
        return True
    return False


def _compute_lead_score(
    *,
    estimated_monthly_listeners: int,
    estimate_confidence: float,
    engagement_comment_count: int,
    ai_fit_score: int | None,
) -> int:
    normalized_listeners = min(100, int((estimated_monthly_listeners / 250_000) * 100))
    normalized_confidence = int(max(0.0, min(estimate_confidence, 1.0)) * 100)
    normalized_engagement = min(100, engagement_comment_count * 15)
    ai_component = ai_fit_score if ai_fit_score is not None else 0
    score = int(
        normalized_listeners * 0.45
        + normalized_confidence * 0.25
        + normalized_engagement * 0.2
        + ai_component * 0.1
    )
    return max(1, min(score, 100))


def _state_record_to_lead(record: dict[str, Any]) -> LeadRecord:
    return LeadRecord(
        instagram_handle=str(record.get("instagram_handle", "")),
        instagram_profile_url=str(record.get("instagram_profile_url", "")),
        is_verified=record.get("is_verified"),
        podcast_urls=list(record.get("podcast_urls", [])),
        estimated_monthly_listeners=int(record.get("estimated_monthly_listeners", 0)),
        estimate_confidence=float(record.get("estimate_confidence", 0.0)),
        email=record.get("email"),
        website=record.get("website"),
        source_media_permalink=record.get("source_media_permalink"),
        source_media_share_count=record.get("source_media_share_count"),
        source_comment_id=str(record.get("source_comment_id", "")),
        source_comment_text=str(record.get("source_comment_text", "")),
        source_comment_timestamp=_parse_iso(record.get("source_comment_timestamp")),
        notes=list(record.get("notes", [])),
        engagement_comment_count=int(record.get("engagement_comment_count", 1)),
        lead_score=record.get("lead_score"),
        ai_fit_score=record.get("ai_fit_score"),
        ai_summary=record.get("ai_summary"),
        ai_outreach_angle=record.get("ai_outreach_angle"),
    )


def _preview_rows(records: list[dict[str, Any]], limit: int = 25) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for record in records[:limit]:
        preview.append(
            {
                "instagram_handle": record.get("instagram_handle"),
                "podcast_urls": record.get("podcast_urls", []),
                "estimated_monthly_listeners": record.get("estimated_monthly_listeners"),
                "estimate_confidence": record.get("estimate_confidence"),
                "lead_score": record.get("lead_score"),
                "engagement_comment_count": record.get("engagement_comment_count"),
                "ai_fit_score": record.get("ai_fit_score"),
                "ai_summary": record.get("ai_summary"),
                "ai_outreach_angle": record.get("ai_outreach_angle"),
                "email": record.get("email"),
                "website": record.get("website"),
            }
        )
    return preview


def _finalize_success(
    *,
    run: dict[str, Any],
    state: dict[str, Any],
    output_dir: Path,
) -> None:
    records = sorted(
        state.get("records", []),
        key=lambda item: (
            int(item.get("lead_score") or 0),
            int(item.get("estimated_monthly_listeners") or 0),
        ),
        reverse=True,
    )
    lead_records = [_state_record_to_lead(item) for item in records]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{_slug(str(run['profile_name']))}_{timestamp}.csv"
    csv_content = render_csv(lead_records)
    save_report_file(run_id=int(run["id"]), output_filename=filename, csv_content=csv_content)
    try:
        write_csv_content(csv_content, str(output_dir / filename))
    except Exception:  # noqa: BLE001
        # Downloads use persistent DB storage first; local file write is a best-effort cache.
        pass
    preview_json = json.dumps(_preview_rows(records), ensure_ascii=True)
    finish_run_success(
        int(run["id"]),
        lead_count=len(lead_records),
        output_filename=filename,
        preview_json=preview_json,
    )


def process_run_step(
    *,
    run_id: int,
    output_dir: Path,
    step_budget_seconds: float | None = None,
) -> dict[str, Any]:
    run = get_run(run_id)
    if run is None:
        raise ValueError("Run not found")

    if run.get("status") in {"success", "failed"}:
        return run

    budget = step_budget_seconds
    if budget is None or budget <= 0:
        budget = float(_env_int("RUN_STEP_BUDGET_SECONDS", 7))
    start = time.monotonic()

    try:
        profile = get_profile(int(run["profile_id"]))
        if profile is None:
            raise RuntimeError("Selected account is no longer available.")

        settings = build_settings(
            access_token=str(profile["access_token"]),
            business_account_id=str(profile["business_account_id"]),
            graph_version=str(profile["graph_version"]),
            timeout_seconds=int(profile["timeout_seconds"]),
            retry_count=int(profile["retry_count"]),
            retry_backoff_seconds=float(profile["retry_backoff_seconds"]),
        )
        client = InstagramGraphClient(settings)

        state = _normalize_state(_load_state(run.get("state_json")))
        phase = str(run.get("phase") or "queued")
        media_batch = max(1, _env_int("RUN_MEDIA_BATCH_SIZE", 2))
        profile_batch = max(1, _env_int("RUN_PROFILE_BATCH_SIZE", 3))
        ai_batch = max(1, _env_int("RUN_AI_BATCH_SIZE", 5))

        update_run_progress(run_id, status="running")

        while time.monotonic() - start < budget:
            if phase in {"queued", "collect_media"}:
                if not state["media"]:
                    media_items = client.list_media(
                        media_limit=int(run["media_limit"]),
                        lookback_days=int(run["lookback_days"]),
                    )
                    state["media"] = [
                        {
                            "media_id": item.media_id,
                            "permalink": item.permalink,
                            "timestamp": item.timestamp.isoformat() if item.timestamp else None,
                        }
                        for item in media_items
                    ]
                    state["media_cursor"] = 0
                    state["user_best"] = {}
                    phase = "collect_interactions"
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message="Collecting comments from selected media",
                        progress_current=0,
                        progress_total=max(len(state["media"]), 1),
                        state_json=_serialize_state(state),
                    )
                    run["phase"] = phase
                    if not state["media"]:
                        phase = "enrich_profiles"
                        state["usernames"] = []
                        state["user_cursor"] = 0
                    continue
                phase = "collect_interactions"
                continue

            if phase == "collect_interactions":
                media = state.get("media", [])
                cursor = int(state.get("media_cursor", 0))
                total_media = len(media)
                if cursor >= total_media:
                    usernames = sorted(state.get("user_best", {}).keys())
                    if run.get("max_profiles") is not None:
                        usernames = usernames[: int(run["max_profiles"])]
                    state["usernames"] = usernames
                    state["user_cursor"] = 0
                    phase = "enrich_profiles"
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message="Enriching verified podcast profiles",
                        progress_current=0,
                        progress_total=max(len(usernames), 1),
                        state_json=_serialize_state(state),
                    )
                    run["phase"] = phase
                    continue

                processed = 0
                while cursor < total_media and processed < media_batch:
                    media_item = media[cursor]
                    media_id = str(media_item.get("media_id", ""))
                    if media_id:
                        share_count = client.get_media_share_count(media_id)
                        comments = client.list_comments_for_media(
                            media_id=media_id,
                            comments_per_media=int(run["comments_per_media"]),
                        )
                        for comment in comments:
                            commenter_username = (
                                comment.get("username")
                                or comment.get("from", {}).get("username")
                                or ""
                            ).strip()
                            if not commenter_username:
                                continue
                            key = commenter_username.lower()
                            candidate = {
                                "commenter_username": commenter_username,
                                "media_permalink": media_item.get("permalink"),
                                "media_share_count": share_count,
                                "comment_id": str(comment.get("id", "")),
                                "comment_text": (comment.get("text") or "").strip(),
                                "comment_timestamp": comment.get("timestamp"),
                            }
                            existing = state["user_best"].get(key)
                            if existing is None:
                                candidate["engagement_comment_count"] = 1
                                state["user_best"][key] = candidate
                            else:
                                existing["engagement_comment_count"] = int(
                                    existing.get("engagement_comment_count", 1)
                                ) + 1
                                if _is_better_candidate(candidate, existing):
                                    candidate["engagement_comment_count"] = existing["engagement_comment_count"]
                                    state["user_best"][key] = candidate
                    cursor += 1
                    processed += 1
                    state["media_cursor"] = cursor
                    if time.monotonic() - start >= budget:
                        break

                update_run_progress(
                    run_id,
                    phase=phase,
                    progress_message=f"Collected comments for {cursor} of {total_media} media items",
                    progress_current=cursor,
                    progress_total=max(total_media, 1),
                    state_json=_serialize_state(state),
                )
                break

            if phase == "enrich_profiles":
                usernames = list(state.get("usernames", []))
                cursor = int(state.get("user_cursor", 0))
                total = len(usernames)
                if cursor >= total:
                    phase = "ai_enrichment" if ai_enabled() and state.get("records") else "finalize"
                    state["ai_cursor"] = int(state.get("ai_cursor", 0))
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message=(
                            "Applying AI enrichment"
                            if phase == "ai_enrichment"
                            else "Finalizing report"
                        ),
                        progress_current=0,
                        progress_total=max(len(state.get("records", [])), 1),
                        state_json=_serialize_state(state),
                    )
                    continue

                processed = 0
                while cursor < total and processed < profile_batch:
                    username_key = usernames[cursor]
                    sample = state["user_best"].get(username_key)
                    if sample:
                        canonical_username = str(sample.get("commenter_username") or username_key)
                        profile_data = enrich_profile(client, canonical_username)
                        if profile_data.is_verified is True and profile_data.podcast_urls:
                            estimate = estimate_monthly_listeners(profile_data)
                            notes = list(profile_data.notes)
                            notes.append(estimate.explanation)

                            engagement_comment_count = int(sample.get("engagement_comment_count", 1))
                            lead_score = _compute_lead_score(
                                estimated_monthly_listeners=estimate.monthly_listeners,
                                estimate_confidence=estimate.confidence,
                                engagement_comment_count=engagement_comment_count,
                                ai_fit_score=None,
                            )

                            state["records"].append(
                                {
                                    "instagram_handle": canonical_username,
                                    "instagram_profile_url": f"https://instagram.com/{canonical_username}",
                                    "is_verified": profile_data.is_verified,
                                    "podcast_urls": profile_data.podcast_urls,
                                    "estimated_monthly_listeners": estimate.monthly_listeners,
                                    "estimate_confidence": estimate.confidence,
                                    "email": profile_data.email,
                                    "website": profile_data.website,
                                    "source_media_permalink": sample.get("media_permalink"),
                                    "source_media_share_count": sample.get("media_share_count"),
                                    "source_comment_id": sample.get("comment_id"),
                                    "source_comment_text": sample.get("comment_text"),
                                    "source_comment_timestamp": sample.get("comment_timestamp"),
                                    "notes": notes,
                                    "engagement_comment_count": engagement_comment_count,
                                    "lead_score": lead_score,
                                    "ai_fit_score": None,
                                    "ai_summary": None,
                                    "ai_outreach_angle": None,
                                    "followers_count": profile_data.followers_count,
                                    "biography": profile_data.biography,
                                }
                            )

                    cursor += 1
                    processed += 1
                    state["user_cursor"] = cursor
                    if time.monotonic() - start >= budget:
                        break

                update_run_progress(
                    run_id,
                    phase=phase,
                    progress_message=f"Enriched {cursor} of {total} candidate profiles",
                    progress_current=cursor,
                    progress_total=max(total, 1),
                    state_json=_serialize_state(state),
                )
                break

            if phase == "ai_enrichment":
                records = list(state.get("records", []))
                cursor = int(state.get("ai_cursor", 0))
                total = len(records)
                if cursor >= total:
                    phase = "finalize"
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message="Finalizing report",
                        progress_current=total,
                        progress_total=max(total, 1),
                        state_json=_serialize_state(state),
                    )
                    continue

                chunk = records[cursor: cursor + ai_batch]
                mapped, ai_notes = enrich_leads_with_ai(
                    leads=chunk,
                    timeout_seconds=max(10, int(profile["timeout_seconds"])),
                )
                for lead in chunk:
                    handle = str(lead.get("instagram_handle", "")).lower()
                    ai_data = mapped.get(handle)
                    if not ai_data:
                        continue
                    lead["ai_fit_score"] = ai_data.get("ai_fit_score")
                    lead["ai_summary"] = ai_data.get("ai_summary")
                    lead["ai_outreach_angle"] = ai_data.get("ai_outreach_angle")
                    lead["lead_score"] = _compute_lead_score(
                        estimated_monthly_listeners=int(lead.get("estimated_monthly_listeners", 0)),
                        estimate_confidence=float(lead.get("estimate_confidence", 0)),
                        engagement_comment_count=int(lead.get("engagement_comment_count", 1)),
                        ai_fit_score=lead.get("ai_fit_score"),
                    )

                if ai_notes:
                    for lead in chunk:
                        notes = list(lead.get("notes") or [])
                        notes.extend(ai_notes)
                        lead["notes"] = notes

                cursor += len(chunk)
                state["ai_cursor"] = cursor
                state["records"] = records
                update_run_progress(
                    run_id,
                    phase=phase,
                    progress_message=f"AI-enriched {cursor} of {total} qualified leads",
                    progress_current=cursor,
                    progress_total=max(total, 1),
                    state_json=_serialize_state(state),
                )
                break

            if phase == "finalize":
                _finalize_success(run=run, state=state, output_dir=output_dir)
                return get_run(run_id) or run

            phase = "collect_media"

        update_run_progress(run_id, phase=phase, state_json=_serialize_state(state))
        return get_run(run_id) or run
    except Exception as err:  # noqa: BLE001
        finish_run_failure(run_id, str(err))
        failed = get_run(run_id)
        if failed is None:
            raise
        return failed
