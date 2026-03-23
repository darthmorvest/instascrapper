from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .config import build_settings
from .enrichment import enrich_profile
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
    state.setdefault("active_media", None)
    state.setdefault("user_best", {})
    state.setdefault("usernames", [])
    state.setdefault("user_cursor", 0)
    state.setdefault("records", [])
    state.setdefault("ai_cursor", 0)
    raw_stats = state.get("stats")
    if not isinstance(raw_stats, dict):
        raw_stats = {}
    raw_stats.setdefault("candidate_profiles_total", 0)
    raw_stats.setdefault("qualified_leads", 0)
    raw_stats.setdefault("discovery_unavailable", 0)
    raw_stats.setdefault("with_profile_link", 0)
    raw_stats.setdefault("without_profile_link", 0)
    state["stats"] = raw_stats
    return state


def _parse_selected_media_ids(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        source = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        source = parsed
    else:
        return []

    deduped: list[str] = []
    seen: set[str] = set()
    for item in source:
        candidate = str(item).strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _strip_access_token_from_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlsplit(url)
    query_items = [(k, v) for (k, v) in parse_qsl(parsed.query, keep_blank_values=True) if k != "access_token"]
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_items), parsed.fragment))


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


def _state_record_to_lead(record: dict[str, Any]) -> LeadRecord:
    return LeadRecord(
        instagram_handle=str(record.get("instagram_handle", "")),
        instagram_profile_url=str(record.get("instagram_profile_url", "")),
        is_verified=record.get("is_verified"),
        followers_count=record.get("followers_count"),
        podcast_urls=list(record.get("podcast_urls", [])),
        podcast_genre=record.get("podcast_genre"),
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


def _append_candidate_record(
    *,
    state: dict[str, Any],
    sample: dict[str, Any],
    canonical_username: str,
    profile_data: Any | None,
    notes: list[str],
) -> None:
    state["records"].append(
        {
            "instagram_handle": canonical_username,
            "instagram_profile_url": f"https://instagram.com/{canonical_username}",
            "is_verified": getattr(profile_data, "is_verified", None),
            "podcast_urls": list(getattr(profile_data, "podcast_urls", []) or []),
            "podcast_genre": getattr(profile_data, "podcast_genre", None),
            "estimated_monthly_listeners": 0,
            "estimate_confidence": 0.0,
            "email": getattr(profile_data, "email", None),
            "website": getattr(profile_data, "website", None),
            "source_media_permalink": sample.get("media_permalink"),
            "source_media_share_count": sample.get("media_share_count"),
            "source_comment_id": sample.get("comment_id"),
            "source_comment_text": sample.get("comment_text"),
            "source_comment_timestamp": sample.get("comment_timestamp"),
            "notes": list(notes),
            "engagement_comment_count": int(sample.get("engagement_comment_count", 1)),
            "lead_score": None,
            "ai_fit_score": None,
            "ai_summary": None,
            "ai_outreach_angle": None,
            "followers_count": getattr(profile_data, "followers_count", None),
            "biography": getattr(profile_data, "biography", None),
        }
    )
    state["stats"]["qualified_leads"] = int(
        state["stats"].get("qualified_leads", 0)
    ) + 1


def _completion_message(*, lead_count: int, state: dict[str, Any]) -> str:
    stats = state.get("stats") if isinstance(state.get("stats"), dict) else {}
    candidate_total = int(stats.get("candidate_profiles_total", 0) or 0)
    discovery_unavailable = int(stats.get("discovery_unavailable", 0) or 0)
    with_profile_link = int(stats.get("with_profile_link", 0) or 0)
    without_profile_link = int(stats.get("without_profile_link", 0) or 0)

    if lead_count > 0:
        if candidate_total > 0:
            return (
                f"Completed - exported {lead_count} commenters from {candidate_total} candidate profiles; "
                f"{with_profile_link} with profile links."
            )
        return f"Completed - exported {lead_count} commenters."

    parts = []
    if candidate_total > 0:
        parts.append(f"0 commenters exported from {candidate_total} candidate profiles")
    else:
        parts.append("0 commenters exported")
    if with_profile_link > 0:
        parts.append(f"{with_profile_link} with profile links")
    if without_profile_link > 0:
        parts.append(f"{without_profile_link} without profile links returned by API")
    if discovery_unavailable > 0:
        parts.append(f"{discovery_unavailable} unavailable to profile discovery")
    return "Completed - " + "; ".join(parts) + "."


def _preview_rows(records: list[dict[str, Any]], limit: int = 25) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for record in records[:limit]:
        preview.append(
            {
                "instagram_handle": record.get("instagram_handle"),
                "instagram_profile_url": record.get("instagram_profile_url"),
                "bio_link": record.get("website"),
                "followers_count": record.get("followers_count"),
                "comment_text": record.get("source_comment_text"),
                "profile_lookup_status": ";".join(record.get("notes", [])[:3]),
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
            1 if item.get("website") else 0,
            int(item.get("followers_count") or 0),
            int(item.get("engagement_comment_count") or 0),
        ),
        reverse=True,
    )
    lead_records = [_state_record_to_lead(item) for item in records]
    completion_message = _completion_message(lead_count=len(lead_records), state=state)
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
        progress_message=completion_message,
    )


def process_run_step(
    *,
    run_id: int,
    output_dir: Path,
    workspace_id: int | None = None,
    step_budget_seconds: float | None = None,
) -> dict[str, Any]:
    run = get_run(run_id, workspace_id=workspace_id)
    if run is None:
        raise ValueError("Run not found")

    if run.get("status") in {"success", "failed"}:
        return run

    budget = step_budget_seconds
    if budget is None or budget <= 0:
        budget = float(_env_int("RUN_STEP_BUDGET_SECONDS", 7))
    start = time.monotonic()

    try:
        profile = get_profile(int(run["profile_id"]), workspace_id=workspace_id)
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
        update_run_progress(run_id, status="running")

        while time.monotonic() - start < budget:
            if phase in {"queued", "collect_media"}:
                if not state["media"]:
                    selected_media_ids = _parse_selected_media_ids(run.get("selected_media_ids_json"))
                    if selected_media_ids:
                        media_items = [client.get_media_item(media_id) for media_id in selected_media_ids]
                    else:
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
                    state["active_media"] = None
                    state["user_best"] = {}
                    phase = "collect_interactions"
                    scope_note = (
                        "Collecting all comments for selected posts"
                        if selected_media_ids
                        else "Collecting comments from selected media"
                    )
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message=scope_note,
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
                comments_per_media = int(run.get("comments_per_media") or 0)
                all_comments_mode = comments_per_media <= 0

                if cursor >= total_media and not state.get("active_media"):
                    usernames = sorted(state.get("user_best", {}).keys())
                    if run.get("max_profiles") is not None:
                        usernames = usernames[: int(run["max_profiles"])]
                    state["usernames"] = usernames
                    state["user_cursor"] = 0
                    state["stats"]["candidate_profiles_total"] = len(usernames)
                    phase = "enrich_profiles"
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message="Checking commenter profiles",
                        progress_current=0,
                        progress_total=max(len(usernames), 1),
                        state_json=_serialize_state(state),
                    )
                    run["phase"] = phase
                    continue

                processed_media = 0
                while cursor < total_media and processed_media < media_batch:
                    active_media = state.get("active_media")
                    if not active_media:
                        media_item = media[cursor]
                        media_id = str(media_item.get("media_id", ""))
                        fetch_shares = os.getenv("FETCH_SHARE_COUNTS", "0") == "1"
                        share_count = client.get_media_share_count(media_id) if (media_id and fetch_shares) else None
                        active_media = {
                            "media_id": media_id,
                            "permalink": media_item.get("permalink"),
                            "share_count": share_count,
                            "next_url": None,
                            "processed_comments": 0,
                        }
                        state["active_media"] = active_media

                    media_id = str(active_media.get("media_id", ""))
                    if not media_id:
                        cursor += 1
                        state["media_cursor"] = cursor
                        state["active_media"] = None
                        processed_media += 1
                        continue

                    page_limit = 100
                    comments, next_url = client.list_comments_page(
                        media_id=media_id,
                        page_limit=page_limit,
                        next_url=active_media.get("next_url"),
                    )
                    for comment in comments:
                        processed_comments = int(active_media.get("processed_comments", 0))
                        if (not all_comments_mode) and processed_comments >= comments_per_media:
                            break
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
                            "media_permalink": active_media.get("permalink"),
                            "media_share_count": active_media.get("share_count"),
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
                        active_media["processed_comments"] = int(active_media.get("processed_comments", 0)) + 1

                    processed_comments = int(active_media.get("processed_comments", 0))
                    reached_comment_limit = (not all_comments_mode) and processed_comments >= comments_per_media
                    active_media["next_url"] = (
                        None
                        if reached_comment_limit
                        else _strip_access_token_from_url(next_url)
                    )
                    state["active_media"] = active_media

                    if active_media.get("next_url"):
                        if time.monotonic() - start >= budget:
                            break
                        continue

                    cursor += 1
                    state["media_cursor"] = cursor
                    state["active_media"] = None
                    processed_media += 1
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
                    phase = "finalize"
                    update_run_progress(
                        run_id,
                        phase=phase,
                        progress_message="Finalizing report",
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
                        if "business_discovery_unavailable_for_username" in profile_data.notes:
                            state["stats"]["discovery_unavailable"] = int(
                                state["stats"].get("discovery_unavailable", 0)
                            ) + 1
                            _append_candidate_record(
                                state=state,
                                sample=sample,
                                canonical_username=canonical_username,
                                profile_data=profile_data,
                                notes=[*profile_data.notes, "profile_lookup_unavailable"],
                            )
                        else:
                            if (profile_data.website or "").strip():
                                state["stats"]["with_profile_link"] = int(
                                    state["stats"].get("with_profile_link", 0)
                                ) + 1
                                notes = [*profile_data.notes, "profile_link_found"]
                            else:
                                state["stats"]["without_profile_link"] = int(
                                    state["stats"].get("without_profile_link", 0)
                                ) + 1
                                notes = [*profile_data.notes, "no_profile_link_returned_by_api"]

                            _append_candidate_record(
                                state=state,
                                sample=sample,
                                canonical_username=canonical_username,
                                profile_data=profile_data,
                                notes=notes,
                            )

                    cursor += 1
                    processed += 1
                    state["user_cursor"] = cursor
                    if time.monotonic() - start >= budget:
                        break

                update_run_progress(
                    run_id,
                    phase=phase,
                    progress_message=f"Checked {cursor} of {total} commenter profiles",
                    progress_current=cursor,
                    progress_total=max(total, 1),
                    state_json=_serialize_state(state),
                )
                break

            if phase == "finalize":
                _finalize_success(run=run, state=state, output_dir=output_dir)
                return get_run(run_id, workspace_id=workspace_id) or run

            phase = "collect_media"

        update_run_progress(run_id, phase=phase, state_json=_serialize_state(state))
        return get_run(run_id, workspace_id=workspace_id) or run
    except Exception as err:  # noqa: BLE001
        finish_run_failure(run_id, str(err))
        failed = get_run(run_id, workspace_id=workspace_id)
        if failed is None:
            raise
        return failed
