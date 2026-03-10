from __future__ import annotations

from collections import defaultdict

from .enrichment import enrich_profile, profile_has_podcast_signal
from .estimation import estimate_monthly_listeners
from .instagram_api import InstagramGraphClient
from .models import CommentInteraction, LeadRecord


def _pick_best_interaction(interactions: list[CommentInteraction]) -> CommentInteraction:
    # Prefer the interaction on the post with the highest share count if present.
    return sorted(
        interactions,
        key=lambda x: (x.media_share_count is not None, x.media_share_count or -1),
        reverse=True,
    )[0]


def build_leads(
    client: InstagramGraphClient,
    media_limit: int,
    comments_per_media: int,
    lookback_days: int | None,
    max_profiles: int | None = None,
) -> list[LeadRecord]:
    interactions = client.collect_comment_interactions(
        media_limit=media_limit,
        comments_per_media=comments_per_media,
        lookback_days=lookback_days,
    )
    grouped: dict[str, list[CommentInteraction]] = defaultdict(list)
    for interaction in interactions:
        grouped[interaction.commenter_username.lower()].append(interaction)

    usernames = sorted(grouped.keys())
    if max_profiles is not None and max_profiles > 0:
        usernames = usernames[:max_profiles]

    records: list[LeadRecord] = []
    for uname_lower in usernames:
        sample_interaction = _pick_best_interaction(grouped[uname_lower])
        canonical_username = sample_interaction.commenter_username

        profile = enrich_profile(client, canonical_username)
        if profile.is_verified is not True:
            continue
        if not profile_has_podcast_signal(profile):
            continue

        estimate = estimate_monthly_listeners(profile)
        notes = list(profile.notes)
        notes.append(estimate.explanation)

        records.append(
            LeadRecord(
                instagram_handle=canonical_username,
                instagram_profile_url=f"https://instagram.com/{canonical_username}",
                is_verified=profile.is_verified,
                podcast_urls=profile.podcast_urls,
                podcast_genre=profile.podcast_genre,
                estimated_monthly_listeners=estimate.monthly_listeners,
                estimate_confidence=estimate.confidence,
                email=profile.email,
                website=profile.website,
                source_media_permalink=sample_interaction.media_permalink,
                source_media_share_count=sample_interaction.media_share_count,
                source_comment_id=sample_interaction.comment_id,
                source_comment_text=sample_interaction.comment_text,
                source_comment_timestamp=sample_interaction.comment_timestamp,
                notes=notes,
            )
        )

    return sorted(records, key=lambda x: x.estimated_monthly_listeners, reverse=True)
