from __future__ import annotations

from .models import ListenerEstimate, ProfileEnrichment


def estimate_monthly_listeners(profile: ProfileEnrichment) -> ListenerEstimate:
    if profile.followers_count is None:
        base = 700
        signals = ["follower_count_missing_default_used"]
    else:
        base = max(300, int(profile.followers_count * 0.03))
        signals = [f"follower_based={profile.followers_count}*0.03"]

    multiplier = 1.0
    if profile.is_verified is True:
        multiplier *= 1.2
        signals.append("verified_multiplier=1.2")

    if profile.podcast_urls:
        multiplier *= 1.35
        signals.append("podcast_url_multiplier=1.35")
    elif profile.podcast_signal_score >= 3:
        multiplier *= 1.18
        signals.append(f"inferred_podcast_signal_multiplier=1.18(score={profile.podcast_signal_score})")

    if profile.website:
        multiplier *= 1.05
        signals.append("website_multiplier=1.05")

    if profile.email:
        multiplier *= 1.05
        signals.append("email_multiplier=1.05")

    listeners = int(base * multiplier)
    listeners = max(200, min(listeners, 2_500_000))

    confidence = 0.2
    if profile.followers_count is not None:
        confidence += 0.25
    if profile.is_verified is not None:
        confidence += 0.2
    if profile.podcast_urls:
        confidence += 0.25
    elif profile.podcast_signal_score >= 3:
        confidence += 0.15
    if profile.website:
        confidence += 0.15
    confidence = round(min(confidence, 0.95), 2)

    return ListenerEstimate(
        monthly_listeners=listeners,
        confidence=confidence,
        explanation=", ".join(signals),
    )
