from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CommentInteraction:
    media_id: str
    media_permalink: str | None
    media_timestamp: datetime | None
    media_share_count: int | None
    comment_id: str
    comment_text: str
    comment_timestamp: datetime | None
    commenter_ig_id: str | None
    commenter_username: str


@dataclass
class ProfileEnrichment:
    username: str
    ig_user_id: str | None = None
    is_verified: bool | None = None
    biography: str | None = None
    website: str | None = None
    email: str | None = None
    followers_count: int | None = None
    podcast_urls: list[str] = field(default_factory=list)
    podcast_genre: str | None = None
    podcast_signal_score: int = 0
    podcast_signal_sources: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class ListenerEstimate:
    monthly_listeners: int
    confidence: float
    explanation: str


@dataclass
class LeadRecord:
    instagram_handle: str
    instagram_profile_url: str
    is_verified: bool | None
    followers_count: int | None
    podcast_urls: list[str]
    podcast_genre: str | None
    estimated_monthly_listeners: int
    estimate_confidence: float
    email: str | None
    website: str | None
    source_media_permalink: str | None
    source_media_share_count: int | None
    source_comment_id: str
    source_comment_text: str
    source_comment_timestamp: datetime | None
    notes: list[str]
    engagement_comment_count: int = 1
    lead_score: int | None = None
    ai_fit_score: int | None = None
    ai_summary: str | None = None
    ai_outreach_angle: str | None = None
