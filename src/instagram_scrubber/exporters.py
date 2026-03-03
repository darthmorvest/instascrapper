from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path

from .models import LeadRecord

FIELDNAMES = [
    "instagram_handle",
    "instagram_profile_url",
    "is_verified",
    "podcast_urls",
    "podcast_genre",
    "estimated_monthly_listeners",
    "estimate_confidence",
    "lead_score",
    "engagement_comment_count",
    "ai_fit_score",
    "ai_summary",
    "ai_outreach_angle",
    "email",
    "website",
    "source_media_permalink",
    "source_media_share_count",
    "source_comment_id",
    "source_comment_text",
    "source_comment_timestamp",
    "notes",
]

def _record_to_row(rec: LeadRecord) -> dict[str, object]:
    return {
        "instagram_handle": rec.instagram_handle,
        "instagram_profile_url": rec.instagram_profile_url,
        "is_verified": rec.is_verified,
        "podcast_urls": ";".join(rec.podcast_urls),
        "podcast_genre": rec.podcast_genre or "",
        "estimated_monthly_listeners": rec.estimated_monthly_listeners,
        "estimate_confidence": rec.estimate_confidence,
        "lead_score": rec.lead_score,
        "engagement_comment_count": rec.engagement_comment_count,
        "ai_fit_score": rec.ai_fit_score,
        "ai_summary": rec.ai_summary or "",
        "ai_outreach_angle": rec.ai_outreach_angle or "",
        "email": rec.email or "",
        "website": rec.website or "",
        "source_media_permalink": rec.source_media_permalink or "",
        "source_media_share_count": rec.source_media_share_count,
        "source_comment_id": rec.source_comment_id,
        "source_comment_text": rec.source_comment_text,
        "source_comment_timestamp": rec.source_comment_timestamp.isoformat()
        if rec.source_comment_timestamp
        else "",
        "notes": ";".join(rec.notes),
    }


def render_csv(records: list[LeadRecord]) -> str:
    buffer = StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=FIELDNAMES)
    writer.writeheader()
    for rec in records:
        writer.writerow(_record_to_row(rec))
    return buffer.getvalue()


def write_csv_content(csv_content: str, output_path: str) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(csv_content, encoding="utf-8")
    return path


def write_csv(records: list[LeadRecord], output_path: str) -> Path:
    csv_content = render_csv(records)
    return write_csv_content(csv_content, output_path)


def write_csv_to_file_object(records: list[LeadRecord], file_obj) -> None:
    writer = csv.DictWriter(file_obj, fieldnames=FIELDNAMES)
    writer.writeheader()
    for rec in records:
        writer.writerow(_record_to_row(rec))
