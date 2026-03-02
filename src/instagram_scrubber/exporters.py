from __future__ import annotations

import csv
from pathlib import Path

from .models import LeadRecord


def write_csv(records: list[LeadRecord], output_path: str) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "instagram_handle",
                "instagram_profile_url",
                "is_verified",
                "podcast_urls",
                "estimated_monthly_listeners",
                "estimate_confidence",
                "email",
                "website",
                "source_media_permalink",
                "source_media_share_count",
                "source_comment_id",
                "source_comment_text",
                "source_comment_timestamp",
                "notes",
            ],
        )
        writer.writeheader()
        for rec in records:
            writer.writerow(
                {
                    "instagram_handle": rec.instagram_handle,
                    "instagram_profile_url": rec.instagram_profile_url,
                    "is_verified": rec.is_verified,
                    "podcast_urls": ";".join(rec.podcast_urls),
                    "estimated_monthly_listeners": rec.estimated_monthly_listeners,
                    "estimate_confidence": rec.estimate_confidence,
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
            )

    return path

