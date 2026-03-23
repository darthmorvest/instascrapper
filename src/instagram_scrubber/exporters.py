from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path

from .models import LeadRecord

FIELDNAMES = [
    "instagram_handle",
    "bio_link",
    "comment_text",
]

def _record_to_row(rec: LeadRecord) -> dict[str, object]:
    return {
        "instagram_handle": rec.instagram_handle,
        "bio_link": rec.website or "",
        "comment_text": rec.source_comment_text,
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
