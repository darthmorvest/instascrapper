from __future__ import annotations

import argparse
import sys


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build verified podcast lead list from Instagram comments."
    )
    parser.add_argument("--output", default="leads.csv", help="Output CSV path")
    parser.add_argument("--media-limit", type=int, default=25, help="Max source media to scan")
    parser.add_argument(
        "--comments-per-media",
        type=int,
        default=200,
        help="Max comments to fetch per media",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        help="Only include media newer than this many days",
    )
    parser.add_argument(
        "--max-profiles",
        type=int,
        default=None,
        help="Optional cap on number of unique commenters to enrich",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    from .config import load_settings
    from .exporters import write_csv
    from .instagram_api import InstagramGraphClient
    from .pipeline import build_leads

    settings = load_settings()
    client = InstagramGraphClient(settings)

    records = build_leads(
        client=client,
        media_limit=args.media_limit,
        comments_per_media=args.comments_per_media,
        lookback_days=args.lookback_days,
        max_profiles=args.max_profiles,
    )
    output = write_csv(records, args.output)

    print(f"Leads found: {len(records)}")
    print(f"CSV written: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
