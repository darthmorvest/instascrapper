from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

from .config import Settings
from .models import CommentInteraction


@dataclass
class MediaItem:
    media_id: str
    permalink: str | None
    timestamp: datetime | None
    media_type: str | None = None
    comments_count: int | None = None
    like_count: int | None = None
    caption: str | None = None


class InstagramGraphClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()

    def _request(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if params is None:
            params = {}
        params = dict(params)
        params["access_token"] = self.settings.access_token
        url = f"{self.settings.base_url}/{path.lstrip('/')}"

        last_error: Exception | None = None
        for attempt in range(self.settings.retry_count + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.settings.timeout_seconds)
                if response.status_code >= 500:
                    raise RuntimeError(f"Instagram API {response.status_code}: {response.text}")
                payload = response.json()
                if "error" in payload:
                    raise RuntimeError(str(payload["error"]))
                return payload
            except Exception as err:  # noqa: BLE001
                last_error = err
                if attempt < self.settings.retry_count:
                    sleep_for = self.settings.retry_backoff_seconds * (attempt + 1)
                    time.sleep(sleep_for)
                    continue
                break
        raise RuntimeError(f"Request failed for {path}: {last_error}")

    def _request_next_page(self, next_url: str) -> dict[str, Any]:
        parsed = urlparse(next_url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if "access_token" not in query:
            query["access_token"] = [self.settings.access_token]
            next_url = urlunparse(
                parsed._replace(
                    query=urlencode(
                        [(k, v) for k, values in query.items() for v in values],
                        doseq=True,
                    )
                )
            )

        last_error: Exception | None = None
        for attempt in range(self.settings.retry_count + 1):
            try:
                response = self.session.get(next_url, timeout=self.settings.timeout_seconds)
                if response.status_code >= 500:
                    raise RuntimeError(f"Instagram API {response.status_code}: {response.text}")
                payload = response.json()
                if "error" in payload:
                    raise RuntimeError(str(payload["error"]))
                return payload
            except Exception as err:  # noqa: BLE001
                last_error = err
                if attempt < self.settings.retry_count:
                    sleep_for = self.settings.retry_backoff_seconds * (attempt + 1)
                    time.sleep(sleep_for)
                    continue
                break
        raise RuntimeError(f"Request failed for pagination URL: {last_error}")

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _paginate(self, initial_path: str, params: dict[str, Any]) -> Iterable[dict[str, Any]]:
        payload = self._request(initial_path, params=params)
        while True:
            for item in payload.get("data", []):
                yield item
            next_url = payload.get("paging", {}).get("next")
            if not next_url:
                break
            payload = self._request_next_page(next_url)

    def list_media(self, media_limit: int, lookback_days: int | None) -> list[MediaItem]:
        fields = "id,permalink,timestamp,media_type,comments_count,like_count,caption"
        items: list[MediaItem] = []
        cutoff: datetime | None = None
        if lookback_days is not None and lookback_days > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        for media in self._paginate(
            initial_path=f"{self.settings.business_account_id}/media",
            params={"fields": fields, "limit": min(max(media_limit, 1), 100)},
        ):
            ts = self._parse_dt(media.get("timestamp"))
            if cutoff and ts and ts < cutoff:
                continue
            items.append(
                MediaItem(
                    media_id=str(media["id"]),
                    permalink=media.get("permalink"),
                    timestamp=ts,
                    media_type=media.get("media_type"),
                    comments_count=(
                        int(media["comments_count"])
                        if media.get("comments_count") is not None
                        else None
                    ),
                    like_count=(
                        int(media["like_count"])
                        if media.get("like_count") is not None
                        else None
                    ),
                    caption=(media.get("caption") or "").strip()[:220] or None,
                )
            )
            if len(items) >= media_limit:
                break
        return items

    def get_media_share_count(self, media_id: str) -> int | None:
        # Share metrics availability depends on media type/account eligibility.
        # This returns None if the metric is unavailable.
        try:
            payload = self._request(f"{media_id}/insights", params={"metric": "shares"})
            data = payload.get("data", [])
            if not data:
                return None
            values = data[0].get("values", [])
            if not values:
                return None
            return int(values[0].get("value", 0))
        except Exception:  # noqa: BLE001
            return None

    def get_media_item(self, media_id: str) -> MediaItem:
        try:
            payload = self._request(
                media_id,
                params={"fields": "id,permalink,timestamp,media_type,comments_count,like_count,caption"},
            )
        except Exception:
            return MediaItem(media_id=str(media_id), permalink=None, timestamp=None)
        return MediaItem(
            media_id=str(payload.get("id") or media_id),
            permalink=payload.get("permalink"),
            timestamp=self._parse_dt(payload.get("timestamp")),
            media_type=payload.get("media_type"),
            comments_count=(
                int(payload["comments_count"])
                if payload.get("comments_count") is not None
                else None
            ),
            like_count=(
                int(payload["like_count"])
                if payload.get("like_count") is not None
                else None
            ),
            caption=(payload.get("caption") or "").strip()[:220] or None,
        )

    def list_comments_page(
        self,
        media_id: str,
        *,
        page_limit: int = 100,
        next_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        fields = "id,text,timestamp,username,from,like_count"
        if next_url:
            payload = self._request_next_page(next_url)
        else:
            payload = self._request(
                f"{media_id}/comments",
                params={"fields": fields, "limit": min(max(page_limit, 1), 100)},
            )
        comments = list(payload.get("data", []))
        next_cursor = payload.get("paging", {}).get("next")
        return (comments, next_cursor)

    def list_comments_for_media(self, media_id: str, comments_per_media: int) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        next_url: str | None = None
        all_comments = comments_per_media <= 0
        while True:
            page, next_url = self.list_comments_page(
                media_id,
                page_limit=100,
                next_url=next_url,
            )
            for comment in page:
                comments.append(comment)
                if not all_comments and len(comments) >= comments_per_media:
                    return comments
            if not next_url:
                break
        return comments

    def collect_comment_interactions(
        self,
        media_limit: int,
        comments_per_media: int,
        lookback_days: int | None,
    ) -> list[CommentInteraction]:
        interactions: list[CommentInteraction] = []
        media_items = self.list_media(media_limit=media_limit, lookback_days=lookback_days)
        for media in media_items:
            share_count = self.get_media_share_count(media.media_id)
            comments = self.list_comments_for_media(media.media_id, comments_per_media=comments_per_media)
            for comment in comments:
                commenter_username = (
                    comment.get("username")
                    or comment.get("from", {}).get("username")
                    or ""
                ).strip()
                if not commenter_username:
                    continue
                interactions.append(
                    CommentInteraction(
                        media_id=media.media_id,
                        media_permalink=media.permalink,
                        media_timestamp=media.timestamp,
                        media_share_count=share_count,
                        comment_id=str(comment.get("id", "")),
                        comment_text=(comment.get("text") or "").strip(),
                        comment_timestamp=self._parse_dt(comment.get("timestamp")),
                        commenter_ig_id=(comment.get("from", {}) or {}).get("id"),
                        commenter_username=commenter_username,
                    )
                )
        return interactions

    def business_discovery(self, username: str) -> dict[str, Any] | None:
        # Only works for discoverable professional accounts via business discovery.
        fields = (
            "business_discovery.username("
            f"{username}"
            "){id,username,name,biography,website,is_verified,followers_count}"
        )
        try:
            payload = self._request(
                path=self.settings.business_account_id,
                params={"fields": fields},
            )
        except Exception:
            return None
        return payload.get("business_discovery")
