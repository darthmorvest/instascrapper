from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import requests

from .config import Settings
from .models import CommentInteraction


@dataclass
class MediaItem:
    media_id: str
    permalink: str | None
    timestamp: datetime | None


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
        fields = "id,permalink,timestamp"
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

    def list_comments_for_media(self, media_id: str, comments_per_media: int) -> list[dict[str, Any]]:
        fields = "id,text,timestamp,username,from,like_count"
        comments: list[dict[str, Any]] = []
        for comment in self._paginate(
            initial_path=f"{media_id}/comments",
            params={"fields": fields, "limit": min(max(comments_per_media, 1), 100)},
        ):
            comments.append(comment)
            if len(comments) >= comments_per_media:
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
