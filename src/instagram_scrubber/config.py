from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    access_token: str
    business_account_id: str
    graph_version: str = "v21.0"
    timeout_seconds: int = 25
    retry_count: int = 3
    retry_backoff_seconds: float = 1.5

    @property
    def base_url(self) -> str:
        return f"https://graph.facebook.com/{self.graph_version}"


def build_settings(
    *,
    access_token: str,
    business_account_id: str,
    graph_version: str = "v21.0",
    timeout_seconds: int = 25,
    retry_count: int = 3,
    retry_backoff_seconds: float = 1.5,
) -> Settings:
    access_token_clean = access_token.strip()
    business_id_clean = business_account_id.strip()
    if not access_token_clean:
        raise ValueError("Missing required setting: access_token")
    if not business_id_clean:
        raise ValueError("Missing required setting: business_account_id")

    return Settings(
        access_token=access_token_clean,
        business_account_id=business_id_clean,
        graph_version=graph_version.strip() or "v21.0",
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
        retry_backoff_seconds=retry_backoff_seconds,
    )


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> Settings:
    load_dotenv()
    return build_settings(
        access_token=_required("IG_ACCESS_TOKEN"),
        business_account_id=_required("IG_BUSINESS_ACCOUNT_ID"),
        graph_version=os.getenv("IG_GRAPH_VERSION", "v21.0").strip() or "v21.0",
        timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "25")),
        retry_count=int(os.getenv("REQUEST_RETRY_COUNT", "3")),
        retry_backoff_seconds=float(os.getenv("REQUEST_RETRY_BACKOFF_SECONDS", "1.5")),
    )
