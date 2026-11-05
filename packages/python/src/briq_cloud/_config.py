"""The key and the API origin: arguments first, then the environment, then the public API."""

from __future__ import annotations

import os
from urllib.parse import urlsplit

import httpx

from ._errors import BriqError
from ._version import __version__

DEFAULT_URL = "https://api.briq.cloud"
USER_AGENT = f"briq-cloud-python/{__version__}"


def resolve(api_key: str | None, base_url: str | None) -> tuple[str, str]:
    """The key and the origin to use. A bad one fails here, before anything is sent."""
    key = api_key or os.environ.get("BRIQ_API_KEY") or ""
    if not key:
        raise BriqError("auth", "A briq API key is required: pass api_key or set BRIQ_API_KEY")
    if any(c.isspace() or ord(c) < 32 or ord(c) > 126 for c in key):
        raise BriqError("auth", "The API key holds a character a key never has")
    return key, origin_of(base_url or os.environ.get("BRIQ_API_URL") or DEFAULT_URL)


def origin_of(url: str) -> str:
    """``scheme://host[:port]`` of an API URL. It is nothing more: the client builds the paths."""
    try:
        parts = urlsplit(url)
        valid = (
            parts.scheme in ("http", "https")
            and bool(parts.hostname)
            and not parts.username
            and not parts.password
            and not parts.query
            and not parts.fragment
            and parts.path in ("", "/")
            and parts.port != 0
        )
    except ValueError:
        valid = False
    if not valid:
        raise BriqError("invalid_argument", "The API URL must be an HTTP(S) origin")
    return f"{parts.scheme}://{parts.netloc}"


def headers(key: str, extra: dict[str, str]) -> dict[str, str]:
    return {
        "authorization": f"Bearer {key}",
        "user-agent": USER_AGENT,
        "accept": "application/json, */*",
        **extra,
    }


def timeout_of(seconds: float) -> httpx.Timeout:
    """A deadline for each step of a request; connecting is never given more than ten seconds."""
    return httpx.Timeout(seconds, connect=min(seconds, 10.0))
