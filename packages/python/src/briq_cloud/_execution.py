"""Execution access is short lived and used for exactly one command."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from ._config import origin_of
from ._errors import BriqError

ExecutionMode = Literal["central", "auto", "regional"]


def mode_of(mode: ExecutionMode) -> ExecutionMode:
    if mode not in ("central", "auto", "regional"):
        raise BriqError("invalid_argument", "execution_mode must be central, auto or regional")
    return mode


@dataclass(frozen=True, slots=True)
class Access:
    origin: str
    token: str
    expires_at: float

    def check_expiry(self) -> None:
        if self.expires_at <= datetime.now(timezone.utc).timestamp() + 1:
            raise BriqError("unavailable", "Regional execution access has expired")


def parse_access(value: dict[str, Any], id: str) -> Access:
    try:
        origin = value["origin"]
        token = value["token"]
        expiry = value["expiresAt"]
        if not isinstance(expiry, str):
            raise ValueError
        date = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        if date.tzinfo is None:
            raise ValueError
        expires_at = date.timestamp()
        now = datetime.now(timezone.utc).timestamp()
        if (
            type(value["version"]) is not int
            or value["version"] != 1
            or value["briqId"] != id
            or not isinstance(value["region"], str)
            or not value["region"]
            or not isinstance(origin, str)
            or not origin.startswith("https://")
            or origin_of(origin) != origin
            or value["operations"] != ["exec"]
            or value["transports"] != ["http"]
            or not isinstance(token, str)
            or not token
            or len(token) > 16_384
            or any(ord(c) < 33 or ord(c) > 126 for c in token)
            or not now + 1 < expires_at <= now + 65
            or not isinstance(value["routeGeneration"], str)
            or not value["routeGeneration"]
        ):
            raise ValueError
    except (ValueError, KeyError, TypeError, OverflowError, BriqError):
        raise BriqError("provider", "The API returned invalid execution access") from None
    return Access(origin, token, expires_at)
