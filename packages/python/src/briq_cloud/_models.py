"""What the client returns: small frozen dataclasses, read from the API's JSON by hand."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from ._errors import BriqError


@dataclass(frozen=True, slots=True)
class OutputChunk:
    """Output of a command, as it arrived."""

    stream: Literal["stdout", "stderr"]
    text: str


@dataclass(frozen=True, slots=True)
class ExecResult:
    """How a command ended. ``stdout`` and ``stderr`` are text; read bytes with ``read_file``."""

    stdout: str
    stderr: str
    #: The command's exit status; ``None`` when it was stopped at its timeout.
    exit_code: int | None
    timed_out: bool = False


@dataclass(frozen=True, slots=True)
class FileInfo:
    """One entry of a directory."""

    path: str
    type: Literal["file", "directory", "symlink"]
    size: int
    modified_at: datetime


@dataclass(frozen=True, slots=True)
class SandboxInfo:
    """What briq knows about a sandbox at one moment."""

    id: str
    name: str
    image: str
    size: str
    #: ``creating``, ``starting``, ``running``, ``sleeping``, ``stopping``, ``stopped``,
    #: ``resizing``, ``moving``, ``destroying``, ``destroyed`` or ``error``.
    state: str
    #: When it goes to sleep without use: ``"30m"``, ``"3h"``, ``"7d"`` or ``"never"``.
    sleep: str
    created_at: datetime
    sleeps_at: datetime | None
    cost_cents_so_far: float
    #: Why it is in ``error``, or what its last start said.
    error: str | None
    #: The team the sandbox belongs to.
    team_id: str | None = None


def parse_time(value: Any) -> datetime:
    """An ISO 8601 time as the API writes it (``…Z``); Python 3.10 reads no ``Z``."""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise BriqError("provider", "briq sent a time that is not ISO 8601") from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_sandbox(row: Any) -> SandboxInfo:
    """A briq of the API as a ``SandboxInfo``."""
    try:
        return SandboxInfo(
            id=str(row["id"]),
            name=str(row["name"]),
            image=str(row["image"]),
            size=str(row["size"]),
            state=str(row["state"]),
            sleep=str(row["sleep"]),
            created_at=parse_time(row["createdAt"]),
            sleeps_at=parse_time(row["sleepsAt"]) if row.get("sleepsAt") else None,
            cost_cents_so_far=float(row["costCentsSoFar"]),
            error=row.get("error"),
            team_id=row["teamId"] if isinstance(row.get("teamId"), str) else None,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        raise BriqError("provider", "briq sent a sandbox the client cannot read") from None
