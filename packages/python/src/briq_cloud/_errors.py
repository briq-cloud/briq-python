"""The one exception the client raises, and how an HTTP answer becomes one."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any, Literal

#: What went wrong, as a stable word. The same words as the TypeScript client's.
#:
#: - ``auth``: no key, a wrong key, or a key that may not do this.
#: - ``not_found``: the sandbox does not exist (or is not yours).
#: - ``file_not_found``: the file or directory does not exist.
#: - ``quota``: a limit stopped it: credits, plan, spend cap, rate limit, size.
#: - ``unavailable``: briq could not take the call; it was not applied. Safe to try again.
#: - ``invalid_argument``: the call is wrong; trying again changes nothing.
#: - ``provider``: briq answered with a refusal that fits none of the others.
#: - ``outcome_unknown``: the call was sent and its result is not known (a command may have run,
#:   a sandbox may exist). The client never repeats such a call itself.
#: - ``timeout``: a deadline of the client ended the call.
#: - ``unsupported``: this API or image cannot do what was asked.
#: - ``aborted``: kept for parity with the TypeScript client, which cancels with an abort signal.
Code = Literal[
    "auth",
    "not_found",
    "file_not_found",
    "quota",
    "unavailable",
    "invalid_argument",
    "provider",
    "outcome_unknown",
    "timeout",
    "unsupported",
    "aborted",
]


class BriqError(Exception):
    """A call to briq failed. ``code`` says how; the other fields say what briq answered."""

    def __init__(
        self,
        code: Code,
        message: str,
        *,
        status: int | None = None,
        api_code: str | None = None,
        request_id: str | None = None,
        retry_after: int | None = None,
        details: Any = None,
        idempotency_key: str | None = None,
        sandbox_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code: Code = code
        self.message = message
        #: The HTTP status, when briq answered.
        self.status = status
        #: The API's own error code (``payment_required``, ``plan_limit``, ``invalid_state``...).
        self.api_code = api_code
        #: The API's id for the failed request: quote it when asking for help.
        self.request_id = request_id
        #: On a rate limit (429): the seconds until the window ends.
        self.retry_after = retry_after
        #: What the API said about the limit or the operation, as codes, ids and numbers only
        #: (``{"limit": "size", "plan": "hobby"}``, ``{"op": "exec"}``). The text a provider
        #: gave is never kept: it is not for a caller's logs.
        self.details = details
        #: On a create whose result is not known: the key it was sent with. Pass it as
        #: ``idempotency_key`` to repeat the call; the API then returns the sandbox it made.
        self.idempotency_key = idempotency_key
        #: The sandbox the error is about, when it is known (a create that could not clean up).
        self.sandbox_id = sandbox_id

    def __str__(self) -> str:
        return f"{self.message} ({self.code})"


def status_code(status: int, api_code: str | None = None) -> Code:
    """The code for an HTTP status, as the TypeScript client maps it."""
    if status in (402, 429, 507, 413) or api_code == "plan_limit":
        return "quota"
    if status in (401, 403):
        return "auth"
    if status == 404:
        return "not_found"
    if status >= 500:
        return "unavailable"
    if status in (400, 422):
        return "invalid_argument"
    return "provider"


_ID = re.compile(r"^[\w.-]{1,64}$")
#: A value of ``details`` worth keeping: a code, an id, a plan or an operation name.
_WORD = re.compile(r"^[\w.:-]{1,64}$")


def request_id_of(value: object) -> str | None:
    """A request id worth repeating in an error: the API's own, shaped as ours."""
    return value if isinstance(value, str) and _ID.match(value) else None


def safe_details(details: Any) -> Any:
    """The codes, ids and numbers of an error's ``details``; the text of a provider is dropped."""
    if not isinstance(details, dict):
        return None
    kept = {
        key: value
        for key, value in details.items()
        if isinstance(key, str)
        and _WORD.match(key)
        and key != "providerSaid"
        and (
            isinstance(value, (bool, int, float))
            or (isinstance(value, str) and _WORD.match(value) is not None)
        )
    }
    return kept or None


def envelope(content: bytes) -> dict[str, Any]:
    """The ``error`` object of an error answer; empty when the body is not ours (a proxy's page)."""
    try:
        error = json.loads(content).get("error")
    except (ValueError, AttributeError):
        return {}
    return error if isinstance(error, dict) else {}


def refusal(
    status: int,
    headers: Mapping[str, str],
    content: bytes,
    code: Code | None = None,
) -> BriqError:
    """An error answer as a ``BriqError``: ``code`` by status unless the caller knows better."""
    error = envelope(content)
    said_code, said = error.get("code"), error.get("message")
    api_code = said_code if isinstance(said_code, str) else None
    message = said if isinstance(said, str) else f"HTTP {status}"
    retry = headers.get("retry-after", "")
    return BriqError(
        code or status_code(status, api_code),
        message,
        status=status,
        api_code=api_code,
        request_id=request_id_of(headers.get("x-request-id"))
        or request_id_of(error.get("requestId")),
        retry_after=int(retry) if status == 429 and re.fullmatch(r"\d{1,4}", retry) else None,
        details=safe_details(error.get("details")),
    )
