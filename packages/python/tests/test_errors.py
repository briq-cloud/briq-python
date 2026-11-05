from __future__ import annotations

import pytest

from briq_cloud import BriqError
from briq_cloud._errors import refusal, status_code


@pytest.mark.parametrize(
    ("status", "api_code", "code"),
    [
        (401, None, "auth"),
        (403, "forbidden", "auth"),
        (404, None, "not_found"),
        (402, "payment_required", "quota"),
        (429, None, "quota"),
        (403, "plan_limit", "quota"),
        (409, "invalid_state", "provider"),
        (422, "validation", "invalid_argument"),
        (400, None, "invalid_argument"),
        (502, "provider_error", "unavailable"),
        (503, None, "unavailable"),
    ],
)
def test_status_maps_to_a_stable_code(status: int, api_code: str | None, code: str) -> None:
    assert status_code(status, api_code) == code


def test_a_refusal_carries_what_the_api_said() -> None:
    body = b'{"error":{"code":"quota_exceeded","message":"Too many","details":{"n":1}}}'
    error = refusal(429, {"retry-after": "12", "x-request-id": "req_9"}, body)
    assert (error.code, error.status, error.api_code) == ("quota", 429, "quota_exceeded")
    assert (error.request_id, error.retry_after, error.details) == ("req_9", 12, {"n": 1})
    assert str(error) == "Too many (quota)"


def test_a_page_that_is_not_ours_still_makes_an_error() -> None:
    error = refusal(502, {"x-request-id": "bad id with spaces"}, b"<html>Bad gateway</html>")
    assert (error.code, error.message, error.request_id) == ("unavailable", "HTTP 502", None)


def test_retry_after_is_read_only_on_a_rate_limit() -> None:
    assert refusal(503, {"retry-after": "5"}, b"{}").retry_after is None
    assert refusal(429, {"retry-after": "soon"}, b"{}").retry_after is None


def test_it_is_an_exception_with_the_code() -> None:
    assert isinstance(BriqError("auth", "x"), Exception)
