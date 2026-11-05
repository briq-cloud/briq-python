from __future__ import annotations

import pytest

from briq_cloud import Briq, BriqError
from briq_cloud._config import DEFAULT_URL, resolve


def test_key_and_url_come_from_the_arguments_first(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRIQ_API_KEY", "from-env")
    monkeypatch.setenv("BRIQ_API_URL", "https://env.example.com")
    assert resolve("arg", "https://arg.example.com/") == ("arg", "https://arg.example.com")
    assert resolve(None, None) == ("from-env", "https://env.example.com")


def test_the_url_defaults_to_the_public_api() -> None:
    assert resolve("k", None) == ("k", DEFAULT_URL)


def test_a_missing_key_is_an_auth_error() -> None:
    with pytest.raises(BriqError) as raised:
        Briq()
    assert raised.value.code == "auth"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com",
        "https://user:pass@example.com",
        "https://example.com/v1",
        "https://example.com?x=1",
        "https://example.com#top",
        "example.com",
        "https://",
    ],
)
def test_a_url_that_is_not_an_origin_is_refused(url: str) -> None:
    with pytest.raises(BriqError) as raised:
        resolve("k", url)
    assert raised.value.code == "invalid_argument"


def test_a_local_http_origin_with_a_port_is_fine() -> None:
    assert resolve("k", "http://localhost:3001")[1] == "http://localhost:3001"


def test_a_key_with_a_newline_is_refused() -> None:
    with pytest.raises(BriqError) as raised:
        resolve("k\nx-evil: 1", None)
    assert raised.value.code == "auth"
