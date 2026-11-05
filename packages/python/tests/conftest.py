from __future__ import annotations

from types import SimpleNamespace

import pytest

from briq_cloud import Briq

from .fake_api import FakeAPI


@pytest.fixture(autouse=True)
def _no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRIQ_API_KEY", raising=False)
    monkeypatch.delenv("BRIQ_API_URL", raising=False)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pauses of the client are not waited for: a pause moves a fake clock forward instead.

    The flows measure their deadlines on that clock, so a timeout test does not depend on how
    fast the machine polls.
    """
    now = [0.0]

    def pause(seconds: float) -> None:
        now[0] += seconds

    async def apause(seconds: float) -> None:
        now[0] += seconds

    monkeypatch.setattr("briq_cloud._flows.time", SimpleNamespace(monotonic=lambda: now[0]))
    monkeypatch.setattr("briq_cloud._sync._pause", pause)
    monkeypatch.setattr("briq_cloud._async._pause", apause)


@pytest.fixture
def api() -> FakeAPI:
    return FakeAPI()


@pytest.fixture
def briq(api: FakeAPI) -> Briq:
    return Briq("test-key", base_url="https://api.example.com", transport=api.transport())
