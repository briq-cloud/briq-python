from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from briq_cloud import AsyncBriq, Briq, BriqError, ExecutionMode

from .fake_api import FakeAPI, error

PATH = "/v1/briqs/bq_1"


def access(**changes: Any) -> dict[str, Any]:
    return {
        "version": 1,
        "briqId": "bq_1",
        "region": "north-america-east",
        "origin": "https://commands.example.com",
        "operations": ["exec"],
        "transports": ["http"],
        "token": "scoped-execution-token",
        "expiresAt": (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
        "routeGeneration": "run-1",
        **changes,
    }


def result() -> dict[str, Any]:
    return {
        "stdout": "regional\n",
        "stderr": "",
        "exitCode": 0,
        "timedOut": False,
        "truncated": False,
        "costLine": "cost snapshot",
    }


def unexpected(_: httpx.Request) -> httpx.Response:
    raise AssertionError("regional command must not be sent")


def configure(api: FakeAPI, descriptor: dict[str, Any] | None = None) -> None:
    api.fail["POST " + PATH + "/access"] = httpx.Response(200, json=descriptor or access())


def test_central_default_does_not_discover(api: FakeAPI) -> None:
    configure(api)
    with Briq("test-key", transport=api.transport()) as client:
        client.create_sandbox().exec("true")
    assert api.count("POST", PATH + "/access") == 0
    assert api.count("POST", PATH + "/exec") == 1


@pytest.mark.parametrize("mode", ["auto", "regional"])
def test_separate_credentials_and_fresh_admission_per_command(
    api: FakeAPI, mode: ExecutionMode
) -> None:
    configure(api)
    sent: list[httpx.Request] = []

    def regional(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json=result())

    with (
        httpx.Client(
            transport=api.transport(),
            headers={"x-control-secret": "private"},
            cookies={"control": "private"},
        ) as control,
        httpx.Client(
            transport=httpx.MockTransport(regional),
            headers={"x-execution-default": "private"},
            cookies={"session": "private"},
            auth=("execution", "private"),
        ) as execution,
        Briq(
            "test-key",
            http_client=control,
            execution_http_client=execution,
            execution_mode=mode,
        ) as client,
    ):
        sandbox = client.create_sandbox()
        assert sandbox.exec(["echo", "--execution", "regional"]).stdout == "regional\n"
        configure(api)
        assert sandbox.exec("true").stdout == "regional\n"
        assert not control.is_closed and not execution.is_closed
    assert api.count("POST", PATH + "/access") == 2
    assert api.count("POST", PATH + "/exec") == 0
    assert len(sent) == 2
    request = sent[0]
    assert str(request.url) == "https://commands.example.com" + PATH + "/exec"
    assert request.headers["authorization"] == "Bearer scoped-execution-token"
    assert "cookie" not in request.headers
    assert "x-control-secret" not in request.headers
    assert "x-execution-default" not in request.headers
    assert json.loads(request.content)["cmd"] == ["echo", "--execution", "regional"]


@pytest.mark.parametrize("status", [404, 405, 501, 503])
def test_auto_falls_back_only_during_unsupported_discovery(api: FakeAPI, status: int) -> None:
    api.fail["POST " + PATH + "/access"] = error(status, "provider_error")
    with Briq(
        "test-key",
        transport=api.transport(),
        execution_transport=httpx.MockTransport(unexpected),
        execution_mode="auto",
    ) as client:
        client.create_sandbox().exec("true")
    assert api.count("POST", PATH + "/exec") == 1


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_auto_does_not_hide_discovery_refusal(api: FakeAPI, status: int) -> None:
    api.fail["POST " + PATH + "/access"] = error(status, "unauthorized")
    with Briq(
        "test-key",
        transport=api.transport(),
        execution_transport=httpx.MockTransport(unexpected),
        execution_mode="auto",
    ) as client:
        sandbox = client.create_sandbox()
        with pytest.raises(BriqError):
            sandbox.exec("true")
    assert api.count("POST", PATH + "/exec") == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"origin": "http://commands.example.com"},
        {"origin": "https://user:pass@commands.example.com"},
        {"origin": "https://commands.example.com/path"},
        {"briqId": "bq_other"},
        {"version": True},
        {"operations": ["exec", "shell"]},
        {"transports": ["websocket"]},
        {"token": "bad\nheader"},
        {"token": "x" * 16_385},
        {"expiresAt": "2020-01-01T00:00:00Z"},
        {"expiresAt": "2999-01-01T00:00:00Z"},
        {"expiresAt": "2026-11-09T12:00:00"},
    ],
)
def test_invalid_access_never_dispatches_or_falls_back(
    api: FakeAPI, changes: dict[str, Any]
) -> None:
    configure(api, access(**changes))
    with (
        Briq(
            "test-key",
            transport=api.transport(),
            execution_transport=httpx.MockTransport(unexpected),
            execution_mode="auto",
        ) as client,
        pytest.raises(BriqError) as raised,
    ):
        client.create_sandbox().exec("true")
    assert raised.value.code == "provider"
    assert api.count("POST", PATH + "/exec") == 0


@pytest.mark.parametrize("failure", ["connect", "timeout", "redirect", "refused", "malformed"])
def test_uncertain_regional_command_is_sent_once_without_central_fallback(
    api: FakeAPI, failure: str
) -> None:
    configure(api)
    calls = []

    def regional(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if failure == "connect":
            raise httpx.ConnectError("unreachable")
        if failure == "timeout":
            raise httpx.ReadTimeout("lost reply")
        if failure == "redirect":
            return httpx.Response(307, headers={"location": "https://elsewhere.example.com"})
        if failure == "refused":
            return error(401, "unauthorized")
        return httpx.Response(200, json={"stdout": "incomplete"})

    with (
        Briq(
            "test-key",
            transport=api.transport(),
            execution_transport=httpx.MockTransport(regional),
            execution_mode="auto",
        ) as client,
        pytest.raises(BriqError) as raised,
    ):
        client.create_sandbox().exec("true")
    assert raised.value.code == "outcome_unknown"
    assert len(calls) == 1
    assert api.count("POST", PATH + "/exec") == 0


def test_custom_control_requires_separate_transport(api: FakeAPI) -> None:
    with Briq("test-key", transport=api.transport(), execution_mode="auto") as client:
        client.create_sandbox().exec("true")
    assert api.count("POST", PATH + "/access") == 0
    with (
        Briq("test-key", transport=api.transport(), execution_mode="regional") as client,
        pytest.raises(BriqError) as raised,
    ):
        client.sandbox("bq_1").exec("true")
    assert raised.value.code == "unavailable"


def test_regional_async_refuses_before_discovery(api: FakeAPI) -> None:
    with (
        Briq(
            "test-key",
            transport=api.transport(),
            execution_transport=httpx.MockTransport(unexpected),
            execution_mode="regional",
        ) as client,
        pytest.raises(BriqError) as raised,
    ):
        client.create_sandbox().exec("true", timeout=61)
    assert raised.value.code == "unsupported"
    assert api.count("POST", PATH + "/access") == 0
    assert api.count("POST", PATH + "/exec") == 0


def test_async_client_uses_separate_transport_and_never_replays(api: FakeAPI) -> None:
    configure(api)
    sent: list[httpx.Request] = []

    def regional(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        if len(sent) == 2:
            raise httpx.ReadTimeout("lost")
        return httpx.Response(200, json=result())

    async def main() -> None:
        async with AsyncBriq(
            "test-key",
            transport=api.transport(),
            execution_transport=httpx.MockTransport(regional),
            execution_mode="auto",
        ) as client:
            sandbox = await client.create_sandbox()
            assert (await sandbox.exec("true")).stdout == "regional\n"
            configure(api)
            with pytest.raises(BriqError) as raised:
                await sandbox.exec("true")
            assert raised.value.code == "outcome_unknown"

    asyncio.run(main())
    assert len(sent) == 2
    assert all(r.headers["authorization"] == "Bearer scoped-execution-token" for r in sent)
    assert api.count("POST", PATH + "/exec") == 0


@pytest.mark.parametrize("kind", ["output", "stderr", "missing_cost"])
def test_invalid_regional_result_is_unknown(api: FakeAPI, kind: str) -> None:
    configure(api)
    payload = result()
    if kind == "output":
        payload["stdout"] = "x" * (12 * 1024 * 1024 + 1)
        payload["stderr"] = "x"
    elif kind == "stderr":
        payload["stderr"] = "x" * (12 * 1024 * 1024 + 1)
    else:
        del payload["costLine"]
    with (
        Briq(
            "test-key",
            transport=api.transport(),
            execution_mode="regional",
            execution_transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload)),
        ) as client,
        pytest.raises(BriqError) as raised,
    ):
        client.create_sandbox().exec("true")
    assert raised.value.code == "outcome_unknown"
    assert api.count("POST", PATH + "/exec") == 0


@pytest.mark.parametrize("asynchronous", [False, True])
def test_regional_capture_and_transport_envelope(api: FakeAPI, asynchronous: bool) -> None:
    configure(api)
    payload = result()
    # Below the per-stream capture limit, but JSON escaping exceeds the old 8 MiB wire cap.
    payload["stdout"] = "\0" * (2 * 1024 * 1024)
    payload["stderr"] = "e" * (2 * 1024 * 1024)
    sent: list[httpx.Request] = []

    def regional(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        assert request.extensions["timeout"]["read"] == 95
        return httpx.Response(200, json=payload)

    async def run_async() -> None:
        async with AsyncBriq(
            "test-key",
            transport=api.transport(),
            execution_mode="regional",
            execution_transport=httpx.MockTransport(regional),
        ) as client:
            sandbox = await client.create_sandbox()
            answer = await sandbox.exec("true", timeout=60)
            assert answer.stdout == payload["stdout"]
            assert answer.stderr == payload["stderr"]

    if asynchronous:
        asyncio.run(run_async())
    else:
        with Briq(
            "test-key",
            transport=api.transport(),
            execution_mode="regional",
            execution_transport=httpx.MockTransport(regional),
        ) as client:
            answer = client.create_sandbox().exec("true", timeout=60)
            assert answer.stdout == payload["stdout"]
            assert answer.stderr == payload["stderr"]
    assert len(sent) == 1
    assert api.count("POST", PATH + "/exec") == 0


def test_auto_long_command_keeps_async_central_path(api: FakeAPI) -> None:
    with Briq(
        "test-key",
        transport=api.transport(),
        execution_mode="auto",
        execution_transport=httpx.MockTransport(unexpected),
    ) as client:
        assert client.create_sandbox().exec("true", timeout=61).exit_code == 0
    assert api.count("POST", PATH + "/access") == 0


def test_invalid_mode_refuses_before_transport_creation() -> None:
    with pytest.raises(BriqError) as raised:
        Briq("test-key", execution_mode="invalid")  # type: ignore[arg-type]
    assert raised.value.code == "invalid_argument"
