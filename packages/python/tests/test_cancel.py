"""An interrupted call leaves nothing running or billed behind, and says what it could not undo."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from briq_cloud import AsyncBriq, Briq, BriqError, OutputChunk

from .fake_api import Exec, FakeAPI, error

BQ = "/v1/briqs/bq_1"


def booting(api: FakeAPI) -> None:
    """The create answers; the sandbox needs more reads than the test allows."""
    api.create_state, api.boot_polls = "creating", 10_000


# --- sync ---


def test_an_interrupt_while_the_sandbox_boots_destroys_it(briq: Briq, api: FakeAPI) -> None:
    booting(api)
    api.fail["GET " + BQ] = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        briq.create_sandbox()
    assert api.count("DELETE", BQ) == 1 and not api.sandboxes


def test_the_interrupt_is_what_the_caller_gets_when_the_cleanup_fails(
    briq: Briq, api: FakeAPI
) -> None:
    booting(api)
    api.fail["GET " + BQ] = KeyboardInterrupt()
    api.fail["DELETE " + BQ] = error(502, "provider_error")
    with pytest.raises(KeyboardInterrupt):
        briq.create_sandbox()
    assert api.count("DELETE", BQ) == 1


def test_a_failed_cleanup_after_an_error_carries_the_cause_the_id_and_the_key(
    briq: Briq, api: FakeAPI
) -> None:
    booting(api)
    api.fail["DELETE " + BQ] = error(502, "provider_error")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(wait_timeout=0.0, idempotency_key="k-1")
    err = raised.value
    assert err.code == "outcome_unknown"
    assert (err.sandbox_id, err.idempotency_key) == ("bq_1", "k-1")
    assert isinstance(err.__cause__, BriqError) and err.__cause__.code == "timeout"
    assert "bq_1" in str(err) and "destroy" in str(err)


def test_an_interrupted_streamed_exec_is_stopped(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="abcdefgh")

    def stop(chunk: OutputChunk) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        sandbox.exec("x", on_output=stop)
    assert api.count("DELETE", BQ + "/execs/ex_1") == 1


def test_an_interrupt_in_a_read_of_a_streamed_exec_is_stopped(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="abcdefgh")
    api.fail["GET " + BQ + "/execs/ex_1"] = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        sandbox.exec("x", on_output=lambda c: None)
    assert api.count("DELETE", BQ + "/execs/ex_1") == 1


def test_a_failed_stop_does_not_hide_the_interrupt(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="abcdefgh")
    api.fail["GET " + BQ + "/execs/ex_1"] = KeyboardInterrupt()
    api.fail["DELETE " + BQ + "/execs/ex_1"] = httpx.ReadTimeout("t")
    with pytest.raises(KeyboardInterrupt):
        sandbox.exec("x", on_output=lambda c: None)


# --- async ---


class Hanging(httpx.AsyncBaseTransport):
    """The fake API, except that a request matching ``hang`` never answers."""

    def __init__(self, api: FakeAPI, hang: str) -> None:
        self.api, self.hang = api, hang

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if f"{request.method} {request.url.path}" == self.hang:
            await asyncio.sleep(3600)
        return self.api.handle(request)


def test_a_timeout_while_the_sandbox_boots_destroys_it(api: FakeAPI) -> None:
    booting(api)

    async def main() -> None:
        transport = Hanging(api, "GET " + BQ)
        async with AsyncBriq("test-key", base_url="https://a.example", transport=transport) as c:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(c.create_sandbox(), 0.2)

    asyncio.run(main())
    assert api.count("DELETE", BQ) == 1 and not api.sandboxes


def test_a_cancelled_create_survives_a_failed_cleanup(api: FakeAPI) -> None:
    booting(api)
    api.fail["DELETE " + BQ] = error(502, "provider_error")

    async def main() -> None:
        transport = Hanging(api, "GET " + BQ)
        async with AsyncBriq("test-key", base_url="https://a.example", transport=transport) as c:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(c.create_sandbox(), 0.2)

    asyncio.run(main())
    assert api.count("DELETE", BQ) == 1


def test_a_cancelled_streamed_exec_is_stopped(api: FakeAPI) -> None:
    api.on_exec = lambda cmd, body: Exec(stdout="abcdefgh")

    async def main() -> None:
        transport = Hanging(api, "GET " + BQ + "/execs/ex_1")
        async with AsyncBriq("test-key", base_url="https://a.example", transport=transport) as c:
            sandbox = await c.create_sandbox()
            task = asyncio.ensure_future(sandbox.exec("x", on_output=lambda chunk: None))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(main())
    assert api.count("DELETE", BQ + "/execs/ex_1") == 1


def test_a_cleanup_cancelled_again_still_finishes(api: FakeAPI) -> None:
    """A second cancel while the sandbox is being removed does not leave it half removed."""
    booting(api)

    async def main() -> None:
        transport = Hanging(api, "GET " + BQ)
        async with AsyncBriq("test-key", base_url="https://a.example", transport=transport) as c:
            task = asyncio.ensure_future(c.create_sandbox())
            await asyncio.sleep(0.1)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(main())
    assert api.count("DELETE", BQ) == 1 and not api.sandboxes
