"""The async client runs the same flows: these tests cover its own driver and its surface."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx
import pytest

from briq_cloud import AsyncBriq, BriqError, ExecResult, OutputChunk

from .fake_api import Exec, FakeAPI

T = TypeVar("T")
BQ = "/v1/briqs/bq_1"


def run(test: Callable[[AsyncBriq], Awaitable[T]], api: FakeAPI) -> T:
    async def main() -> T:
        async with AsyncBriq(
            "test-key", base_url="https://api.example.com", transport=api.transport()
        ) as client:
            return await test(client)

    return asyncio.run(main())


def test_the_whole_surface(api: FakeAPI) -> None:
    api.create_state, api.boot_polls = "creating", 1
    api.on_exec = lambda cmd, body: Exec(stdout="hi\n", code=1 if "echo hi" in cmd else 0)

    async def main(briq: AsyncBriq) -> None:
        async with await briq.create_sandbox(name="a") as sandbox:
            assert sandbox.info.state == "running"
            assert await sandbox.exec("echo hi") == ExecResult("hi\n", "", 1, False)
            await sandbox.write_file("/work/x", b"\x00\x01")
            assert await sandbox.read_file("/work/x") == b"\x00\x01"
            assert await sandbox.preview_url(80, public=True) == "https://bq_1-80.briq.example/"
            assert (await sandbox.stop()).state == "stopped"
            before_image = len(api.requests)
            info = await sandbox.set_image("ubuntu:24.04")
            assert info is sandbox.info
            assert (info.image, info.state) == ("ubuntu:24.04", "stopped")
            assert len(api.requests) == before_image + 1
            assert api.requests[-1].method == "PATCH"
            assert api.requests[-1].body == {"image": "ubuntu:24.04"}
            assert (await sandbox.start()).state == "running"
            assert (await sandbox.set_sleep("2h")).sleep == "2h"
            assert (await sandbox.refresh()).sleep == "2h"
            again = await briq.sandbox(sandbox.id)
            assert again.id == sandbox.id
            await sandbox.remove("/work/x")
        assert not api.sandboxes

    run(main, api)


def test_output_reaches_a_sync_or_an_async_callback(api: FakeAPI) -> None:
    api.on_exec = lambda cmd, body: Exec(stdout="line one\n", stderr="err\n")
    sync_seen: list[OutputChunk] = []
    async_seen: list[OutputChunk] = []

    async def collect(chunk: OutputChunk) -> None:
        async_seen.append(chunk)

    async def main(briq: AsyncBriq) -> None:
        sandbox = await briq.create_sandbox()
        first = await sandbox.exec("x", on_output=sync_seen.append)
        second = await sandbox.exec("x", on_output=collect)
        assert first == second == ExecResult("line one\n", "err\n", 0, False)

    run(main, api)
    assert sync_seen == async_seen and sync_seen


def test_the_errors_are_the_same(api: FakeAPI) -> None:
    async def main(briq: AsyncBriq) -> None:
        api.fail["POST /v1/briqs"] = httpx.ReadTimeout("slow")
        with pytest.raises(BriqError) as raised:
            await briq.create_sandbox()
        assert raised.value.code == "outcome_unknown"
        assert api.count("POST", "/v1/briqs") == 1
        sandbox = await briq.create_sandbox()
        api.fail["POST " + BQ + "/exec"] = httpx.ReadTimeout("slow")
        with pytest.raises(BriqError) as raised:
            await sandbox.exec("x")
        assert raised.value.code == "outcome_unknown"
        with pytest.raises(BriqError) as raised:
            await sandbox.read_file("/nope")
        assert raised.value.code == "file_not_found"
        with pytest.raises(BriqError) as raised:
            await briq.sandbox("bq_404")
        assert raised.value.code == "not_found"

    run(main, api)


def test_a_failing_callback_stops_the_command(api: FakeAPI) -> None:
    api.on_exec = lambda cmd, body: Exec(stdout="abcdef")

    def boom(chunk: OutputChunk) -> None:
        raise RuntimeError("callback")

    async def main(briq: AsyncBriq) -> None:
        sandbox = await briq.create_sandbox()
        with pytest.raises(RuntimeError, match="callback"):
            await sandbox.exec("x", on_output=boom)

    run(main, api)
    assert api.count("DELETE", BQ + "/execs/ex_1") == 1
