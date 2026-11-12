from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from briq_cloud import Briq, BriqError, ExecResult, OutputChunk

from .fake_api import Exec, FakeAPI, error

BQ = r"/v1/briqs/bq_1"


def code_of(raised: pytest.ExceptionInfo[BriqError]) -> str:
    return raised.value.code


# --- create ---


def test_create_sends_one_post_with_an_idempotency_key(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox(name="work", size="small", sleep="1h", env={"A": "1"})
    post = next(r for r in api.requests if r.method == "POST")
    assert post.path == "/v1/briqs"
    assert post.body["recipe"] == "sandbox"
    assert (post.body["name"], post.body["size"], post.body["sleep"]) == ("work", "small", "1h")
    assert post.body["env"] == {"A": "1"}
    assert post.headers["idempotency-key"]
    assert post.headers["authorization"] == "Bearer test-key"
    assert sandbox.id == "bq_1" and sandbox.info.state == "running"
    assert sandbox.info.sleeps_at is None and sandbox.info.created_at.year == 2026


def test_create_waits_until_it_runs(briq: Briq, api: FakeAPI) -> None:
    api.create_state, api.boot_polls = "creating", 2
    assert briq.create_sandbox().info.state == "running"
    assert api.count("GET", BQ) == 3


def test_create_that_boots_into_error_destroys_and_says_why(briq: Briq, api: FakeAPI) -> None:
    api.create_state = "error"
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox()
    assert code_of(raised) == "unavailable"
    assert api.count("DELETE", BQ) == 1 and not api.sandboxes


def test_create_that_never_boots_times_out_and_cleans_up(briq: Briq, api: FakeAPI) -> None:
    api.create_state, api.boot_polls = "creating", 10_000
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(wait_timeout=0.0)
    assert code_of(raised) == "timeout"
    assert api.count("DELETE", BQ) == 1


def test_create_is_never_replayed_when_the_answer_is_lost(briq: Briq, api: FakeAPI) -> None:
    api.fail["POST /v1/briqs"] = httpx.ReadTimeout("slow")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(idempotency_key="k1")
    assert code_of(raised) == "outcome_unknown"
    assert api.count("POST", "/v1/briqs") == 1


def test_a_second_create_with_the_same_key_returns_the_same_sandbox(
    briq: Briq, api: FakeAPI
) -> None:
    assert (
        briq.create_sandbox(idempotency_key="k1").id == briq.create_sandbox(idempotency_key="k1").id
    )
    assert len(api.sandboxes) == 1


def test_create_that_cannot_connect_is_unavailable(briq: Briq, api: FakeAPI) -> None:
    api.fail["POST /v1/briqs"] = httpx.ConnectError("refused")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox()
    assert code_of(raised) == "unavailable"


@pytest.mark.parametrize(("status", "api_code", "code"), [
    (500, "provider_error", "outcome_unknown"),
    (503, "provider_error", "outcome_unknown"),
    (409, "idempotency_in_progress", "outcome_unknown"),
    (402, "payment_required", "quota"),
    (403, "plan_limit", "quota"),
    (401, "unauthorized", "auth"),
    (422, "validation", "invalid_argument"),
])  # fmt: skip
def test_create_refusals(briq: Briq, api: FakeAPI, status: int, api_code: str, code: str) -> None:
    api.fail["POST /v1/briqs"] = error(status, api_code)
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox()
    assert code_of(raised) == code
    assert raised.value.api_code == api_code and raised.value.request_id == "req_1"


@pytest.mark.parametrize("body", [b"not json", b'{"briqs":[]}', b'{"briqs":[{"id":"x"}]}'])
def test_create_with_an_unusable_answer_is_outcome_unknown(
    briq: Briq, api: FakeAPI, body: bytes
) -> None:
    api.fail["POST /v1/briqs"] = httpx.Response(201, content=body)
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox()
    assert code_of(raised) == "outcome_unknown"


def test_secret_env_is_sent_literal_and_write_only(briq: Briq, api: FakeAPI) -> None:
    briq.create_sandbox(env={"A": "1"}, secret_env={"TOKEN": "s3cret"})
    env = next(r for r in api.requests if r.method == "POST").body["env"]
    assert env["A"] == "1"
    assert env["TOKEN"]["value"] == "s3cret" and env["TOKEN"]["literal"] is True
    assert env["TOKEN"]["secret"].startswith("SANDBOX_")


def test_secret_env_needs_an_api_that_keeps_it_write_only(briq: Briq, api: FakeAPI) -> None:
    api.literal_secrets = False
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(secret_env={"TOKEN": "x"})
    assert code_of(raised) == "unsupported"
    assert api.count("POST", "/v1/briqs") == 0


def test_a_variable_in_both_env_and_secret_env_is_refused(briq: Briq) -> None:
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(env={"A": "1"}, secret_env={"A": "2"})
    assert code_of(raised) == "invalid_argument"


def test_reconnect_by_id(briq: Briq, api: FakeAPI) -> None:
    first = briq.create_sandbox()
    again = briq.sandbox(first.id)
    assert again.id == first.id
    with pytest.raises(BriqError) as raised:
        briq.sandbox("bq_404")
    assert code_of(raised) == "not_found"


def test_an_id_cannot_reach_another_route(briq: Briq, api: FakeAPI) -> None:
    with pytest.raises(BriqError):
        briq.sandbox("bq_1/../../quota")
    assert api.requests[-1].path == "/v1/briqs/bq_1%2F..%2F..%2Fquota"
    with pytest.raises(BriqError) as raised:
        briq.sandbox("..")
    assert code_of(raised) == "invalid_argument"


def test_a_wrong_key_is_an_auth_error(api: FakeAPI) -> None:
    client = Briq("wrong", base_url="https://api.example.com", transport=api.transport())
    with pytest.raises(BriqError) as raised:
        client.create_sandbox()
    assert code_of(raised) == "auth"


def test_a_redirect_is_not_followed(briq: Briq, api: FakeAPI) -> None:
    api.fail["GET /v1/briqs/bq_1"] = httpx.Response(
        302, headers={"location": "https://evil.example"}
    )
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.refresh()
    assert code_of(raised) == "provider"
    assert all(r.headers.get("host") != "evil.example" for r in api.requests)


# --- exec over one request ---


def test_exec_runs_a_string_in_a_shell(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="hi\n", stderr="warn\n", code=3)
    result = sandbox.exec("echo hi && false", stdin="in", env={"A": "1"}, timeout=5)
    assert result == ExecResult("hi\n", "warn\n", 3, False)
    body = next(r for r in api.requests if r.path.endswith("/exec")).body
    assert body == {
        "cmd": ["sh", "-c", "echo hi && false"],
        "timeoutMs": 5000,
        "stdin": "in",
        "env": {"A": "1"},
    }


def test_exec_runs_an_argv_as_it_is(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.exec(["ls", "-la", "a b"])
    assert next(r for r in api.requests if r.path.endswith("/exec")).body["cmd"] == [
        "ls",
        "-la",
        "a b",
    ]


def test_exec_in_a_directory_passes_the_directory_apart_from_the_command(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    sandbox.exec(["echo", "$(x)"], cwd="/work/../work/app")
    cmd = next(r for r in api.requests if r.path.endswith("/exec")).body["cmd"]
    assert cmd[:2] == ["/bin/sh", "-c"] and cmd[3:] == ["briq-cwd", "/work/app", "echo", "$(x)"]
    assert "$(x)" not in cmd[2] and "/work" not in cmd[2]


def test_exec_in_a_relative_directory_is_refused(briq: Briq) -> None:
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.exec("ls", cwd="work")
    assert code_of(raised) == "invalid_argument"


def test_exec_that_hits_its_timeout(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="part", code=124, timed_out=True)
    result = sandbox.exec("sleep 99", timeout=2)
    assert result.timed_out and result.exit_code is None and result.stdout == "part"


@pytest.mark.parametrize("command", ["", [], [""], [1], "x"])
def test_exec_validates_what_it_sends(briq: Briq, api: FakeAPI, command: Any) -> None:
    sandbox = briq.create_sandbox()
    before = len(api.requests)
    if command == "x":
        for bad in (0.5, 90_000):
            with pytest.raises(BriqError):
                sandbox.exec(command, timeout=bad)
    else:
        with pytest.raises(BriqError) as raised:
            sandbox.exec(command)
        assert code_of(raised) == "invalid_argument"
    assert len(api.requests) == before


@pytest.mark.parametrize(("response", "code"), [
    (error(409, "invalid_state", "x", providerSaid="machine went down during the command"),
     "outcome_unknown"),
    (error(409, "invalid_state", "x", providerSaid="exec request failed: EOF"), "outcome_unknown"),
    (error(409, "invalid_state", "The sandbox is stopped"), "unavailable"),
    (error(503, "provider_error", "x", op="wake"), "unavailable"),
    (error(503, "provider_error", "x", op="exec"), "outcome_unknown"),
    (error(502, "provider_error"), "outcome_unknown"),
    (error(413, "validation"), "invalid_argument"),
    (error(404, "not_found"), "not_found"),
    (error(429, "quota_exceeded"), "quota"),
    (httpx.Response(200, json={"stdout": "x"}), "outcome_unknown"),
    (httpx.Response(200, json={"stdout": "", "stderr": "", "exitCode": 999, "timedOut": False}),
     "outcome_unknown"),
])  # fmt: skip
def test_exec_errors_say_whether_the_command_may_have_run(
    briq: Briq, api: FakeAPI, response: httpx.Response, code: str
) -> None:
    sandbox = briq.create_sandbox()
    api.fail["POST " + BQ + "/exec"] = response
    with pytest.raises(BriqError) as raised:
        sandbox.exec("x")
    assert code_of(raised) == code
    assert api.count("POST", BQ + "/exec") == 1  # never repeated


@pytest.mark.parametrize(
    ("failure", "code"),
    [(httpx.ReadTimeout("t"), "outcome_unknown"), (httpx.ReadError("r"), "outcome_unknown"),
     (httpx.ConnectError("c"), "unavailable")],
)  # fmt: skip
def test_exec_without_an_answer(briq: Briq, api: FakeAPI, failure: Exception, code: str) -> None:
    sandbox = briq.create_sandbox()
    api.fail["POST " + BQ + "/exec"] = failure
    with pytest.raises(BriqError) as raised:
        sandbox.exec("x")
    assert code_of(raised) == code
    assert api.count("POST", BQ + "/exec") == 1


def test_exec_output_over_the_http_limit_is_an_error_not_a_cut(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.fail["POST " + BQ + "/exec"] = httpx.Response(
        200, json={"stdout": "x", "stderr": "", "exitCode": 0, "timedOut": False, "truncated": True}
    )
    with pytest.raises(BriqError) as raised:
        sandbox.exec("yes")
    assert code_of(raised) == "quota"


def test_exec_output_over_the_clients_limit_is_bounded(
    briq: Briq, api: FakeAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    sandbox = briq.create_sandbox()
    monkeypatch.setattr("briq_cloud._flows.MAX_OUTPUT", 100)
    api.on_exec = lambda cmd, body: Exec(stdout="x" * 5000)
    with pytest.raises(BriqError) as raised:
        sandbox.exec("yes")
    assert code_of(raised) == "quota"


# --- exec as it comes ---


def test_exec_with_on_output_reads_both_streams_from_their_offsets(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="hello world\n", stderr="oops\n", code=2)
    seen: list[OutputChunk] = []
    result = sandbox.exec("run", on_output=seen.append)
    assert result == ExecResult("hello world\n", "oops\n", 2, False)
    assert "".join(c.text for c in seen if c.stream == "stdout") == "hello world\n"
    assert "".join(c.text for c in seen if c.stream == "stderr") == "oops\n"
    start = next(r for r in api.requests if r.method == "POST" and r.path.endswith("/exec"))
    assert start.body["async"] is True
    assert api.count("POST", BQ + "/exec") == 1


def test_a_long_timeout_runs_as_an_async_exec_even_without_on_output(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="done\n")
    assert sandbox.exec("make", timeout=600).stdout == "done\n"
    assert next(r for r in api.requests if r.path.endswith("/exec")).body["async"] is True


def test_async_exec_that_timed_out(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="a", state="timed_out")
    result = sandbox.exec("x", on_output=lambda c: None)
    assert result.timed_out and result.exit_code is None


def test_async_exec_that_was_stopped_or_lost(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(state="stopped")
    with pytest.raises(BriqError) as raised:
        sandbox.exec("x", on_output=lambda c: None)
    assert code_of(raised) == "aborted"
    api.on_exec = lambda cmd, body: Exec(state="lost")
    with pytest.raises(BriqError) as raised:
        sandbox.exec("x", on_output=lambda c: None)
    assert code_of(raised) == "outcome_unknown"


def test_a_failed_read_is_tried_again(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="abc")
    api.fail["GET " + BQ + "/execs/ex_1"] = httpx.ReadTimeout("t")
    assert sandbox.exec("x", on_output=lambda c: None).stdout == "abc"
    assert api.count("POST", BQ + "/exec") == 1


def test_reads_that_keep_failing_end_as_outcome_unknown_and_stop_the_command(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="abc")
    api.always["GET " + BQ + "/execs/ex_1"] = httpx.ReadTimeout("t")
    with pytest.raises(BriqError) as raised:
        sandbox.exec("x", on_output=lambda c: None)
    assert code_of(raised) == "outcome_unknown"
    assert api.count("GET", BQ + "/execs/ex_1") == 4
    assert api.count("DELETE", BQ + "/execs/ex_1") == 1
    assert api.count("POST", BQ + "/exec") == 1


def test_a_failing_on_output_stops_the_command_and_raises(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout="abcdef")

    def boom(chunk: OutputChunk) -> None:
        raise RuntimeError("callback")

    with pytest.raises(RuntimeError, match="callback"):
        sandbox.exec("x", on_output=boom)
    assert api.count("DELETE", BQ + "/execs/ex_1") == 1


# --- files ---


def test_files_round_trip_as_bytes(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    data = bytes(range(256))
    sandbox.write_file("/work/a/b.bin", data)
    put = next(r for r in api.requests if r.method == "PUT")
    assert put.query == {"path": "/work/a/b.bin"} and put.body == data
    assert put.headers["content-type"] == "application/octet-stream"
    assert sandbox.read_file("/work/a/b.bin") == data


def test_write_file_sets_the_mode(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.write_file("/work/run.sh", b"#!/bin/sh", mode=0o755)
    cmd = next(r for r in api.requests if r.path.endswith("/exec")).body["cmd"]
    assert cmd == ["chmod", "755", "--", "/work/run.sh"]
    api.on_exec = lambda cmd, body: Exec(code=1)
    with pytest.raises(BriqError):
        sandbox.write_file("/work/run.sh", b"x", mode=0o644)


def test_write_file_notices_a_file_stored_changed(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.fail["PUT " + BQ + "/files"] = httpx.Response(
        200, json={"path": "/x", "bytes": 1, "sha256": "0" * 64, "verified": True}
    )
    with pytest.raises(BriqError) as raised:
        sandbox.write_file("/x", b"a")
    assert code_of(raised) == "provider"


def test_a_missing_file_is_file_not_found_and_a_missing_sandbox_is_not_found(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.read_file("/nope")
    assert code_of(raised) == "file_not_found"
    api.sandboxes.clear()
    with pytest.raises(BriqError) as raised:
        sandbox.read_file("/nope")
    assert code_of(raised) == "not_found"


def test_a_file_that_arrives_cut_short_is_an_error(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.fail["GET " + BQ + "/files/raw"] = httpx.Response(
        200, content=b"abc", headers={"content-length": "10"}
    )
    with pytest.raises(BriqError) as raised:
        sandbox.read_file("/x")
    assert code_of(raised) == "unavailable"


@pytest.mark.parametrize("path", ["relative", "", "/a\0b"])
def test_file_paths_are_absolute(briq: Briq, path: str) -> None:
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.read_file(path)
    assert code_of(raised) == "invalid_argument"


def test_list_files(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    rows = [
        {"path": "/work/a.txt", "type": "file", "size": 3, "modifiedAt": 1_700_000_000_000},
        {"path": "/work/d", "type": "directory", "size": 4096, "modifiedAt": 1_700_000_001_000},
    ]
    api.on_exec = lambda cmd, body: Exec(stdout=json.dumps({"value": rows}))
    files = sandbox.list_files("/work")
    assert [(f.path, f.type, f.size) for f in files] == [
        ("/work/a.txt", "file", 3),
        ("/work/d", "directory", 4096),
    ]
    assert files[0].modified_at.tzinfo is not None
    sent = next(r for r in api.requests if r.path.endswith("/exec")).body
    assert sent["cmd"][:2] == ["python3", "-c"] and json.loads(sent["stdin"]) == {"path": "/work"}


@pytest.mark.parametrize(
    ("out", "code"),
    [({"error": "file_not_found"}, "file_not_found"),
     ({"error": "invalid_argument"}, "invalid_argument"),
     ({"error": "provider"}, "provider"), ({"nope": 1}, "provider")],
)  # fmt: skip
def test_list_files_errors(briq: Briq, api: FakeAPI, out: dict[str, Any], code: str) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stdout=json.dumps(out))
    with pytest.raises(BriqError) as raised:
        sandbox.list_files("/work")
    assert code_of(raised) == code


def test_list_files_without_python_is_unsupported(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.on_exec = lambda cmd, body: Exec(stderr="python3: not found", code=127)
    with pytest.raises(BriqError) as raised:
        sandbox.list_files("/work")
    assert code_of(raised) == "unsupported"


def test_remove(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.remove("/work/x")
    sandbox.remove("/work/d", recursive=True)
    cmds = [r.body["cmd"] for r in api.requests if r.path.endswith("/exec")]
    assert cmds == [["rm", "-f", "--", "/work/x"], ["rm", "-rf", "--", "/work/d"]]
    with pytest.raises(BriqError) as raised:
        sandbox.remove("/", recursive=True)
    assert code_of(raised) == "invalid_argument"
    api.on_exec = lambda cmd, body: Exec(code=1)
    with pytest.raises(BriqError):
        sandbox.remove("/work/d")


# --- the rest ---


def test_preview_url(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    assert sandbox.preview_url(8000) == "https://bq_1-8000.briq.example/"
    sandbox.preview_url(3000, public=True)
    bodies = [r.body for r in api.requests if r.path.endswith("/expose")]
    assert bodies == [{"port": 8000, "mode": "private"}, {"port": 3000, "mode": "public"}]
    for port in (0, 70000):
        with pytest.raises(BriqError):
            sandbox.preview_url(port)


def test_stop_start_and_sleep(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    assert sandbox.stop().state == "stopped" and sandbox.info.state == "stopped"
    assert next(r for r in reversed(api.requests) if r.path.endswith("/stop")).body == b""
    assert sandbox.start().state == "running"
    assert sandbox.stop(cold=True).state == "stopped"
    cold_request = next(r for r in reversed(api.requests) if r.path.endswith("/stop"))
    assert cold_request.body == {"mode": "cold"}
    assert sandbox.start().state == "running"
    assert sandbox.set_sleep("never").sleep == "never"
    assert sandbox.refresh().sleep == "never"
    assert next(r for r in api.requests if r.path.endswith("/sleep")).body == {"sleep": "never"}


def test_destroy_waits_and_is_fine_twice(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.destroy()
    sandbox.destroy()
    assert not api.sandboxes


def test_destroy_that_cannot_be_confirmed_is_outcome_unknown(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    row = api.sandboxes["bq_1"]
    api.fail["DELETE " + BQ] = httpx.Response(200, json={"briq": {**row, "state": "destroying"}})
    api.fail["GET " + BQ] = error(502, "provider_error")
    with pytest.raises(BriqError) as raised:
        sandbox.destroy()
    assert code_of(raised) == "unavailable"


def test_the_with_block_destroys_the_sandbox(briq: Briq, api: FakeAPI) -> None:
    with briq.create_sandbox() as sandbox:
        sandbox.exec("true")
    assert not api.sandboxes
    with pytest.raises(ValueError, match="inside"), briq.create_sandbox():
        raise ValueError("inside")
    assert not api.sandboxes


def test_the_error_of_a_block_is_not_hidden_by_a_failed_destroy(briq: Briq, api: FakeAPI) -> None:
    with pytest.raises(ValueError, match="inside"), briq.create_sandbox():
        api.fail["DELETE " + BQ] = error(502, "provider_error")
        raise ValueError("inside")


def test_the_client_is_a_context_manager(api: FakeAPI) -> None:
    with Briq("test-key", base_url="https://api.example.com", transport=api.transport()) as client:
        client.create_sandbox()


def test_secrets_never_reach_an_error_message(briq: Briq, api: FakeAPI) -> None:
    api.fail["POST /v1/briqs"] = error(500, "provider_error", "boom")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(secret_env={"T": "hunter2"})
    assert "hunter2" not in str(raised.value)
    assert "hunter2" not in repr(raised.value.details)


def test_set_image_keeps_stopped_and_sends_only_image(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.stop()
    api.requests.clear()
    info = sandbox.set_image("ubuntu:24.04")
    assert info is sandbox.info
    assert (info.image, info.state) == ("ubuntu:24.04", "stopped")
    assert len(api.requests) == 1
    assert (api.requests[0].method, api.requests[0].path) == ("PATCH", BQ)
    assert api.requests[0].body == {"image": "ubuntu:24.04"}


def test_set_image_running_refusal_does_not_stop_or_start(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.requests.clear()
    with pytest.raises(BriqError) as raised:
        sandbox.set_image("ubuntu:24.04")
    assert raised.value.api_code == "invalid_state"
    assert sandbox.info.state == "running"
    assert len(api.requests) == 1 and api.requests[0].method == "PATCH"


def test_set_image_returns_pending_without_starting(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.stop()
    api.fail["PATCH " + BQ] = httpx.Response(
        200, json={"briq": {**api.sandboxes[sandbox.id], "state": "resizing"}}
    )
    api.requests.clear()
    assert sandbox.set_image("ubuntu:24.04").state == "resizing"
    assert len(api.requests) == 1
