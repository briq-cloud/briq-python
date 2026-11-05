"""Paths, sizes, the creation receipt and what an error exposes."""

from __future__ import annotations

import gzip

import httpx
import pytest

from briq_cloud import Briq, BriqError
from briq_cloud._flows import MAX_ARGV, MAX_FILE, MiB

from .fake_api import FakeAPI, error

BQ = "/v1/briqs/bq_1"


# --- paths ---


@pytest.mark.parametrize("path", ["/", "//", "///", "/.", "/..", "/a/..", "//a/../..", "/./"])
def test_the_root_cannot_be_removed_however_it_is_written(
    briq: Briq, api: FakeAPI, path: str
) -> None:
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.remove(path, recursive=True)
    assert raised.value.code == "invalid_argument"
    assert api.count("POST", BQ + "/exec") == 0


def test_paths_are_normalized_the_same_way_everywhere(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    sandbox.write_file("//work//./x/../y", b"1")
    assert next(r for r in api.requests if r.method == "PUT").query["path"] == "/work/y"
    sandbox.remove("//work/y")
    assert api.requests[-1].body["cmd"] == ["rm", "-f", "--", "/work/y"]
    sandbox.exec("true", cwd="//work//")
    assert api.requests[-1].body["cmd"][4] == "/work"


# --- argv ---


def test_a_command_over_the_argv_limit_is_refused_before_it_is_sent(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.exec("echo " + "x" * MAX_ARGV)
    assert raised.value.code == "invalid_argument" and "write_file" in str(raised.value)
    with pytest.raises(BriqError):
        sandbox.exec(["echo", "x" * MAX_ARGV], cwd="/work")
    assert api.count("POST", BQ + "/exec") == 0
    sandbox.exec("echo " + "x" * (MAX_ARGV - 100))  # under it: sent


# --- files ---


def test_a_file_over_the_limit_is_quota_not_a_retryable_cut(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.files["/big"] = b"\0" * (MAX_FILE + 1)
    with pytest.raises(BriqError) as raised:
        sandbox.read_file("/big")
    assert raised.value.code == "quota" and "100 MiB" in str(raised.value)


def test_a_file_between_64_and_100_mib_is_read(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    api.files["/big"] = b"\1" * (70 * MiB)
    assert len(sandbox.read_file("/big")) == 70 * MiB


def test_a_file_over_the_limit_is_not_written(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    with pytest.raises(BriqError) as raised:
        sandbox.write_file("/big", b"\0" * (MAX_FILE + 1))
    assert raised.value.code == "quota"
    assert api.count("PUT", BQ + "/files") == 0


def test_a_compressed_file_is_not_a_cut_one(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    data = b"hello " * 1000
    packed = gzip.compress(data)
    assert len(packed) != len(data)
    api.fail["GET " + BQ + "/files/raw"] = httpx.Response(
        200,
        headers={"content-encoding": "gzip", "content-length": str(len(packed))},
        stream=httpx.ByteStream(packed),
    )
    assert sandbox.read_file("/x") == data


def test_a_compressed_file_that_is_cut_short_is_still_caught(briq: Briq, api: FakeAPI) -> None:
    sandbox = briq.create_sandbox()
    packed = gzip.compress(b"hello " * 1000)
    api.fail["GET " + BQ + "/files/raw"] = httpx.Response(
        200,
        headers={"content-encoding": "gzip", "content-length": str(len(packed) + 5)},
        stream=httpx.ByteStream(packed),
    )
    with pytest.raises(BriqError) as raised:
        sandbox.read_file("/x")
    assert raised.value.code == "unavailable"


# --- the creation receipt and the key ---


def test_the_error_of_a_lost_create_carries_the_key_the_caller_can_repeat(
    briq: Briq, api: FakeAPI
) -> None:
    api.fail["POST /v1/briqs"] = httpx.ReadTimeout("slow")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox()
    key = raised.value.idempotency_key
    assert key and key in str(raised.value) and "repeat the call" in str(raised.value)
    assert raised.value.sandbox_id is None


@pytest.mark.parametrize("status", [409, 500, 503])
def test_a_create_refusal_that_may_have_been_applied_carries_the_key(
    briq: Briq, api: FakeAPI, status: int
) -> None:
    api.fail["POST /v1/briqs"] = error(status, "x", "boom")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(idempotency_key="mine")
    assert raised.value.code == "outcome_unknown" and raised.value.idempotency_key == "mine"
    assert "idempotency_key='mine'" in str(raised.value)


def test_secret_env_names_follow_the_key(briq: Briq, api: FakeAPI) -> None:
    briq.create_sandbox(secret_env={"T": "a"}, idempotency_key="k")
    briq.create_sandbox(secret_env={"T": "a"}, idempotency_key="k")
    bodies = [r.body for r in api.requests if r.method == "POST"]
    assert bodies[0] == bodies[1]
    assert len(api.sandboxes) == 1


def test_a_reused_key_that_answers_another_sandbox_is_not_taken(briq: Briq, api: FakeAPI) -> None:
    old = briq.create_sandbox(name="old", idempotency_key="k")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(name="new", idempotency_key="k")
    assert raised.value.code == "outcome_unknown" and "use a new key" in str(raised.value)
    assert api.count("DELETE", BQ) == 0 and old.id in api.sandboxes


def test_a_receipt_of_another_image_or_team_is_not_taken(briq: Briq, api: FakeAPI) -> None:
    briq.create_sandbox(image="a:1", idempotency_key="k")
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(image="b:2", idempotency_key="k")
    assert raised.value.code == "outcome_unknown"
    row = {**api.sandboxes["bq_1"], "teamId": "team_other"}
    api.fail["POST /v1/briqs"] = httpx.Response(201, json={"briqs": [row]})
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox(secret_env={"T": "x"}, idempotency_key="k2")
    assert raised.value.code == "outcome_unknown"


# --- what an error shows ---


def test_an_error_keeps_codes_and_ids_and_drops_the_text_of_a_provider(
    briq: Briq, api: FakeAPI
) -> None:
    sandbox = briq.create_sandbox()
    api.always["POST " + BQ + "/exec"] = error(
        503, "provider_error", "down", op="exec", providerSaid="host 10.0.0.7 hetzner: boom"
    )
    with pytest.raises(BriqError) as raised:
        sandbox.exec("true")
    assert raised.value.details == {"op": "exec"}
    assert "hetzner" not in repr(vars(raised.value))


def test_plan_details_are_kept(briq: Briq, api: FakeAPI) -> None:
    api.fail["POST /v1/briqs"] = error(403, "plan_limit", "no", limit="size", plan="hobby", used=3)
    with pytest.raises(BriqError) as raised:
        briq.create_sandbox()
    assert raised.value.details == {"limit": "size", "plan": "hobby", "used": 3}
    assert raised.value.code == "quota"


# --- the http client ---


def test_a_client_of_your_own_is_used_and_not_closed(api: FakeAPI) -> None:
    http = httpx.Client(transport=api.transport())
    with Briq("test-key", base_url="https://api.example.com", http_client=http) as client:
        client.create_sandbox()
    assert not http.is_closed and api.sandboxes
    http.close()


def test_a_client_of_your_own_does_not_follow_a_redirect(api: FakeAPI) -> None:
    http = httpx.Client(transport=api.transport(), follow_redirects=True)
    api.fail["GET /v1/quota"] = httpx.Response(302, headers={"location": "https://evil.example/"})
    client = Briq("test-key", base_url="https://api.example.com", http_client=http)
    with pytest.raises(BriqError):
        client.create_sandbox(secret_env={"T": "x"})
    assert [r.path for r in api.requests] == ["/v1/quota"]
