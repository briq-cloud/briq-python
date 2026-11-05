"""Drift check: the client against the API's OpenAPI document.

``openapi.json`` is a snapshot of the API's document, cut down to the operations of
``contract.json``. It is refreshed from the API's source at each release. When the API drops
or retypes a field the client uses, this fails after a refresh.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from .contract_check import load_contract, load_spec, problems


@pytest.fixture
def spec() -> dict[str, Any]:
    return load_spec()


def test_the_api_still_offers_what_the_client_uses(spec: dict[str, Any]) -> None:
    assert problems(load_contract(), spec) == []


def test_the_snapshot_holds_every_operation_of_the_contract(spec: dict[str, Any]) -> None:
    contract = load_contract()
    wanted = {e["op"].split(" ", 1)[1] for e in contract["operations"]}
    assert wanted <= set(spec["paths"])


def _schema(spec: dict[str, Any], op: str, status: str) -> dict[str, Any]:
    path, method = op.split(" ")[1], op.split(" ")[0].lower()
    schema = spec["paths"][path][method]["responses"][status]["content"]["application/json"][
        "schema"
    ]
    if "$ref" in schema:
        schema = spec["components"]["schemas"][schema["$ref"].rsplit("/", 1)[-1]]
    return schema  # type: ignore[no-any-return]


def test_a_removed_response_field_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    del _schema(broken, "POST /v1/briqs/{id}/exec", "200")["properties"]["exitCode"]
    assert any("exitCode" in p and "gone" in p for p in problems(load_contract(), broken))


def test_a_retyped_response_field_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    _schema(broken, "POST /v1/briqs/{id}/exec", "200")["properties"]["stdout"] = {"type": "array"}
    assert any("stdout" in p and "array" in p for p in problems(load_contract(), broken))


def test_a_removed_enum_value_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    props = _schema(broken, "GET /v1/briqs/{id}/execs/{execId}", "200")["properties"]
    exec_ = props["exec"]
    if "$ref" in exec_:
        exec_ = broken["components"]["schemas"][exec_["$ref"].rsplit("/", 1)[-1]]
    exec_["properties"]["state"]["enum"] = ["running", "exited"]
    assert any("exec.state" in p and "timed_out" in p for p in problems(load_contract(), broken))


def test_a_removed_request_field_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    body = broken["paths"]["/v1/briqs/{id}/exec"]["post"]["requestBody"]["content"]
    del body["application/json"]["schema"]["properties"]["timeoutMs"]
    assert any("timeoutMs" in p for p in problems(load_contract(), broken))


def test_a_removed_operation_or_parameter_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    del broken["paths"]["/v1/briqs/{id}/expose"]
    broken["paths"]["/v1/briqs/{id}/files"]["put"]["parameters"] = [
        p
        for p in broken["paths"]["/v1/briqs/{id}/files"]["put"]["parameters"]
        if p["name"] != "path"
    ]
    found = problems(load_contract(), broken)
    assert any("expose" in p and "gone" in p for p in found)
    assert any("parameter 'path'" in p for p in found)


def test_a_changed_error_envelope_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    ref = broken["paths"]["/v1/quota"]["get"]["responses"]["401"]["content"]["application/json"][
        "schema"
    ]
    schema = (
        broken["components"]["schemas"][ref["$ref"].rsplit("/", 1)[-1]] if "$ref" in ref else ref
    )
    inner = schema["properties"]["error"]
    if "$ref" in inner:
        inner = broken["components"]["schemas"][inner["$ref"].rsplit("/", 1)[-1]]
    del inner["properties"]["requestId"]
    assert any("requestId" in p for p in problems(load_contract(), broken))


def test_a_removed_recipe_or_error_detail_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    broken["x-recipes"] = [r for r in broken["x-recipes"] if r != "sandbox"]
    broken["x-error-details"] = ["op"]
    found = problems(load_contract(), broken)
    assert any("recipe 'sandbox'" in p for p in found)
    assert any("providerSaid" in p for p in found)


def test_a_changed_env_value_shape_is_caught(spec: dict[str, Any]) -> None:
    broken = copy.deepcopy(spec)
    branch = broken["components"]["schemas"]["RunBody"]["anyOf"][1]["properties"]["env"]
    value = branch["allOf"][0]["additionalProperties"]["anyOf"][1]
    value["properties"]["literal"] = {"type": "string"}
    del value["properties"]["secret"]
    found = problems(load_contract(), broken)
    assert any("env.*.literal" in p and "string" in p for p in found)
    assert any("env.*.secret" in p and "gone" in p for p in found)
