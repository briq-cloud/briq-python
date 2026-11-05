"""Check what the client uses of the API against the API's OpenAPI document.

``tests/contract.json`` lists, per operation, the parameters, request fields and response fields
the client relies on, with their types. ``problems`` returns what the document no longer offers.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).parent
Schema = dict[str, Any]


def load_contract() -> dict[str, Any]:
    return json.loads((HERE / "contract.json").read_text())  # type: ignore[no-any-return]


def load_spec() -> dict[str, Any]:
    return json.loads((HERE / "openapi.json").read_text())  # type: ignore[no-any-return]


def _resolve(spec: dict[str, Any], schema: Schema) -> list[Schema]:
    """The object schemas a schema can be: a ``$ref`` followed, ``anyOf``/``oneOf`` branches
    listed, ``allOf`` merged."""
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        return _resolve(spec, spec.get("components", {}).get("schemas", {}).get(name, {}))
    for key in ("anyOf", "oneOf"):
        if key in schema:
            return [s for branch in schema[key] for s in _resolve(spec, branch)]
    if "allOf" in schema:
        merged: Schema = {"properties": {}}
        for part in schema["allOf"]:
            for s in _resolve(spec, part):
                merged["properties"].update(s.get("properties", {}))
                merged.update({k: v for k, v in s.items() if k != "properties"})
        return [merged]
    return [schema]


def _walk(spec: dict[str, Any], roots: list[Schema], path: str) -> list[Schema]:
    """The schemas found at a dotted path, one per branch that has it."""
    found = roots
    for part in path.split("."):
        if part == "*":  # the values of a map
            found = [
                r
                for schema in found
                if isinstance(schema.get("additionalProperties"), dict)
                for r in _resolve(spec, schema["additionalProperties"])
            ]
            continue
        array = part.endswith("[]")
        name = part[:-2] if array else part
        nxt: list[Schema] = []
        for schema in found:
            child = schema.get("properties", {}).get(name)
            if child is None:
                continue
            for resolved in _resolve(spec, child):
                if array:
                    if resolved.get("type") != "array":
                        continue
                    nxt.extend(_resolve(spec, resolved.get("items", {})))
                else:
                    nxt.append(resolved)
        found = nxt
    return found


def _fields(entries: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    """Expand the ``$briq`` and ``$briqs`` shorthands."""
    out: dict[str, Any] = {}
    for key, value in entries.items():
        if key == "$briq":
            out.update(contract["briq"])
        elif key == "$briqs":
            out.update({k.replace("briq.", "briqs[].", 1): v for k, v in contract["briq"].items()})
        else:
            out[key] = value
    return out


def _check_field(
    spec: dict[str, Any], roots: list[Schema], path: str, want: Any, where: str
) -> list[str]:
    kind, values = (want, None) if isinstance(want, str) else (want["type"], want.get("values"))
    if path == "_media":
        return []
    schemas = _walk(spec, roots, path)
    if not schemas:
        return [f"{where}: field '{path}' is gone"]
    if kind == "any":
        return []
    types = {s.get("type") for s in schemas}
    # A number the client reads as an int may be an integer in the document, and the reverse.
    ok = {kind} | ({"integer", "number"} if kind in ("integer", "number") else set())
    if not types & ok:
        return [f"{where}: field '{path}' is {sorted(map(str, types))}, the client reads {kind}"]
    if values:
        offered = {v for s in schemas for v in s.get("enum", [])}
        if offered and not set(values) <= offered:
            return [f"{where}: field '{path}' no longer offers {sorted(set(values) - offered)}"]
    return []


def _json_schema(spec: dict[str, Any], body: dict[str, Any], media: str) -> list[Schema]:
    content = body.get("content", {}).get(media, {}).get("schema")
    return _resolve(spec, content) if content is not None else []


def problems(contract: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for entry in contract["operations"]:
        method, path = entry["op"].split(" ", 1)
        op = spec.get("paths", {}).get(path, {}).get(method.lower())
        if op is None:
            out.append(f"{entry['op']}: the operation is gone")
            continue
        where = entry["op"]
        declared = {(p["in"], p["name"]): p for p in op.get("parameters", [])}
        for name, place in entry.get("params", {}).items():
            if (place, name) not in declared:
                out.append(f"{where}: {place} parameter '{name}' is gone")
        for name in entry.get("headers", []):
            if ("header", name) not in declared:
                out.append(f"{where}: header '{name}' is gone")
        if "request" in entry:
            roots = _json_schema(spec, op.get("requestBody", {}), "application/json")
            if not roots:
                out.append(f"{where}: the JSON request body is gone")
            for field, want in entry["request"].items():
                out += _check_field(spec, roots, field, want, f"{where} request")
        for status, fields in entry.get("responses", {}).items():
            response = op.get("responses", {}).get(status)
            if response is None:
                out.append(f"{where}: the {status} response is gone")
                continue
            expanded = _fields(fields, contract)
            media = expanded.get("_media", "application/json")
            if media not in response.get("content", {}):
                out.append(f"{where} {status}: the {media} body is gone")
                continue
            roots = _json_schema(spec, response, media)
            for field, want in expanded.items():
                out += _check_field(spec, roots, field, want, f"{where} {status}")
    for recipe in contract.get("recipes", []):
        if recipe not in spec.get("x-recipes", []):
            out.append(f"recipe '{recipe}' is no longer offered")
    for key in contract.get("error_details", {}).get("keys", []):
        if key not in spec.get("x-error-details", []):
            out.append(f"error.details.{key} is no longer sent by the API")
    first = contract["operations"][0]
    method, path = first["op"].split(" ", 1)
    refusal = (
        spec.get("paths", {}).get(path, {}).get(method.lower(), {}).get("responses", {}).get("401")
    )
    roots = _json_schema(spec, refusal or {}, "application/json")
    for field, want in contract["error"].items():
        out += _check_field(spec, roots, field, want, "error answer")
    return out
