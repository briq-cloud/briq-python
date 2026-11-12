"""An in-memory briq API on ``httpx.MockTransport``: enough of the routes the client uses.

It answers as the real API does (shapes checked against the OpenAPI snapshot in
``test_contract.py``). A test changes what it does through its attributes and reads what was
sent from ``requests``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

NOW = "2026-11-05T10:00:00.000Z"


def error(status: int, code: str, message: str = "nope", **details: Any) -> httpx.Response:
    body = {"error": {"code": code, "message": message, "requestId": "req_1", "details": details}}
    return httpx.Response(status, json=body, headers={"x-request-id": "req_1"})


@dataclass
class Sent:
    method: str
    path: str
    query: dict[str, str]
    body: Any
    headers: httpx.Headers


@dataclass
class Exec:
    """What a command prints, and how it ends."""

    stdout: str = ""
    stderr: str = ""
    code: int = 0
    timed_out: bool = False
    state: str = "exited"


@dataclass
class FakeAPI:
    #: ``creating`` makes a new sandbox start as such and run after ``boot_polls`` reads.
    create_state: str = "running"
    boot_polls: int = 0
    literal_secrets: bool = True
    #: What a command does: ``cmd -> Exec``. Raising ``httpx`` errors simulates the network.
    on_exec: Callable[[list[str], dict[str, Any]], Exec] = lambda cmd, body: Exec(stdout="ok\n")
    #: Fail the next request to ``"METHOD /path"`` (regex) with this: a response or an exception.
    fail: dict[str, Any] = field(default_factory=dict)
    #: The same, for every request that matches.
    always: dict[str, Any] = field(default_factory=dict)
    #: Output is served in pieces of this many characters, to exercise offsets.
    piece: int = 4
    requests: list[Sent] = field(default_factory=list)
    sandboxes: dict[str, dict[str, Any]] = field(default_factory=dict)
    keys: dict[str, str] = field(default_factory=dict)
    files: dict[str, bytes] = field(default_factory=dict)
    execs: dict[str, Exec] = field(default_factory=dict)
    _polls: dict[str, int] = field(default_factory=dict)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def count(self, method: str, pattern: str) -> int:
        return sum(1 for r in self.requests if r.method == method and re.fullmatch(pattern, r.path))

    # --- routing ---

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.raw_path.decode().split("?")[0]  # as sent, not decoded
        raw = request.content
        ctype = request.headers.get("content-type", "")
        body = json.loads(raw) if raw and ctype.startswith("application/json") else raw
        self.requests.append(
            Sent(request.method, path, dict(request.url.params), body, request.headers)
        )
        for table in (self.fail, self.always):
            for pattern, failure in list(table.items()):
                if re.fullmatch(pattern, f"{request.method} {path}"):
                    if table is self.fail:
                        del table[pattern]
                    if isinstance(failure, BaseException):
                        raise failure
                    return failure  # type: ignore[no-any-return]
        if request.headers.get("authorization") != "Bearer test-key":
            return error(401, "unauthorized", "Bad key")
        for method, pattern, handler in self._routes():
            match = re.fullmatch(pattern, path)
            if request.method == method and match:
                return handler(request, body, *match.groups())
        return error(404, "not_found", "No such route")

    def _routes(self) -> list[tuple[str, str, Callable[..., httpx.Response]]]:
        b = r"/v1/briqs/([^/]+)"
        return [
            ("GET", r"/v1/quota", lambda r, body: self._quota()),
            ("POST", r"/v1/briqs", self._create),
            ("GET", b, lambda r, body, id: self._get(id)),
            ("DELETE", b, lambda r, body, id: self._destroy(id)),
            ("PATCH", b, self._set_image),
            ("POST", b + "/stop", lambda r, body, id: self._set(id, "stopped")),
            ("POST", b + "/start", lambda r, body, id: self._set(id, "running")),
            ("POST", b + "/sleep", self._sleep),
            ("POST", b + "/expose", self._expose),
            ("POST", b + "/exec", self._exec),
            ("GET", b + "/execs/([^/]+)", self._read_exec),
            ("DELETE", b + "/execs/([^/]+)", lambda r, body, id, ex: httpx.Response(200, json={})),
            ("PUT", b + "/files", self._put),
            ("GET", b + "/files/raw", self._raw),
        ]

    # --- handlers ---

    def _row(self, id: str) -> dict[str, Any]:
        return self.sandboxes[id]

    def _briq(self, id: str, status: int = 200) -> httpx.Response:
        return httpx.Response(status, json={"briq": self._row(id)})

    def _quota(self) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "features": {"literalEnvSecrets": self.literal_secrets},
                "team": {"id": "team_1"},
            },
        )

    def _create(self, request: httpx.Request, body: Any) -> httpx.Response:
        key = request.headers.get("idempotency-key", "")
        if key in self.keys:
            return self._created(self.keys[key])
        id = f"bq_{len(self.sandboxes) + 1}"
        self.keys[key] = id
        self.sandboxes[id] = {
            "id": id,
            "name": body.get("name", "sandbox-1"),
            "image": body.get("image", "ghcr.io/briq-cloud/sandbox:latest"),
            "size": body.get("size", "medium"),
            "state": self.create_state,
            "sleep": body.get("sleep", "30m"),
            "createdAt": NOW,
            "sleepsAt": None,
            "costCentsSoFar": 0.0,
            "error": None,
            "teamId": "team_1",
        }
        return self._created(id)

    def _created(self, id: str) -> httpx.Response:
        payload = {"briqs": [self._row(id)], "outputs": {}, "recipe": "sandbox", "costLine": ""}
        return httpx.Response(201, json=payload)

    def _get(self, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        row = self._row(id)
        if row["state"] in ("creating", "starting"):
            self._polls[id] = self._polls.get(id, 0) + 1
            if self._polls[id] > self.boot_polls:
                row["state"] = "running"
        return self._briq(id)

    def _set(self, id: str, state: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        self._row(id)["state"] = "starting" if state == "running" else state
        self._polls[id] = 0
        return self._briq(id)

    def _set_image(self, request: httpx.Request, body: Any, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        if self._row(id)["state"] != "stopped":
            return error(409, "invalid_state", "Stop the sandbox before changing its image")
        self._row(id)["image"] = body["image"]
        return self._briq(id)

    def _destroy(self, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        row = self.sandboxes.pop(id)
        return httpx.Response(200, json={"briq": {**row, "state": "destroyed"}})

    def _sleep(self, request: httpx.Request, body: Any, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        self._row(id)["sleep"] = body["sleep"]
        return httpx.Response(200, json={"briqs": [self._row(id)]})

    def _expose(self, request: httpx.Request, body: Any, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        host = f"{id}-{body['port']}.briq.example"
        return httpx.Response(
            201,
            json={"id": "ex_1", "port": body["port"], "mode": body.get("mode", "private"),
                  "url": f"https://{host}/", "hostname": host},
        )  # fmt: skip

    def _exec(self, request: httpx.Request, body: Any, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        result = self.on_exec(body["cmd"], body)
        if body.get("async"):
            ex_id = f"ex_{len(self.execs) + 1}"
            self.execs[ex_id] = result
            return httpx.Response(202, json={"exec": {"id": ex_id, "state": "running"}})
        return httpx.Response(
            200,
            json={"stdout": result.stdout, "stderr": result.stderr, "exitCode": result.code,
                  "timedOut": result.timed_out, "truncated": False, "costLine": ""},
        )  # fmt: skip

    def _read_exec(self, request: httpx.Request, body: Any, id: str, ex_id: str) -> httpx.Response:
        result = self.execs[ex_id]
        q = request.url.params
        stream, offset = q["stream"], int(q["offset"])
        text = result.stdout if stream == "stdout" else result.stderr
        data = text[offset : offset + self.piece]
        read = {"stream": stream, "offset": offset, "data": data, "nextOffset": offset + len(data),
                "bytes": len(text), "eof": offset + len(data) >= len(text)}  # fmt: skip
        exec_ = {"id": ex_id, "state": result.state if read["eof"] else "running",
                 "exitCode": result.code, "read": read}  # fmt: skip
        return httpx.Response(200, json={"exec": exec_})

    def _put(self, request: httpx.Request, body: Any, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        path = request.url.params["path"]
        self.files[path] = body
        digest = hashlib.sha256(body).hexdigest()
        return httpx.Response(
            200, json={"path": path, "bytes": len(body), "sha256": digest, "verified": True}
        )

    def _raw(self, request: httpx.Request, body: Any, id: str) -> httpx.Response:
        if id not in self.sandboxes:
            return error(404, "not_found", "No such sandbox")
        path = request.url.params["path"]
        if path not in self.files:
            return error(404, "not_found", "No such file")
        return httpx.Response(200, content=self.files[path])
