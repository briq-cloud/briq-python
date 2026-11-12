"""The calls, written once.

A flow is a generator. It yields what it needs done, a :class:`Request` to send, a :class:`Pause`
or an :class:`Emit` of output, and is sent back the answer. The sync client and the async client
each drive the same flows with their own transport, so what a call does, and which errors it
raises, is the same in both.

A call whose result is not known (a command, a create) is sent once and never repeated: when no
answer comes, the flow raises ``outcome_unknown`` and the caller decides.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import posixpath
import time
import uuid
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeVar
from urllib.parse import quote

from ._errors import BriqError, Code, envelope, refusal
from ._execution import Access, ExecutionMode, parse_access
from ._models import ExecResult, FileInfo, OutputChunk, SandboxInfo, parse_sandbox

T = TypeVar("T")

MiB = 1024 * 1024
#: The most of a command's output that is held: more stops the command and raises ``quota``.
MAX_OUTPUT = 32 * MiB
#: The largest file read or written: the file API's own limit (``GET /v1/quota`` ``maxFileBytes``
#: says what the connected API allows; this is the most it documents).
MAX_FILE = 100 * MiB
#: The most argv bytes (the command and its directory) sent in one call. Some hosts carry only
#: about 16 KB of request per command in all, and this client has no WebSocket to send more over.
MAX_ARGV = 8_192
#: What a plain HTTP exec may run for; longer, or read as it comes, is an async exec.
HTTP_EXEC_MAX_MS = 60_000
#: Covers regional admission, a full synchronous command and response transport.
REGIONAL_EXEC_TIMEOUT = 95.0
#: The HTTP API captures 4 MiB per stream; replacement UTF-8 can expand each byte threefold.
HTTP_CAPTURE_BYTES = 4 * MiB
#: JSON control-character escaping needs more space than the decoded output.
REGIONAL_RESPONSE_BYTES = 64 * MiB
#: Time on top of a command's own timeout for the rest of the call: a wake, the round trip.
ROUND_TRIP = 30.0


@dataclass(slots=True)
class Request:
    """One HTTP request. ``timeout`` ``None`` is the client's own."""

    method: str
    path: str
    query: dict[str, str] | None = None
    json: Any = None
    content: bytes | None = None
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float | None = None
    #: The most of the answer that is read.
    max_bytes: int = 64 * MiB
    #: A separate origin and scoped credential, never the control client's configuration.
    execution: Access | None = None


@dataclass(slots=True)
class Reply:
    """An answer: lower-case header names, the body read up to ``Request.max_bytes``."""

    status: int
    headers: dict[str, str]
    content: bytes
    too_large: bool = False
    #: The bytes that crossed the wire (before any ``content-encoding`` was undone), when known.
    wire_bytes: int | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


@dataclass(slots=True)
class Pause:
    seconds: float


@dataclass(slots=True)
class Emit:
    callback: Callable[[OutputChunk], Any]
    chunk: OutputChunk


class NoAnswer(Exception):
    """The transport got no answer. ``never_sent``: no connection was made, so nothing was sent."""

    def __init__(self, never_sent: bool) -> None:
        super().__init__("no answer")
        self.never_sent = never_sent


Flow = Generator[Request | Pause | Emit, Any, T]


# --- helpers -----------------------------------------------------------------------------------


def seg(value: str) -> str:
    """An id as one path segment, whatever it holds: a client must not reach another route."""
    if not value or value in (".", ".."):
        raise BriqError("invalid_argument", "A sandbox id is required")
    return quote(value, safe="")


def _path(path: str) -> str:
    if not path.startswith("/") or "\0" in path:
        raise BriqError("invalid_argument", "File paths must be absolute POSIX paths")
    # normpath keeps exactly two leading slashes ("//" stays "//"): one root, as the TypeScript
    # client's posix.normalize makes it, so the guard on the root cannot be walked around.
    return "/" + posixpath.normpath(path).lstrip("/")


def _json(reply: Reply) -> dict[str, Any]:
    if not reply.ok:
        raise refusal(reply.status, reply.headers, reply.content)
    try:
        data = json.loads(reply.content)
    except ValueError:
        data = None
    if not isinstance(data, dict):
        raise BriqError(
            "provider", "The API answered with something that is not JSON", status=reply.status
        )
    return data


def _call(request: Request) -> Flow[dict[str, Any]]:
    """A call whose result is simply read: no answer is ``unavailable``."""
    try:
        reply = yield request
    except NoAnswer as exc:
        raise BriqError("unavailable", "Could not reach the briq API") from exc
    return _json(reply)


def _briq(data: dict[str, Any]) -> SandboxInfo:
    return parse_sandbox(data.get("briq"))


def _unconfirmed() -> BriqError:
    return BriqError("outcome_unknown", "The command's completion could not be confirmed")


# --- sandboxes ---------------------------------------------------------------------------------


def create_sandbox(
    *,
    name: str | None,
    size: str | None,
    env: Mapping[str, str] | None,
    secret_env: Mapping[str, str] | None,
    sleep: str | None,
    image: str | None,
    wait_timeout: float,
    idempotency_key: str | None,
) -> Flow[SandboxInfo]:
    """Make a sandbox and wait until it runs. One POST with an idempotency key, never repeated.

    After the POST succeeded, anything that ends the call before the sandbox runs (an error, a
    timeout, a cancel, an interrupt) destroys it, best effort, and the call fails as it did.
    """
    key = idempotency_key or str(uuid.uuid4())
    body: dict[str, Any] = {
        "recipe": "sandbox",
        "wait": True,
        "waitTimeoutMs": int(min(wait_timeout, 300) * 1000),
    }
    for field_, value in (("name", name), ("size", size), ("sleep", sleep), ("image", image)):
        if value is not None:
            body[field_] = value
    values: dict[str, Any] = dict(env or {})
    team: str | None = None
    if secret_env:
        if set(values) & set(secret_env):
            raise BriqError("invalid_argument", "A variable is in both env and secret_env")
        # Only an API that says so keeps a value exactly and write-only.
        quota = yield from _call(Request("GET", "/v1/quota"))
        if (quota.get("features") or {}).get("literalEnvSecrets") is not True:
            raise BriqError("unsupported", "This API cannot keep environment values write-only")
        team_row = quota.get("team")
        team = team_row.get("id") if isinstance(team_row, dict) else None
        # The names follow from the key, so a repeat with the same key is the same request.
        nonce = hashlib.sha256(key.encode()).hexdigest()[:32]
        for index, (var, value) in enumerate(secret_env.items()):
            values[var] = {"secret": f"SANDBOX_{nonce}_{index}", "value": value, "literal": True}
    if values:
        body["env"] = values
    started = time.monotonic()
    try:
        reply = yield Request(
            "POST",
            "/v1/briqs",
            json=body,
            headers={"idempotency-key": key},
            timeout=wait_timeout + ROUND_TRIP,
        )
    except NoAnswer as exc:
        if exc.never_sent:
            raise BriqError("unavailable", "Could not reach the briq API") from exc
        raise _create_unknown(key) from exc
    if not reply.ok:
        error = refusal(reply.status, reply.headers, reply.content)
        # 409: the first request with this key is still running. 5xx: it may have been applied.
        if reply.status >= 500 or reply.status == 409:
            error.code = "outcome_unknown"
            error.idempotency_key = key
            error.message = f"{error.message}. {_repeat(key)}"
            error.args = (error.message,)
        raise error
    try:
        briqs = json.loads(reply.content)["briqs"]
        info = parse_sandbox(briqs[0])
        valid = len(briqs) == 1 and info.id.startswith("bq_")
    except (ValueError, KeyError, IndexError, TypeError, BriqError):
        valid = False
    if not valid:
        raise _create_unknown(key)
    # The key may have been used before, for another sandbox: the receipt must be this call's.
    if (
        (name is not None and info.name != name)
        or (image is not None and info.image != image)
        or (team is not None and info.team_id != team)
    ):
        raise BriqError(
            "outcome_unknown",
            "briq answered with a sandbox that is not the one asked for (was this "
            "idempotency_key used before for another call?). It was left as it is: "
            "use a new key to make a new sandbox",
            idempotency_key=key,
        )
    try:
        return (yield from _until_running(info, wait_timeout - (time.monotonic() - started)))
    except GeneratorExit:
        raise
    except BaseException as failure:
        try:
            yield from destroy(info.id)
        except Exception:
            if not isinstance(failure, Exception):
                raise failure from None  # an interrupt is what the caller gets
            stuck = BriqError(
                "outcome_unknown",
                f"Creating the sandbox failed ({failure}) and sandbox {info.id} could not be "
                f"removed: destroy it with briq.sandbox({info.id!r}).destroy()",
                idempotency_key=key,
                sandbox_id=info.id,
            )
            raise stuck from failure
        raise


def _repeat(key: str) -> str:
    return (
        f"The sandbox may exist: repeat the call with idempotency_key={key!r} "
        "(the API keeps a key for 24 hours and answers a repeat with the sandbox it made)"
    )


def _create_unknown(key: str) -> BriqError:
    return BriqError(
        "outcome_unknown",
        f"The creation was sent and no usable answer came. {_repeat(key)}",
        idempotency_key=key,
    )


def _until_running(info: SandboxInfo, timeout: float) -> Flow[SandboxInfo]:
    deadline = time.monotonic() + max(timeout, 1.0)
    while info.state != "running":
        if info.state not in ("creating", "starting"):
            raise BriqError(
                "unavailable", f"The sandbox is {info.state}, not running" + _why(info.error)
            )
        if time.monotonic() >= deadline:
            raise BriqError("timeout", "The sandbox did not start in time")
        yield Pause(0.5)
        info = yield from get_sandbox(info.id)
    return info


def _why(error: str | None) -> str:
    return f": {error}" if error else ""


def get_sandbox(id: str) -> Flow[SandboxInfo]:
    return _briq((yield from _call(Request("GET", f"/v1/briqs/{seg(id)}"))))


def stop_sandbox(id: str, cold: bool = False) -> Flow[SandboxInfo]:
    body = {"mode": "cold"} if cold else None
    return _briq((yield from _call(Request("POST", f"/v1/briqs/{seg(id)}/stop", json=body))))


def start_sandbox(id: str, wait_timeout: float) -> Flow[SandboxInfo]:
    started = _briq((yield from _call(Request("POST", f"/v1/briqs/{seg(id)}/start"))))
    return (yield from _until_running(started, wait_timeout))


def set_image(id: str, image: str) -> Flow[SandboxInfo]:
    return _briq(
        (yield from _call(Request("PATCH", f"/v1/briqs/{seg(id)}", json={"image": image})))
    )


def set_sleep(id: str, sleep: str) -> Flow[SandboxInfo]:
    data = yield from _call(Request("POST", f"/v1/briqs/{seg(id)}/sleep", json={"sleep": sleep}))
    for row in data.get("briqs") or []:
        if isinstance(row, dict) and row.get("id") == id:
            return parse_sandbox(row)
    raise BriqError("provider", "The API did not answer with the sandbox")


def destroy(id: str) -> Flow[None]:
    """Destroy a sandbox and wait until it is gone. One that is already gone is not an error."""
    try:
        data = yield from _call(Request("DELETE", f"/v1/briqs/{seg(id)}"))
    except BriqError as error:
        if error.code == "not_found":
            return
        raise
    state = _briq(data).state
    deadline = time.monotonic() + 60
    while state != "destroyed":
        if time.monotonic() >= deadline:
            raise BriqError("outcome_unknown", "The sandbox's deletion could not be confirmed")
        yield Pause(0.5)
        try:
            state = (yield from get_sandbox(id)).state
        except BriqError as error:
            if error.code == "not_found":
                return
            raise


def preview_url(id: str, port: int, public: bool) -> Flow[str]:
    if not 1 <= port <= 65535:
        raise BriqError("invalid_argument", "A port is 1 to 65535")
    data = yield from _call(
        Request(
            "POST",
            f"/v1/briqs/{seg(id)}/expose",
            json={"port": port, "mode": "public" if public else "private"},
        )
    )
    url = data.get("url")
    if not isinstance(url, str):
        raise BriqError("provider", "The API did not answer with a URL")
    return url


# --- commands ----------------------------------------------------------------------------------

# The exec route has no working directory. The command runs from a shell that changes into it
# first ($1 is the directory, the rest is the command, never part of the script): a directory that
# is not there ends the command with 126 before it starts, and PWD and OLDPWD are unset again.
_IN_DIR = (
    'cd "$1" || { printf \'briq: cannot enter %s\\n\' "$1" >&2; exit 126; }; '
    'unset PWD OLDPWD; shift; exec "$@"'
)

# A machine that went down under a command that had started: providers word it either way.
_WENT_DOWN = ("went down during the command", "exec request failed: eof")


def exec_command(
    id: str,
    command: str | Sequence[str],
    *,
    cwd: str | None,
    env: Mapping[str, str] | None,
    stdin: str | None,
    timeout: float,
    on_output: Callable[[OutputChunk], Any] | None,
    execution_mode: ExecutionMode = "central",
    execution_ready: bool = False,
) -> Flow[ExecResult]:
    """Run a command. Over one HTTP call when it is short and nobody reads its output as it comes,
    else as an async exec, followed over HTTP."""
    argv = ["sh", "-c", command] if isinstance(command, str) else list(command)
    if (
        not argv
        or not all(isinstance(part, str) for part in argv)
        or not argv[0]
        or (isinstance(command, str) and not command.strip())
    ):
        raise BriqError("invalid_argument", "A command is a string or a non-empty list of strings")
    timeout_ms = int(timeout * 1000)
    if not 1000 <= timeout_ms <= 86_400_000:
        raise BriqError("invalid_argument", "timeout is 1 second to 24 hours")
    if stdin is not None and len(stdin.encode()) > 1_000_000:
        raise BriqError("invalid_argument", "stdin is 1 MB at most")
    size = sum(len(part.encode("utf-8", "replace")) for part in argv)
    if cwd is not None:
        size += len(cwd.encode("utf-8", "replace"))
    if size > MAX_ARGV:
        raise BriqError(
            "invalid_argument",
            f"The command is {size} bytes with its directory; the most this client sends is "
            f"{MAX_ARGV}. Write the script with write_file and run that file",
        )
    if cwd is not None:
        argv = ["/bin/sh", "-c", _IN_DIR, "briq-cwd", _path(cwd), *argv]
    body: dict[str, Any] = {"cmd": argv, "timeoutMs": timeout_ms}
    if stdin:
        body["stdin"] = stdin
    if env:
        body["env"] = dict(env)
    if on_output is None and timeout_ms <= HTTP_EXEC_MAX_MS:
        access = None
        if execution_mode != "central":
            if execution_ready:
                access = yield from _execution_access(id, execution_mode)
            elif execution_mode == "regional":
                raise BriqError("unavailable", "A separate execution transport is required")
        return (yield from _exec_http(id, body, timeout, access))
    if execution_mode == "regional":
        raise BriqError("unsupported", "Regional execution supports synchronous commands only")
    return (yield from _exec_async(id, body, on_output))


def _execution_access(id: str, mode: ExecutionMode) -> Flow[Access | None]:
    try:
        reply = yield Request(
            "POST",
            f"/v1/briqs/{seg(id)}/access",
            json={"version": 1, "operations": ["exec"]},
            max_bytes=64 * 1024,
        )
    except NoAnswer as exc:
        raise BriqError("unavailable", "Could not discover regional execution access") from exc
    if mode == "auto" and (
        reply.status in (404, 405, 501)
        or (reply.status == 503 and envelope(reply.content).get("code") == "provider_error")
    ):
        return None
    if reply.too_large:
        raise BriqError("provider", "The API returned invalid execution access")
    return parse_access(_json(reply), id)


def _exec_refusal(reply: Reply) -> BriqError:
    """A refusal of an exec: its code says whether the command may have run."""
    status = reply.status
    details = envelope(reply.content).get("details")
    details = details if isinstance(details, dict) else {}
    said = details.get("providerSaid")
    op = details.get("op")
    code: Code
    if status == 409:
        went_down = isinstance(said, str) and any(s in said.lower() for s in _WENT_DOWN)
        code = "outcome_unknown" if went_down else "unavailable"
    elif status == 413:
        code = "invalid_argument"
    elif status == 503:
        code = "unavailable" if isinstance(op, str) and op != "exec" else "outcome_unknown"
    elif status >= 500:
        code = "outcome_unknown"
    else:
        return refusal(status, reply.headers, reply.content)
    return refusal(status, reply.headers, reply.content, code)


def _sent(exc: NoAnswer) -> BriqError:
    if exc.never_sent:
        return BriqError("unavailable", "Could not reach the briq API")
    return _unconfirmed()


def _exec_http(
    id: str, body: dict[str, Any], timeout: float, access: Access | None = None
) -> Flow[ExecResult]:
    try:
        reply = yield Request(
            "POST",
            f"/v1/briqs/{seg(id)}/exec",
            json=body,
            timeout=REGIONAL_EXEC_TIMEOUT if access else timeout + ROUND_TRIP,
            max_bytes=REGIONAL_RESPONSE_BYTES if access else MAX_OUTPUT,
            execution=access,
        )
    except NoAnswer as exc:
        raise (_unconfirmed() if access else _sent(exc)) from exc
    if not reply.ok:
        if access:
            raise refusal(reply.status, reply.headers, b"", "outcome_unknown")
        raise _exec_refusal(reply)
    if reply.too_large:
        if access:
            raise _unconfirmed()
        raise BriqError("quota", "The command's output is over the client's limit; use on_output")
    try:
        a = json.loads(reply.content)
        stdout, stderr, timed_out, code = a["stdout"], a["stderr"], a["timedOut"], a["exitCode"]
        valid = (
            isinstance(stdout, str)
            and isinstance(stderr, str)
            and isinstance(timed_out, bool)
            and isinstance(code, int)
            and not isinstance(code, bool)
            and (timed_out or 0 <= code <= 255)
            and isinstance(a.get("truncated", False), bool)
            and (
                not access
                or (
                    isinstance(a.get("truncated"), bool)
                    and isinstance(a.get("costLine"), str)
                    and len(stdout.encode()) <= 3 * HTTP_CAPTURE_BYTES
                    and len(stderr.encode()) <= 3 * HTTP_CAPTURE_BYTES
                )
            )
        )
    except (ValueError, KeyError, TypeError, UnicodeError):
        valid = False
    if not valid:
        raise _unconfirmed()
    # The provider kept only the start of a stream. The async route delivers all of it, and a
    # caller counting on that is told, not handed a cut.
    if a.get("truncated"):
        raise BriqError("quota", "The command's output reached the HTTP limit; use on_output")
    return ExecResult(stdout, stderr, None if timed_out else code, timed_out)


def _exec_async(
    id: str, body: dict[str, Any], on_output: Callable[[OutputChunk], Any] | None
) -> Flow[ExecResult]:
    """Start the command detached, then read both streams from their offsets until it ends."""
    try:
        reply = yield Request("POST", f"/v1/briqs/{seg(id)}/exec", json={**body, "async": True})
    except NoAnswer as exc:
        raise _sent(exc) from exc
    if not reply.ok:
        raise _exec_refusal(reply)
    try:
        exec_id = json.loads(reply.content)["exec"]["id"]
        if not isinstance(exec_id, str):
            raise TypeError
    except (ValueError, KeyError, TypeError):
        raise _unconfirmed() from None
    url = f"/v1/briqs/{seg(id)}/execs/{seg(exec_id)}"
    offsets = {"stdout": 0, "stderr": 0}
    done = {"stdout": False, "stderr": False}
    parts: dict[str, list[str]] = {"stdout": [], "stderr": []}
    size = 0
    failures = 0
    final: dict[str, Any] = {}
    try:
        while not all(done.values()):
            wait = 1000  # the first open stream waits for output, the other only looks
            for stream in ("stdout", "stderr"):
                if done[stream]:
                    continue
                try:
                    reply = yield Request(
                        "GET",
                        url,
                        query={
                            "offset": str(offsets[stream]),
                            "stream": stream,
                            "waitMs": str(wait),
                        },
                        timeout=wait / 1000 + ROUND_TRIP,
                    )
                except NoAnswer:
                    reply = Reply(0, {}, b"")
                wait = 0
                exec_ = _read_of(reply)
                if exec_ is None:
                    # The command is running somewhere; reading again is safe, a few times.
                    failures += 1
                    if failures > 3 or (0 < reply.status < 500 and reply.status != 429):
                        raise _unconfirmed()
                    yield Pause(1.0)
                    continue
                failures = 0
                final = exec_
                read = exec_["read"]
                if read["data"]:
                    size += len(read["data"])
                    if size > MAX_OUTPUT:
                        raise BriqError("quota", "The command's output is over the client's limit")
                    parts[stream].append(read["data"])
                    if on_output is not None:
                        yield Emit(on_output, OutputChunk(stream, read["data"]))
                offsets[stream] = read["nextOffset"]
                done[stream] = read["eof"]
    except GeneratorExit:
        raise
    except BaseException:
        # Stop what is still running (an error, a cancel, an interrupt); a failure to do so
        # changes nothing about the error.
        with contextlib.suppress(Exception):
            yield Request("DELETE", url)
        raise
    state = final.get("state")
    if state == "exited" and isinstance(final.get("exitCode"), int):
        return ExecResult("".join(parts["stdout"]), "".join(parts["stderr"]), final["exitCode"])
    if state == "timed_out":
        return ExecResult("".join(parts["stdout"]), "".join(parts["stderr"]), None, True)
    if state == "stopped":
        raise BriqError("aborted", "The command was stopped")
    raise _unconfirmed()


def _read_of(reply: Reply) -> dict[str, Any] | None:
    """The ``exec`` of an answer that carries a readable stream; ``None`` for anything else."""
    if not reply.ok:
        return None
    try:
        exec_ = json.loads(reply.content)["exec"]
        read = exec_["read"]
        if not (
            isinstance(read["data"], str)
            and isinstance(read["nextOffset"], int)
            and isinstance(read["eof"], bool)
        ):
            return None
    except (ValueError, KeyError, TypeError):
        return None
    return exec_ if isinstance(exec_, dict) else None


# --- files -------------------------------------------------------------------------------------

_LIST = r"""
import json, os, stat, sys
path = json.load(sys.stdin)["path"]
def entry(p):
    s = os.lstat(p)
    t = ("symlink" if stat.S_ISLNK(s.st_mode) else "directory" if stat.S_ISDIR(s.st_mode)
         else "file" if stat.S_ISREG(s.st_mode) else None)
    return t and dict(path=p, type=t, size=s.st_size, modifiedAt=int(s.st_mtime * 1000))
try:
    names = sorted(os.listdir(path), key=os.fsencode)
    rows = [e for e in (entry(os.path.join(path, n)) for n in names) if e]
    print(json.dumps(dict(value=rows)))
except FileNotFoundError:
    print(json.dumps(dict(error="file_not_found")))
except NotADirectoryError:
    print(json.dumps(dict(error="invalid_argument")))
except OSError:
    print(json.dumps(dict(error="provider")))
"""
_LIST_MAX = 4 * MiB


def _sh(id: str, argv: list[str], stdin: str | None = None) -> Flow[ExecResult]:
    return (
        yield from exec_command(
            id, argv, cwd=None, env=None, stdin=stdin, timeout=30, on_output=None
        )
    )


def write_file(id: str, path: str, data: bytes, mode: int | None) -> Flow[None]:
    path = _path(path)
    if len(data) > MAX_FILE:
        raise BriqError("quota", f"A file is {MAX_FILE // MiB} MiB at most")
    if mode is not None and not 0 <= mode <= 0o7777:
        raise BriqError("invalid_argument", "A file mode is 0 to 0o7777")
    try:
        reply = yield Request(
            "PUT",
            f"/v1/briqs/{seg(id)}/files",
            query={"path": path},
            content=data,
            headers={"content-type": "application/octet-stream"},
        )
    except NoAnswer as exc:
        raise BriqError("unavailable", "Could not reach the briq API") from exc
    stored = _json(reply)  # parent directories are made; a 404 here is the sandbox
    if stored.get("bytes") != len(data) or stored.get("sha256") != hashlib.sha256(data).hexdigest():
        raise BriqError("provider", "The file was stored changed; write it again")
    if mode is not None:
        result = yield from _sh(id, ["chmod", f"{mode:o}", "--", path])
        if result.exit_code != 0:
            raise BriqError("provider", "The file was written, its mode could not be set")


def read_file(id: str, path: str) -> Flow[bytes]:
    path = _path(path)
    try:
        reply = yield Request(
            "GET", f"/v1/briqs/{seg(id)}/files/raw", query={"path": path}, max_bytes=MAX_FILE
        )
    except NoAnswer as exc:
        raise BriqError("unavailable", "Could not reach the briq API") from exc
    if not reply.ok:
        error = refusal(reply.status, reply.headers, reply.content)
        if error.code == "not_found":
            # A 404 is the sandbox or the file: ask which.
            yield from get_sandbox(id)
            error.code = "file_not_found"
        raise error
    if reply.too_large:
        raise BriqError(
            "quota", f"The file is over {MAX_FILE // MiB} MiB, the most this client reads"
        )
    # content-length counts the bytes on the wire: when the body was decoded (a compressing edge),
    # compare with those, not with the decoded size.
    expected = reply.headers.get("content-length")
    encoded = reply.headers.get("content-encoding", "").strip().lower() not in ("", "identity")
    got = reply.wire_bytes if encoded and reply.wire_bytes else len(reply.content)
    if expected and expected.isdigit() and int(expected) != got:
        raise BriqError("unavailable", "The file arrived cut short; read it again")
    return bytes(reply.content)


def list_files(id: str, path: str) -> Flow[list[FileInfo]]:
    path = _path(path)
    result = yield from _sh(id, ["python3", "-c", _LIST], json.dumps({"path": path}))
    if result.exit_code != 0 or result.timed_out:
        raise BriqError("unsupported", "Listing a directory needs python3 in the sandbox")
    if len(result.stdout) > _LIST_MAX:
        raise BriqError("quota", "The directory is too large to list")
    try:
        answer = json.loads(result.stdout)
        if "error" in answer:
            code = {"file_not_found", "invalid_argument"} & {answer["error"]}
            raise BriqError(code.pop() if code else "provider", "The directory could not be listed")  # type: ignore[arg-type]
        return [
            FileInfo(
                row["path"],
                row["type"],
                int(row["size"]),
                datetime.fromtimestamp(row["modifiedAt"] / 1000, timezone.utc),
            )
            for row in answer["value"]
        ]
    except (ValueError, KeyError, TypeError):
        raise BriqError("provider", "The directory listing could not be read") from None


def remove(id: str, path: str, recursive: bool) -> Flow[None]:
    path = _path(path)
    if path == "/":
        raise BriqError("invalid_argument", "The root cannot be removed")
    result = yield from _sh(id, ["rm", "-rf" if recursive else "-f", "--", path])
    if result.exit_code != 0:
        raise BriqError("provider", "The path could not be removed (a directory needs recursive)")
