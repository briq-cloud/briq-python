"""The synchronous client: the flows of ``_flows`` driven over ``httpx.Client``."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import Any, TypeVar

import httpx

from . import _flows as f
from ._config import headers, resolve, timeout_of
from ._errors import BriqError
from ._models import ExecResult, FileInfo, OutputChunk, SandboxInfo

T = TypeVar("T")

_pause = time.sleep  # a name of its own, so tests can skip the waits


class Briq:
    """A client for the briq API.

    ``api_key`` falls back to ``BRIQ_API_KEY`` and ``base_url`` to ``BRIQ_API_URL``, then to the
    public API. ``timeout`` is the seconds each request may take, a command and a sandbox's start
    each add their own. ``http_client`` is an ``httpx.Client`` of your own (a proxy, a CA bundle,
    a mounted transport): it is used as it is, except that no redirect is ever followed, its
    ``base_url`` is ignored, and ``close`` leaves it open. ``transport`` is for tests.
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._key, self._origin = resolve(api_key, base_url)
        self._timeout = timeout
        # The environment's proxy and CA settings apply (``HTTP(S)_PROXY``, ``SSL_CERT_FILE``).
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(transport=transport)

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> Briq:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def create_sandbox(
        self,
        *,
        name: str | None = None,
        size: str | None = None,
        image: str | None = None,
        env: Mapping[str, str] | None = None,
        secret_env: Mapping[str, str] | None = None,
        sleep: str | None = None,
        wait_timeout: float = 120.0,
        idempotency_key: str | None = None,
    ) -> Sandbox:
        """Make a sandbox and wait until it runs.

        ``env`` values show in the sandbox's status; ``secret_env`` values are kept write-only
        (the API must support it, else ``unsupported``). The request is sent once. When no
        usable answer comes, the error is ``outcome_unknown``: the sandbox may exist, and the
        same ``idempotency_key`` makes a second call return it instead of another.
        """
        info = self._run(
            f.create_sandbox(
                name=name,
                size=size,
                env=env,
                secret_env=secret_env,
                sleep=sleep,
                image=image,
                wait_timeout=wait_timeout,
                idempotency_key=idempotency_key,
            )
        )
        return Sandbox(self, info)

    def sandbox(self, id: str) -> Sandbox:
        """Reconnect to a sandbox by id. It must exist: ``not_found`` otherwise."""
        return Sandbox(self, self._run(f.get_sandbox(id)))

    # --- the driver ---

    def _send(self, request: f.Request) -> f.Reply:
        built = self._http.build_request(
            request.method,
            self._origin + request.path,
            params=request.query,
            json=request.json,
            content=request.content,
            headers=headers(self._key, request.headers),
            timeout=timeout_of(request.timeout or self._timeout),
        )
        try:
            response = self._http.send(built, stream=True, follow_redirects=False)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            raise f.NoAnswer(never_sent=True) from exc
        except httpx.HTTPError as exc:
            raise f.NoAnswer(never_sent=False) from exc
        try:
            body = bytearray()
            too_large = False
            for chunk in response.iter_bytes():
                body += chunk
                if len(body) > request.max_bytes:
                    too_large = True
                    break
        except httpx.HTTPError as exc:
            raise f.NoAnswer(never_sent=False) from exc
        finally:
            response.close()
        return f.Reply(
            response.status_code,
            dict(response.headers),
            bytes(body),
            too_large,
            response.num_bytes_downloaded,
        )

    def _perform(self, step: f.Request | f.Pause | f.Emit) -> Any:
        if isinstance(step, f.Request):
            return self._send(step)
        if isinstance(step, f.Pause):
            _pause(step.seconds)
        else:
            step.callback(step.chunk)
        return None

    def _run(self, flow: f.Flow[T]) -> T:
        """Drive a flow: perform what it yields, send back the answer, throw in what fails."""
        answer: Any = None
        error: Exception | None = None
        try:
            while True:
                try:
                    step = flow.throw(error) if error else flow.send(answer)
                except StopIteration as done:
                    return done.value  # type: ignore[no-any-return]
                answer, error = None, None
                try:
                    answer = self._perform(step)
                except Exception as exc:
                    error = exc
                except BaseException as interrupt:
                    self._unwind(flow, interrupt)
        finally:
            flow.close()

    def _unwind(self, flow: f.Flow[T], interrupt: BaseException) -> None:
        """An interrupt (``KeyboardInterrupt``...) reached the driver: let the flow clean up.

        The flow gets it thrown in, may yield more steps to undo what it started (destroy a
        sandbox, stop a command), and ends by raising it again. A step of the cleanup that fails
        is thrown into the flow, which decides what it means.
        """
        thrown: BaseException | None = interrupt
        answer: Any = None
        while True:
            try:
                step = flow.throw(thrown) if thrown else flow.send(answer)
            except StopIteration:
                raise interrupt from None
            thrown, answer = None, None
            try:
                answer = self._perform(step)
            except Exception as exc:
                thrown = exc


class Sandbox:
    """One sandbox. ``id`` is all that is needed to reconnect to it with ``Briq.sandbox``."""

    def __init__(self, client: Briq, info: SandboxInfo) -> None:
        self._client = client
        self.info = info

    @property
    def id(self) -> str:
        return self.info.id

    def refresh(self) -> SandboxInfo:
        """Read the sandbox's state again."""
        self.info = self._client._run(f.get_sandbox(self.id))
        return self.info

    def exec(
        self,
        command: str | Sequence[str],
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        stdin: str | None = None,
        timeout: float = 60.0,
        on_output: Callable[[OutputChunk], Any] | None = None,
    ) -> ExecResult:
        """Run a command and wait for it.

        A string runs in a shell, a list is the program and its arguments. With ``on_output`` the
        output reaches it as it is written, and the command may run up to 24 hours. A command that
        outlives ``timeout`` is stopped: ``timed_out`` is true and ``exit_code`` is ``None``.
        A command is sent once; when its end is not known the error is ``outcome_unknown``.
        """
        return self._client._run(
            f.exec_command(
                self.id,
                command,
                cwd=cwd,
                env=env,
                stdin=stdin,
                timeout=timeout,
                on_output=on_output,
            )
        )

    def write_file(self, path: str, data: bytes, *, mode: int | None = None) -> None:
        """Write a file, and any directory above it that is missing. ``mode`` is e.g. ``0o755``."""
        self._client._run(f.write_file(self.id, path, data, mode))

    def read_file(self, path: str) -> bytes:
        """The bytes of a file. ``file_not_found`` when it is not there."""
        return self._client._run(f.read_file(self.id, path))

    def list_files(self, path: str) -> list[FileInfo]:
        """The entries of a directory. Needs ``python3`` in the sandbox."""
        return self._client._run(f.list_files(self.id, path))

    def remove(self, path: str, *, recursive: bool = False) -> None:
        """Remove a file, or with ``recursive`` a directory. A path that is not there is fine."""
        self._client._run(f.remove(self.id, path, recursive))

    def preview_url(self, port: int, *, public: bool = False) -> str:
        """A URL for a port of the sandbox. Private: only whoever holds the URL can open it."""
        return self._client._run(f.preview_url(self.id, port, public))

    def stop(self) -> SandboxInfo:
        """Stop it; the disk is kept and nothing is billed until ``start``."""
        self.info = self._client._run(f.stop_sandbox(self.id))
        return self.info

    def start(self, *, wait_timeout: float = 120.0) -> SandboxInfo:
        """Start a stopped or sleeping sandbox and wait until it runs. Any call wakes it too."""
        self.info = self._client._run(f.start_sandbox(self.id, wait_timeout))
        return self.info

    def set_sleep(self, after: str) -> SandboxInfo:
        """How long without use before it sleeps: ``"30m"``, ``"3h"``, ``"7d"`` or ``"never"``."""
        self.info = self._client._run(f.set_sleep(self.id, after))
        return self.info

    def destroy(self) -> None:
        """Destroy it and wait until it is gone. Destroying one that is gone is fine."""
        self._client._run(f.destroy(self.id))

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Destroy it when the block ends."""
        try:
            self.destroy()
        except BriqError:
            if exc is None:
                raise
