# briq-cloud

Python client for [briq](https://briq.cloud). Create a sandbox, run commands in it, move files in and out.

```python
from briq_cloud import Briq

with Briq() as briq:  # reads BRIQ_API_KEY
    with briq.create_sandbox(sleep="30m") as sandbox:  # destroyed when the block ends
        sandbox.write_file("/work/hello.py", b"print('hello')")
        result = sandbox.exec("python3 hello.py", cwd="/work")
        print(result.stdout, result.exit_code)
        print(sandbox.read_file("/work/hello.py"))
        print(sandbox.preview_url(8000))
```

The async client has the same methods, awaited:

```python
from briq_cloud import AsyncBriq

async with AsyncBriq() as briq:
    sandbox = await briq.create_sandbox()
    result = await sandbox.exec(["ls", "-la"])
    await sandbox.destroy()
```

Install: `pip install briq-cloud`. Python 3.10 or later. The only dependency is `httpx`.

## Configure

| Argument   | Default                  | Environment variable |
| ---------- | ------------------------ | -------------------- |
| `api_key`  | none, required           | `BRIQ_API_KEY`       |
| `base_url` | `https://api.briq.cloud` | `BRIQ_API_URL`       |

`timeout` is the seconds one request may take. The client reads the usual proxy and certificate variables (`HTTPS_PROXY`, `SSL_CERT_FILE`). To set up the connection yourself, pass `http_client=` an `httpx.Client` (`httpx.AsyncClient` for `AsyncBriq`): it is used as it is, `close` leaves it open, and a redirect is never followed, whatever it says. `transport` takes an `httpx` transport, for tests.

### Regional commands

```python
with Briq(execution_mode="auto") as briq:
    sandbox = briq.sandbox("bq_...")
    result = sandbox.exec(["python3", "--version"])
```

`execution_mode` defaults to `"central"`. `"auto"` asks the API for regional access before each synchronous command and uses central execution when discovery explicitly reports it unsupported. Authentication, quota, network errors and invalid access descriptors do not trigger fallback. After regional submission, any uncertain reply raises `outcome_unknown`; the command is never sent again.

`"regional"` requires regional access. It refuses commands over 60 seconds or with `on_output` before discovery. With `"auto"`, those commands keep using central execution. File operations and sandbox management use the central API in every mode. Availability depends on the connected API.

Regional execution uses a separate HTTP client with its own scoped credential. Its default connection ignores environment proxies and certificate overrides. If you supply `http_client` or `transport` for the central API, also supply a separate `execution_http_client` or `execution_transport` to use regional execution. Otherwise `"auto"` stays central and `"regional"` raises `unavailable`. A supplied execution client remains open when briq closes. Default client headers, cookies and authentication are not forwarded to execution origins; configure proxies and certificates on the separate execution client itself.

## Sandbox

| Method                               | Does                                                          |
| ------------------------------------ | ------------------------------------------------------------- |
| `briq.create_sandbox(...)`           | Makes a sandbox, waits until it runs.                         |
| `briq.sandbox(id)`                   | Reconnects to a sandbox by id.                                |
| `sandbox.exec(command, ...)`         | Runs a command, returns `ExecResult`.                         |
| `sandbox.write_file(path, data)`     | Writes bytes, creates missing directories.                    |
| `sandbox.read_file(path)`            | Returns the bytes of a file.                                  |
| `sandbox.list_files(path)`           | Returns the entries of a directory. Needs `python3` inside.   |
| `sandbox.remove(path, recursive=)`   | Removes a file or a directory.                                |
| `sandbox.preview_url(port)`          | Returns a URL for a port. `public=True` for anyone.           |
| `sandbox.stop()` / `sandbox.start()` | Stops it, keeps the disk, bills nothing. Starts it again.     |
| `sandbox.set_image("ubuntu:24.04")` | Changes a stopped sandbox image; start it explicitly afterward. |
| `sandbox.set_sleep("30m")`           | Sets the idle time before it sleeps. `"never"` stays awake.   |
| `sandbox.refresh()`                  | Reads its state again.                                        |
| `sandbox.destroy()`                  | Removes it and its disk.                                      |

Commands and file access wake a sleeping sandbox. Call `stop()` before `set_image()`.
If `set_image()` returns `state="resizing"`, use `refresh()` until it finishes before calling `start()`.

### Run a command

```python
sandbox.exec(
    "make test",
    cwd="/work",
    env={"CI": "1"},
    timeout=600,
    on_output=lambda chunk: print(chunk.text, end=""),
)
```

- A string runs in a shell. A list is the program and its arguments.
- `timeout` is in seconds, up to 24 hours. A command that outlives it is stopped: `timed_out` is true and `exit_code` is `None`.
- Without `on_output`, a command up to 60 seconds runs in one request.
- With `on_output`, or past 60 seconds, the output reaches `on_output` as it is written. Both streams are read, in order within each stream.
- `stdout` and `stderr` are text. Read bytes with `read_file`.

### Secrets

```python
briq.create_sandbox(env={"MODE": "test"}, secret_env={"API_TOKEN": token})
```

`env` values show in the sandbox's status. `secret_env` values are write-only. If the API cannot keep them so, the call raises `unsupported`.

## Errors

Every failure is a `BriqError` with a stable `code`.

| `code`             | Means                                                          | Try again?      |
| ------------------ | -------------------------------------------------------------- | --------------- |
| `auth`             | No key, a wrong key, or a key that may not do this.            | No              |
| `not_found`        | The sandbox does not exist.                                    | No              |
| `file_not_found`   | The file or directory does not exist.                          | No              |
| `quota`            | A limit stopped it: credits, plan, rate limit, size, a file.   | Later           |
| `unavailable`      | briq could not take the call. Nothing was applied.             | Yes             |
| `invalid_argument` | The call is wrong.                                             | No              |
| `provider`         | A refusal that fits no other code.                             | Maybe           |
| `outcome_unknown`  | The call was sent. Its result is not known.                    | Check first     |
| `timeout`          | A deadline of the client ended the call.                       | Maybe           |
| `unsupported`      | This API or image cannot do it.                                | No              |
| `aborted`          | The command was stopped by someone else.                       | No              |

`quota` also covers HTTP 413 and the API's `plan_limit` refusals: trying again changes nothing until the limit does. A file over 100 MiB is `quota`, from `read_file` and `write_file`.

Also on the error: `status`, `api_code`, `request_id` (quote it when you ask for help), `retry_after`, `details`, `idempotency_key` and `sandbox_id`. `details` holds codes, ids and numbers (`{"limit": "size", "plan": "hobby"}`); the text a provider gave is never kept.

A command and its directory may total 8,192 bytes. A longer one raises `invalid_argument` before anything is sent: write the script with `write_file` and run that file.

The client never repeats a call whose effect it cannot see. When a command's end is not known, or a sandbox's creation got no answer, it raises `outcome_unknown`. To retry a creation safely, pass the same `idempotency_key` again: briq returns the first sandbox. Without a key of your own the client makes one, and `error.idempotency_key` is that key.

```python
try:
    sandbox = briq.create_sandbox()
except BriqError as error:
    if error.code == "outcome_unknown" and error.idempotency_key:
        sandbox = briq.create_sandbox(idempotency_key=error.idempotency_key)
    else:
        raise
```

If creating fails after the sandbox exists (it did not start in time, or the call was cancelled or interrupted), the client destroys it and raises the original error. When it cannot destroy it, the error is `outcome_unknown` with `error.sandbox_id`: call `briq.sandbox(error.sandbox_id).destroy()`. The cause of the failure is `error.__cause__`.

### Cancel a call

A cancelled task (`asyncio.wait_for` timing out, `task.cancel()`) or a `KeyboardInterrupt` ends a call cleanly. A sandbox made by the call is destroyed, even though the cancel is running, and a command started with `on_output` is stopped. The cancel or the interrupt is raised again as it was; a cleanup that fails never replaces it. A cancel that arrives while the create request is still in flight cannot be undone, because the id is not known yet: pass your own `idempotency_key` and repeat the call to get the sandbox.

## License

MIT
