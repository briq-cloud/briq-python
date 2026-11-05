# briq Python client

The source of [`briq-cloud`](https://pypi.org/project/briq-cloud/), the Python client for the
[briq](https://briq.cloud) REST API: create a sandbox, run commands in it, move files in and out.

## Install

```bash
pip install briq-cloud
```

```python
from briq_cloud import Briq

with Briq() as briq:  # reads BRIQ_API_KEY
    with briq.create_sandbox(sleep="30m") as sandbox:  # destroyed when the block ends
        print(sandbox.exec("echo hello").stdout)
```

More: [packages/python/README.md](packages/python/README.md) and the
[API reference](https://briq.cloud/docs/api/reference).

## Build and test

From `packages/python`, with [uv](https://docs.astral.sh/uv/) and Python 3.10 or later:

```bash
uv sync
uv run pytest
uv run ruff check . && uv run mypy
uv build
```

## Changes

Each release writes this repo again from briq's own repository: a change made here is replaced by
the next release. For a bug or an idea, write to [hello@briq.cloud](mailto:hello@briq.cloud).
