"""The package and its README are published: they must not name our infrastructure or code.

The same patterns as the TypeScript packages' check (`packages/cli/src/published.test.ts`).
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
INTERNAL = [
    r"\bfly(?:\.io)?\b",
    r"firecracker",
    r"stripe",
    r"railway",
    r"cloudflare",
    r"hetzner|\bovh\b|scaleway",
    r"\bADR\b",
    r"\bapps/",
    r"bqt-",
    r"@briq/(?:core|api|db|providers|recipes|mcp)\b",
    r"\bledger\b",
    r"briqyard",
]
# The two variables the client reads are part of its interface.
ALLOWED_ENV = {"BRIQ_API_KEY", "BRIQ_API_URL"}


def published() -> list[Path]:
    return [*sorted((ROOT / "src").rglob("*.py")), ROOT / "README.md"]


def test_nothing_published_names_our_infrastructure() -> None:
    # The snapshot is not published (the sdist leaves it out), but a refresh must not bring
    # wording of the API's reference into the repository's public copy either.
    for path in [*published(), ROOT / "tests" / "openapi.json"]:
        text = path.read_text()
        for pattern in INTERNAL:
            assert not re.search(pattern, text, re.IGNORECASE), f"{path.name} matches {pattern}"


def test_the_only_variables_named_are_the_two_the_client_reads() -> None:
    for path in published():
        named = set(re.findall(r"\bBRIQ_[A-Z_]+\b", path.read_text()))
        assert named <= ALLOWED_ENV, f"{path.name} names {sorted(named - ALLOWED_ENV)}"


def test_the_sdist_holds_only_what_a_consumer_needs() -> None:
    text = (ROOT / "pyproject.toml").read_text()
    include = re.search(
        r"\[tool\.hatch\.build\.targets\.sdist\]\n(?:#.*\n)*include = (\[.*\])", text
    )
    assert include, "the sdist must list what it includes"
    assert set(re.findall(r'"([^"]+)"', include.group(1))) == {
        "src",
        "README.md",
        "LICENSE",
        "pyproject.toml",
    }
