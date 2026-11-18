"""Release metadata stays consistent (ADR 0013): versions, the MCP shim, server.json, README."""

from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml

import failslice

REPO = Path(__file__).resolve().parents[1]
VERSION = failslice.__version__
SERVER_NAME = "io.github.sdmhdn/failslice"


def _toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _server() -> dict[str, Any]:
    return dict(json.loads((REPO / "server.json").read_text(encoding="utf-8")))


def test_version_is_a_plain_release() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", VERSION)


def test_shim_matches_the_core_version_and_pins_it_exactly() -> None:
    shim = _toml(REPO / "packages" / "failslice-mcp" / "pyproject.toml")["project"]
    assert shim["name"] == "failslice-mcp"
    assert shim["version"] == VERSION
    assert shim["dependencies"] == [f"failslice=={VERSION}"]
    assert shim["scripts"] == {"failslice-mcp": "failslice.mcp_server:main"}
    core = _toml(REPO / "pyproject.toml")["project"]
    assert "failslice-mcp" not in core["scripts"]  # one owner per console script
    assert shim["requires-python"] == core["requires-python"]
    assert shim["authors"] == core["authors"] and shim["license"] == core["license"]
    shim_license = REPO / "packages" / "failslice-mcp" / "LICENSE"
    assert shim_license.read_bytes() == (REPO / "LICENSE").read_bytes()


def test_citation_and_changelog_name_the_version() -> None:
    citation = yaml.safe_load((REPO / "CITATION.cff").read_text(encoding="utf-8"))
    assert str(citation["version"]) == VERSION
    changelog = (REPO / "CHANGELOG.md").read_text(encoding="utf-8")
    assert re.search(rf"^## \[?{re.escape(VERSION)}\]?", changelog, re.MULTILINE)


def test_server_json_is_valid_and_points_at_the_shim() -> None:
    server = _server()
    schema = json.loads(
        (REPO / "tests" / "data" / "server.schema.2025-12-11.json").read_text(encoding="utf-8")
    )
    assert server["$schema"] == schema["$id"]
    jsonschema.validate(server, schema)
    assert server["name"] == SERVER_NAME and server["version"] == VERSION
    (package,) = server["packages"]
    assert package["registryType"] == "pypi" and package["identifier"] == "failslice-mcp"
    assert package["version"] == VERSION and package["transport"] == {"type": "stdio"}
    (root,) = package["packageArguments"]
    assert root["name"] == "--root" and root["isRequired"] is True


@pytest.mark.parametrize("readme", ["README.md", "packages/failslice-mcp/README.md"])
def test_readmes_carry_the_registry_ownership_marker(readme: str) -> None:
    text = (REPO / readme).read_text(encoding="utf-8")
    assert f"<!-- mcp-name: {SERVER_NAME} -->" in text


@pytest.mark.parametrize("readme", ["README.md", "packages/failslice-mcp/README.md"])
def test_readme_links_work_on_pypi(readme: str) -> None:
    """PyPI renders the README without the repository, so links must be absolute."""
    text = (REPO / readme).read_text(encoding="utf-8")
    targets = re.findall(r"\]\(([^)\s]+)\)", text) + re.findall(r'(?:src|href)="([^"]+)"', text)
    relative = [t for t in targets if not re.match(r"(https://|mailto:|#)", t)]
    assert relative == [], f"relative links in {readme}: {relative}"


def test_readme_has_no_unpublished_install_instructions() -> None:
    text = (REPO / "README.md").read_text(encoding="utf-8")
    assert "Not published on PyPI yet" not in text
    assert "uvx --from failslice failslice-mcp" not in text
