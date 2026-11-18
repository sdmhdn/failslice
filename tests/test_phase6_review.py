"""Phase 6 review: no spill files outside the root; inputs must be regular files.

No DuckDB connection of FailSlice may spill to disk.

An in-memory DuckDB database spills to ``.tmp`` in the working directory by default,
which can be outside the allowed root; every connection sets ``temp_directory = ''``.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest
import yaml

import failslice
from failslice.config import parse_project
from failslice.engine import Engine
from failslice.inference import render_config, suggest_relationships
from failslice.mcp_server import ConfigIn

PACKAGE = Path(failslice.__file__).parent


def _connect_calls() -> list[tuple[str, ast.Call]]:
    calls = []
    for path in sorted(PACKAGE.rglob("*.py")):
        if "_demo" in path.parts:  # the user's data generator, not FailSlice's engine
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "connect"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "duckdb"
            ):
                calls.append((f"{path.name}:{node.lineno}", node))
    return calls


def test_every_connection_disables_spilling() -> None:
    calls = _connect_calls()
    assert len(calls) >= 9
    missing = []
    for where, call in calls:
        config = next((k.value for k in call.keywords if k.arg == "config"), None)
        ok = isinstance(config, ast.Dict) and any(
            isinstance(k, ast.Constant)
            and k.value == "temp_directory"
            and isinstance(v, ast.Constant)
            and v.value == ""
            for k, v in zip(config.keys, config.values, strict=True)
        )
        if not ok:
            missing.append(where)
    assert missing == []


def test_engine_connection_has_no_temp_directory(tmp_path: Path) -> None:
    (tmp_path / "t.csv").write_text("id\n1\n", encoding="utf-8")
    (tmp_path / "failslice.yaml").write_text(
        "version: 1\ntables: { t: { path: t.csv } }\npredicate: { sql: SELECT true }\n",
        encoding="utf-8",
    )
    engine = Engine(failslice.load_project(tmp_path / "failslice.yaml"))
    try:
        row = engine.con.execute("SELECT current_setting('temp_directory')").fetchone()
    finally:
        engine.close()
    assert row == ("",)


@pytest.mark.parametrize("kind", ["fifo", "directory"])
def test_expected_file_must_be_a_regular_file(tmp_path: Path, kind: str) -> None:
    # A FIFO would block hashing (Phase 6 review); refused before it is opened.
    if kind == "fifo":
        if not hasattr(os, "mkfifo"):
            pytest.skip("no FIFOs on this platform")
        os.mkfifo(tmp_path / "expected.csv")
    else:
        (tmp_path / "expected.csv").mkdir()
    (tmp_path / "t.csv").write_text("id\n1\n", encoding="utf-8")
    (tmp_path / "failslice.yaml").write_text(
        "version: 1\ntables: { t: { path: t.csv } }\n"
        "predicate: { expect: { query: SELECT 1, expected_file: expected.csv } }\n",
        encoding="utf-8",
    )
    project = failslice.load_project(tmp_path / "failslice.yaml")
    with pytest.raises(failslice.ConfigError, match="not a regular file"):
        Engine(project)


def test_init_draft_is_accepted_by_file_and_inline_mcp_config(tmp_path: Path) -> None:
    # An agent may turn the YAML that `init` writes into an inline MCP config.

    (tmp_path / "customers.csv").write_text("id\n1\n2\n", encoding="utf-8")
    (tmp_path / "orders.csv").write_text("order_id,customer_id\n10,1\n11,2\n", encoding="utf-8")
    report = suggest_relationships(
        {"customers": tmp_path / "customers.csv", "orders": tmp_path / "orders.csv"},
        root=tmp_path,
        base_dir=tmp_path,
        sample_rows=None,
    )
    assert report.foreign_keys  # the draft has the empty `foreign_keys:` block
    data = yaml.safe_load(render_config(report))
    assert data["foreign_keys"] is None
    assert ConfigIn.model_validate(data).foreign_keys == []
    assert parse_project(data, base_dir=tmp_path).foreign_keys == ()
