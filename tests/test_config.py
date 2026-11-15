from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from failslice.config import (
    ConfigError,
    ExpectPredicate,
    ForeignKey,
    RaisesPredicate,
    SqlPredicate,
    TableSpec,
    load_project,
    parse_project,
    topological_order,
)


def _cfg(**over: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "tables": {"a": {"path": "a.csv"}, "b": {"path": "b.parquet", "primary_key": "id"}},
        "foreign_keys": ["a.b_id -> b.id"],
        "predicate": {"sql": "SELECT true"},
    }
    cfg.update(over)
    return cfg


def test_parse_minimal(tmp_path: Path) -> None:
    p = parse_project(_cfg(), base_dir=tmp_path)
    assert p.topo_order == ("b", "a")
    assert p.foreign_keys == (ForeignKey("a", "b_id", "b", "id"),)
    assert isinstance(p.predicate, SqlPredicate)
    assert p.table_path("a") == (tmp_path / "a.csv").resolve()
    assert p.descendants("b") == {"a"}
    assert p.parents_of("a") == p.foreign_keys
    assert p.predicate_queries() == ("SELECT true",)
    assert p.settings.seed == 0 and p.settings.threads == 1


def test_load_project_from_yaml(tmp_path: Path) -> None:
    (tmp_path / "failslice.yaml").write_text(
        "tables:\n  t: {path: t.csv}\npredicate:\n  sql: SELECT true\nsettings: {seed: 3}\n"
    )
    p = load_project(tmp_path / "failslice.yaml")
    assert p.settings.seed == 3
    assert p.base_dir == tmp_path.resolve()


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("tables: [", "invalid YAML"),
        ("- 1\n", "must be a mapping"),
        ("tables: {}\npredicate: {sql: SELECT true}\n", "at least one table"),
        ("tables: {t: {path: t.csv}}\npredicate: {sql: x}\nbogus: 1\n", "bogus"),
    ],
)
def test_load_project_errors(tmp_path: Path, text: str, match: str) -> None:
    (tmp_path / "f.yaml").write_text(text)
    with pytest.raises(ConfigError, match=match):
        load_project(tmp_path / "f.yaml")


def test_missing_config_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read"):
        load_project(tmp_path / "nope.yaml")


@pytest.mark.parametrize(
    ("over", "match"),
    [
        ({"foreign_keys": ["a.x -> b.id", "b.y -> a.z"]}, r"cycle.*a -> b -> a|cycle.*b -> a -> b"),
        ({"foreign_keys": ["a.x -> a.id"]}, "self-referencing"),
        ({"foreign_keys": ["a.x -> c.id"]}, "unknown table 'c'"),
        ({"foreign_keys": ["a.x-b.id"]}, "must look like"),
        ({"foreign_keys": ["a.x -> b.id", "a.x -> b.id"]}, "declared twice"),
        ({"tables": {"1bad": {"path": "x.csv"}}, "foreign_keys": []}, "plain SQL identifier"),
        ({"tables": {"__fs_t": {"path": "x.csv"}}, "foreign_keys": []}, "reserved prefix"),
        (
            {"tables": {"T": {"path": "x.csv"}, "t": {"path": "y.csv"}}, "foreign_keys": []},
            "ignoring case",
        ),
        ({"predicate": {"sql": "x", "raises": {}}}, "exactly one"),
        ({"predicate": {"nope": "x"}}, "exactly one"),
        ({"predicate": {"sql": 1}}, "must be a string"),
        ({"predicate": {"expect": "x"}}, "must be a mapping"),
        ({"predicate": {"expect": {"query": "SELECT 1"}}}, "exactly one of"),
        (
            {"predicate": {"expect": {"query": "q", "expected": 1, "expected_file": "f"}}},
            "exactly one of",
        ),
        ({"predicate": {"raises": {"query": "q", "match": "("}}}, "invalid regex"),
        ({"predicate": {"raises": {"query": "q"}}}, "match"),
        ({"settings": {"threads": 0}}, "threads"),
        (
            {"tables": {"a": {"path": "../outside.csv"}}, "foreign_keys": []},
            "outside the allowed root",
        ),
    ],
)
def test_config_errors(tmp_path: Path, over: dict[str, Any], match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        parse_project(_cfg(**over), base_dir=tmp_path)


def test_root_can_be_widened(tmp_path: Path) -> None:
    sub = tmp_path / "proj"
    sub.mkdir()
    cfg = _cfg(tables={"a": {"path": "../a.csv"}}, foreign_keys=[])
    p = parse_project(cfg, base_dir=sub, root=tmp_path)
    assert p.table_path("a") == (tmp_path / "a.csv").resolve()


def test_symlink_escape_is_rejected(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "a.csv").write_text("x\n1\n")
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "a.csv").symlink_to(outside / "a.csv")
    with pytest.raises(ConfigError, match="outside the allowed root"):
        parse_project(_cfg(tables={"a": {"path": "a.csv"}}, foreign_keys=[]), base_dir=proj)


def test_expected_file_must_be_inside_root(tmp_path: Path) -> None:
    cfg = _cfg(predicate={"expect": {"query": "SELECT 1", "expected_file": "/etc/passwd"}})
    with pytest.raises(ConfigError, match="outside"):
        parse_project(cfg, base_dir=tmp_path)


def test_predicate_forms(tmp_path: Path) -> None:
    e = parse_project(
        _cfg(predicate={"expect": {"query": "SELECT 1", "expected": [[1]], "ordered": True}}),
        base_dir=tmp_path,
    ).predicate
    assert isinstance(e, ExpectPredicate) and e.ordered and e.mode == "differs"
    r = parse_project(
        _cfg(predicate={"raises": {"query": "SELECT 1", "match": "conv", "flags": ["IGNORECASE"]}}),
        base_dir=tmp_path,
    ).predicate
    assert isinstance(r, RaisesPredicate)
    assert r.compiled().search("CONVERSION")


@pytest.mark.parametrize(
    ("path", "fmt"),
    [("x.csv", "csv"), ("x.CSV.gz", "csv"), ("x.parquet", "parquet"), ("x.duckdb", "duckdb")],
)
def test_format_inference(path: str, fmt: str) -> None:
    assert TableSpec(path=path).resolved_format() == fmt


def test_format_inference_failure() -> None:
    with pytest.raises(ConfigError, match="cannot infer"):
        TableSpec(path="x.json").resolved_format()
    assert TableSpec(path="x.json", format="csv").resolved_format() == "csv"


def test_topological_order_is_deterministic() -> None:
    fks = [ForeignKey.parse("c.x -> a.id"), ForeignKey.parse("c.y -> b.id")]
    assert topological_order(["c", "b", "a", "d"], fks) == ("a", "b", "c", "d")
    assert str(fks[0]) == "c.x -> a.id"


def test_with_settings(tmp_path: Path) -> None:
    p = parse_project(_cfg(), base_dir=tmp_path).with_settings(seed=9, max_evals=10)
    assert (p.settings.seed, p.settings.max_evals) == (9, 10)
    with pytest.raises(ConfigError, match="max_evals"):
        p.with_settings(max_evals=0)
