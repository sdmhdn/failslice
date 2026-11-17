"""File-backed tests for reviewable PK/FK suggestions and the init draft."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml
from pydantic import ValidationError

from failslice.config import ConfigError, Settings, parse_project
from failslice.engine import _lit
from failslice.inference import (
    ColumnProfile,
    ForeignKeySuggestion,
    InferenceReport,
    PrimaryKeySuggestion,
    TableProfile,
    discover_tables,
    render_config,
    suggest_relationships,
)
from tests.conftest import posix_only


def _write(path: Path, query: str) -> Path:
    """Write a real CSV/Parquet fixture through DuckDB COPY."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = "csv, HEADER true" if path.suffix.lower() == ".csv" else "parquet"
    with duckdb.connect(":memory:") as con:
        con.execute(f"COPY ({query}) TO {_lit(str(path))} (FORMAT {fmt})")
    return path


def _suggest(
    directory: Path, queries: dict[str, str], *, sample_rows: int | None = 10_000
) -> InferenceReport:
    """Profile a collection of files without depending on discovery."""
    tables = {name: _write(directory / f"{name}.parquet", query) for name, query in queries.items()}
    return suggest_relationships(
        tables, root=directory, base_dir=directory, sample_rows=sample_rows
    )


def test_discover_normalizes_and_sorts_without_recursing(tmp_path: Path) -> None:
    filenames = ["z.csv", "Mixed Case-Name!.parquet", "123.PQ", "no.csv"]
    for filename in reversed(filenames):
        _write(tmp_path / filename, "SELECT 1 AS id")
    _write(tmp_path / "nested" / "hidden.csv", "SELECT 1 AS id")
    _write(tmp_path / "ignored.txt", "SELECT 1 AS id")
    (tmp_path / "directory.csv").mkdir()
    tables = discover_tables(tmp_path, root=tmp_path)
    assert list(tables) == ["t_123", "mixed_case_name_", "no", "z"]
    assert list(tables.values()) == [tmp_path / name for name in sorted(filenames)]
    report = suggest_relationships(tables, root=tmp_path, base_dir=tmp_path)
    assert {table.name: table.format for table in report.tables} == {
        "t_123": "parquet",
        "mixed_case_name_": "parquet",
        "no": "csv",
        "z": "csv",
    }
    project = parse_project(yaml.safe_load(render_config(report)), base_dir=tmp_path)
    assert set(project.tables) == set(tables)


@pytest.mark.parametrize(
    "filenames", [("A-b.csv", "a_b.pq"), ("a.csv", "a.parquet"), ("A.csv", "a.csv")]
)
def test_discover_rejects_duplicate_names(tmp_path: Path, filenames: tuple[str, str]) -> None:
    for filename in filenames:
        _write(tmp_path / filename, "SELECT 1 AS id")
    with pytest.raises(ConfigError, match="duplicate table name"):
        discover_tables(tmp_path, root=tmp_path)


def test_discover_rejects_reserved_prefix(tmp_path: Path) -> None:
    _write(tmp_path / "__FS-private.csv", "SELECT 1 AS id")
    with pytest.raises(ConfigError, match="reserved prefix"):
        discover_tables(tmp_path, root=tmp_path)


def test_symlinks_are_resolved_and_root_escape_refused(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = _write(tmp_path / "outside.parquet", "SELECT 1 AS id")
    alias = root / "alias.parquet"
    alias.symlink_to(outside)
    with pytest.raises(ConfigError, match="outside the allowed root"):
        discover_tables(root, root=root)
    with pytest.raises(ConfigError, match="outside the allowed root"):
        suggest_relationships({"alias": alias}, root=root, base_dir=root)
    alias.unlink()
    inside = _write(root / "inside.parquet", "SELECT 1 AS id")
    alias.symlink_to(inside)
    assert discover_tables(root, root=root)["alias"] == inside.resolve()


@pytest.mark.parametrize("character", ["*", "?", "[", "]", "{", "}"])
def test_glob_characters_are_refused(tmp_path: Path, character: str) -> None:
    path = _write(tmp_path / f"bad{character}.parquet", "SELECT 1 AS id")
    with pytest.raises(ConfigError, match="glob character"):
        discover_tables(tmp_path, root=tmp_path)
    with pytest.raises(ConfigError, match="glob character"):
        suggest_relationships({"bad": path}, root=tmp_path, base_dir=tmp_path)


@posix_only
def test_glob_directory_and_glob_named_symlink_are_refused(tmp_path: Path) -> None:
    path = _write(tmp_path / "glob[dir]" / "data.parquet", "SELECT 1 AS id")
    with pytest.raises(ConfigError, match="glob character"):
        discover_tables(path.parent, root=tmp_path)
    target = _write(tmp_path / "valid.parquet", "SELECT 1 AS id")
    (tmp_path / "alias*.parquet").symlink_to(target)
    with pytest.raises(ConfigError, match="glob character"):
        discover_tables(tmp_path, root=tmp_path)


@pytest.mark.parametrize("column", ["id", "orders_id", "order_id", "ID"])
def test_pk_high_for_conventional_names(tmp_path: Path, column: str) -> None:
    report = _suggest(tmp_path, {"orders": f'SELECT i AS "{column}" FROM range(3) r(i)'})
    (pk,) = report.primary_keys
    assert pk.table == "orders" and pk.column == column and pk.confidence == "high"
    assert pk.rows_examined == 3 and pk.complete_scan
    assert pk.nulls == 0 and pk.duplicates == 0
    assert "full scan" in pk.reason
    assert report.tables[0].columns == (
        ColumnProfile(name=column, type="BIGINT", nulls=0, distinct=3),
    )


def test_pk_medium_for_identifier_names_and_quoted_identifiers(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {"t": 'SELECT i AS "a""key_code", i AS external_code, 1 AS repeated FROM range(3) r(i)'},
    )
    assert [(pk.column, pk.confidence) for pk in report.primary_keys] == [
        ('a"key_code', "medium"),
        ("external_code", "medium"),
    ]
    text = render_config(report)
    assert '# primary_key: "a\\"key_code"' in text
    assert parse_project(yaml.safe_load(text), base_dir=tmp_path).tables["t"].primary_key is None


@pytest.mark.parametrize("column", ["external_id", "external_key", "external_code", "uuid", "UUID"])
def test_pk_medium_for_unique_identifier_like_names(tmp_path: Path, column: str) -> None:
    report = _suggest(tmp_path, {"customers": f'SELECT i AS "{column}" FROM range(3) r(i)'})
    (pk,) = report.primary_keys
    assert pk.column == column and pk.confidence == "medium"


def test_unique_names_and_other_ordinary_columns_are_not_pk_suggestions(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT i AS id, 'customer ' || i AS name, 'email' || i AS email, "
            "i AS amount, i AS code, i AS key, i AS uuid_name FROM range(3) r(i)"
        },
    )
    assert [(pk.column, pk.confidence) for pk in report.primary_keys] == [("id", "high")]
    assert {col.name for col in report.tables[0].columns if col.distinct == 3} == {
        "id",
        "name",
        "email",
        "amount",
        "code",
        "key",
        "uuid_name",
    }
    assert "# primary_key: name" not in render_config(report)


@pytest.mark.parametrize("column", ["id", "orders_id", "order_id"])
def test_pk_duplicate_rows_and_nulls_are_separate_evidence(tmp_path: Path, column: str) -> None:
    report = _suggest(
        tmp_path,
        {
            "orders": f'SELECT v AS "{column}", v AS ordinary '
            "FROM (VALUES (1), (1), (1), (NULL), (NULL)) r(v)"
        },
    )
    (pk,) = report.primary_keys
    assert pk.confidence == "low" and pk.complete_scan and pk.rows_examined == 5
    assert pk.nulls == 2 and pk.duplicates == 2
    assert "nulls or duplicate rows weaken this suggestion" in pk.reason
    assert "duplicates are often the bug" in pk.reason
    assert all(column.distinct == 1 for column in report.tables[0].columns)


@pytest.mark.parametrize("query", ["SELECT NULL::INTEGER AS id", "SELECT 1 AS id FROM range(3)"])
def test_pk_low_for_nulls_or_duplicates_alone(tmp_path: Path, query: str) -> None:
    (pk,) = _suggest(tmp_path, {"t": query}).primary_keys
    assert pk.confidence == "low" and (pk.nulls > 0 or pk.duplicates > 0)


@pytest.mark.parametrize(
    ("rows", "sample_rows", "examined", "complete"),
    [(0, 3, 0, True), (2, 3, 2, True), (3, 3, 3, True), (4, 3, 3, False), (4, None, 4, True)],
)
def test_complete_scan_boundary(
    tmp_path: Path, rows: int, sample_rows: int | None, examined: int, complete: bool
) -> None:
    report = _suggest(
        tmp_path, {"t": f"SELECT i AS id FROM range({rows}) r(i)"}, sample_rows=sample_rows
    )
    table = report.tables[0]
    assert table.rows_examined == examined and table.complete_scan is complete
    assert table.columns[0].distinct == examined
    assert report.primary_keys[0].confidence == ("high" if complete else "low")


@pytest.mark.parametrize("suffix", ["csv", "parquet", "pq"])
def test_sample_is_a_file_prefix_and_does_not_include_lookahead(
    tmp_path: Path, suffix: str
) -> None:
    path = _write(
        tmp_path / f"t.{suffix}",
        "SELECT v AS id, v AS other_code FROM (VALUES (30), (10), (20), (30)) r(v)",
    )
    sampled = suggest_relationships({"t": path}, root=tmp_path, base_dir=tmp_path, sample_rows=3)
    assert [(pk.column, pk.confidence, pk.duplicates) for pk in sampled.primary_keys] == [
        ("id", "low", 0),
        ("other_code", "low", 0),
    ]
    assert all("sampled prefix" in pk.reason for pk in sampled.primary_keys)
    full = suggest_relationships({"t": path}, root=tmp_path, base_dir=tmp_path, sample_rows=None)
    assert [(pk.column, pk.confidence, pk.duplicates) for pk in full.primary_keys] == [
        ("id", "low", 1)
    ]


def test_default_sample_limit(tmp_path: Path) -> None:
    report = _suggest(tmp_path, {"t": "SELECT i AS id FROM range(10001) r(i)"})
    assert report.tables[0].rows_examined == 10_000
    assert not report.tables[0].complete_scan
    assert report.primary_keys[0].confidence == "low"


def test_fk_high_counts_non_null_child_rows(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "orders": "SELECT * FROM (VALUES (1, 10), (2, 10), (3, 20), (4, NULL)) r(id, customer_id)",
            "customers": "SELECT * FROM (VALUES (10), (20)) r(id)",
        },
    )
    (fk,) = report.foreign_keys
    assert (fk.child, fk.child_column, fk.parent, fk.parent_column) == (
        "orders",
        "customer_id",
        "customers",
        "id",
    )
    assert fk.confidence == "high" and fk.complete_scan and fk.parent_unique
    assert (fk.rows_examined, fk.child_non_null, fk.matched, fk.unmatched) == (4, 3, 3, 0)
    assert "3/3 non-NULL child rows matched" in fk.reason


@pytest.mark.parametrize(
    ("unmatched", "sample_rows", "confidence", "non_null", "matched"),
    [
        (1, None, "medium", 200, 199),
        (2, None, "medium", 200, 198),
        (3, None, "low", 200, 197),
        (1, 10, "low", 10, 10),
    ],
)
def test_fk_confidence_thresholds(
    tmp_path: Path,
    unmatched: int,
    sample_rows: int | None,
    confidence: str,
    non_null: int,
    matched: int,
) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT 1 AS id",
            "orders": f"SELECT i AS id, CASE WHEN i >= {200 - unmatched} THEN 999 ELSE 1 END AS customer_id FROM range(200) r(i)",
        },
        sample_rows=sample_rows,
    )
    (fk,) = report.foreign_keys
    assert fk.confidence == confidence
    assert fk.child_non_null == non_null and fk.matched == matched
    assert fk.unmatched == non_null - matched
    assert fk.complete_scan is (sample_rows is None)


def test_fk_low_when_only_parent_is_sampled(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT i AS id FROM range(3) r(i)",
            "orders": "SELECT i AS id, 0 AS customer_id FROM range(2) r(i)",
        },
        sample_rows=2,
    )
    (fk,) = report.foreign_keys
    assert fk.confidence == "low" and not fk.complete_scan and fk.matched == 2
    assert report.tables[1].complete_scan


@pytest.mark.parametrize("customer", [1, 999])
def test_fk_medium_with_non_unique_parent_without_join_multiplication(
    tmp_path: Path, customer: int
) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT 1 AS id FROM range(3)",
            "orders": f"SELECT i AS id, {customer} AS customer_id FROM range(4) r(i)",
        },
    )
    (fk,) = report.foreign_keys
    assert fk.confidence == "medium" and not fk.parent_unique
    assert fk.child_non_null == 4 and fk.matched == (4 if customer == 1 else 0)
    assert fk.unmatched == (0 if customer == 1 else 4)
    assert "parent key not unique (1 duplicated values)" in fk.reason


@pytest.mark.parametrize(("sample_rows", "confidence"), [(None, "medium"), (2, "low")])
def test_quickstart_like_duplicate_order_key_keeps_fk_suggestion(
    tmp_path: Path, sample_rows: int | None, confidence: str
) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT i AS id, 'customer ' || i AS name FROM range(2) r(i)",
            "orders": "SELECT * FROM (VALUES (10, 0), (10, 0), (11, 1)) r(order_id, customer_id)",
            "lines": "SELECT * FROM (VALUES (1, 10), (2, 10), (3, 11)) r(line_id, order_id)",
        },
        sample_rows=sample_rows,
    )
    pk = next(pk for pk in report.primary_keys if pk.table == "orders")
    assert pk.column == "order_id" and pk.confidence == "low"
    assert pk.duplicates == 1 and pk.nulls == 0
    assert "duplicates are often the bug" in pk.reason
    assert not any(pk.column == "name" for pk in report.primary_keys)
    fk = next(fk for fk in report.foreign_keys if fk.child == "lines" and fk.parent == "orders")
    assert fk.child_column == fk.parent_column == "order_id"
    assert not fk.parent_unique and fk.confidence == confidence
    assert fk.child_non_null == fk.matched == (3 if sample_rows is None else 2)
    assert fk.unmatched == 0
    assert "parent key not unique (1 duplicated values)" in fk.reason
    text = render_config(report)
    assert f"#  - lines.order_id -> orders.order_id  # suggestion, {confidence}:" in text
    assert "parent key not unique (1 duplicated values)" in text


def test_fk_reports_duplicated_values_separately_from_duplicate_rows_and_nulls(
    tmp_path: Path,
) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT * FROM (VALUES (1), (1), (1), (2), (2), (NULL), (NULL)) r(id)",
            "orders": "SELECT i AS id, 1 + i % 2 AS customer_id FROM range(4) r(i)",
        },
    )
    (fk,) = report.foreign_keys
    assert fk.confidence == "medium" and not fk.parent_unique and fk.matched == 4
    assert "parent key not unique (2 duplicated values), 2 parent nulls" in fk.reason
    pk = next(pk for pk in report.primary_keys if pk.table == "customers")
    assert pk.duplicates == 3 and pk.nulls == 2


def test_fk_low_confidence_pk_precedes_same_name_parent_column(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT 1 AS id, 2 AS customer_id FROM range(3)",
            "orders": "SELECT i AS id, 1 AS customer_id FROM range(4) r(i)",
        },
    )
    (fk,) = report.foreign_keys
    assert fk.parent_column == "id" and fk.matched == 4
    assert fk.confidence == "medium" and not fk.parent_unique


@pytest.mark.parametrize("column", ["external_id", "external_key", "external_code"])
def test_fk_same_name_parent_fallback_without_pk_suggestion(tmp_path: Path, column: str) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": f'SELECT 1 AS "{column}" FROM range(3)',
            "orders": f'SELECT i AS id, 1 AS "{column}" FROM range(4) r(i)',
        },
    )
    assert not any(pk.table == "customers" for pk in report.primary_keys)
    (fk,) = report.foreign_keys
    assert fk.child_column == fk.parent_column == column
    assert fk.confidence == "medium" and not fk.parent_unique and fk.matched == 4


def test_fk_same_name_fallback_precedes_id(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT {'value': 99} AS id, 1 AS external_key FROM range(3)",
            "orders": "SELECT i AS id, 1 AS external_key FROM range(4) r(i)",
        },
    )
    (fk,) = report.foreign_keys
    assert fk.parent_column == "external_key" and fk.matched == 4


@pytest.mark.parametrize(
    ("parent_expr", "child_expr", "suggested"),
    [
        ("1::INTEGER", "1::DECIMAL(9, 2)", True),
        ("1::HUGEINT", "1::DOUBLE", True),
        ("'one'::VARCHAR", "'one'::ENUM('one', 'two')", True),
        ("DATE '2026-01-01'", "DATE '2026-01-01'", True),
        ("1::INTEGER", "'1'::VARCHAR", False),
        ("DATE '2026-01-01'", "'2026-01-01'::VARCHAR", False),
        ("1::INTEGER", "true", False),
    ],
)
def test_fk_comparable_type_families(
    tmp_path: Path, parent_expr: str, child_expr: str, suggested: bool
) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": f"SELECT {parent_expr} AS id",
            "orders": f"SELECT i AS id, {child_expr} AS customer_id FROM range(2) r(i)",
        },
    )
    assert bool(report.foreign_keys) is suggested
    if suggested:
        assert report.foreign_keys[0].confidence == "high"


def test_fk_parent_pk_name_and_deterministic_preference(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT i AS z, i AS customer_code, i + 100 AS customers_id, i + 200 AS customer_id FROM range(2) r(i)",
            "orders": "SELECT i AS id, 100 AS customers_id, 100 AS customer_code FROM range(3) r(i)",
        },
    )
    assert [(fk.child_column, fk.parent_column) for fk in report.foreign_keys] == [
        ("customers_id", "customers_id")
    ]
    report = _suggest(
        tmp_path / "custom",
        {
            "customers": "SELECT i AS z, i AS customer_code FROM range(2) r(i)",
            "orders": "SELECT i AS id, 0 AS customer_code FROM range(3) r(i)",
        },
    )
    (fk,) = report.foreign_keys
    assert fk.child_column == fk.parent_column == "customer_code" and fk.confidence == "high"


def test_fk_prefers_valid_pk_evidence_over_an_invalid_id(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT 1 AS id, i AS customer_code FROM range(2) r(i)",
            "orders": "SELECT i AS id, 0 AS customer_code FROM range(3) r(i)",
        },
    )
    assert report.foreign_keys[0].parent_column == "customer_code"


def test_fk_excludes_own_preferred_pk_and_self_references(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT i AS id FROM range(3) r(i)",
            "orders": "SELECT i AS customer_id FROM range(3) r(i)",
        },
    )
    assert report.foreign_keys == ()
    self_reference = _suggest(
        tmp_path / "self", {"customers": "SELECT i AS id, i AS customer_id FROM range(3) r(i)"}
    )
    assert self_reference.foreign_keys == ()


def test_fk_retains_cycles_and_warns_with_cycle_names(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "a": "SELECT i AS id, 0 AS b_id FROM range(2) r(i)",
            "b": "SELECT i AS id, 0 AS a_id FROM range(2) r(i)",
            "unrelated": "SELECT i AS id FROM range(2) r(i)",
        },
    )
    assert [(fk.child, fk.parent) for fk in report.foreign_keys] == [("a", "b"), ("b", "a")]
    assert len(report.warnings) == 1
    assert "suggestions" in report.warnings[0] and "cycle" in report.warnings[0]
    assert "a -> b -> a" in report.warnings[0]
    project = parse_project(yaml.safe_load(render_config(report)), base_dir=tmp_path)
    assert project.foreign_keys == ()


@pytest.mark.parametrize(
    "expression",
    [
        "[1, 2]",
        "[1, 2]::INTEGER[2]",
        "{'value': 1}",
        "MAP(['key'], [1])",
        "union_value(value := 1)",
    ],
)
def test_nested_columns_are_profiled_but_not_candidates(tmp_path: Path, expression: str) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": f"SELECT {expression} AS id FROM range(2)",
            "orders": f"SELECT i AS id, {expression} AS customer_id FROM range(2) r(i)",
        },
    )
    assert report.tables[0].columns[0].distinct == 1
    assert all(pk.table != "customers" for pk in report.primary_keys)
    assert report.foreign_keys == ()


def test_parent_without_any_pk_or_id_has_no_fk_suggestion(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT 1 AS repeated FROM range(3)",
            "orders": "SELECT i AS id, 1 AS customer_id FROM range(2) r(i)",
        },
    )
    assert report.foreign_keys == ()


def test_all_null_fk_has_zero_non_null_counts(tmp_path: Path) -> None:
    report = _suggest(
        tmp_path,
        {
            "customers": "SELECT 1 AS id",
            "orders": "SELECT i AS id, NULL::INTEGER AS customer_id FROM range(2) r(i)",
        },
    )
    (fk,) = report.foreign_keys
    assert (fk.child_non_null, fk.matched, fk.unmatched) == (0, 0, 0)


def test_render_round_trips_with_only_tables_and_placeholder_active(tmp_path: Path) -> None:
    path = _write(tmp_path / "data" / "a 'quoted': file.csv", "SELECT i AS id FROM range(2) r(i)")
    orders = _write(tmp_path / "orders.pq", "SELECT i AS id, 0 AS customer_id FROM range(3) r(i)")
    report = suggest_relationships(
        {"orders": orders, "customers": path}, root=tmp_path, base_dir=tmp_path
    )
    text = render_config(report)
    data = yaml.safe_load(text)
    project = parse_project(data, base_dir=tmp_path)
    assert data["version"] == 1 and data["foreign_keys"] == []
    assert data["predicate"] == {"sql": "SELECT false"}
    assert "settings" not in data and project.settings == Settings()
    assert all(table.primary_key is None for table in project.tables.values())
    assert all("primary_key" not in table for table in data["tables"].values())
    assert project.table_path("customers") == path
    assert data["tables"]["customers"]["path"] == "data/a 'quoted': file.csv"
    assert "Generated by" in text and "failslice init" in text
    assert "Suggestions are inferred from data and must be reviewed" in text
    assert "# primary_key: id  # suggestion, high:" in text
    assert "#  - orders.customer_id -> customers.id  # suggestion, high:" in text
    assert "# REPLACE: a SELECT that is TRUE when the bug is present" in text
    for line in yaml.safe_dump({"settings": Settings().model_dump()}, sort_keys=False).splitlines():
        assert "# " + line in text
    assert "detected" not in text.lower() and "found key" not in text.lower()


def test_relative_paths_can_include_parent_components_with_explicit_root(tmp_path: Path) -> None:
    base = tmp_path / "config"
    base.mkdir()
    path = _write(tmp_path / "data" / "t.parquet", "SELECT 1 AS id")
    report = suggest_relationships({"t": Path("../data/t.parquet")}, root=tmp_path, base_dir=base)
    assert report.tables[0].path == "../data/t.parquet"
    assert (
        parse_project(
            yaml.safe_load(render_config(report)), base_dir=base, root=tmp_path
        ).table_path("t")
        == path
    )


def test_determinism_independent_of_mapping_and_report_order(tmp_path: Path) -> None:
    tables = {
        "orders": _write(
            tmp_path / "orders.csv", "SELECT i AS id, 1 AS customer_id FROM range(3) r(i)"
        ),
        "customers": _write(tmp_path / "customers.parquet", "SELECT 1 AS id"),
    }
    first = suggest_relationships(tables, root=tmp_path, base_dir=tmp_path)
    second = suggest_relationships(
        dict(reversed(list(tables.items()))), root=tmp_path, base_dir=tmp_path
    )
    assert first == second and render_config(first) == render_config(second)
    reversed_report = first.model_copy(
        update={
            "tables": tuple(reversed(first.tables)),
            "primary_keys": tuple(reversed(first.primary_keys)),
            "foreign_keys": tuple(reversed(first.foreign_keys)),
        }
    )
    assert render_config(first) == render_config(reversed_report)


@pytest.mark.parametrize(
    "line_break", ["\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"]
)
def test_render_comments_cannot_inject_active_yaml(tmp_path: Path, line_break: str) -> None:
    report = _suggest(
        tmp_path, {"t": f'SELECT i AS "line{line_break}break_code" FROM range(2) r(i)'}
    )
    expected = yaml.safe_load(render_config(report))
    pk = report.primary_keys[0].model_copy(
        update={"reason": f"evidence{line_break}primary_key: injected"}
    )
    fk = ForeignKeySuggestion(
        child=f"t{line_break}child: injected",
        child_column=f"x{line_break}child_column: injected",
        parent=f"t{line_break}parent: injected",
        parent_column=f"id{line_break}parent_column: injected",
        confidence="low",
        rows_examined=2,
        complete_scan=False,
        child_non_null=2,
        matched=0,
        unmatched=2,
        parent_unique=False,
        reason=f"suggestion{line_break}settings: injected",
    )
    modified = report.model_copy(
        update={
            "primary_keys": (pk,),
            "foreign_keys": (fk,),
            "warnings": (f"warning{line_break}warning: injected",),
        }
    )
    text = render_config(modified)
    data = yaml.safe_load(text)
    assert set(data) == {"version", "tables", "foreign_keys", "predicate"}
    assert data == expected
    assert data["foreign_keys"] == []
    assert parse_project(data, base_dir=tmp_path).tables["t"].primary_key is None


@pytest.mark.parametrize(
    "line_break", ["\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"]
)
def test_render_column_line_breaks_cannot_activate_settings(
    tmp_path: Path, line_break: str
) -> None:
    column = f"parent_id{line_break}settings:{line_break}  max_evals: 1{line_break}#_id"
    parent_path = _write(
        tmp_path / f"parents{line_break}data.parquet",
        f'SELECT i AS "{column}" FROM range(2) r(i)',
    )
    child_path = _write(
        tmp_path / "children.parquet",
        f'SELECT i AS id, 1 AS "{column}" FROM range(3) r(i)',
    )
    report = suggest_relationships(
        {"parents": parent_path, "children": child_path}, root=tmp_path, base_dir=tmp_path
    )
    before = report.model_dump()
    (fk,) = report.foreign_keys
    assert (fk.child, fk.child_column, fk.parent, fk.parent_column) == (
        "children",
        column,
        "parents",
        column,
    )
    text = render_config(report)
    data = yaml.safe_load(text)
    assert set(data) == {"version", "tables", "foreign_keys", "predicate"}
    assert data["tables"] == {
        "children": {"path": "children.parquet", "format": "parquet"},
        "parents": {"path": parent_path.name, "format": "parquet"},
    }
    assert data["foreign_keys"] == []
    assert report.model_dump() == before
    project = parse_project(data, base_dir=tmp_path)
    assert project.settings == Settings()
    assert project.foreign_keys == ()
    assert all(table.primary_key is None for table in project.tables.values())
    assert project.table_path("parents") == parent_path
    assert project.table_path("children") == child_path
    assert all(
        not any(
            char in line
            for char in ("\x85", "\u2028", "\u2029", "\v", "\f", "\x1c", "\x1d", "\x1e")
        )
        for line in text.split("\n")
    )


@pytest.mark.parametrize(
    "line_break", ["\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"]
)
def test_render_quoted_scalars_preserve_line_breaks(tmp_path: Path, line_break: str) -> None:
    report = _suggest(tmp_path, {"t": "SELECT 1 AS id"})
    table = report.tables[0].model_copy(
        update={
            "name": f"t{line_break}settings:{line_break}  max_evals: 1",
            "path": f"data/{line_break}quoted{line_break}file.parquet",
        }
    )
    pk = report.primary_keys[0].model_copy(
        update={"table": table.name, "column": f"column{line_break}settings:{line_break}_id"}
    )
    modified = report.model_copy(update={"tables": (table,), "primary_keys": (pk,)})
    data = yaml.safe_load(render_config(modified))
    assert set(data) == {"version", "tables", "foreign_keys", "predicate"}
    assert data["tables"] == {table.name: {"path": table.path, "format": table.format}}
    assert data["foreign_keys"] == []
    assert data["predicate"] == {"sql": "SELECT false"}


@pytest.mark.parametrize("sample_rows", [0, -1, True])
def test_invalid_sample_rows_refused(tmp_path: Path, sample_rows: int) -> None:
    with pytest.raises(ConfigError, match="positive integer or None"):
        suggest_relationships({}, root=tmp_path, base_dir=tmp_path, sample_rows=sample_rows)


@pytest.mark.parametrize("names", [["bad-name"], ["__fs_reserved"], ["T", "t"]])
def test_invalid_table_names_refused(tmp_path: Path, names: list[str]) -> None:
    with pytest.raises(ConfigError, match=r"identifier|reserved prefix|duplicate"):
        suggest_relationships(
            dict.fromkeys(names, tmp_path / "t.csv"), root=tmp_path, base_dir=tmp_path
        )


@pytest.mark.parametrize(
    ("filename", "message"),
    [("missing.csv", "file not found"), ("input.txt", "expected a CSV or Parquet")],
)
def test_invalid_files_refused(tmp_path: Path, filename: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        suggest_relationships({"t": tmp_path / filename}, root=tmp_path, base_dir=tmp_path)


def test_empty_discovery_and_report(tmp_path: Path) -> None:
    assert discover_tables(tmp_path, root=tmp_path) == {}
    report = suggest_relationships({}, root=tmp_path, base_dir=tmp_path)
    assert report.tables == report.primary_keys == report.foreign_keys == report.warnings == ()
    with pytest.raises(ConfigError, match="without tables"):
        render_config(report)


def test_models_are_frozen_and_forbid_extra_fields() -> None:
    column = ColumnProfile(name="id", type="INTEGER", nulls=0, distinct=1)
    table = TableProfile(
        name="t", path="t.csv", format="csv", rows_examined=1, complete_scan=True, columns=(column,)
    )
    pk = PrimaryKeySuggestion(
        table="t",
        column="id",
        confidence="high",
        rows_examined=1,
        complete_scan=True,
        nulls=0,
        duplicates=0,
        reason="suggestion",
    )
    fk = ForeignKeySuggestion(
        child="t",
        child_column="x",
        parent="p",
        parent_column="id",
        confidence="high",
        rows_examined=1,
        complete_scan=True,
        child_non_null=1,
        matched=1,
        unmatched=0,
        parent_unique=True,
        reason="suggestion",
    )
    report = InferenceReport(tables=(table,), primary_keys=(pk,), foreign_keys=(fk,), warnings=())
    assert report.format == 1
    for model in (column, table, pk, fk, report):
        field = next(iter(type(model).model_fields))
        with pytest.raises(ValidationError, match="frozen"):
            setattr(model, field, getattr(model, field))
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            type(model).model_validate({**model.model_dump(), "unexpected": True})


def test_one_connection_is_restricted_before_any_file_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _write(tmp_path / "t.parquet", "SELECT 1 AS id")
    original_connect = duckdb.connect
    connections = []
    statements: list[str] = []

    class RecordingConnection:
        """Observe actual settings when a file reader is about to execute."""

        def __init__(self) -> None:
            self.con = original_connect(":memory:")

        def __enter__(self) -> RecordingConnection:
            return self

        def __exit__(self, *args: object) -> None:
            self.con.close()

        def execute(self, sql: str) -> Any:
            statements.append(sql)
            if "read_parquet(" in sql:
                settings = self.con.execute(
                    "SELECT current_setting('enable_external_access'), current_setting('threads'), "
                    "current_setting('autoload_known_extensions'), current_setting('autoinstall_known_extensions'), "
                    "current_setting('preserve_insertion_order'), current_setting('allowed_directories')"
                ).fetchone()
                assert settings is not None
                assert settings[:5] == (False, 1, False, False, True)
                assert str(tmp_path) + "/" in settings[5]
                with pytest.raises(duckdb.PermissionException):
                    self.con.execute("SELECT * FROM read_csv('/etc/passwd')")
                assert _lit(str(path.resolve())) in sql
            return self.con.execute(sql)

    def connect(database: str) -> RecordingConnection:
        assert database == ":memory:"
        con = RecordingConnection()
        connections.append(con)
        return con

    monkeypatch.setattr("failslice.inference.duckdb.connect", connect)
    suggest_relationships({"t": path}, root=tmp_path, base_dir=tmp_path)
    assert len(connections) == 1
    assert statements[:6] == [
        f"SET allowed_directories = [{_lit(str(tmp_path) + '/')}]",
        "SET enable_external_access = false",
        "SET autoinstall_known_extensions = false",
        "SET autoload_known_extensions = false",
        "SET threads = 1",
        "SET preserve_insertion_order = true",
    ]
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        connections[0].con.execute("SELECT 1")
