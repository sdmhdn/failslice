"""File-backed inspection checks: exact evidence, safe reads and bounded replies."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import duckdb
import pytest
import yaml
from pydantic import ValidationError

from failslice.config import ConfigError, parse_project
from failslice.engine import _lit
from failslice.inspect import DatasetInspection, ForeignKeyCheck, Limits, inspect_tables
from tests.conftest import posix_only


def _write(path: Path, query: str, *, fmt: str | None = None) -> Path:
    """Write deterministic, typed CSV/Parquet fixtures with DuckDB COPY."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = fmt or ("csv, HEADER true" if path.suffix == ".csv" else "parquet")
    with duckdb.connect(":memory:") as con:
        con.execute(f"COPY ({query}) TO {_lit(str(path))} (FORMAT {fmt})")
    return path


def _inspect(tmp_path: Path, query: str, **kwargs: Any) -> DatasetInspection:
    """Inspect a single real Parquet fixture with optional limits."""
    path = _write(tmp_path / "t.parquet", query)
    return inspect_tables({"t": path}, root=tmp_path, base_dir=tmp_path, **kwargs)


def _size(result: DatasetInspection) -> int:
    """Use a common JSON serializer independently of the module's budget helper."""
    return len(json.dumps(result.model_dump(mode="json"), allow_nan=False).encode())


@pytest.mark.parametrize("suffix", [".csv", ".parquet"])
def test_full_scan_counts_null_children_and_duplicate_parents(tmp_path: Path, suffix: str) -> None:
    parent = _write(
        tmp_path / f"parent{suffix}",
        "SELECT * FROM (VALUES (1), (1), (2), (NULL)) t(id)",
    )
    child = _write(
        tmp_path / f"child{suffix}",
        "SELECT i AS id, CASE WHEN i % 5 = 0 THEN NULL "
        "WHEN i < 30 THEN 1 ELSE 99 END AS pid FROM range(100) r(i)",
    )
    result = inspect_tables(
        {"parent": parent, "child": child},
        root=tmp_path,
        base_dir=tmp_path,
        foreign_keys=[" child.pid  -> parent.id "],
        limits=Limits(preview_rows=2),
    )
    assert result.format == 1 and result.truncated and result.warnings == ()
    assert [t.name for t in result.tables] == ["child", "parent"]
    c, p = result.tables
    assert c.rows == 100 and p.rows == 4
    assert [(col.name, col.nulls, col.distinct) for col in c.columns] == [
        ("id", 0, 100),
        ("pid", 20, 2),
    ]
    assert p.columns[0].nulls == 1 and p.columns[0].distinct == 2
    assert c.preview.rows == ((0, None), (1, 1))
    assert p.preview.rows == ((1,), (1,))
    (fk,) = result.foreign_keys
    assert fk.fk == "child.pid -> parent.id" and fk.child_non_null == 80 and fk.orphans == 56
    assert c.format == suffix[1:] and c.path == f"child{suffix}"


def test_null_child_has_no_reference_and_all_null_column_has_no_distinct(tmp_path: Path) -> None:
    parent = _write(tmp_path / "p.parquet", "SELECT NULL::INTEGER AS id WHERE false")
    child = _write(tmp_path / "c.parquet", "SELECT NULL::INTEGER AS pid FROM range(3)")
    result = inspect_tables(
        {"p": parent, "c": child},
        root=tmp_path,
        base_dir=tmp_path,
        foreign_keys=["c.pid -> p.id"],
    )
    assert not result.truncated
    assert result.foreign_keys[0].child_non_null == 0 and result.foreign_keys[0].orphans == 0
    assert result.tables[0].columns[0].nulls == 3 and result.tables[0].columns[0].distinct == 0
    assert result.tables[1].rows == 0 and result.tables[1].preview.rows == ()


@pytest.mark.parametrize("byte_limit", [1, 2, 3, 4, 5, 6, 7, 8, 9, 1024])
def test_multibyte_prefix_and_giant_cells_are_bounded(tmp_path: Path, byte_limit: int) -> None:
    value = "é猫😀z"
    result = _inspect(
        tmp_path,
        f"SELECT {_lit(value)} AS text, repeat('猫', 1000000) AS giant, NULL::VARCHAR AS nil",
        limits=Limits(cell_bytes=byte_limit),
    )
    t = result.tables[0]
    row = t.preview.rows[0]
    assert row == (
        value.encode()[:byte_limit].decode(errors="ignore"),
        "猫" * (byte_limit // 3),
        None,
    )
    assert all(v is None or len(str(v).encode()) <= byte_limit for v in row)
    assert t.preview.truncated_cells == 1 + (len(value.encode()) > byte_limit)
    assert result.truncated and [(c.nulls, c.distinct) for c in t.columns] == [
        (0, 1),
        (0, 1),
        (1, 0),
    ]


def test_sql_clipping_and_lockdown_precede_all_external_reads(tmp_path: Path) -> None:
    path = _write(tmp_path / "t.parquet", "SELECT repeat('😀', 1000000) AS giant")
    original_connect = duckdb.connect
    fetched: list[tuple[Any, ...]] = []
    opened: list[str] = []

    class AuditedConnection:
        """Observe the actual result crossing from DuckDB to Python."""

        def __init__(self, con: Any) -> None:
            self.con = con
            self.preview_query = False

        def execute(self, sql: str, parameters: Any = None) -> AuditedConnection:
            if "read_parquet(" in sql:
                assert self.con.execute(
                    "SELECT current_setting('autoinstall_known_extensions'), "
                    "current_setting('autoload_known_extensions'), current_setting('threads'), "
                    "current_setting('enable_external_access'), "
                    "current_setting('allowed_directories'), current_setting('temp_directory')"
                ).fetchone() == (False, False, 1, False, [str(tmp_path) + "/"], "")
            self.preview_query = sql.startswith("SELECT decode(")
            self.con.execute(sql, parameters)
            return self

        def fetchone(self) -> Any:
            return self.con.fetchone()

        def fetchall(self) -> Any:
            rows = self.con.fetchall()
            if self.preview_query:
                fetched.extend(rows)
                assert all(len(row[0].encode()) <= 7 for row in rows)
            return rows

    @contextmanager
    def connect(database: str) -> Any:
        opened.append(database)
        with original_connect(database) as con:
            yield AuditedConnection(con)

    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with patch("failslice.inspect.duckdb.connect", connect):
        result = inspect_tables(
            {"t": path},
            root=tmp_path,
            base_dir=tmp_path,
            limits=Limits(cell_bytes=7),
        )
    assert opened == [":memory:"] and fetched == [("😀", True)]
    assert result.tables[0].preview.rows == (("😀",),)
    after = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert before == after


def test_json_types_finite_floats_precise_decimals_and_large_integers(tmp_path: Path) -> None:
    result = _inspect(
        tmp_path,
        "SELECT true AS yes, false AS no, 42 AS small, -9007199254740991::BIGINT AS safe, "
        "9007199254740992::BIGINT AS large, 1e300::DOUBLE AS finite, "
        "'NaN'::DOUBLE AS nan, 'Infinity'::DOUBLE AS inf, '-Infinity'::DOUBLE AS neg_inf, "
        "1.25::DECIMAL(5,2) AS exact, 123456789012345678.12::DECIMAL(20,2) AS precise, "
        "DATE '2026-11-17' AS day, '42'::VARCHAR AS numeric_text",
    )
    assert result.tables[0].preview.rows == (
        (
            True,
            False,
            42,
            -9007199254740991,
            "9007199254740992",
            1e300,
            "nan",
            "inf",
            "-inf",
            1.25,
            "123456789012345678.12",
            "2026-11-17",
            "42",
        ),
    )
    json.dumps(result.model_dump(mode="json"), allow_nan=False)
    assert not result.truncated


def test_clipped_numbers_and_booleans_stay_strings(tmp_path: Path) -> None:
    result = _inspect(tmp_path, "SELECT 123 AS n, true AS b", limits=Limits(cell_bytes=1))
    assert result.tables[0].preview.rows == (("1", "t"),)
    assert result.tables[0].preview.truncated_cells == 2


def test_enormous_native_integers_stay_strings_without_python_integer_conversion(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute(
            "CREATE TABLE t AS SELECT repeat('9', 5000)::BIGNUM AS n, "
            "12345678901234567890::HUGEINT AS h, -12345678901234567890::HUGEINT AS neg"
        )
    result = inspect_tables(
        {"t": path},
        root=tmp_path,
        base_dir=tmp_path,
        limits=Limits(cell_bytes=6000),
    )
    assert result.tables[0].preview.rows == (
        (
            "9" * 5000,
            "12345678901234567890",
            "-12345678901234567890",
        ),
    )
    assert not result.truncated


def test_nested_types_use_varchar_and_skip_distinct(tmp_path: Path) -> None:
    result = _inspect(
        tmp_path,
        "SELECT [1, NULL, 2] AS list, {'a': 3} AS struct, map(['x'], [1]) AS map, "
        "[4, 5]::INTEGER[2] AS arr, NULL::INTEGER[] AS nil",
    )
    t = result.tables[0]
    assert [c.distinct for c in t.columns] == [None] * 5
    assert [c.nulls for c in t.columns] == [0, 0, 0, 0, 1]
    assert t.preview.rows == (("[1, NULL, 2]", "{'a': 3}", "{x=1}", "[4, 5]", None),)


def test_native_duckdb_union_uses_varchar_and_skips_distinct(tmp_path: Path) -> None:
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE TABLE t AS SELECT union_value(n := 6) AS tagged")
    result = inspect_tables({"t": path}, root=tmp_path, base_dir=tmp_path)
    assert result.tables[0].columns[0].type == "UNION(n INTEGER)"
    assert result.tables[0].columns[0].distinct is None
    assert result.tables[0].preview.rows == (("6",),)


@pytest.mark.parametrize("suffix", [".csv", ".parquet"])
def test_preview_keeps_file_column_and_row_order(tmp_path: Path, suffix: str) -> None:
    path = _write(
        tmp_path / f"t{suffix}",
        'SELECT i AS z, 10000-i AS rowid, i+1 AS "quoted""column", i+2 AS a '
        "FROM range(10000) r(i) ORDER BY (i * 7919) % 10000",
    )
    kwargs = {
        "root": tmp_path,
        "base_dir": tmp_path,
        "limits": Limits(preview_rows=20, max_columns=3),
    }
    a = inspect_tables({"t": path}, **kwargs)
    b = inspect_tables({"t": path}, **kwargs)
    assert a == b
    t = a.tables[0]
    assert t.preview.columns == ("z", "rowid", 'quoted"column')
    with duckdb.connect(":memory:") as con:
        reader = "read_csv" if suffix == ".csv" else "read_parquet"
        expected = tuple(
            con.execute(f"SELECT * FROM {reader}({_lit(str(path))}) LIMIT 20").fetchall()
        )
    assert t.preview.rows == tuple(r[:3] for r in expected)
    assert t.preview.truncated_rows and t.preview.truncated_columns
    assert len(t.columns) == 4 and t.rows == 10000


def test_many_wide_tables_fit_budget_with_explicit_flags(tmp_path: Path) -> None:
    query = (
        "SELECT " + ", ".join(f"repeat('猫', 1000) AS c{i}" for i in range(64)) + " FROM range(10)"
    )
    tables = {f"t{i}": _write(tmp_path / f"t{i}.parquet", query) for i in range(8)}
    result = inspect_tables(tables, root=tmp_path, base_dir=tmp_path, limits=Limits(cell_bytes=32))
    assert _size(result) <= 65536 and len(result.model_dump_json().encode()) <= 65536
    assert result.truncated and len(result.tables) == 8
    assert any(not t.preview.rows for t in result.tables)
    assert any(t.preview.rows for t in result.tables)
    for t in result.tables:
        assert t.rows == 10 and len(t.columns) == 64
        assert all(c.distinct == 1 and c.nulls == 0 for c in t.columns)
        assert t.preview.truncated_rows == (len(t.preview.rows) < 10)
        assert t.preview.truncated_columns and len(t.preview.columns) == 50
        assert t.preview.truncated_cells == len(t.preview.rows) * 50


def test_budget_discards_largest_table_suffix_first(tmp_path: Path) -> None:
    small = _write(tmp_path / "small.parquet", "SELECT i AS id FROM range(3) r(i)")
    large = _write(
        tmp_path / "large.parquet", "SELECT i AS id, repeat('x', 1000) AS s FROM range(3) r(i)"
    )
    tables = {"a_small": small, "z_large": large}
    original = inspect_tables(tables, root=tmp_path, base_dir=tmp_path)
    limit = _size(original) - 500
    result = inspect_tables(
        tables, root=tmp_path, base_dir=tmp_path, limits=Limits(reply_bytes=limit)
    )
    assert result.tables[0].preview == original.tables[0].preview
    assert result.tables[1].preview.rows == original.tables[1].preview.rows[:-1]
    assert result.tables[1].preview.truncated_rows and result.truncated
    assert _size(result) <= limit
    exact = inspect_tables(
        tables,
        root=tmp_path,
        base_dir=tmp_path,
        limits=Limits(reply_bytes=_size(result)),
    )
    assert exact == result
    smaller = inspect_tables(
        tables,
        root=tmp_path,
        base_dir=tmp_path,
        limits=Limits(reply_bytes=_size(result) - 1),
    )
    assert len(smaller.tables[1].preview.rows) == 1


def test_reply_metadata_cannot_exceed_budget_even_without_previews(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="dataset too large to describe within the reply limit"):
        _inspect(tmp_path, "SELECT 1 AS id", limits=Limits(reply_bytes=1))
    with pytest.raises(ConfigError, match="dataset too large to describe within the reply limit"):
        inspect_tables(
            {"t": tmp_path / "t.parquet"},
            root=tmp_path,
            base_dir=tmp_path,
            limits=Limits(preview_rows=0, reply_bytes=1),
        )


def test_zero_preview_rows_and_exact_budget_boundary(tmp_path: Path) -> None:
    original = _inspect(
        tmp_path, "SELECT i AS id FROM range(3) r(i)", limits=Limits(preview_rows=0)
    )
    assert original.tables[0].preview.rows == () and original.tables[0].preview.truncated_rows
    result = inspect_tables(
        {"t": tmp_path / "t.parquet"},
        root=tmp_path,
        base_dir=tmp_path,
        limits=Limits(preview_rows=0, reply_bytes=_size(original)),
    )
    assert result == original


def test_config_template_round_trip_relative_to_base_and_format_override(tmp_path: Path) -> None:
    base = tmp_path / "configs"
    base.mkdir()
    parent = _write(tmp_path / "data" / "a'parent.bin", "SELECT 1 AS id")
    child = _write(base / "child.csv", "SELECT 1 AS pid")
    result = inspect_tables(
        {"parent": parent, "child": Path("child.csv")},
        root=tmp_path,
        base_dir=base,
        foreign_keys=["child.pid -> parent.id"],
        formats={"parent": "parquet"},
    )
    assert result.config_template.startswith(
        "# generated by failslice inspect_dataset; replace the predicate\n"
    )
    project = parse_project(yaml.safe_load(result.config_template), base_dir=base, root=tmp_path)
    assert project.tables["parent"].path == "../data/a'parent.bin"
    assert project.tables["parent"].format == "parquet"
    assert project.table_path("parent") == parent and project.table_path("child") == child
    assert project.predicate_queries() == ("SELECT false",)
    assert tuple(map(str, project.foreign_keys)) == ("child.pid -> parent.id",)
    assert result.tables[1].path == "data/a'parent.bin" and not result.truncated


def test_symlink_escape_refused_and_inside_alias_resolved(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = _write(tmp_path / "outside.parquet", "SELECT 1 AS id")
    alias = root / "alias.parquet"
    alias.symlink_to(outside)
    with pytest.raises(ConfigError, match="outside the allowed root"):
        inspect_tables({"t": alias}, root=root, base_dir=root)
    alias.unlink()
    inside = _write(root / "inside.parquet", "SELECT 1 AS id")
    alias.symlink_to(inside)
    result = inspect_tables({"t": alias}, root=root, base_dir=root)
    assert result.tables[0].path == "inside.parquet"


@pytest.mark.parametrize("character", ["*", "?", "[", "]", "{", "}"])
def test_glob_paths_are_rejected_before_open(tmp_path: Path, character: str) -> None:
    with pytest.raises(ConfigError, match="glob character"):
        inspect_tables({"t": Path(f"bad{character}.parquet")}, root=tmp_path, base_dir=tmp_path)


@posix_only
def test_glob_named_symlink_is_rejected(tmp_path: Path) -> None:
    target = _write(tmp_path / "t.parquet", "SELECT 1 AS id")
    alias = tmp_path / "alias*.parquet"
    alias.symlink_to(target)
    with pytest.raises(ConfigError, match="glob character"):
        inspect_tables({"t": alias}, root=tmp_path, base_dir=tmp_path)


@posix_only
def test_fifo_is_rejected_without_opening(tmp_path: Path) -> None:
    path = tmp_path / "pipe.csv"
    os.mkfifo(path)
    with pytest.raises(ConfigError, match="regular file"):
        inspect_tables({"t": path}, root=tmp_path, base_dir=tmp_path)


@posix_only
def test_device_is_rejected_without_opening() -> None:
    with pytest.raises(ConfigError, match="regular file"):
        inspect_tables({"t": Path("/dev/null")}, root=Path("/dev"), base_dir=Path("/dev"))


@pytest.mark.parametrize("filename", ["missing.csv", "directory.parquet"])
def test_regular_file_required(tmp_path: Path, filename: str) -> None:
    if filename.startswith("directory"):
        (tmp_path / filename).mkdir()
    with pytest.raises(ConfigError, match="regular file"):
        inspect_tables({"t": Path(filename)}, root=tmp_path, base_dir=tmp_path)


def test_too_many_tables_rejected_before_read(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="too many tables"):
        inspect_tables(
            {f"t{i}": Path("missing.parquet") for i in range(33)},
            root=tmp_path,
            base_dir=tmp_path,
        )
    with pytest.raises(ConfigError, match="too many tables"):
        inspect_tables(
            {"a": Path("missing.parquet"), "b": Path("missing.parquet")},
            root=tmp_path,
            base_dir=tmp_path,
            limits=Limits(max_tables=1),
        )


@pytest.mark.parametrize("names", [["bad-name"], ["1bad"], ["__FS_private"], ["A", "a"], []])
def test_table_names_match_config_rules(tmp_path: Path, names: list[str]) -> None:
    path = _write(tmp_path / "t.parquet", "SELECT 1 AS id")
    with pytest.raises(ConfigError):
        inspect_tables(dict.fromkeys(names, path), root=tmp_path, base_dir=tmp_path)


@pytest.mark.parametrize(
    "foreign_keys",
    [
        ["bad"],
        ["c.id -> unknown.id"],
        ["c.missing -> p.id"],
        ["c.id -> p.missing"],
        ["c.id -> c.id"],
        ["c.id -> p.id", "p.id -> c.id"],
        ["c.id -> p.id", "c.id -> p.id"],
    ],
)
def test_bad_foreign_keys_and_columns(tmp_path: Path, foreign_keys: list[str]) -> None:
    path = _write(tmp_path / "t.parquet", "SELECT 1 AS id")
    with pytest.raises(ConfigError):
        inspect_tables(
            {"c": path, "p": path},
            root=tmp_path,
            base_dir=tmp_path,
            foreign_keys=foreign_keys,
        )


def test_incomparable_foreign_key_columns_raise_config_error(tmp_path: Path) -> None:
    parent = _write(tmp_path / "p.parquet", "SELECT 1 AS id")
    child = _write(tmp_path / "c.parquet", "SELECT {'x': 1} AS id")
    with pytest.raises(ConfigError, match="columns cannot be compared"):
        inspect_tables(
            {"c": child, "p": parent},
            root=tmp_path,
            base_dir=tmp_path,
            foreign_keys=["c.id -> p.id"],
        )


def test_duckdb_physical_tables_are_copied_read_only(tmp_path: Path) -> None:
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE TABLE p AS SELECT * FROM (VALUES (1), (1)) t(id)")
        con.execute("CREATE TABLE c AS SELECT * FROM (VALUES (1), (2), (NULL)) t(pid)")
    before = path.read_bytes()
    result = inspect_tables(
        {"p": path, "c": path},
        root=tmp_path,
        base_dir=tmp_path,
        foreign_keys=["c.pid -> p.id"],
    )
    assert path.read_bytes() == before
    assert [(t.format, t.rows) for t in result.tables] == [("duckdb", 3), ("duckdb", 2)]
    assert result.tables[0].preview.rows == ((1,), (2,), (None,))
    assert result.foreign_keys[0].child_non_null == 2 and result.foreign_keys[0].orphans == 1
    assert (
        parse_project(yaml.safe_load(result.config_template), base_dir=tmp_path).tables["p"].format
        == "duckdb"
    )
    assert set(tmp_path.iterdir()) == {path}


@pytest.mark.parametrize("source", ["external", "simple", "missing"])
def test_duckdb_views_and_missing_tables_rejected(tmp_path: Path, source: str) -> None:
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE VIEW simple AS SELECT 1 AS id")
        # Store a valid view whose file is removed before inspection. Rejecting
        # the catalog entry must not bind or execute its external reader.
        external = _write(tmp_path / "private.parquet", "SELECT 1 AS id")
        con.execute(f"CREATE VIEW external AS SELECT * FROM read_parquet({_lit(str(external))})")
    external.unlink()
    match = "only physical tables" if source != "missing" else "no table"
    with pytest.raises(ConfigError, match=match):
        inspect_tables({source: path}, root=tmp_path, base_dir=tmp_path)


@pytest.mark.parametrize("fmt", [None, "parquet", "not-a-format"])
def test_unreadable_and_invalid_formats_raise_config_error(tmp_path: Path, fmt: str | None) -> None:
    path = tmp_path / "t.unknown"
    path.write_text("not a dataset")
    with pytest.raises(ConfigError):
        inspect_tables(
            {"t": path},
            root=tmp_path,
            base_dir=tmp_path,
            formats=None if fmt is None else {"t": fmt},
        )


def test_unknown_format_table_and_invalid_base_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown table"):
        inspect_tables({}, root=tmp_path, base_dir=tmp_path, formats={"missing": "csv"})
    with pytest.raises(ConfigError, match="existing directories"):
        inspect_tables({}, root=tmp_path, base_dir=tmp_path / "missing")
    with pytest.raises(ConfigError, match="existing directories"):
        inspect_tables({}, root=tmp_path / "missing", base_dir=tmp_path / "missing")
    with pytest.raises(ConfigError, match="outside the allowed root"):
        inspect_tables({}, root=tmp_path, base_dir=tmp_path.parent)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"preview_rows": -1},
        {"preview_rows": 21},
        {"max_columns": 0},
        {"max_columns": 51},
        {"cell_bytes": 0},
        {"reply_bytes": 0},
        {"max_tables": 0},
        {"max_tables": 33},
        {"preview_rows": True},
        {"preview_rows": 1.0},
        {"extra": 1},
    ],
)
def test_limits_validate(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Limits.model_validate(kwargs)


def test_all_models_are_frozen_forbid_extra_and_round_trip(tmp_path: Path) -> None:
    result = _inspect(tmp_path, "SELECT 1 AS id")
    table = result.tables[0]
    fk = ForeignKeyCheck(fk="c.id -> p.id", child_non_null=1, orphans=0)
    for model in (Limits(), result, table, table.columns[0], table.preview, fk):
        data = model.model_dump()
        with pytest.raises(ValidationError):
            type(model).model_validate({**data, "unexpected": 1})
        field = next(iter(type(model).model_fields))
        with pytest.raises(ValidationError):
            setattr(model, field, getattr(model, field))
    assert DatasetInspection.model_validate_json(result.model_dump_json()) == result
