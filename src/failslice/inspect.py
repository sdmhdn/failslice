"""Predicate-free, exact dataset statistics and bounded, untrusted previews.

Only validated local files are read. Preview values are rendered and limited in
DuckDB before crossing into Python; counts always describe the entire input.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from failslice.config import ConfigError, ForeignKey, literal_path, parse_project, resolve_inside
from failslice.engine import _lit, q

__all__ = [
    "ColumnInfo",
    "DatasetInspection",
    "ForeignKeyCheck",
    "Limits",
    "TableInspection",
    "TablePreview",
    "inspect_tables",
]

_INTEGER_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
        "BIGNUM",
    }
)
_MAX_JSON_INT = 2**53 - 1


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Limits(_Model):
    """Preview and reply ceilings; zero preview rows suppresses all row values."""

    preview_rows: Annotated[int, Field(ge=0, le=20, strict=True)] = 10
    max_columns: Annotated[int, Field(ge=1, le=50, strict=True)] = 50
    cell_bytes: Annotated[int, Field(ge=1, strict=True)] = 1024
    reply_bytes: Annotated[int, Field(ge=1, strict=True)] = 65536
    max_tables: Annotated[int, Field(ge=1, le=32, strict=True)] = 32


_DEFAULT_LIMITS = Limits()


class ColumnInfo(_Model):
    """Exact full-scan counts; distinct excludes NULL and is omitted for nested types."""

    name: str
    type: str
    nulls: int
    distinct: int | None


class TablePreview(_Model):
    """An input-order prefix; truncated_cells counts clipped cells in retained rows."""

    columns: tuple[str, ...]
    rows: tuple[tuple[JsonValue, ...], ...]
    truncated_rows: bool
    truncated_columns: bool
    truncated_cells: int


class TableInspection(_Model):
    """One input's exact statistics, root-relative resolved path and bounded preview."""

    name: str
    path: str
    format: Literal["csv", "parquet", "duckdb"]
    rows: int
    columns: tuple[ColumnInfo, ...]
    preview: TablePreview


class ForeignKeyCheck(_Model):
    """Exact child-row counts; NULL is no reference and duplicate parents do not multiply."""

    fk: str
    child_non_null: int
    orphans: int


class DatasetInspection(_Model):
    """Exact input descriptions, untrusted previews and a replaceable predicate draft."""

    format: Literal[1] = 1
    tables: tuple[TableInspection, ...]
    foreign_keys: tuple[ForeignKeyCheck, ...]
    config_template: str
    truncated: bool
    warnings: tuple[str, ...]


def _load_table(
    con: duckdb.DuckDBPyConnection, name: str, path: Path, fmt: str, stage: str
) -> None:
    """Copy only a literal file or a physical, read-only database table into memory."""
    if fmt != "duckdb":
        reader = "read_csv" if fmt == "csv" else "read_parquet"
        con.execute(f"CREATE TABLE {stage} AS SELECT * FROM {reader}({_lit(str(path))})")
        return
    alias = "__fs_source"
    con.execute(f"ATTACH {_lit(str(path))} AS {q(alias)} (READ_ONLY)")
    try:
        views = con.execute(
            "SELECT 1 FROM duckdb_views() WHERE database_name = ? AND view_name = ?",
            [alias, name],
        ).fetchone()
        if views:
            raise ConfigError(f"table {name!r}: is a view; only physical tables are accepted")
        found = con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = ? "
            "AND schema_name = 'main' AND table_name = ?",
            [alias, name],
        ).fetchone()
        if not found:
            raise ConfigError(f"table {name!r}: no table {name!r} in {path.name}")
        con.execute(f"CREATE TABLE {stage} AS SELECT * FROM {q(alias)}.main.{q(name)}")
    finally:
        con.execute(f"DETACH {q(alias)}")


def _nested(dtype: str) -> bool:
    """Recognize DuckDB list/array, struct, map and union type renderings."""
    return dtype.endswith("]") or dtype.startswith(("STRUCT(", "MAP(", "UNION("))


def _json_cell(text: str | None, dtype: str, clipped: bool) -> JsonValue:
    """Restore small JSON numbers and booleans from bounded SQL text, preserving others."""
    if text is None or clipped:
        return text
    if dtype == "BOOLEAN":
        return text == "true"
    if dtype in _INTEGER_TYPES:
        if len(text.lstrip("-")) > len(str(_MAX_JSON_INT)):
            return text
        number = int(text)
        return number if abs(number) <= _MAX_JSON_INT else text
    decimal = dtype.startswith(("DECIMAL(", "NUMERIC("))
    if decimal or dtype in {"FLOAT", "REAL", "DOUBLE"}:
        value = float(text)
        if math.isfinite(value) and (not decimal or Decimal(str(value)) == Decimal(text)):
            return value
    return text


def _preview(
    con: duckdb.DuckDBPyConnection,
    stage: str,
    columns: tuple[ColumnInfo, ...],
    rows: int,
    limits: Limits,
) -> tuple[TablePreview, tuple[int, ...]]:
    """Fetch only bounded VARCHAR cells and truncation booleans, in insertion order."""
    shown = columns[: limits.max_columns]
    expressions = []
    for column in shown:
        text = f"CAST({q(column.name)} AS VARCHAR)"
        # A character prefix bounds SQL intermediates too. Cutting its encoded hex
        # to whole bytes and ignoring an incomplete final codepoint gives the
        # longest valid UTF-8 prefix, including when the limit is below four bytes.
        expressions.extend(
            [
                f"decode(unhex(left(hex(encode(left({text}, {limits.cell_bytes}))), "
                f"{2 * limits.cell_bytes})), 'ignore')",
                f"coalesce(octet_length(encode(left({text}, {limits.cell_bytes + 1}))) "
                f"> {limits.cell_bytes}, false)",
            ]
        )
    data = con.execute(
        f"SELECT {', '.join(expressions)} FROM {stage} LIMIT {limits.preview_rows}"
    ).fetchall()
    values = tuple(
        tuple(_json_cell(row[2 * i], col.type, row[2 * i + 1]) for i, col in enumerate(shown))
        for row in data
    )
    clipped = tuple(sum(bool(row[2 * i + 1]) for i in range(len(shown))) for row in data)
    return (
        TablePreview(
            columns=tuple(col.name for col in shown),
            rows=values,
            truncated_rows=rows > len(values),
            truncated_columns=len(columns) > len(shown),
            truncated_cells=sum(clipped),
        ),
        clipped,
    )


def _profile(
    con: duckdb.DuckDBPyConnection,
    name: str,
    path: str,
    fmt: Literal["csv", "parquet", "duckdb"],
    stage: str,
    limits: Limits,
) -> tuple[TableInspection, tuple[int, ...]]:
    """Aggregate every row and column before building the separately bounded preview."""
    schema = [(str(row[0]), str(row[1])) for row in con.execute(f"DESCRIBE {stage}").fetchall()]
    expressions = ["count(*)"]
    for column, dtype in schema:
        expressions.extend(
            [
                f"count(*) - count({q(column)})",
                "NULL" if _nested(dtype) else f"count(DISTINCT {q(column)})",
            ]
        )
    counts = con.execute(f"SELECT {', '.join(expressions)} FROM {stage}").fetchone()
    assert counts is not None
    rows = int(counts[0])
    columns = tuple(
        ColumnInfo(
            name=col,
            type=dtype,
            nulls=int(counts[2 * i + 1]),
            distinct=None if _nested(dtype) else int(counts[2 * i + 2]),
        )
        for i, (col, dtype) in enumerate(schema)
    )
    preview, clipped = _preview(con, stage, columns, rows, limits)
    return TableInspection(
        name=name,
        path=path,
        format=fmt,
        rows=rows,
        columns=columns,
        preview=preview,
    ), clipped


def _check_fk(
    con: duckdb.DuckDBPyConnection,
    fk: ForeignKey,
    stages: Mapping[str, str],
    tables: Mapping[str, TableInspection],
) -> ForeignKeyCheck:
    """Count child rows with an anti-existence test so parent duplicates have no effect."""
    for table, column in ((fk.child, fk.child_column), (fk.parent, fk.parent_column)):
        if column not in {col.name for col in tables[table].columns}:
            raise ConfigError(f"foreign key {fk}: table {table!r} has no column {column!r}")
    child, parent = f"c.{q(fk.child_column)}", f"p.{q(fk.parent_column)}"
    try:
        row = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE NOT EXISTS "
            f"(SELECT 1 FROM {stages[fk.parent]} p WHERE {parent} = {child})) "
            f"FROM {stages[fk.child]} c WHERE {child} IS NOT NULL"
        ).fetchone()
    except duckdb.Error as exc:
        raise ConfigError(f"foreign key {fk}: columns cannot be compared: {exc}") from exc
    assert row is not None
    return ForeignKeyCheck(fk=str(fk), child_non_null=int(row[0]), orphans=int(row[1]))


def _json_size(value: object) -> int:
    """Budget the standard ASCII-escaped JSON representation, including whitespace."""
    return len(json.dumps(value, allow_nan=False).encode("utf-8"))


def _fit_reply(
    result: DatasetInspection,
    clipped: Mapping[str, tuple[int, ...]],
    limit: int,
) -> DatasetInspection:
    """Drop row suffixes from the largest previews first, preserving all statistics."""
    size = _json_size(result.model_dump(mode="json"))
    if size <= limit:
        return result
    tables = list(result.tables)
    preview_sizes = [_json_size(table.preview.model_dump(mode="json")) for table in tables]
    order = sorted(
        range(len(tables)),
        key=preview_sizes.__getitem__,
        reverse=True,
    )
    size += _json_size(True) - _json_size(result.truncated)
    for i in order:
        while tables[i].preview.rows:
            table = tables[i]
            remaining = table.preview.rows[:-1]
            preview = table.preview.model_copy(
                update={
                    "rows": remaining,
                    "truncated_rows": True,
                    "truncated_cells": sum(clipped[table.name][: len(remaining)]),
                }
            )
            tables[i] = table.model_copy(update={"preview": preview})
            new_size = _json_size(preview.model_dump(mode="json"))
            size += new_size - preview_sizes[i]
            preview_sizes[i] = new_size
            if size <= limit:
                return result.model_copy(update={"tables": tuple(tables), "truncated": True})
    raise ConfigError("dataset too large to describe within the reply limit")


def inspect_tables(
    tables: Mapping[str, Path],
    *,
    root: Path,
    base_dir: Path,
    foreign_keys: Sequence[str] = (),
    formats: Mapping[str, str] | None = None,
    limits: Limits = _DEFAULT_LIMITS,
) -> DatasetInspection:
    """Inspect local files without running a predicate or writing any files.

    Counts cover full scans. The template uses config's table-name, FK graph and
    format rules, with paths relative to base_dir; reported paths are root-relative.
    Reply budgeting covers both standard json.dumps and compact Pydantic JSON.
    """
    if len(tables) > limits.max_tables:
        raise ConfigError(f"too many tables: maximum is {limits.max_tables}")
    root = literal_path(root.resolve())
    base_dir = resolve_inside(Path.cwd(), base_dir, root)
    if not root.is_dir() or not base_dir.is_dir():
        raise ConfigError("root and base_dir must be existing directories")
    if formats is not None and set(formats) - tables.keys():
        raise ConfigError("formats refers to an unknown table")
    paths = {
        name: resolve_inside(base_dir, literal_path(path), root)
        for name, path in sorted(tables.items())
    }
    for name, path in paths.items():
        if not path.is_file():
            raise ConfigError(f"table {name!r}: expected a regular file: {path}")
    specs = {
        name: {
            "path": Path(os.path.relpath(path, base_dir)).as_posix(),
            "format": formats.get(name) if formats is not None else None,
        }
        for name, path in paths.items()
    }
    config = {
        "tables": specs,
        "foreign_keys": list(foreign_keys),
        "predicate": {"sql": "SELECT false"},
    }
    project = parse_project(config, base_dir=base_dir, root=root)
    resolved_formats = {name: spec.resolved_format() for name, spec in project.tables.items()}
    for name, fmt in resolved_formats.items():
        specs[name]["format"] = fmt
    template = "# generated by failslice inspect_dataset; replace the predicate\n" + yaml.safe_dump(
        config, sort_keys=False
    )
    stages = {name: q(f"__fs_inspect_{i}") for i, name in enumerate(paths)}
    inspected: dict[str, TableInspection] = {}
    clipped: dict[str, tuple[int, ...]] = {}
    with duckdb.connect(":memory:") as con:
        con.execute("SET autoinstall_known_extensions = false")
        con.execute("SET autoload_known_extensions = false")
        con.execute("SET threads = 1")
        con.execute("SET temp_directory = ''")
        con.execute(f"SET allowed_directories = [{_lit(str(root).rstrip('/') + '/')}]")
        con.execute("SET enable_external_access = false")
        con.execute("SET preserve_insertion_order = true")
        for name, path in paths.items():
            fmt = resolved_formats[name]
            try:
                _load_table(con, name, path, fmt, stages[name])
                inspected[name], clipped[name] = _profile(
                    con,
                    name,
                    path.relative_to(root).as_posix(),
                    fmt,
                    stages[name],
                    limits,
                )
            except duckdb.Error as exc:
                raise ConfigError(f"table {name!r}: cannot inspect input: {exc}") from exc
        checks = tuple(_check_fk(con, fk, stages, inspected) for fk in project.foreign_keys)
    result = DatasetInspection(
        tables=tuple(inspected.values()),
        foreign_keys=checks,
        config_template=template,
        truncated=any(
            t.preview.truncated_rows or t.preview.truncated_columns or t.preview.truncated_cells
            for t in inspected.values()
        ),
        warnings=(),
    )
    return _fit_reply(result, clipped, limits.reply_bytes)
