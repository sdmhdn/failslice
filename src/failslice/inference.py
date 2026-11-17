"""Data-based PK/FK suggestions and an explicitly unconfirmed ``init`` draft.

All evidence describes only the examined prefix. Suggestions never enable keys in
the generated configuration; the user must review them first.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict

from failslice.config import (
    RESERVED_PREFIX,
    ConfigError,
    ForeignKey,
    Settings,
    literal_path,
    resolve_inside,
    topological_order,
)
from failslice.engine import _lit, q

__all__ = [
    "ColumnProfile",
    "Confidence",
    "ForeignKeySuggestion",
    "InferenceReport",
    "PrimaryKeySuggestion",
    "TableProfile",
    "discover_tables",
    "render_config",
    "suggest_relationships",
]

Confidence = Literal["high", "medium", "low"]
_FORMATS: dict[str, Literal["csv", "parquet"]] = {
    ".csv": "csv",
    ".parquet": "parquet",
    ".pq": "parquet",
}
_NUMERIC = frozenset(
    [
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
        "FLOAT",
        "REAL",
        "DOUBLE",
        "DECIMAL",
        "NUMERIC",
    ]
)
_STRING = frozenset({"VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING", "ENUM"})


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ColumnProfile(_Model):
    """A column's DuckDB type and counts (distinct excludes NULL)."""

    name: str
    type: str
    nulls: int
    distinct: int


class TableProfile(_Model):
    """A file and its examined prefix; paths are relative to ``base_dir``."""

    name: str
    path: str
    format: Literal["csv", "parquet"]
    rows_examined: int
    complete_scan: bool
    columns: tuple[ColumnProfile, ...]


class PrimaryKeySuggestion(_Model):
    """A PK suggestion; duplicates count extra non-NULL rows per value."""

    table: str
    column: str
    confidence: Confidence
    rows_examined: int
    complete_scan: bool
    nulls: int
    duplicates: int
    reason: str


class ForeignKeySuggestion(_Model):
    """An FK suggestion with child-row evidence, without join multiplication.

    ``complete_scan`` covers both tables. ``parent_unique`` means the examined
    parent column is unique and NULL-free; ``rows_examined`` counts child rows.
    """

    child: str
    child_column: str
    parent: str
    parent_column: str
    confidence: Confidence
    rows_examined: int
    complete_scan: bool
    child_non_null: int
    matched: int
    unmatched: int
    parent_unique: bool
    reason: str


class InferenceReport(_Model):
    """Deterministic file profiles, reviewable suggestions and warnings."""

    format: Literal[1] = 1
    tables: tuple[TableProfile, ...]
    primary_keys: tuple[PrimaryKeySuggestion, ...]
    foreign_keys: tuple[ForeignKeySuggestion, ...]
    warnings: tuple[str, ...]


def _validate_names(names: list[str]) -> None:
    for name in names:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ConfigError(f"table name {name!r} must be a plain SQL identifier")
        if name.lower().startswith(RESERVED_PREFIX):
            raise ConfigError(f"table name {name!r} uses the reserved prefix {RESERVED_PREFIX!r}")
    if len({name.lower() for name in names}) != len(names):
        raise ConfigError("duplicate table names (ignoring case)")


def discover_tables(directory: Path, *, root: Path) -> dict[str, Path]:
    """List local CSV/Parquet files, normalize names and reject unsafe paths.

    Discovery is non-recursive and sorted by filename. Name collisions and the
    reserved ``__fs`` prefix require the user to rename an input.
    """
    directory = resolve_inside(Path.cwd(), directory, root)
    tables: dict[str, Path] = {}
    for entry in sorted(directory.iterdir(), key=lambda path: path.name):
        if entry.suffix.lower() not in _FORMATS:
            continue
        path = resolve_inside(directory, literal_path(entry), root)
        if not path.is_file():
            continue
        name = re.sub(r"[^a-z0-9_]", "_", entry.stem.lower())
        if name[0].isdigit():
            name = "t_" + name
        _validate_names([name])
        if name in tables:
            raise ConfigError(f"duplicate table name {name!r} after filename normalization")
        tables[name] = path
    return tables


def _scalar(dtype: str) -> bool:
    return not (
        dtype.split("(", 1)[0].upper() in {"LIST", "ARRAY", "STRUCT", "MAP", "UNION"}
        or re.search(r"\[\d*\]$", dtype)
    )


def _comparable(left: str, right: str) -> bool:
    left_base, right_base = (dtype.split("(", 1)[0].upper() for dtype in (left, right))
    return (
        left == right
        or (left_base in _NUMERIC and right_base in _NUMERIC)
        or (left_base in _STRING and right_base in _STRING)
    )


def _key_names(table: str) -> tuple[str, ...]:
    return ("id", f"{table.lower()}_id", f"{table.lower().removesuffix('s')}_id")


def _id_like(name: str, table: str) -> bool:
    """Limit PK suggestions and same-name FK fallbacks to identifier-like names."""
    lowered = name.lower()
    return (
        lowered in _key_names(table)
        or lowered == "uuid"
        or lowered.endswith(("_id", "_key", "_code"))
    )


def _scan_label(complete_scan: bool) -> str:
    return "full scan" if complete_scan else "sampled prefix"


def _profile(
    con: duckdb.DuckDBPyConnection,
    name: str,
    path: Path,
    base_dir: Path,
    stage: str,
    sample_rows: int | None,
) -> TableProfile:
    fmt = _FORMATS.get(path.suffix.lower())
    if fmt is None:
        raise ConfigError(f"table {name!r}: expected a CSV or Parquet file: {path}")
    if not path.is_file():
        raise ConfigError(f"table {name!r}: file not found: {path}")
    reader = "read_csv" if fmt == "csv" else "read_parquet"
    limit = "" if sample_rows is None else f" LIMIT {sample_rows + 1}"
    con.execute(f"CREATE TABLE {stage} AS SELECT * FROM {reader}({_lit(str(path))}){limit}")
    count = con.execute(f"SELECT count(*) FROM {stage}").fetchone()
    assert count is not None
    rows = int(count[0])
    complete_scan = sample_rows is None or rows <= sample_rows
    if not complete_scan:
        con.execute(f"CREATE OR REPLACE TABLE {stage} AS SELECT * FROM {stage} LIMIT {sample_rows}")
        assert sample_rows is not None
        rows = sample_rows
    columns = []
    for column, dtype, *_ in sorted(con.execute(f"DESCRIBE {stage}").fetchall()):
        counts = con.execute(
            f"SELECT count(*) - count({q(column)}), count(DISTINCT {q(column)}) FROM {stage}"
        ).fetchone()
        assert counts is not None
        columns.append(
            ColumnProfile(name=column, type=dtype, nulls=int(counts[0]), distinct=int(counts[1]))
        )
    return TableProfile(
        name=name,
        path=Path(os.path.relpath(path, base_dir)).as_posix(),
        format=fmt,
        rows_examined=rows,
        complete_scan=complete_scan,
        columns=tuple(columns),
    )


def _primary_keys(table: TableProfile) -> list[PrimaryKeySuggestion]:
    suggestions = []
    for column in table.columns:
        if not _scalar(column.type) or not _id_like(column.name, table.name):
            continue
        duplicates = table.rows_examined - column.nulls - column.distinct
        valid = column.nulls == 0 and duplicates == 0
        conventional = column.name.lower() in _key_names(table.name)
        if not valid and not conventional:
            continue
        confidence: Confidence = "low"
        if valid and table.complete_scan:
            confidence = "high" if conventional else "medium"
        evidence = (
            f"{table.rows_examined} rows examined, {column.nulls} nulls, "
            f"{duplicates} duplicate rows, {_scan_label(table.complete_scan)}"
        )
        if not valid:
            evidence += (
                "; nulls or duplicate rows weaken this suggestion; duplicates are often the bug"
            )
        suggestions.append(
            PrimaryKeySuggestion(
                table=table.name,
                column=column.name,
                confidence=confidence,
                rows_examined=table.rows_examined,
                complete_scan=table.complete_scan,
                nulls=column.nulls,
                duplicates=duplicates,
                reason=evidence,
            )
        )
    return suggestions


def _preferred_pk(
    table: TableProfile, suggestions: list[PrimaryKeySuggestion]
) -> PrimaryKeySuggestion | None:
    """Prefer NULL-free unique evidence, then conventional names, then lexical order."""
    names = _key_names(table.name)
    return min(
        (pk for pk in suggestions if pk.table == table.name),
        key=lambda pk: (
            bool(pk.nulls or pk.duplicates),
            names.index(pk.column.lower()) if pk.column.lower() in names else len(names),
            pk.column,
        ),
        default=None,
    )


def _foreign_key(
    con: duckdb.DuckDBPyConnection,
    stages: Mapping[str, str],
    child: TableProfile,
    column: ColumnProfile,
    parent: TableProfile,
    parent_column: ColumnProfile,
) -> ForeignKeySuggestion:
    counts = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE supported) FROM ("
        f"SELECT EXISTS (SELECT 1 FROM {stages[parent.name]} p "
        f"WHERE p.{q(parent_column.name)} = c.{q(column.name)}) AS supported "
        f"FROM {stages[child.name]} c WHERE c.{q(column.name)} IS NOT NULL)"
    ).fetchone()
    assert counts is not None
    non_null, matched = (int(value) for value in counts)
    unmatched = non_null - matched
    complete = child.complete_scan and parent.complete_scan
    unique = parent_column.nulls == 0 and parent_column.distinct == parent.rows_examined
    parent_evidence = "examined parent is unique and NULL-free"
    if not unique:
        duplicated = con.execute(
            f"SELECT count(*) FROM (SELECT {q(parent_column.name)} FROM {stages[parent.name]} "
            f"WHERE {q(parent_column.name)} IS NOT NULL GROUP BY {q(parent_column.name)} "
            "HAVING count(*) > 1)"
        ).fetchone()
        assert duplicated is not None
        parent_evidence = f"parent key not unique ({int(duplicated[0])} duplicated values)"
        if parent_column.nulls:
            parent_evidence += f", {parent_column.nulls} parent nulls"
    confidence: Confidence = "low"
    if complete:
        if unmatched == 0 and unique:
            confidence = "high"
        elif unmatched * 100 <= non_null or not unique:
            confidence = "medium"
    return ForeignKeySuggestion(
        child=child.name,
        child_column=column.name,
        parent=parent.name,
        parent_column=parent_column.name,
        confidence=confidence,
        rows_examined=child.rows_examined,
        complete_scan=complete,
        child_non_null=non_null,
        matched=matched,
        unmatched=unmatched,
        parent_unique=unique,
        reason=(
            f"{matched}/{non_null} non-NULL child rows matched, {unmatched} unmatched, "
            f"{_scan_label(complete)}; {parent_evidence}"
        ),
    )


def suggest_relationships(
    tables: Mapping[str, Path],
    *,
    root: Path,
    base_dir: Path,
    sample_rows: int | None = 10_000,
) -> InferenceReport:
    """Profile deterministic file prefixes in one restricted in-memory DuckDB.

    Reading N+1 rows distinguishes a complete N-row file from a sampled prefix.
    Only scalar, identifier-like columns participate in PK suggestions. FK
    comparisons prefer a PK suggestion of any confidence, then a same-name
    parent column, then the parent's id column.
    """
    if sample_rows is not None and (isinstance(sample_rows, bool) or sample_rows < 1):
        raise ConfigError("sample_rows must be a positive integer or None")
    _validate_names(list(tables))
    root, base_dir = root.resolve(), base_dir.resolve()
    paths = {
        name: resolve_inside(base_dir, literal_path(path), root)
        for name, path in sorted(tables.items())
    }
    stages = {name: q(f"__fs_inference_{i}") for i, name in enumerate(paths)}
    with duckdb.connect(":memory:") as con:
        con.execute(f"SET allowed_directories = [{_lit(str(root).rstrip('/') + '/')}]")
        con.execute("SET enable_external_access = false")
        con.execute("SET autoinstall_known_extensions = false")
        con.execute("SET autoload_known_extensions = false")
        con.execute("SET threads = 1")
        con.execute("SET preserve_insertion_order = true")
        profiles = tuple(
            _profile(con, name, path, base_dir, stages[name], sample_rows)
            for name, path in paths.items()
        )
        primary_keys = [pk for table in profiles for pk in _primary_keys(table)]
        preferred = {table.name: _preferred_pk(table, primary_keys) for table in profiles}
        foreign_keys = []
        for child in profiles:
            own_pk = preferred[child.name]
            for parent in profiles:
                if child.name == parent.name:
                    continue
                parent_pk = preferred[parent.name]
                candidates = set(_key_names(parent.name)[1:])
                if parent_pk is not None and parent_pk.column.lower() != "id":
                    candidates.add(parent_pk.column.lower())
                if parent_pk is None:
                    candidates.update(
                        col.name.lower()
                        for col in parent.columns
                        if col.name.lower() != "id" and _id_like(col.name, parent.name)
                    )
                for column in child.columns:
                    if (
                        column.name.lower() not in candidates
                        or (own_pk is not None and column.name == own_pk.column)
                        or not _scalar(column.type)
                    ):
                        continue
                    parent_names = (
                        (parent_pk.column,) if parent_pk is not None else (column.name, "id")
                    )
                    parent_column = next(
                        (
                            col
                            for name in parent_names
                            for col in parent.columns
                            if col.name.lower() == name.lower()
                        ),
                        None,
                    )
                    if (
                        parent_column is None
                        or not _scalar(parent_column.type)
                        or not _comparable(column.type, parent_column.type)
                    ):
                        continue
                    foreign_keys.append(
                        _foreign_key(con, stages, child, column, parent, parent_column)
                    )
    foreign_keys.sort(key=lambda fk: (fk.child, fk.child_column, fk.parent, fk.parent_column))
    warnings = []
    try:
        topological_order(
            list(paths),
            [
                ForeignKey(fk.child, fk.child_column, fk.parent, fk.parent_column)
                for fk in foreign_keys
            ],
        )
    except ConfigError as exc:
        warnings.append(f"Foreign key suggestions need review: {exc}")
    return InferenceReport(
        tables=profiles,
        primary_keys=tuple(primary_keys),
        foreign_keys=tuple(foreign_keys),
        warnings=tuple(warnings),
    )


def _comment(text: str) -> str:
    """Keep arbitrary evidence and identifiers on a single YAML comment line."""
    return text.replace("\r", r"\r").replace("\n", r"\n")


def render_config(report: InferenceReport) -> str:
    """Render a valid YAML draft with all PK/FK suggestions commented out."""
    if not report.tables:
        raise ConfigError("cannot render a configuration without tables")
    lines = [
        "# Generated by `failslice init`.",
        "# Suggestions are inferred from data and must be reviewed.",
    ]
    lines.extend(f"# {_comment(warning)}" for warning in sorted(report.warnings))
    lines.extend(["version: 1", "tables:"])
    for table in sorted(report.tables, key=lambda table: table.name):
        lines.extend(
            [
                f"  {json.dumps(table.name)}:",
                f"    path: {json.dumps(table.path)}",
                f"    format: {table.format}",
            ]
        )
        for pk in sorted(report.primary_keys, key=lambda pk: (pk.table, pk.column)):
            if pk.table == table.name:
                column = (
                    pk.column
                    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", pk.column)
                    else json.dumps(pk.column)
                )
                lines.append(
                    f"    # primary_key: {column}  # suggestion, {pk.confidence}: "
                    f"{_comment(pk.reason)}"
                )
    lines.extend(["", "foreign_keys: []"])
    for fk in sorted(
        report.foreign_keys,
        key=lambda fk: (fk.child, fk.child_column, fk.parent, fk.parent_column),
    ):
        lines.append(
            _comment(
                f"#  - {fk.child}.{fk.child_column} -> {fk.parent}.{fk.parent_column}"
                f"  # suggestion, {fk.confidence}: {fk.reason}"
            )
        )
    lines.extend(
        [
            "",
            "predicate:",
            "  sql: SELECT false  # REPLACE: a SELECT that is TRUE when the bug is present",
            "",
        ]
    )
    lines.extend(
        "# " + line
        for line in yaml.safe_dump(
            {"settings": Settings().model_dump()}, sort_keys=False
        ).splitlines()
    )
    return "\n".join(lines) + "\n"
