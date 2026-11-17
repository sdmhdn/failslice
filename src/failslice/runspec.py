"""The run-directory contract: ``spec.json`` (format 3).

:class:`RunSpec` is written by :meth:`failslice.engine.Engine.export` and is the only
description of a reproducer that replay (``failslice.verify``) and later consumers
(report, generated pytest, CLI, MCP) read. Every file it names is relative to the run
directory and must stay inside it.

Column types: ``source_columns`` is the schema as loaded; ``columns`` is the schema of
the exported Parquet file. They differ where Parquet has no equivalent type (e.g. an
``ENUM`` is stored as ``VARCHAR``); replay casts such columns back to the source type
(:func:`restoring_select`), so the predicate sees the same types as during reduction.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlglot import exp
from sqlglot.errors import SqlglotError

from failslice.config import (
    RESERVED_PREFIX,
    ExpectPredicate,
    ForeignKey,
    RaisesPredicate,
    SqlPredicate,
    literal_path,
)

__all__ = [
    "SPEC_FILE",
    "InputFile",
    "ReplaySettings",
    "RunSpec",
    "RunSpecError",
    "TableEntry",
    "restoring_exprs",
    "restoring_select",
]

SPEC_FILE = "spec.json"
MAX_SPEC_BYTES = 16 * 1024 * 1024
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FORMAT = 3

Column = tuple[str, str]


class RunSpecError(ValueError):
    """The run directory's ``spec.json`` is missing, malformed or points outside it."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InputFile(_Model):
    """Identity of one original input file at load time (provenance only; replay never
    reads it)."""

    path: str
    sha256: str
    rows: int | None = None
    """Rows loaded from it (tables only)."""


class TableEntry(_Model):
    """One exported table."""

    file: str
    rows: int
    columns: tuple[Column, ...]
    """Schema of the exported Parquet file."""
    source_columns: tuple[Column, ...]
    """Schema as loaded from the input (what the predicate saw)."""
    primary_key: str | None = None
    """Declared primary key (informational, never enforced)."""


class ReplaySettings(_Model):
    """Settings replay needs."""

    threads: Annotated[int, Field(ge=1)]
    eval_timeout_s: Annotated[float, Field(gt=0)]


class RunSpec(_Model):
    """Everything needed to replay and describe a reduced reproducer."""

    format: Literal[3] = 3
    failslice_version: str
    duckdb_version: str
    tables: dict[str, TableEntry]
    """In parents-first order."""
    foreign_keys: tuple[str, ...]
    predicate: Annotated[
        SqlPredicate | ExpectPredicate | RaisesPredicate, Field(discriminator="kind")
    ]
    expected_file: str | None = None
    """Snapshot of the expectation file (Parquet, inside the run directory)."""
    expected_yaml: str | None = None
    """Inline expectation as YAML (keeps types such as dates that JSON would flatten)."""
    inputs: dict[str, InputFile] = Field(default_factory=dict)
    """``table:<name>`` and ``expected`` -> original file identity."""
    settings: ReplaySettings
    orphans_file: str = "orphans.parquet"
    rowmap_file: str = "rowmap.parquet"
    warnings: tuple[str, ...] = ()
    """Export problems replay cannot repair (e.g. a lossy type round trip)."""
    run_settings: dict[str, Any] = Field(default_factory=dict)
    """The effective run settings (seed, budgets, ...)."""
    input_orphans: dict[str, int] = Field(default_factory=dict)
    """Input orphans per FK edge in the full input (``child.col -> parent.col`` -> rows)."""

    @model_validator(mode="after")
    def _names_are_identifiers(self) -> RunSpec:
        """Table names become file names and SQL identifiers: plain identifiers only, and
        every foreign key must parse and refer to listed tables (a tampered spec must not
        steer file writes or SQL)."""
        for name in self.tables:
            if not _IDENT.fullmatch(name) or name.lower().startswith(RESERVED_PREFIX):
                raise ValueError(f"invalid table name {name!r}")
        for text in self.foreign_keys:
            fk = ForeignKey.parse(text)
            if fk.child not in self.tables or fk.parent not in self.tables:
                raise ValueError(f"foreign key {text!r} refers to an unknown table")
        return self

    def write(self, run_dir: Path) -> None:
        """Write ``spec.json`` into ``run_dir``."""
        (run_dir / SPEC_FILE).write_text(self.model_dump_json(indent=2) + "\n", encoding="utf-8")

    @classmethod
    def read(cls, run_dir: str | Path) -> RunSpec:
        """Read and validate ``spec.json`` from ``run_dir``."""
        path = Path(run_dir) / SPEC_FILE
        if not path.is_file():  # missing, or a FIFO / device that would block the read
            raise RunSpecError(f"{path} is missing or not a regular file")
        if path.stat().st_size > MAX_SPEC_BYTES:
            raise RunSpecError(f"{path} exceeds {MAX_SPEC_BYTES} bytes")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RunSpecError(f"cannot read {path}: {exc}") from exc
        if not isinstance(data, dict) or data.get("format") != FORMAT:
            got = data.get("format") if isinstance(data, dict) else None
            raise RunSpecError(f"{path}: unsupported spec format {got!r} (expected {FORMAT})")
        try:
            return cls.model_validate(data)
        except ValueError as exc:
            raise RunSpecError(f"{path}: {exc}") from exc

    def resolve(self, run_dir: str | Path, relative: str) -> Path:
        """Resolve a file named by the spec; it must stay inside ``run_dir`` (after
        following symlinks), be an existing regular file and a literal path for DuckDB's
        readers."""
        root = Path(run_dir).resolve()
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise RunSpecError(f"file outside the run directory: {relative}")
        if not path.is_file():  # regular files only: a FIFO or device would block a read
            raise RunSpecError(f"missing or not a regular file: {relative}")
        try:
            return literal_path(path)
        except ValueError as exc:
            raise RunSpecError(str(exc)) from exc


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _type_sql(text: str) -> str:
    """Re-render a type name through sqlglot's type parser, so a (tampered) type string
    can never smuggle SQL into a CAST: only a parsed data type is emitted. A string that
    does not round-trip exactly fails the later schema check instead."""
    try:
        return exp.DataType.build(text, dialect="duckdb").sql(dialect="duckdb")
    except (SqlglotError, ValueError) as exc:
        raise RunSpecError(f"invalid column type {text!r}: {exc}") from exc


def restoring_select(source: str, exported: Sequence[Column], wanted: Sequence[Column]) -> str:
    """``SELECT`` over ``source`` that casts columns whose exported type differs from
    the wanted (source) type back to it, keeping column order."""
    return f"SELECT {', '.join(restoring_exprs(exported, wanted)) or '*'} FROM {source}"


def restoring_exprs(exported: Sequence[Column], wanted: Sequence[Column]) -> list[str]:
    """Select expressions of :func:`restoring_select` (casts canonicalized by sqlglot)."""
    if [c for c, _ in exported] != [c for c, _ in wanted]:
        raise RunSpecError(
            f"column names differ: {[c for c, _ in exported]} != {[c for c, _ in wanted]}"
        )
    return [
        _q(name) if have == want else f"CAST({_q(name)} AS {_type_sql(want)}) AS {_q(name)}"
        for (name, have), (_, want) in zip(exported, wanted, strict=True)
    ]
