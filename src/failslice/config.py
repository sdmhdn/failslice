"""Configuration: ``failslice.yaml`` parsing and validation.

The config names the tables (local CSV / Parquet files, or tables in a read-only
``.duckdb`` file), optional primary keys, single-column foreign keys (which must form an
acyclic graph), the failure predicate and run settings. See ``docs/design.md`` §3.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

__all__ = [
    "ConfigError",
    "ExpectPredicate",
    "ForeignKey",
    "PredicateSpec",
    "Project",
    "RaisesPredicate",
    "Settings",
    "SqlPredicate",
    "TableSpec",
    "load_project",
    "parse_project",
]

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FK = re.compile(
    r"^\s*(?P<ct>[A-Za-z_][A-Za-z0-9_]*)\.(?P<cc>[A-Za-z_][A-Za-z0-9_]*)\s*->\s*"
    r"(?P<pt>[A-Za-z_][A-Za-z0-9_]*)\.(?P<pc>[A-Za-z_][A-Za-z0-9_]*)\s*$"
)
RESERVED_PREFIX = "__fs"
_REGEX_FLAGS = {"IGNORECASE": re.IGNORECASE, "MULTILINE": re.MULTILINE, "DOTALL": re.DOTALL}


class ConfigError(ValueError):
    """The configuration is invalid (bad file, schema, FK graph, path or predicate spec)."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TableSpec(_Model):
    """One input table."""

    path: str
    format: Literal["csv", "parquet", "duckdb"] | None = None
    source_table: str | None = None
    """Table name inside a ``.duckdb`` file (defaults to the configured table name)."""
    primary_key: str | None = None
    """Informational only: never enforced, since duplicate keys are often the bug."""

    def resolved_format(self) -> Literal["csv", "parquet", "duckdb"]:
        """Return the explicit format, or infer it from the file extension."""
        if self.format is not None:
            return self.format
        suffix = Path(self.path).suffix.lower()
        if suffix in {".csv", ".tsv", ".txt"} or self.path.lower().endswith(".csv.gz"):
            return "csv"
        if suffix in {".parquet", ".pq"}:
            return "parquet"
        if suffix in {".duckdb", ".db"}:
            return "duckdb"
        raise ConfigError(f"cannot infer the format of {self.path!r}; set `format:`")


class SqlPredicate(_Model):
    """Boolean form: a SELECT returning exactly one row with one BOOLEAN column."""

    kind: Literal["sql"] = "sql"
    sql: str


class ExpectPredicate(_Model):
    """Expectation form: the failure is present when the query result differs from
    (or, with ``mode: equals``, equals) the expected result."""

    kind: Literal["expect"] = "expect"
    query: str
    expected: Any = None
    """Inline expectation: a scalar (1x1 result) or a list of rows (lists)."""
    expected_file: str | None = None
    """CSV or Parquet file with the expected rows (read once when the project loads)."""
    mode: Literal["differs", "equals"] = "differs"
    ordered: bool = False
    float_tolerance: Annotated[float, Field(ge=0)] | None = None


class RaisesPredicate(_Model):
    """Error form: the failure is present when the query raises a DuckDB error whose
    message matches ``match`` (``re.search``) and, if given, whose class name is
    ``error_class`` (e.g. ``ConversionException``)."""

    kind: Literal["raises"] = "raises"
    query: str
    match: str
    error_class: str | None = None
    flags: tuple[Literal["IGNORECASE", "MULTILINE", "DOTALL"], ...] = ()

    @field_validator("match")
    @classmethod
    def _regex_compiles(cls, value: str) -> str:
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError(f"invalid regex: {exc}") from exc
        return value

    def compiled(self) -> re.Pattern[str]:
        """Return the compiled message regex with the configured flags."""
        flags = 0
        for name in self.flags:
            flags |= _REGEX_FLAGS[name]
        return re.compile(self.match, flags)


PredicateSpec = SqlPredicate | ExpectPredicate | RaisesPredicate


class Settings(_Model):
    """Run settings. Time values are seconds."""

    seed: int = 0
    threads: Annotated[int, Field(ge=1)] = 1
    eval_timeout_s: Annotated[float, Field(gt=0)] = 30.0
    max_evals: Annotated[int, Field(ge=1)] = 5000
    wall_clock_s: Annotated[float, Field(gt=0)] = 900.0
    minimality: bool = True
    allow_nondeterministic: bool = False


class _RawConfig(_Model):
    version: Literal[1] = 1
    tables: dict[str, TableSpec]
    foreign_keys: list[str] = Field(default_factory=list)
    predicate: dict[str, Any]
    settings: Settings = Field(default_factory=Settings)


@dataclass(frozen=True)
class ForeignKey:
    """A single-column reference ``child.child_column -> parent.parent_column``.

    References are by value; the parent column need not be unique. NULL means "no
    reference".
    """

    child: str
    child_column: str
    parent: str
    parent_column: str

    def __str__(self) -> str:
        return f"{self.child}.{self.child_column} -> {self.parent}.{self.parent_column}"

    @classmethod
    def parse(cls, text: str) -> ForeignKey:
        """Parse ``"child.col -> parent.col"``."""
        m = _FK.match(text)
        if m is None:
            raise ConfigError(f"foreign key {text!r} must look like 'child.col -> parent.col'")
        return cls(m["ct"], m["cc"], m["pt"], m["pc"])


@dataclass(frozen=True)
class Project:
    """A validated project: tables, FK graph, predicate and settings.

    ``base_dir`` is the directory relative paths are resolved against; ``root`` is the
    allowed root every input path must stay inside.
    """

    base_dir: Path
    root: Path
    tables: Mapping[str, TableSpec]
    foreign_keys: tuple[ForeignKey, ...]
    predicate: PredicateSpec
    settings: Settings
    topo_order: tuple[str, ...]
    """Table names, parents before children (ties broken by name)."""

    def table_path(self, name: str) -> Path:
        """Return the resolved, root-checked path of a table's source file."""
        return resolve_inside(self.base_dir, self.tables[name].path, self.root)

    def parents_of(self, table: str) -> tuple[ForeignKey, ...]:
        """FK edges whose child is ``table``."""
        return tuple(fk for fk in self.foreign_keys if fk.child == table)

    def descendants(self, table: str) -> frozenset[str]:
        """All tables reachable from ``table`` along child edges (excluding itself)."""
        seen: set[str] = set()
        stack = [table]
        while stack:
            current = stack.pop()
            for fk in self.foreign_keys:
                if fk.parent == current and fk.child not in seen:
                    seen.add(fk.child)
                    stack.append(fk.child)
        return frozenset(seen)

    def predicate_queries(self) -> tuple[str, ...]:
        """The SQL text(s) the predicate executes."""
        p = self.predicate
        return (p.sql,) if isinstance(p, SqlPredicate) else (p.query,)

    def with_settings(self, **changes: Any) -> Project:
        """Return a copy with some settings replaced (validated)."""
        merged = {**self.settings.model_dump(), **changes}
        try:
            settings = Settings.model_validate(merged)
        except ValidationError as exc:
            raise ConfigError(_format_validation(exc)) from exc
        return Project(
            self.base_dir,
            self.root,
            self.tables,
            self.foreign_keys,
            self.predicate,
            settings,
            self.topo_order,
        )


def resolve_inside(base: Path, path: str | Path, root: Path) -> Path:
    """Resolve ``path`` against ``base`` (following symlinks) and require it to be
    inside ``root``."""
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = base / candidate
    resolved = candidate.resolve()
    root_resolved = root.resolve()
    if not resolved.is_relative_to(root_resolved):
        raise ConfigError(f"path {str(path)!r} resolves outside the allowed root {root_resolved}")
    return resolved


def _format_validation(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(x) for x in err["loc"]) or "<root>"
        parts.append(f"{loc}: {err['msg']}")
    return "invalid config: " + "; ".join(parts)


def _parse_predicate(raw: Mapping[str, Any]) -> PredicateSpec:
    keys = [k for k in ("sql", "expect", "raises") if k in raw]
    if len(keys) != 1 or len(raw) != 1:
        raise ConfigError("predicate must have exactly one of the keys: sql, expect, raises")
    key = keys[0]
    value = raw[key]
    try:
        if key == "sql":
            if not isinstance(value, str):
                raise ConfigError("predicate.sql must be a string")
            return SqlPredicate(sql=value)
        if not isinstance(value, Mapping):
            raise ConfigError(f"predicate.{key} must be a mapping")
        if key == "expect":
            spec = ExpectPredicate.model_validate(dict(value))
            if (spec.expected is None) == (spec.expected_file is None):
                raise ConfigError("predicate.expect needs exactly one of: expected, expected_file")
            return spec
        return RaisesPredicate.model_validate(dict(value))
    except ValidationError as exc:
        raise ConfigError(_format_validation(exc)) from exc


def topological_order(tables: Sequence[str], fks: Sequence[ForeignKey]) -> tuple[str, ...]:
    """Order tables parents-first; raise ``ConfigError`` naming a cycle if there is one."""
    parents: dict[str, set[str]] = {t: set() for t in tables}
    for fk in fks:
        parents[fk.child].add(fk.parent)
    order: list[str] = []
    state: dict[str, int] = {}  # 1 = visiting, 2 = done

    def visit(node: str, path: list[str]) -> None:
        if state.get(node) == 2:
            return
        if state.get(node) == 1:
            cycle = [*path[path.index(node) :], node]
            raise ConfigError(
                "foreign keys form a cycle (not supported in v1): " + " -> ".join(cycle)
            )
        state[node] = 1
        for parent in sorted(parents[node]):
            visit(parent, [*path, node])
        state[node] = 2
        order.append(node)

    for table in sorted(tables):
        visit(table, [])
    return tuple(order)


def parse_project(data: Mapping[str, Any], *, base_dir: Path, root: Path | None = None) -> Project:
    """Validate an already-parsed config mapping. See :func:`load_project`."""
    if not isinstance(data, Mapping):
        raise ConfigError("config must be a mapping")
    try:
        raw = _RawConfig.model_validate(dict(data))
    except ValidationError as exc:
        raise ConfigError(_format_validation(exc)) from exc
    if not raw.tables:
        raise ConfigError("config must declare at least one table")
    for name in raw.tables:
        if not _IDENT.match(name):
            raise ConfigError(f"table name {name!r} must be a plain SQL identifier")
        if name.lower().startswith(RESERVED_PREFIX):
            raise ConfigError(f"table name {name!r} uses the reserved prefix {RESERVED_PREFIX!r}")
    lowered = [n.lower() for n in raw.tables]
    if len(set(lowered)) != len(lowered):
        raise ConfigError("table names must be unique ignoring case")

    fks: list[ForeignKey] = []
    for text in raw.foreign_keys:
        fk = ForeignKey.parse(text)
        for table in (fk.child, fk.parent):
            if table not in raw.tables:
                raise ConfigError(f"foreign key {fk} refers to unknown table {table!r}")
        if fk.child == fk.parent:
            raise ConfigError(
                f"foreign key {fk} is self-referencing (a cycle; not supported in v1)"
            )
        if fk in fks:
            raise ConfigError(f"foreign key {fk} is declared twice")
        fks.append(fk)

    predicate = _parse_predicate(raw.predicate)
    base = base_dir.resolve()
    allowed_root = (root if root is not None else base).resolve()
    project = Project(
        base_dir=base,
        root=allowed_root,
        tables=dict(raw.tables),
        foreign_keys=tuple(fks),
        predicate=predicate,
        settings=raw.settings,
        topo_order=topological_order(list(raw.tables), fks),
    )
    for name in project.tables:
        project.table_path(name)  # root check
    if isinstance(predicate, ExpectPredicate) and predicate.expected_file is not None:
        resolve_inside(base, predicate.expected_file, allowed_root)
    return project


def load_project(path: str | Path, *, root: str | Path | None = None) -> Project:
    """Load and validate a ``failslice.yaml``.

    Relative paths in the config are resolved against the config file's directory.
    Every input path must stay inside ``root`` (default: the config file's directory).
    Column-level checks (FK columns exist, types compare) happen when data is loaded.
    """
    config_path = Path(path).resolve()
    try:
        text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config {str(path)!r}: {exc}") from exc
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {str(path)!r}: {exc}") from exc
    return parse_project(
        data,
        base_dir=config_path.parent,
        root=Path(root) if root is not None else None,
    )
