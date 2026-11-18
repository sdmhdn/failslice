"""Observations on the rows that survived reduction (``failslice explain``).

Everything here is computed from the run directory only (``spec.json`` and the exported
Parquet files) and describes the reduced data: row and NULL counts, duplicate values of
declared primary keys, and per foreign key the NULL references, surviving input orphans,
duplicate parent keys and the number of pairs the declared equality join produces per
key. These are observations, not a diagnosis: FailSlice does not know which join or
column the predicate depends on, and never claims a root cause.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict

from failslice.config import ForeignKey
from failslice.portable import DeadlineConnection
from failslice.runspec import RunSpec, restoring_select

__all__ = ["Explanation", "ForeignKeyObservation", "KeyObservation", "TableObservation", "explain"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TableObservation(_Model):
    """One reduced table."""

    name: str
    rows: int
    null_counts: dict[str, int]
    """Columns with at least one NULL."""
    primary_key: str | None
    pk_duplicate_values: int = 0
    """Values of the declared primary key that occur more than once."""
    pk_excess_rows: int = 0
    """Rows beyond the first for each duplicated value."""
    pk_nulls: int = 0


class KeyObservation(_Model):
    """One key value of a foreign key edge (as text)."""

    key: str
    children: int
    parents: int
    pairs: int
    """Rows the declared equality join produces for this key (children x parents)."""


class ForeignKeyObservation(_Model):
    """One foreign key edge on the reduced data."""

    fk: str
    child_rows: int
    null_references: int
    input_orphans: int
    """Surviving rows exempted as input orphans (their parent was missing in the input)."""
    duplicate_parent_keys: int
    """Key values referenced by a child row that have more than one parent row."""
    join_pairs: int
    keys: tuple[KeyObservation, ...]
    """Keys with the most join pairs (bounded by ``limit``)."""
    truncated: bool


class Explanation(_Model):
    """Observations on a run directory's reduced tables."""

    format: Literal[1] = 1
    tables: tuple[TableObservation, ...]
    foreign_keys: tuple[ForeignKeyObservation, ...]

    def render_text(self) -> str:
        """Plain-text rendering (also used in ``report.md``)."""
        lines = ["Observations on the rows that survived reduction (not a diagnosis):", ""]
        for t in self.tables:
            line = f"- table {t.name}: {_n(t.rows, 'row')}"
            if t.null_counts:
                nulls = ", ".join(f"{c} ({n})" for c, n in sorted(t.null_counts.items()))
                line += f"; NULLs in {nulls}"
            if t.primary_key is not None:
                if t.pk_duplicate_values:
                    line += (
                        f"; declared primary key {t.primary_key!r} has "
                        f"{_n(t.pk_duplicate_values, 'duplicated value')} "
                        f"({_n(t.pk_excess_rows, 'extra row')})"
                    )
                if t.pk_nulls:
                    line += f"; {_n(t.pk_nulls, 'NULL value')} in {t.primary_key!r}"
            lines.append(line)
        for f in self.foreign_keys:
            parts = [_n(f.child_rows, "child row"), _n(f.join_pairs, "join pair")]
            if f.null_references:
                parts.append(_n(f.null_references, "NULL reference"))
            if f.input_orphans:
                parts.append(_n(f.input_orphans, "input orphan") + " (no parent in the input)")
            if f.duplicate_parent_keys:
                parts.append(_n(f.duplicate_parent_keys, "key") + " with more than one parent row")
            lines.append(f"- {f.fk}: " + ", ".join(parts))
            for k in f.keys:
                if k.parents > 1 or k.children > 1:
                    lines.append(
                        f"  - key {k.key}: {k.children} child x {_n(k.parents, 'parent row')} "
                        f"-> this declared equality join produces {_n(k.pairs, 'pair')}"
                    )
            if f.truncated:
                lines.append("  - (more keys not shown)")
        return "\n".join(lines) + "\n"


def _n(count: int, noun: str) -> str:
    """``1 row``, ``2 rows``."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def explain(run_dir: str | Path, *, limit: int = 20, deadline: float | None = None) -> Explanation:
    """Compute observations on the reduced tables of ``run_dir``; every query is bounded
    by ``deadline`` (absolute ``time.monotonic()``) and raises ``TimeoutError`` past it."""
    root = Path(run_dir).resolve()
    spec = RunSpec.read(root)
    raw = duckdb.connect(
        ":memory:",
        config={
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
            "temp_directory": "",
        },
    )
    con = DeadlineConnection(raw, deadline)
    try:
        for name, entry in spec.tables.items():
            source = f"read_parquet({_lit(str(spec.resolve(root, entry.file)))})"
            con.execute(
                f"CREATE TABLE {_q(name)} AS "
                + restoring_select(source, entry.columns, entry.source_columns)
            )
        orphans = spec.resolve(root, spec.orphans_file)
        con.execute(
            f"CREATE TABLE __fs_orphans AS SELECT edge FROM read_parquet({_lit(str(orphans))})"
        )
        con.execute("SET enable_external_access = false")
        tables = [
            _table(con, name, entry.primary_key, entry.source_columns)
            for name, entry in spec.tables.items()
        ]
        fks = [
            _foreign_key(con, i, ForeignKey.parse(text), limit)
            for i, text in enumerate(spec.foreign_keys)
        ]
        return Explanation(tables=tuple(tables), foreign_keys=tuple(fks))
    finally:
        raw.close()


def _one(con: DeadlineConnection, sql: str) -> tuple[int, ...]:
    row = con.fetchone(sql)
    assert row is not None
    return tuple(int(v or 0) for v in row)


def _table(
    con: DeadlineConnection,
    name: str,
    primary_key: str | None,
    columns: tuple[tuple[str, str], ...],
) -> TableObservation:
    t = _q(name)
    (rows,) = _one(con, f"SELECT count(*) FROM {t}")
    nulls: dict[str, int] = {}
    if columns:
        counts = _one(
            con,
            "SELECT " + ", ".join(f"count(*) - count({_q(c)})" for c, _ in columns) + f" FROM {t}",
        )
        nulls = {c: n for (c, _), n in zip(columns, counts, strict=True) if n}
    dup_values = excess = pk_nulls = 0
    if primary_key is not None and primary_key in {c for c, _ in columns}:
        k = _q(primary_key)
        dup_values, excess = _one(
            con,
            f"SELECT count(*), coalesce(sum(n - 1), 0) FROM (SELECT count(*) AS n FROM {t} "
            f"WHERE {k} IS NOT NULL GROUP BY {k} HAVING count(*) > 1)",
        )
        pk_nulls = nulls.get(primary_key, 0)
    return TableObservation(
        name=name,
        rows=rows,
        null_counts=nulls,
        primary_key=primary_key,
        pk_duplicate_values=dup_values,
        pk_excess_rows=excess,
        pk_nulls=pk_nulls,
    )


def _foreign_key(
    con: DeadlineConnection, edge: int, fk: ForeignKey, limit: int
) -> ForeignKeyObservation:
    c, p = _q(fk.child), _q(fk.parent)
    cc, pc = _q(fk.child_column), _q(fk.parent_column)
    child_rows, null_refs = _one(con, f"SELECT count(*), count(*) - count({cc}) FROM {c}")
    (orphans,) = _one(con, f"SELECT count(*) FROM __fs_orphans WHERE edge = {edge}")
    (dup_parents,) = _one(
        con,
        f"SELECT count(*) FROM (SELECT {pc} AS key FROM {p} WHERE {pc} IS NOT NULL "
        f"GROUP BY {pc} HAVING count(*) > 1) d WHERE EXISTS "
        f"(SELECT 1 FROM {c} x WHERE x.{cc} = d.key)",
    )
    per_key = (
        f"SELECT k.key, k.children, coalesce(pp.parents, 0) AS parents, "
        f"k.children * coalesce(pp.parents, 0) AS pairs FROM "
        f"(SELECT {cc} AS key, count(*) AS children FROM {c} WHERE {cc} IS NOT NULL "
        f"GROUP BY {cc}) k LEFT JOIN (SELECT {pc} AS key, count(*) AS parents FROM {p} "
        f"GROUP BY {pc}) pp ON pp.key = k.key"
    )
    (pairs, n_keys) = _one(con, f"SELECT coalesce(sum(pairs), 0), count(*) FROM ({per_key})")
    rows = con.fetchall(
        f"SELECT CAST(key AS VARCHAR), children, parents, pairs FROM ({per_key}) "
        f"ORDER BY pairs DESC, children DESC, CAST(key AS VARCHAR) LIMIT {int(limit)}"
    )
    keys = tuple(
        KeyObservation(key=str(k), children=int(ch), parents=int(pa), pairs=int(pr))
        for k, ch, pa, pr in rows
    )
    return ForeignKeyObservation(
        fk=str(fk),
        child_rows=child_rows,
        null_references=null_refs,
        input_orphans=orphans,
        duplicate_parent_keys=dup_parents,
        join_pairs=pairs,
        keys=keys,
        truncated=n_keys > len(keys),
    )
