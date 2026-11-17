"""Pure-Python reference model for property tests and the brute-force oracle.

Random schemas: tables ``t0..tN``; every table has columns ``k`` (key, duplicates
allowed), ``v`` (payload) and one FK column ``f<j>`` per parent ``t<j>`` (j < i, so the
graph is acyclic). FK values come from a small domain, may be NULL, and may be orphans.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from hypothesis import strategies as st

from failslice.config import Project
from tests.conftest import make_project

Rows = list[dict[str, Any]]


@dataclass(frozen=True)
class Schema:
    parents: dict[str, tuple[str, ...]]  # table -> parent tables
    rows: dict[str, Rows]

    @property
    def tables(self) -> list[str]:
        return list(self.parents)

    def fk_texts(self) -> list[str]:
        return [f"{t}.f{p[1:]} -> {p}.k" for t, ps in self.parents.items() for p in ps]

    def columns(self, table: str) -> list[tuple[str, str]]:
        cols = [("k", "BIGINT"), ("v", "BIGINT")]
        cols += [(f"f{p[1:]}", "BIGINT") for p in self.parents[table]]
        return cols

    def edges(self) -> list[tuple[str, str, str]]:
        """(child, fk column, parent) in the same order as ``fk_texts``."""
        return [(t, f"f{p[1:]}", p) for t, ps in self.parents.items() for p in ps]

    def orphans(self) -> set[tuple[int, int]]:
        """(edge index, child row id) pairs that are orphans in the full input."""
        out = set()
        for e, (child, col, parent) in enumerate(self.edges()):
            keys = {r["k"] for r in self.rows[parent]}
            for rid, r in enumerate(self.rows[child]):
                if r[col] is not None and r[col] not in keys:
                    out.add((e, rid))
        return out

    def project(self, base: Path, predicate: str, **settings: Any) -> Project:
        tables = {
            t: (self.columns(t), [tuple(r[c] for c, _ in self.columns(t)) for r in self.rows[t]])
            for t in self.tables
        }
        return make_project(
            base,
            tables,
            fks=self.fk_texts(),
            predicate={"sql": predicate},
            settings=settings or None,
        )


def greatest_intact(schema: Schema, kept: Mapping[str, set[int]]) -> dict[str, set[int]]:
    """Reference implementation of G: drop unsupported child rows, parents first."""
    orphans = schema.orphans()
    edges = schema.edges()
    out: dict[str, set[int]] = {}
    for table in schema.tables:  # t0..tN is a topological order
        keep = set()
        for rid in kept[table]:
            row = schema.rows[table][rid]
            ok = True
            for e, (child, col, parent) in enumerate(edges):
                if child != table or row[col] is None or (e, rid) in orphans:
                    continue
                if not any(schema.rows[parent][p]["k"] == row[col] for p in out[parent]):
                    ok = False
                    break
            if ok:
                keep.add(rid)
        out[table] = keep
    return out


def is_intact(schema: Schema, kept: Mapping[str, set[int]]) -> bool:
    return greatest_intact(schema, kept) == {t: set(kept[t]) for t in schema.tables}


def evaluate(schema: Schema, kept: Mapping[str, Iterable[int]], predicate: str) -> bool | None:
    """Evaluate a boolean predicate on a configuration in a fresh DuckDB (None = error)."""
    con = duckdb.connect()
    try:
        for t in schema.tables:
            cols = schema.columns(t)
            con.execute(f"CREATE TABLE {t} ({', '.join(f'{c} {ty}' for c, ty in cols)})")
            rows = [[schema.rows[t][rid][c] for c, _ in cols] for rid in sorted(kept[t])]
            if rows:
                con.executemany(f"INSERT INTO {t} VALUES ({', '.join('?' * len(cols))})", rows)
        try:
            res = con.execute(predicate).fetchall()
        except duckdb.Error:
            return None
        return (
            bool(res[0][0])
            if len(res) == 1 and len(res[0]) == 1 and res[0][0] is not None
            else False
        )
    finally:
        con.close()


def brute_force_minimum(schema: Schema, predicate: str) -> int | None:
    """Smallest total size of an intact configuration where the predicate is TRUE."""
    universe = [(t, rid) for t in schema.tables for rid in range(len(schema.rows[t]))]
    for size in range(len(universe) + 1):
        for combo in itertools.combinations(universe, size):
            kept: dict[str, set[int]] = {t: set() for t in schema.tables}
            for t, rid in combo:
                kept[t].add(rid)
            if is_intact(schema, kept) and evaluate(schema, kept, predicate):
                return size
    return None


_vals = st.integers(min_value=0, max_value=3)


@st.composite
def schemas(draw: st.DrawFn, max_tables: int = 4, max_rows: int = 6) -> Schema:
    n = draw(st.integers(min_value=1, max_value=max_tables))
    parents: dict[str, tuple[str, ...]] = {}
    rows: dict[str, Rows] = {}
    for i in range(n):
        ps = tuple(f"t{j}" for j in range(i) if draw(st.booleans()))
        parents[f"t{i}"] = ps
        count = draw(st.integers(min_value=0, max_value=max_rows))
        table_rows = []
        for _ in range(count):
            row: dict[str, Any] = {"k": draw(_vals), "v": draw(_vals)}
            for p in ps:
                row[f"f{p[1:]}"] = draw(st.one_of(st.none(), _vals, st.just(9)))  # 9 = orphan
            table_rows.append(row)
        rows[f"t{i}"] = table_rows
    return Schema(parents, rows)


@st.composite
def predicates(draw: st.DrawFn, schema: Schema) -> str:
    """Planted-bug predicates, monotone and non-monotone, single- and multi-table."""
    t = draw(st.sampled_from(schema.tables))
    c = draw(_vals)
    m = draw(st.integers(min_value=1, max_value=3))
    options = [
        f"SELECT count(*) >= {m} FROM {t} WHERE v = {c}",
        f"SELECT count(DISTINCT k) >= {m} FROM {t}",
        f"SELECT count(*) = {m} FROM {t} WHERE v >= {c}",  # non-monotone
        f"SELECT NOT EXISTS (SELECT 1 FROM {t} WHERE v = {c}) AND count(*) >= {m} FROM {t}",
    ]
    for child, col, parent in schema.edges():
        options.append(
            f"SELECT count(*) >= {m} FROM {child} x JOIN {parent} y ON x.{col} = y.k "
            f"WHERE y.v = {c}"
        )
        options.append(  # join fan-out: duplicated parent keys inflate the join
            f"SELECT (SELECT count(*) FROM {child} x JOIN {parent} y ON x.{col} = y.k) "
            f"> (SELECT count(*) FROM {child} x WHERE x.{col} IN (SELECT k FROM {parent}))"
        )
    return draw(st.sampled_from(options))


def kept_sets(rids: Mapping[str, Sequence[int]]) -> dict[str, set[int]]:
    return {t: set(v) for t, v in rids.items()}
