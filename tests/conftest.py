from __future__ import annotations

import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml

from failslice.config import Project, load_project

TableData = tuple[Sequence[tuple[str, str]], Sequence[Sequence[Any]]]
"""(columns as (name, duckdb type), rows)."""


def write_table(
    path: Path, columns: Sequence[tuple[str, str]], rows: Sequence[Sequence[Any]]
) -> None:
    """Write rows with explicit DuckDB types to Parquet or CSV (by suffix)."""
    con = duckdb.connect()
    try:
        cols = ", ".join(f'"{n}" {t}' for n, t in columns)
        con.execute(f"CREATE TABLE t ({cols})")
        if rows:
            marks = ", ".join("?" for _ in columns)
            con.executemany(f"INSERT INTO t VALUES ({marks})", [list(r) for r in rows])
        fmt = "csv" if path.suffix == ".csv" else "parquet"
        con.execute(f"COPY (SELECT * FROM t) TO '{path}' (FORMAT {fmt})")
    finally:
        con.close()


def make_project(
    base: Path,
    tables: Mapping[str, TableData],
    *,
    fks: Sequence[str] = (),
    predicate: Mapping[str, Any],
    settings: Mapping[str, Any] | None = None,
    suffix: str = ".parquet",
) -> Project:
    """Write tables and a failslice.yaml under ``base`` and load the project."""
    base.mkdir(parents=True, exist_ok=True)
    spec: dict[str, Any] = {"tables": {}, "foreign_keys": list(fks), "predicate": dict(predicate)}
    for name, (columns, rows) in tables.items():
        write_table(base / f"{name}{suffix}", columns, rows)
        spec["tables"][name] = {"path": f"{name}{suffix}"}
    if settings:
        spec["settings"] = dict(settings)
    (base / "failslice.yaml").write_text(yaml.safe_dump(spec, sort_keys=False))
    return load_project(base / "failslice.yaml")


@pytest.fixture
def project_factory(tmp_path: Path) -> Callable[..., Project]:
    counter = iter(range(1_000_000))

    def factory(tables: Mapping[str, TableData], **kwargs: Any) -> Project:
        return make_project(tmp_path / f"p{next(counter)}", tables, **kwargs)

    return factory


# A small three-level schema used by several tests: one customer has a duplicated
# order row ("late-arriving duplicate"), which inflates the joined revenue.
def fanout_tables(
    n_customers: int = 30, n_orders: int = 120, n_lines: int = 400
) -> dict[str, TableData]:
    customers = [(i, f"c{i}") for i in range(n_customers)]
    orders = [(i, i % n_customers) for i in range(n_orders)] + [(7, 7 % n_customers)]
    lines = [(i, i % n_orders, float(i % 5)) for i in range(n_lines)]
    return {
        "customers": ([("id", "BIGINT"), ("name", "VARCHAR")], customers),
        "orders": ([("order_id", "BIGINT"), ("customer_id", "BIGINT")], orders),
        "lines": ([("line_id", "BIGINT"), ("order_id", "BIGINT"), ("amount", "DOUBLE")], lines),
    }


FANOUT_FKS = ("orders.customer_id -> customers.id", "lines.order_id -> orders.order_id")
FANOUT_PREDICATE = {
    "sql": "SELECT (SELECT sum(l.amount) FROM lines l JOIN orders o USING (order_id)) "
    "<> (SELECT sum(amount) FROM lines)"
}


# Windows cannot create file names containing "*" or "?" and has no FIFOs; the behaviour
# these tests protect (refusing such paths) is platform-independent code.
posix_only = pytest.mark.skipif(
    sys.platform == "win32", reason="needs POSIX file names (*, ?) or FIFOs"
)
