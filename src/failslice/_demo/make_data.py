"""Generate the quickstart data: 1,000 customers, 5,001 orders, 20,000 order lines.

The planted bug: order 4242 was loaded twice ("late-arriving duplicate"), so joining
lines to orders double-counts that order's lines.

Run from this directory: ``python make_data.py`` (writes ``data/generated/*.csv``).
"""

from pathlib import Path

import duckdb


def _literal(path: Path) -> str:
    """``path`` as a SQL string literal (COPY ... TO takes no parameters)."""
    return "'" + str(path).replace("'", "''") + "'"


def main(out: Path = Path(__file__).parent / "data" / "generated") -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT i AS id, 'customer ' || i AS name FROM range(1000) r(i)) "
            f"TO {_literal(out / 'customers.csv')}"
        )
        con.execute(
            "COPY (SELECT i AS order_id, i % 1000 AS customer_id FROM range(5000) r(i) "
            f"UNION ALL SELECT 4242, 242) TO {_literal(out / 'orders.csv')}"
        )
        con.execute(
            "COPY (SELECT i AS line_id, i % 5000 AS order_id, (i % 7) * 1.5 AS amount "
            f"FROM range(20000) r(i)) TO {_literal(out / 'lines.csv')}"
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
