"""Generate the NOT IN example: 1,000 customers, 5,000 orders, 101 suppression records.

The story: marketing emails go to orders whose customer is not on the suppression
(unsubscribe) list. The list holds customers 0-99, plus one record from a web form whose
e-mail matched no account, so its ``customer_id`` is NULL. The mailing query uses
``NOT IN``; with a NULL in the list, ``x NOT IN (...)`` is never TRUE, and the campaign
silently selects nothing.

Run from this directory: ``python make_data.py`` (writes ``data/generated/*.parquet``).
"""

from pathlib import Path

import duckdb


def main(out: Path = Path(__file__).parent / "data" / "generated") -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE customers AS SELECT i AS customer_id, 'customer' || i || '@example.com'"
            " AS email FROM range(1000) r(i)"
        )
        con.execute(
            "CREATE TABLE orders AS SELECT i AS order_id, i % 1000 AS customer_id,"
            " DATE '2024-01-01' + (i % 300)::INTEGER AS ordered_on FROM range(5000) r(i)"
        )
        con.execute(
            "CREATE TABLE suppressions AS"
            " SELECT i + 1 AS suppression_id, i AS customer_id, 'unsubscribe link' AS source"
            " FROM range(100) r(i)"
            " UNION ALL SELECT 101, NULL, 'web form (no matching account)'"
        )
        for table in ("customers", "orders", "suppressions"):
            target = str(out / f"{table}.parquet").replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO '{target}'")
    finally:
        con.close()


if __name__ == "__main__":
    main()
