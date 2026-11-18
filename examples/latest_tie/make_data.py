"""Generate the latest-record example: 1,000 accounts, 5,000 app events, 5,000 import events.

The story: an account's current status is the status change with the latest timestamp,
taken from two streams, the app and a nightly import. The streams normally interleave
(app events on even seconds, import events on odd seconds), but one import event was
re-stamped with the time of an app event for the same account. "Latest per account" by
``max(updated_at)`` then returns two current rows for that account, and every report keyed
by account counts it twice.

Run from this directory: ``python make_data.py`` (writes ``data/generated/*.parquet``).
"""

from pathlib import Path

import duckdb


def main(out: Path = Path(__file__).parent / "data" / "generated") -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE accounts AS SELECT i AS account_id, 'account ' || i AS name"
            " FROM range(1000) r(i)"
        )
        for table, offset, status in (
            ("app_events", 0, "'active'"),
            ("import_events", 1, "'synced'"),
        ):
            con.execute(
                f"""CREATE TABLE {table} AS
                SELECT i AS event_id, i % 1000 AS account_id,
                       {status} || '-' || (i // 1000) AS status,
                       TIMESTAMP '2024-06-01 00:00:00'
                         + to_seconds(2 * (i // 1000) + {offset}) AS updated_at
                FROM range(5000) r(i)"""
            )
        # The re-stamped import event: now at app event 4242's time, for the same account.
        con.execute(
            "UPDATE import_events SET updated_at = (SELECT updated_at FROM app_events"
            " WHERE event_id = 4242) WHERE event_id = 4242"
        )
        for table in ("accounts", "app_events", "import_events"):
            target = str(out / f"{table}.parquet").replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO '{target}'")
    finally:
        con.close()


if __name__ == "__main__":
    main()
