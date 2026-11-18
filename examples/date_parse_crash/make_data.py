"""Generate the date-parsing crash example: 4 carriers, 30,003 shipment events.

The story: each carrier reports shipment times as text in its own documented style
(``iso``, ``eu`` = day/month, ``us`` = month/day with AM/PM), and the staging query parses
each row according to its carrier's style. One Globex depot (style ``eu``) started writing
US-style dates in March (``03/13/2024 08:00 AM``); read day-first, there is no month 13,
so the nightly load fails.
Three such rows exist; any one of them, with its carrier, is enough to crash the query.

Run from this directory: ``python make_data.py`` (writes ``data/generated/*.parquet``).
"""

from pathlib import Path

import duckdb


def main(out: Path = Path(__file__).parent / "data" / "generated") -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE carriers AS SELECT * FROM (VALUES (1, 'Acme Freight', 'iso'),"
            " (2, 'Globex Logistics', 'eu'), (3, 'Initech Parcel', 'iso'),"
            " (4, 'Umbrella Express', 'us')) t(carrier_id, name, date_style)"
        )
        con.execute(
            """CREATE TABLE shipments AS
            SELECT i AS shipment_id, 1 + i % 4 AS carrier_id,
                   CASE 1 + i % 4
                     WHEN 2 THEN strftime(TIMESTAMP '2024-01-01' + INTERVAL (i * 37) MINUTE,
                                          '%d/%m/%Y %H:%M')
                     WHEN 4 THEN strftime(TIMESTAMP '2024-01-01' + INTERVAL (i * 37) MINUTE,
                                          '%m/%d/%Y %I:%M %p')
                     ELSE strftime(TIMESTAMP '2024-01-01' + INTERVAL (i * 37) MINUTE,
                                   '%Y-%m-%d %H:%M:%S')
                   END AS shipped_at
            FROM range(30000) r(i)
            UNION ALL  -- the Globex depot that switched to the US style
            SELECT 30000 + j, 2,
                   strftime(TIMESTAMP '2024-03-13 08:00' + INTERVAL (j) DAY, '%m/%d/%Y %I:%M %p')
            FROM range(3) r(j)"""
        )
        for table in ("carriers", "shipments"):
            target = str(out / f"{table}.parquet").replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO '{target}'")
    finally:
        con.close()


if __name__ == "__main__":
    main()
