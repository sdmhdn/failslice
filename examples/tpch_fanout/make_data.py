"""Generate the TPC-H fan-out example: TPC-H at scale factor 0.01 plus one duplicated order.

The story: a status correction for one order arrived late and was appended to ``orders``
instead of replacing the original row, so the order key is no longer unique and every
report that joins line items to orders counts that order's lines twice.

The data comes from DuckDB's ``tpch`` extension (``CALL dbgen``). It is loaded from the
local extension cache when present, otherwise installed from extensions.duckdb.org. If you
are behind a proxy, set ``HTTPS_PROXY`` (credentials allowed); it is passed to DuckDB's own
proxy settings. Offline, install a matching ``tpch`` extension file first:
``duckdb.connect().install_extension("/path/to/tpch.duckdb_extension")``.

Run from this directory: ``python make_data.py`` (writes ``data/generated/*.parquet``).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import duckdb

SCALE_FACTOR = 0.01
TABLES = ("region", "nation", "customer", "orders", "lineitem")


class ExtensionUnavailableError(RuntimeError):
    """The ``tpch`` extension could not be loaded or installed."""


def _proxy(con: duckdb.DuckDBPyConnection) -> None:
    """Pass the configured proxy to DuckDB (it cannot parse credentials in the URL)."""
    url = next(
        (
            os.environ[k]
            for k in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")
            if os.environ.get(k)
        ),
        None,
    )
    if url is None:
        return
    parts = urlsplit(url if "://" in url else f"http://{url}")
    if parts.hostname is None:
        return
    con.execute("SET http_proxy = ?", [f"{parts.hostname}:{parts.port or 80}"])
    if parts.username:
        con.execute("SET http_proxy_username = ?", [unquote(parts.username)])
    if parts.password:
        con.execute("SET http_proxy_password = ?", [unquote(parts.password)])


def _redact(err: BaseException) -> str:
    """Never print proxy credentials that DuckDB may echo in its message."""
    return re.sub(r"://[^@/\s'\"]+@", "://***@", str(err))


def load_tpch(con: duckdb.DuckDBPyConnection) -> None:
    """Load ``tpch`` from the cache, installing it through the proxy if needed."""
    try:
        con.execute("LOAD tpch")
        return
    except duckdb.Error:
        pass
    # DuckDB rejects proxy URLs with credentials, so the proxy is set explicitly.
    installer = duckdb.connect()
    try:
        _proxy(installer)
        installer.execute("INSTALL tpch")
    except duckdb.Error as err:
        raise ExtensionUnavailableError(
            f"cannot install DuckDB's tpch extension ({_redact(err)}). Check network/proxy "
            "access to extensions.duckdb.org, or install a tpch extension file matching your "
            "DuckDB version and platform from a local path (see this file's docstring)."
        ) from err
    finally:
        installer.close()
    con.execute("LOAD tpch")


def main(
    out: Path = Path(__file__).parent / "data" / "generated", scale_factor: float = SCALE_FACTOR
) -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        load_tpch(con)
        con.execute("CALL dbgen(sf = ?)", [float(scale_factor)])
        # The late-arriving correction: the lowest order key with a discounted line,
        # appended again with a new status instead of updating the original row.
        con.execute(
            """INSERT INTO orders
               SELECT * REPLACE ('F' AS o_orderstatus) FROM orders
               WHERE o_orderkey = (SELECT min(l_orderkey) FROM lineitem
                                   WHERE l_extendedprice * (1 - l_discount) > 0)"""
        )
        for table in TABLES:
            target = str(out / f"{table}.parquet").replace("'", "''")
            con.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL) TO '{target}'")
    finally:
        con.close()


if __name__ == "__main__":
    try:
        main()
    except ExtensionUnavailableError as err:
        sys.exit(f"error: {err}")
