"""Predicate safety rules, including sqlglot's DuckDB-specific AST forms."""

from collections.abc import Collection

import duckdb
import pytest

from failslice.safety import PredicateValidationError, validate_predicate_sql

_TABLES = {"orders", "lines", "events", "customers", "readings"}


def _rejects(sql: str, reason: str, detail: str = "") -> None:
    with pytest.raises(PredicateValidationError) as raised:
        validate_predicate_sql(sql, _TABLES)
    error = raised.value
    assert isinstance(error, ValueError)
    assert error.reason == reason
    assert error.detail
    assert detail.lower() in error.detail.lower()
    assert str(error) == error.detail


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT true",
        "SELECT 1;",
        "-- leading comment\n SELECT 1; -- trailing comment",
        "/* SELECT 2; */ SELECT '; CREATE TABLE x(a INT)'",
        "SELECT 1; /* trailing comment */",
        "((SELECT 1))",
        "SELECT 1 UNION SELECT 2",
        "SELECT 1 UNION ALL SELECT 2",
        "SELECT 1 INTERSECT SELECT 2",
        "SELECT 1 EXCEPT SELECT 2",
        "(SELECT 1 UNION SELECT 2) INTERSECT (SELECT 2 EXCEPT SELECT 3)",
        "SELECT * FROM (VALUES (1), (2)) AS t(x)",
        "SELECT 1 UNION VALUES (2)",
        "WITH v AS (VALUES (1)) SELECT * FROM v",
        "SELECT o.order_id FROM orders o JOIN lines l USING (order_id)",
        "SELECT * FROM orders, lines",
        "SELECT * FROM (orders JOIN lines USING (order_id))",
        "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM lines l WHERE l.order_id = o.order_id)",
        "SELECT * FROM orders WHERE order_id IN (SELECT order_id FROM lines)",
        "SELECT * FROM orders o, LATERAL (SELECT * FROM lines l "
        "WHERE l.order_id = o.order_id) AS l",
        "SELECT * FROM LATERAL (VALUES (1)) AS v(x)",
        "SELECT abs(-1), coalesce(NULL, 1), greatest(1, 2), list_sum([1, 2])",
        "SELECT CASE WHEN raw_value IS NULL THEN 0 ELSE TRY_CAST(raw_value AS INTEGER) END "
        "FROM readings",
        "SELECT upper(trim(raw_value)), lower(raw_value), length(raw_value), "
        "substr(raw_value, 1, 2), concat(raw_value, ';'), "
        "regexp_matches(raw_value, '[0-9]+'), regexp_replace(raw_value, 'x', 'y') FROM readings",
        "SELECT row_number() OVER (PARTITION BY k ORDER BY ts DESC) AS rn FROM events "
        "QUALIFY rn = 1",
        "SELECT count(*) FILTER (WHERE amount > 0), sum(amount), avg(amount), min(amount), "
        "max(amount) FROM lines",
        "SELECT epoch_ms(0), epoch_ms(TIMESTAMP '2020-01-01')",
        "SELECT random_column, now_column, orders.current_date FROM orders",
        "SELECT 'random() read_text(''x'')', 1 AS current_setting",
        "SELECT 1 AS random FROM orders AS now",
    ],
)
def test_allowed_queries(sql: str) -> None:
    assert validate_predicate_sql(sql, _TABLES) is None


@pytest.mark.parametrize(
    ("sql", "allowed"),
    [
        ("SELECT * FROM ORDERS", {"orders"}),
        ('SELECT * FROM "OrDeRs"', {"ORDERS"}),
        ("SELECT * FROM orders", ["OrDeRs"]),
        ("SELECT * FROM orders", ("orders",)),
        ("SELECT * FROM 'orders'", {"orders"}),
        ('SELECT * FROM "file.parquet"', {"FILE.PARQUET"}),
        ('SELECT * FROM "table with spaces"', {"table with spaces"}),
        ("SELECT 1", []),
    ],
)
def test_allowed_table_collections(sql: str, allowed: Collection[str]) -> None:
    validate_predicate_sql(sql, allowed)


@pytest.mark.parametrize(
    "sql",
    [
        "WITH x AS (SELECT * FROM orders) SELECT * FROM x",
        "WITH x AS (SELECT * FROM orders), y AS (SELECT * FROM x) SELECT * FROM y",
        "WITH X AS (SELECT * FROM orders) SELECT * FROM x",
        'WITH "MiXeD" AS (SELECT * FROM orders) SELECT * FROM mixed',
        "WITH orders AS (SELECT 1) SELECT * FROM orders",
        "WITH orders AS (SELECT * FROM orders) SELECT * FROM orders",
        "WITH x AS (SELECT * FROM orders) SELECT * FROM (SELECT * FROM x)",
        "WITH x AS (SELECT * FROM orders) SELECT 1 WHERE EXISTS (SELECT 1 FROM x)",
        "WITH x AS (SELECT * FROM orders) SELECT * FROM x UNION SELECT * FROM x",
        "WITH x AS (SELECT * FROM orders) SELECT * FROM x, LATERAL (SELECT * FROM x) AS y",
        "WITH x AS (SELECT * FROM orders) SELECT * FROM "
        "(WITH x AS (SELECT * FROM x) SELECT * FROM x)",
        "WITH RECURSIVE counts(n) AS (SELECT 1 UNION ALL "
        "SELECT n + 1 FROM COUNTS WHERE n < 3) SELECT * FROM counts",
        "WITH RECURSIVE x AS (SELECT * FROM orders), y AS (SELECT * FROM x) SELECT * FROM y",
        "WITH x AS (SELECT * FROM orders), y AS (SELECT * FROM (SELECT * FROM x)) SELECT * FROM y",
    ],
)
def test_cte_visibility_and_shadowing(sql: str) -> None:
    validate_predicate_sql(sql, _TABLES)


@pytest.mark.parametrize(
    "sql", ["", " \n\t", "-- comment", "/* comment */", ";", ";;", "; -- trailing comment"]
)
def test_empty_input(sql: str) -> None:
    _rejects(sql, "not_select", "empty")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "SELECT 1; DROP TABLE orders",
        "SELECT 1; SELECT 2;",
        "-- comment\n SELECT 1; /* comment */ SELECT 2",
    ],
)
def test_multiple_statements(sql: str) -> None:
    _rejects(sql, "multiple_statements", "found 2")


@pytest.mark.parametrize("sql", ["42", "orders", "CHECKPOINT", "VALUES (1)"])
def test_non_select_expressions(sql: str) -> None:
    _rejects(sql, "not_select", "SELECT")


@pytest.mark.parametrize(
    ("sql", "construct"),
    [
        ("CREATE TABLE t(x INT)", "CREATE"),
        ("DROP TABLE orders", "DROP"),
        ("ALTER TABLE orders ADD x INT", "ALTER"),
        ("INSERT INTO orders VALUES (1)", "INSERT"),
        ("UPDATE orders SET amount = 0", "UPDATE"),
        ("DELETE FROM orders", "DELETE"),
        (
            "MERGE INTO orders USING lines ON true WHEN MATCHED THEN DELETE",
            "MERGE",
        ),
        ("COPY orders TO '/tmp/out.csv'", "COPY"),
        ("COPY orders FROM '/tmp/in.csv'", "COPY"),
        ("ATTACH 'other.duckdb' AS other", "ATTACH"),
        ("DETACH other", "DETACH"),
        ("INSTALL httpfs", "INSTALL"),
        ("LOAD httpfs", "LOAD"),
        ("PRAGMA version", "PRAGMA"),
        ("SET threads = 1", "SET"),
        ("RESET threads", "RESET"),
        ("CALL checkpoint()", "CALL"),
        ("USE other", "USE"),
        ("BEGIN", "BEGIN"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK", "ROLLBACK"),
        ("EXPLAIN SELECT * FROM orders", "EXPLAIN"),
        ("DESCRIBE SELECT * FROM orders", "DESCRIBE"),
        ("DESCRIBE orders", "DESCRIBE"),
        ("SUMMARIZE SELECT * FROM orders", "SUMMARIZE"),
        ("SUMMARIZE orders", "SUMMARIZE"),
        ("EXECUTE prepared_query", "EXECUTE"),
        ("SELECT * INTO new_table FROM orders", "INTO"),
        ("SELECT * FROM orders FOR UPDATE", "Lock"),
        ("WITH x AS (DELETE FROM orders RETURNING *) SELECT * FROM x", "DELETE"),
        ("WITH x AS (UPDATE orders SET amount = 0 RETURNING *) SELECT * FROM x", "UPDATE"),
        ("WITH x AS (INSERT INTO orders VALUES (1) RETURNING *) SELECT * FROM x", "INSERT"),
        ("WITH x AS (CREATE TABLE t AS SELECT 1) SELECT * FROM x", "CREATE"),
        ("WITH x AS (ANALYZE orders) SELECT 1", "ANALYZE"),
        ("WITH x AS (CHECKPOINT) SELECT 1", "CHECKPOINT"),
    ],
)
def test_forbidden_statements(sql: str, construct: str) -> None:
    _rejects(sql, "forbidden_statement", construct)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT (",
        "SELECT * FROM",
        "SELECT 'unterminated",
        "SELECT /* unterminated",
        "SELECT 1; SELECT (",
        # sqlglot 30.31 requires an argument; parse failures must fail closed.
        "SELECT epoch_ms()",
        "EXPORT DATABASE '/tmp/export'",
        "IMPORT DATABASE '/tmp/import'",
        "SELECT * FROM ['one.csv', 'two.csv']",
    ],
)
def test_parse_failures_are_never_accepted(sql: str) -> None:
    _rejects(sql, "parse_error", "parse")


@pytest.mark.parametrize(
    ("sql", "table"),
    [
        ("SELECT * FROM secret", "secret"),
        ("SELECT * FROM orders JOIN secret USING (id)", "secret"),
        ("SELECT (SELECT count(*) FROM secret) FROM orders", "secret"),
        ("SELECT * FROM orders WHERE id IN (SELECT id FROM secret)", "secret"),
        ("SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM secret)", "secret"),
        ("SELECT * FROM orders, LATERAL (SELECT * FROM secret)", "secret"),
        ("SELECT * FROM LATERAL secret", "secret"),
        ("SELECT * FROM orders UNION SELECT * FROM secret", "secret"),
        ("WITH unused AS (SELECT * FROM secret) SELECT 1", "secret"),
        ("WITH orders AS (SELECT * FROM secret) SELECT * FROM orders", "secret"),
        ("WITH x AS (SELECT * FROM x) SELECT * FROM x", "x"),
        ("WITH x AS (SELECT * FROM y), y AS (SELECT 1) SELECT * FROM x", "y"),
        ("WITH RECURSIVE x AS (SELECT * FROM y), y AS (SELECT 1) SELECT * FROM x", "y"),
        ("SELECT * FROM (WITH hidden AS (SELECT 1) SELECT * FROM hidden), hidden", "hidden"),
        (
            "SELECT * FROM orders WHERE EXISTS "
            "(WITH hidden AS (SELECT 1) SELECT * FROM hidden) "
            "AND EXISTS (SELECT * FROM hidden)",
            "hidden",
        ),
        ("SELECT * FROM orders o JOIN o USING (id)", "o"),
        ("SELECT * FROM 'file.csv'", "file.csv"),
        ('SELECT * FROM "file.parquet"', "file.parquet"),
        ("SELECT * FROM '/tmp/private.csv'", "/tmp/private.csv"),
        ('SELECT * FROM "https://example.test/private.parquet"', "private.parquet"),
        ('SELECT * FROM LATERAL "private.parquet"', "private.parquet"),
    ],
)
def test_unknown_tables_in_every_scope(sql: str, table: str) -> None:
    _rejects(sql, "unknown_table", table)


@pytest.mark.parametrize(
    "table",
    ["__fs_base.orders", "main.orders", "memory.main.orders", '"main"."orders"', '"".orders'],
)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM {}",
        "SELECT * FROM orders JOIN {} USING (id)",
        "SELECT * FROM orders WHERE id IN (SELECT id FROM {})",
        "WITH unused AS (SELECT * FROM {}) SELECT 1",
        "WITH orders AS (SELECT 1) SELECT * FROM {}",
        "SELECT * FROM LATERAL (SELECT * FROM {})",
    ],
)
def test_qualified_tables(query: str, table: str) -> None:
    _rejects(query.format(table), "qualified_table", table)


def test_lateral_qualified_name() -> None:
    _rejects("SELECT * FROM LATERAL main.orders", "qualified_table", "main.orders")


@pytest.mark.parametrize(
    "function",
    [
        "read_csv('private.csv')",
        "read_csv_auto('private.csv')",
        "read_parquet('private.parquet')",
        "read_json('private.json')",
        "read_json_auto('private.json')",
        "read_json_objects('private.json')",
        "read_json_objects_auto('private.json')",
        "read_ndjson('private.json')",
        "read_ndjson_auto('private.json')",
        "read_ndjson_objects('private.json')",
        "glob('/tmp/*')",
        "parquet_scan('private.parquet')",
        "sniff_csv('private.csv')",
        "query('SELECT 1')",
        "query_table('orders')",
        "duckdb_tables()",
        "duckdb_settings()",
        "duckdb_secrets()",
        "range(3)",
        "generate_series(1, 3)",
        "unnest([1, 2])",
        "read_text('private.txt')",
        "read_blob('private.bin')",
        "some_new_function()",
        "ReAd_CsV('private.csv')",
        "\"read_csv\"('private.csv')",
    ],
)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM {}",
        "SELECT * FROM orders JOIN {} ON true",
        "SELECT * FROM LATERAL {}",
    ],
)
def test_all_table_functions_are_forbidden(query: str, function: str) -> None:
    _rejects(query.format(function), "forbidden_table_function")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM (read_csv('private.csv'))",
        "SELECT * FROM (orders JOIN read_csv('private.csv') ON true)",
        "WITH unused AS (SELECT * FROM read_csv('private.csv')) SELECT 1",
        "WITH orders AS (SELECT * FROM read_csv('private.csv')) SELECT * FROM orders",
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM read_csv('private.csv'))",
        "SELECT * FROM orders UNION SELECT * FROM read_csv('private.csv')",
        "SELECT * FROM main.read_csv('private.csv')",
        "SELECT * FROM TABLE(range(3))",
        "SELECT * FROM ROWS FROM(range(3), orders)",
    ],
)
def test_table_functions_cannot_be_hidden(sql: str) -> None:
    reason = "qualified_table" if "main.read_csv" in sql else "forbidden_table_function"
    _rejects(sql, reason)


@pytest.mark.parametrize(
    "function",
    [
        "read_text('private.txt')",
        "read_blob('private.bin')",
        "getenv('HOME')",
        "current_setting('allowed_directories')",
        "query('SELECT 1')",
        "query_table('orders')",
        "read_csv('private.csv')",
        "read_csv_auto('private.csv')",
        "read_parquet('private.parquet')",
        "read_json('private.json')",
        "read_json_auto('private.json')",
        "read_json_objects('private.json')",
        "read_json_objects_auto('private.json')",
        "read_ndjson('private.json')",
        "read_ndjson_auto('private.json')",
        "read_ndjson_objects('private.json')",
        "glob('/tmp/*')",
        "parquet_scan('private.parquet')",
        "sniff_csv('private.csv')",
        "ReAd_TeXt('private.txt')",
        "\"getenv\"('HOME')",
    ],
)
def test_scalar_file_and_system_calls(function: str) -> None:
    _rejects(f"SELECT {function}", "forbidden_function")


@pytest.mark.parametrize(
    "function",
    [
        "random()",
        "setseed(0.5)",
        "uuid()",
        "gen_random_uuid()",
        "uuidv4()",
        "uuidv7()",
        "now()",
        "current_timestamp",
        "current_timestamp()",
        "current_date",
        "current_date()",
        "current_time",
        "current_time()",
        "get_current_time()",
        "get_current_timestamp()",
        "today()",
        "localtimestamp",
        "localtimestamp()",
        "localtime",
        "localtime()",
        "transaction_timestamp()",
        "nextval('sequence')",
        "currval('sequence')",
        "RaNdOm()",
        "CuRrEnT_TiMeStAmP",
        '"random"()',
        "main.random()",
        "CURRENT_LOCALTIMESTAMP()",
    ],
)
def test_volatile_function_names_and_special_expressions(function: str) -> None:
    _rejects(f"SELECT {function}", "volatile_function")


@pytest.mark.parametrize(
    ("function", "reason"),
    [("random()", "volatile_function"), ("read_text('private.txt')", "forbidden_function")],
)
@pytest.mark.parametrize(
    "query",
    [
        "SELECT CASE WHEN true THEN {} ELSE NULL END",
        "SELECT CAST({} AS VARCHAR)",
        "SELECT (SELECT {})",
        "SELECT * FROM orders WHERE {} IS NOT NULL",
        "SELECT * FROM orders WHERE EXISTS (SELECT {} FROM lines)",
        "SELECT * FROM orders ORDER BY {}",
        "SELECT count(*) FROM orders GROUP BY {}",
        "SELECT count(*) FROM orders HAVING {} IS NOT NULL",
        "SELECT row_number() OVER (ORDER BY {}) FROM orders",
        "SELECT * FROM events QUALIFY row_number() OVER (ORDER BY {}) = 1",
        "WITH unused AS (SELECT {}) SELECT 1",
        "SELECT 1 UNION SELECT {}",
        "SELECT 1 INTERSECT SELECT {}",
        "SELECT 1 EXCEPT SELECT {}",
        "SELECT * FROM orders, LATERAL (SELECT {})",
        "SELECT * FROM (VALUES ({})) AS v(x)",
        "SELECT list_transform([1], x -> {})",
    ],
)
def test_unsafe_calls_are_checked_everywhere(query: str, function: str, reason: str) -> None:
    _rejects(query.format(function), reason)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT sum(l.amount) <> (SELECT sum(amount) FROM lines) "
        "FROM lines l JOIN orders o USING (order_id)",
        "WITH latest AS (SELECT *, row_number() OVER "
        "(PARTITION BY k ORDER BY ts DESC) rn FROM events) "
        "SELECT count(*) > 1 FROM latest WHERE rn = 1 GROUP BY k HAVING count(*) > 0",
        "SELECT count(*) = 0 FROM customers WHERE id NOT IN (SELECT customer_id FROM orders)",
        "SELECT CAST(raw_value AS INTEGER) FROM readings",
    ],
)
def test_realistic_predicates_execute_in_duckdb(sql: str) -> None:
    validate_predicate_sql(sql, _TABLES)
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders(order_id INT, customer_id INT)")
        connection.execute("CREATE TABLE lines(order_id INT, amount INT)")
        connection.execute("CREATE TABLE events(k INT, ts TIMESTAMP)")
        connection.execute("CREATE TABLE customers(id INT)")
        connection.execute("CREATE TABLE readings(raw_value VARCHAR)")
        connection.execute(sql).fetchall()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) > 0 FROM (SELECT * FROM orders) USING SAMPLE 10%",
        "SELECT count(*) > 0 FROM orders TABLESAMPLE 5 ROWS",
    ],
)
def test_sampling_is_rejected_as_nondeterministic(sql: str) -> None:
    with pytest.raises(PredicateValidationError) as info:
        validate_predicate_sql(sql, {"orders"})
    assert info.value.reason == "volatile_function"
