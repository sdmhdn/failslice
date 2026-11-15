"""Conservative predicate SQL validation; DuckDB's external-access lock is still required."""

from collections.abc import Collection

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

_QUERY_ROOTS = (exp.Select, exp.Union, exp.Intersect, exp.Except)
_FORBIDDEN_STATEMENTS = (
    exp.Command,
    exp.DDL,
    exp.DML,
    exp.Alter,
    exp.Attach,
    exp.Commit,
    exp.Describe,
    exp.Detach,
    exp.Drop,
    exp.Execute,
    exp.Export,
    exp.Install,
    exp.Into,
    exp.Lock,
    exp.Pragma,
    exp.Rollback,
    exp.Set,
    exp.Summarize,
    exp.Transaction,
    exp.Use,
)

# Block file readers and execution/environment helpers even in scalar expressions.
# Table functions are rejected independently, regardless of their names.
_FORBIDDEN_FUNCTIONS = frozenset(
    {
        "current_setting",
        "getenv",
        "glob",
        "parquet_scan",
        "query",
        "query_table",
        "read_blob",
        "read_csv",
        "read_csv_auto",
        "read_json",
        "read_json_auto",
        "read_json_objects",
        "read_json_objects_auto",
        "read_ndjson",
        "read_ndjson_auto",
        "read_ndjson_objects",
        "read_parquet",
        "read_text",
        "sniff_csv",
    }
)

# sqlglot canonicalizes RANDOM to RAND, GEN_RANDOM_UUID to UUID, TODAY to
# CURRENT_DATE, and GET_CURRENT_TIME to CURRENT_TIME. Include canonical names
# and DuckDB spellings so both specialized expressions and anonymous calls match.
_VOLATILE_FUNCTIONS = frozenset(
    {
        "currval",
        "current_date",
        "current_time",
        "current_timestamp",
        "gen_random_uuid",
        "get_current_time",
        "get_current_timestamp",
        "localtime",
        "localtimestamp",
        "nextval",
        "now",
        "rand",
        "random",
        "setseed",
        "today",
        "transaction_timestamp",
        "uuid",
        "uuidv4",
        "uuidv7",
    }
)


class PredicateValidationError(ValueError):
    """Raised when a predicate query is not an allowed read-only SELECT."""

    reason: str
    detail: str

    def __init__(self, reason: str, detail: str) -> None:
        """Store a machine-readable reason and a human-readable explanation."""
        self.reason = reason
        self.detail = detail
        super().__init__(detail)


def _visible_ctes(node: exp.Expr) -> set[str]:
    """Resolve lexical CTE visibility without exposing inner or later CTEs."""
    visible: set[str] = set()
    child = node
    while (parent := child.parent) is not None:
        if isinstance(parent, exp.With):
            limit = parent.expressions.index(child)
            if parent.args.get("recursive"):
                limit += 1
            visible.update(cte.alias.lower() for cte in parent.expressions[:limit])
        else:
            with_ = parent.args.get("with_")
            if isinstance(with_, exp.With) and child is not with_:
                visible.update(cte.alias.lower() for cte in with_.expressions)
        child = parent
    return visible


def _validate_table(node: exp.Table | exp.Lateral, allowed: set[str]) -> None:
    source = node.this
    if node.args.get("db") or node.args.get("catalog") or isinstance(source, exp.Dot):
        raise PredicateValidationError(
            "qualified_table", f"Qualified table reference is not allowed: {node.sql()}"
        )
    if isinstance(node, exp.Lateral) and isinstance(source, (exp.Subquery, exp.Values)):
        return
    if not isinstance(source, exp.Identifier):
        raise PredicateValidationError(
            "forbidden_table_function", f"Table functions are not allowed: {node.sql()}"
        )
    name = source.name
    if name.lower() not in allowed and name.lower() not in _visible_ctes(node):
        raise PredicateValidationError("unknown_table", f"Unknown table: {name}")


def _validate_function(node: exp.Func) -> None:
    name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
    if name in _FORBIDDEN_FUNCTIONS:
        raise PredicateValidationError(
            "forbidden_function",
            f"File/system function is not allowed: {node.sql(dialect='duckdb')}",
        )
    if name in _VOLATILE_FUNCTIONS:
        raise PredicateValidationError(
            "volatile_function", f"Volatile function is not allowed: {node.sql(dialect='duckdb')}"
        )


def validate_predicate_sql(sql: str, allowed_tables: Collection[str]) -> None:
    """Validate that ``sql`` is exactly one read-only SELECT over the allowed tables.

    Parenthesized SELECTs, set operations, CTEs (including recursive CTEs), and
    inline VALUES are supported. Names match case-insensitively, including CTEs.
    Scalar file/system calls raise ``forbidden_function``; all parse failures
    raise ``parse_error``. Validation never executes SQL or resolves database objects.
    """
    try:
        statements = [
            statement
            for statement in sqlglot.parse(sql, read="duckdb")
            if statement is not None and not isinstance(statement, exp.Semicolon)
        ]
    except SqlglotError as error:
        raise PredicateValidationError(
            "parse_error", f"Cannot parse predicate SQL: {error}"
        ) from error

    if not statements:
        raise PredicateValidationError("not_select", "Predicate SQL is empty; expected a SELECT")
    if len(statements) != 1:
        raise PredicateValidationError(
            "multiple_statements", f"Expected one statement; found {len(statements)}"
        )
    statement = statements[0]
    if isinstance(statement, _FORBIDDEN_STATEMENTS):
        raise PredicateValidationError(
            "forbidden_statement",
            f"Statement is not allowed: {type(statement).__name__}: "
            f"{statement.sql(dialect='duckdb')}",
        )
    if not isinstance(statement.unnest(), _QUERY_ROOTS):
        raise PredicateValidationError(
            "not_select", f"Expected a SELECT query, found: {statement.sql()}"
        )

    allowed = {name.lower() for name in allowed_tables}
    for node in statement.walk():
        if isinstance(node, exp.CTE) and not isinstance(
            node.this.unnest(), (*_QUERY_ROOTS, exp.Values)
        ):
            raise PredicateValidationError(
                "forbidden_statement",
                f"CTE {node.alias} must contain a SELECT query: {node.this.sql(dialect='duckdb')}",
            )
        if isinstance(node, _FORBIDDEN_STATEMENTS):
            raise PredicateValidationError(
                "forbidden_statement",
                f"Statement construct is not allowed: {type(node).__name__}",
            )
        if isinstance(node, exp.TableSample):
            raise PredicateValidationError(
                "volatile_function", f"Sampling is nondeterministic: {node.sql(dialect='duckdb')}"
            )
        if isinstance(node, (exp.Table, exp.Lateral)):
            _validate_table(node, allowed)
        if isinstance(node, exp.Func):
            if isinstance(node.parent, (exp.From, exp.Join)):
                raise PredicateValidationError(
                    "forbidden_table_function", f"Table functions are not allowed: {node.sql()}"
                )
            _validate_function(node)
