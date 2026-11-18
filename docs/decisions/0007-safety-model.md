# ADR 0007: Safety model for predicates and paths

- Status: accepted
- Date: 2026-11-15

## Decision
- All paths are resolved (including symlinks) and must lie inside an allowed root: data
  and expectation files named in a config, CLI outputs, and every path an MCP client
  passes. (Refined in Phase 6: the config file the CLI user names, and the `out_dir` a
  Python caller passes, are the caller's own choice and are not checked.)
- Predicates are parsed with sqlglot (DuckDB dialect): exactly one SELECT (CTEs allowed);
  reject DDL/DML, COPY, ATTACH, INSTALL/LOAD, PRAGMA, SET, CALL, table functions,
  schema-qualified names, unknown table names, volatile functions.
- After loading, `SET allowed_directories=[<run dir>]` then `SET enable_external_access=false`
  (cannot be re-enabled while running); exports go to the run directory only.
- `.duckdb` inputs are attached read-only and only physical tables are accepted: views are
  rejected before any data is read (a stored view can read files outside the root).
- The engine setting is the real barrier; sqlglot validation is defense in depth, since a
  parser can miss DuckDB-specific syntax.

## Non-goals
Protection against DuckDB vulnerabilities and resource exhaustion beyond the configured budgets.
