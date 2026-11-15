# ADR 0006: Config format, CLI framework and runtime dependencies

- Status: accepted
- Date: 2026-11-15

## Decision (versions checked on PyPI, 2026-11-15)
- Runtime deps: `duckdb>=2.0` (engine), `sqlglot>=30.31` (predicate validation),
  `pyyaml>=6.0.3` (config), `typer>=0.29` (CLI; brings `rich` for progress), `mcp>=2.3`
  (MCP server; brings `pydantic`, which we also use for config models).
- Config: YAML (`failslice.yaml`), validated with pydantic models. YAML allows comments and
  multi-line SQL blocks, which matter for predicates; TOML multi-line strings are clumsier.
- CLI: Typer over argparse — typed commands, generated help, and rich progress without extra
  code; it is already a dependency of the ecosystem we target.
- `mcp` is a core dependency (not an extra) so `uvx failslice-mcp` works without extras syntax.
- MCP SDK 2.x API: `mcp.server.mcpserver.MCPServer` (renamed from FastMCP), `Context.report_progress`,
  typed returns for structured output, `mcp.client.Client` with the in-memory transport for tests.

## Consequences
Install footprint is larger than argparse-only, accepted for UX. No pandas/numpy/pyarrow:
DuckDB reads and writes Parquet/CSV itself.
