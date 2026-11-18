# ADR 0011: The MCP server `failslice-mcp`

- Status: accepted (Phase 4 design: Claude draft + independent Codex design + one debate round)
- Date: 2026-11-17

## Decision

### Shape
- `failslice-mcp = failslice.mcp_server:main`; `create_server(root, limits) -> MCPServer`
  (official SDK `mcp` 2.3.0). stdio transport only; stdout carries MCP messages only.
- `--root DIR` is required and must be an existing directory, resolved once. Neither tool
  arguments nor client "roots" can widen it. Every client-derived path (config file,
  `base_dir`, inputs, expectations) is resolved (symlinks followed) and checked inside the
  root before it is opened; glob characters, FIFOs/devices and traversal are refused.
- One distribution: `uvx --from failslice failslice-mcp`. A separate shim package for a bare
  `uvx failslice-mcp` is a release decision for Phase 6. *Superseded by ADR 0013: the
  `failslice-mcp` distribution now provides the command, so `uvx failslice-mcp` works.*

### Tools
`inspect_dataset`, `suggest_relationships`, `check_predicate`, `reduce_failure`,
`verify_reproducer`, `explain_reproducer`, `export_pytest`.
- Projects come either as `config_path` (a `failslice.yaml` inside the root) or as an inline
  `config` with the same schema; the inline schema is fully typed in the tool's
  `inputSchema` (tables, foreign keys, the `sql` / `expect` / `raises` alternatives,
  settings), and `config.parse_project` stays the single validator. Inline paths resolve
  against an optional `base_dir`, itself checked inside the root.
- Runs and exports go to server-assigned directories under `<root>/.failslice/`; tools
  address runs by validated run id. Export always re-verifies and never overwrites.
- Every tool returns `Reply[T] = {format, status, data, error, warnings}`; errors are
  `{code, message, field?, retryable}`. Domain outcomes (predicate never TRUE, replay
  failed) are ordinary replies with their status; invalid input and internal errors are
  replies with `is_error` set and a sanitized message.
- Budgets an agent sets are validated against server ceilings (startup flags; defaults
  equal the library defaults: 5,000 evaluations, 900 s, 30 s per evaluation, 1 thread) and
  rejected when over the ceiling; the effective settings are returned.
- One expensive operation per server process (inspection, inference, check, reduce,
  verify, explain, export); a concurrent call gets `busy` at once. Metadata resources stay
  available.
- Limits: 64 KiB config, 16 KiB SQL, 32 tables, 128 columns per table, 64 KiB per tool
  reply, 256 KiB per metadata resource; previews at most 20 rows x 50 columns with an
  aggregate serialized-byte budget and explicit truncation markers; cells are projected
  and cut in SQL before fetching.

### Long-running reduction and cancellation
- The blocking supervised API runs in a worker thread owned by the server; the request
  handler awaits a completion event and polls a thread-safe latest-progress slot, sending
  `ctx.report_progress(elapsed_seconds, total=None, message=...)` at most twice a second
  (phase, rows kept/input, evaluations/budget, run id). No percentage: remaining work is
  unknown.
- A `threading.Event` cancel flag goes through `reduce` -> `run_supervised` -> `_pump`
  (kill and reap the child), through `check`, and through the verifier, which becomes a
  `Popen` poll loop that drains its pipes and kills and reaps on cancel or timeout. After a
  cancel, no checkpoint recovery runs. The finalizer checks the flag between steps
  (cooperative; a running CSV export, bundle self-check or explain finishes first) and then
  writes only the report and manifest, which record the cancellation.
- When the MCP request is cancelled, the handler sets the flag, keeps the operation slot
  until the worker and child have finished cleanup, then propagates the cancellation.
- *Deferred design idea, not built in 0.1:* a small `RunHandle` (id, run dir, context,
  deadline, cancel event, done event, phase). Built: a
  shared outcome mapping (termination, candidate state none / checkpoint-only / exported,
  verification, minimality) serves both CLI and MCP; cancellation is its own termination,
  distinct from timeouts.

### Resources and prompt
- `failslice://runs` (paginated, 50 per page), `failslice://runs/{id}/manifest`,
  `.../report`, `.../tables/{table}/preview`; `latest` resolves to the newest finalized
  run and the reply names the concrete id. Run ids and table names are validated against
  the run tree and `spec.json`; only these fixed artifact roles are readable.
- Prompt `debug-data-bug` (required `bug`; optional `directory`, `config_path`,
  `expected_behavior`, `expect`): a guided workflow, no filesystem access when rendered.

### Safety (addendum to ADR 0007)
The caller may be a prompt-injected agent: it can read supported datasets inside the
root it was given (the root is an authorization, not confidentiality from the model) and
write only under `<root>/.failslice/`. Data values are untrusted and the instructions say
so. Out of scope: hostile concurrent filesystem mutation, DuckDB vulnerabilities, absolute
memory or disk exhaustion beyond the stated limits.

## Consequences
The CLI gains cancellation-aware internals and the shared outcome mapping; the generated
artifacts are unchanged except for a recorded cancellation. Tests use
`mcp.client.Client(server, mode="legacy")` (real in-memory JSON-RPC), and one real Claude
Code session through a local `.mcp.json` is recorded in `.handoff/`.
