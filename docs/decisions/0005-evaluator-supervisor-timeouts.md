# ADR 0005: In-DuckDB materialization, run supervisor, cooperative timeouts

- Status: accepted (Phase 1 debate with Codex)
- Date: 2026-11-15

## Context
`con.interrupt()` is cooperative: DuckDB stops only at its next interruption check, so it is not
a hard bound. Copying data per evaluation is too slow at scale (measured: 430 ms to push 500k
ids from Python per evaluation).

## Decision
- Materialization: immutable base tables in `__fs_base`; predicate-visible views select via a
  semi-join with kept-id tables; reducer state (`cur_<t>(rid,pos)`) and cascade repair stay in
  DuckDB; candidates are excluded position ranges; each candidate runs in a transaction rolled
  back on rejection; rebuild the connection if rollback fails.
- Per-evaluation timeout: watchdog → `con.interrupt()`, documented as cooperative.
- Run supervisor: CLI, MCP and `reduce(isolate=True)` run the reduction in a child process.
  The parent hands out evaluation tokens, enforces the overall wall clock (covering export and
  replay) and kills the child after `eval_timeout_s + grace`. Atomic checkpoints (initial and
  after each acceptance) let the parent re-load the original files, export the last accepted
  configuration and replay it. Outcomes: verified / unverified candidate / no confirmed candidate.
- An `Evaluator` protocol keeps a process-pool evaluator (parallel evaluation, roadmap) possible.

## Consequences
Library users on the in-process default get cooperative timeouts only; this is documented.
