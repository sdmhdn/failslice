# Changelog

All notable changes to this project are documented here.

## 0.1.0 (not yet published)

First release, as two PyPI distributions: `failslice` (library, CLI, MCP server) and
`failslice-mcp` (provides the `failslice-mcp` command so `uvx failslice-mcp` works).

- Phase 6: `failslice demo [DIR]` writes the quickstart example without a checkout;
  `failslice-mcp` validates its budget flags and reports a bad `--root` as a usage error;
  `explain` wording; a CI job that installs the built wheels outside the checkout and runs
  the CLI, the demo, the generated test and the MCP server over stdio; a manual release
  workflow with Trusted Publishing; `server.json` for the MCP Registry; a vhs tape and
  workflow for the demo GIF; README rewritten as a landing page with absolute links;
  `CONTRIBUTING.md`, `docs/releasing.md` (ADR 0013). The quickstart generator now quotes
  its output paths.

- Phase 5: four example scenarios (`tpch_fanout`, `null_not_in`, `date_parse_crash`,
  `latest_tie`) with a driver `examples/run.py` and generated README sections, and
  `benchmarks/run.py` with a generated README table (ADR 0012). A matching `raises` error's
  signature is now its concrete class plus the matched text, so a file with several bad
  values passes the determinism check; `PORTABLE_VERSION` 2.

- Phase 4: MCP server `failslice-mcp` (stdio; tools `inspect_dataset`,
  `suggest_relationships`, `check_predicate`, `reduce_failure`, `verify_reproducer`,
  `explain_reproducer`, `export_pytest`; run resources; prompt `debug-data-bug`; ADR 0011),
  predicate-free dataset inspection, and cancellation through reduce, check, supervisor,
  recovery, replay and artifacts (status `cancelled`). CI on Linux, macOS and Windows.

- Phase 3: `failslice` CLI (`init`, `check`, `reduce`, `verify`, `export-test`, `explain`)
  with a documented exit-code contract and `--json` (ADR 0010); `manifest.json` (replaces
  `result.json`), `report.md`, CSV copies, `explain` observations, PK/FK suggestions for
  `init`, and a self-contained pytest bundle that shares `failslice/portable.py` with the
  tool (ADR 0009); `check(isolate=True)`; `spec.json` format 3.
- Persian user guide `docs/guide-fa.md`.

- Core engine (Phase 2): config, loading, predicate outcomes, cascade ddmin, negative
  cache, budgets, supervisor with checkpoints and recovery, independent replay.
- Independent audit fixes: exact typed result comparison, glob-safe paths, typed
  run-directory contract `spec.json` format 2 (ADR 0008) with ENUM type restoration in
  replay, validated orphan metadata, supervisor that cannot be blocked by a stalled
  child or a slow progress callback, statistics kept for unconfirmed and recovered runs,
  volatile functions taken from DuckDB's catalog, `expected: null` support.
- README in English and Persian; runnable quickstart example.
