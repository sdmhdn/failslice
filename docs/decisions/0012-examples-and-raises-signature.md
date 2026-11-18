# ADR 0012: Examples, benchmarks, and the `raises` signature

- Status: accepted (Phase 5: Claude draft + independent Codex design + one debate round)
- Date: 2026-11-18

## Context

The Phase 5 crash example (a staging query that fails to parse a date) has three malformed
rows, as real files do. DuckDB names the first offending value it reaches in the error
message, and the plain and view paths of the determinism check reached different rows. The
signature of a `raises` outcome was the error class plus the *full message*, so the check
reported "not deterministic" although every run was TRUE. Any realistic `raises` predicate
with more than one offending value could be refused this way.

## Decision

### `raises` signature
- A TRUE outcome of the `raises` form has the signature `error:<concrete class>:<text the
  match regex matched>`; `detail` keeps the full message. Non-matching errors, the `sql`
  and `expect` forms and timeouts are unchanged.
- The determinism check therefore means "the declared failure (concrete class and matched
  text) is stable", not "the same offending row is reached first". A regex whose *matched
  text* includes the varying part (e.g. the offending value) is still reported as
  nondeterministic; text inside a lookahead or lookbehind is not part of the match and
  does not count.
- This narrows only the startup screen's sensitivity to unmatched message text. Every
  accepted candidate still satisfies the declared predicate when evaluated, certification
  still requires every single-row deletion to be not TRUE, and the replay decides from the
  predicate and integrity, not from a recorded signature. A broad regex can now let
  distinct failures of the same class pass the screen; the reduction already accepted
  those under that predicate.
- `PORTABLE_VERSION` becomes 2 (the shared classifier changed); existing bundles keep
  their own copy.

### Examples
- Four scenarios beside the quickstart, each `examples/<name>/{README.md, make_data.py,
  failslice.yaml}`, run with `uv run python examples/run.py <name>`: TPC-H fan-out
  (sf 0.01, five tables, revenue by region), `NOT IN` with a NULL suppression record,
  a date-parsing crash (`raises`), and a latest-record tie across two event streams. Each
  witness needs rows from several tables.
- Generated data is not committed, and neither are reproducer bundles. Each example README
  has a generated section (counts, label, surviving rows) written by `--update-readme` and
  compared with a fresh run in the tests.
- TPC-H data comes from DuckDB's `tpch` extension: `LOAD` first, otherwise `INSTALL`
  through the proxy set explicitly in DuckDB's settings (DuckDB cannot parse proxy URLs
  with credentials); credentials are never printed. No synthetic stand-in when the
  extension is unavailable: the test skips, or fails when `FAILSLICE_REQUIRE_TPCH=1` (CI,
  one Python version on each OS).

### Benchmarks
- `benchmarks/run.py` writes `benchmarks/results.json` and the README table between
  markers; `--check` compares them. Wall time is the median of three runs of
  `reduce(isolate=True)`, with the environment recorded; only deterministic columns are
  checked against fresh runs.

## Consequences
The examples run as smoke tests on every CI job (about 10 s). Bundles generated before this
change classify `raises` with the old, stricter signature, which only affects their
recorded signature, not their verdict.
