# FailSlice — repo conventions

Target behaviour (pre-alpha, being built in phases): FailSlice reduces multi-table CSV/Parquet data (loaded into DuckDB) to a small,
FK-consistent subset that still makes a failure predicate TRUE, verifies it in a fresh
process, and exports artifacts (Parquet/CSV, manifest, report, pytest). CLI `failslice`,
MCP server `failslice-mcp`. The full brief is
`docs/failslice_prompt.md`; this file is the short version.

## Workflow

- Work in phases; stop after each phase with a short summary and wait for the owner's go-ahead.
- Work on branches (`phase-N/<topic>`), commit at sensible points with clear messages.
- Never create a GitHub remote, push, publish to (Test)PyPI or submit to the MCP Registry
  without explicit approval for that specific step.
- Network only through the configured proxy (`HTTPS_PROXY`); never bypass it.
- Check the current stable version before adding any dependency; prefer few, maintained deps.
- When unsure, pick the simplest design that keeps guarantees honest and record it as a
  short ADR in `docs/decisions/NNNN-title.md`.

## Codex collaboration

- Codex (`codex exec`) is design debater, optional implementer of well-specified modules,
  and independent reviewer at the end of every phase.
- Fresh session for new tasks and every review (the session that wrote code never reviews it).
  `codex exec resume <id>` to answer its questions or to fix findings on its own code.
- Always `< /dev/null`; `--sandbox read-only` for reviews/debates, `--sandbox workspace-write`
  for implementation. Run long tasks in the background.
- Save every prompt and output under `.handoff/` (git-ignored), e.g. `.handoff/phase2-review.md`.
- Triage every finding (fix or reject with a written reason). Never trust "tests pass" from
  any agent without running them yourself.

## Engineering

- Python >= 3.11 (ADR 0001), `src/` layout, `uv` for env and lock.
- `uv run ruff check . && uv run ruff format --check .`
- `uv run mypy` (strict, on `src/failslice`)
- `uv run pytest --cov` — target >= 90% coverage on the core engine; Hypothesis property
  tests and a brute-force oracle for the reducer.
- Small, documented public API.

## Honesty rules (docs, CLI output, MCP output)

- Say "reduced" by default; say "1-minimal (verified)" only when the 1-minimality pass ran to completion.
- Never claim global minimality, root-cause detection, or inventing minimization.
- PK/FK inference is always labelled as a suggestion.
- Every guarantee stated in the README must be enforced by code and covered by a test.
- Benchmark numbers in the README are generated, never hand-written.

## Author

Saeed Mahdian <saeed.mahdian@gmail.com>, GitHub `sdmhdn`; the repo will be public (LICENSE, pyproject authors/maintainers,
CITATION.cff, README "Author" section, repo-local git identity).
