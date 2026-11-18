# ADR 0013: Packaging, the `demo` command and the release process

- Status: accepted (Phase 6: Claude draft + independent Codex design + one debate round)
- Date: 2026-11-18

## Context

The definition of done asks that a new user can run `uvx failslice` on an example and get
a verified reproducer and a passing generated test in under two minutes, following only
the README, and that `uvx failslice-mcp` works. The wheel contained no example, and
`uvx failslice-mcp` only works if a distribution of that name provides the command.
Publishing needs the owner's approval for each step.

## Decision

### `failslice demo [DIR]`
- Writes `failslice.yaml`, `make_data.py` and the generated CSVs (26,001 rows) into a new
  directory, refusing an existing one. Everything is written to a temporary sibling first
  and renamed into place, so a failure leaves nothing behind. Data is generated with
  DuckDB (already a dependency); no download.
- The packaged files live in `src/failslice/_demo/` and are byte-identical to
  `examples/quickstart/` (a test enforces it). A Hatch `force-include` from `examples/`
  was considered but does not apply to editable installs, so `uv run failslice demo` in a
  checkout would fail.
- The packaged generator runs, not the copy written to the user's directory.

### Two distributions
- `failslice` (library, CLI, MCP server implementation) and `failslice-mcp`
  (`packages/failslice-mcp/`, a metadata-only wheel that depends on
  `failslice==<same version>` and provides the `failslice-mcp` script).
- The script has one owner: it was removed from `failslice`. With both declaring it,
  uninstalling `failslice-mcp` deleted the command although `failslice` still declared it.
  `python -m failslice.mcp_server` works with `failslice` alone.
- The repository is a uv workspace with the shim as member and as a dev dependency, so
  `uv sync` in a checkout still provides `failslice-mcp`. Builds use `--no-sources`, so the
  published metadata has only the one-way dependency shim → core.
- Versions move in lockstep; `tests/test_release.py` checks the shim's version and pin,
  `server.json`, `CITATION.cff` and the CHANGELOG against `failslice.__version__`.

### Packaging checks
- The sdist lists its contents explicitly (no hand-off notes, generated data, run outputs
  or caches) and includes the workspace member so it builds when extracted.
- README links are absolute (it is also the PyPI description; a test forbids relative
  links). Mermaid stays as source; PyPI shows it as a readable code block.
- A CI `package` job on three OSes (Python 3.12; the test matrix covers interpreters)
  builds both distributions, runs `twine check --strict`, installs the wheels into a clean
  environment outside the checkout and runs `scripts/smoke_installed.py`: versions, `demo`,
  `reduce`, the generated test, and the MCP server over real stdio with the SDK client
  (discovery, check, reduce, verify, explain, export, a resource).

### Release
- `release.yml` runs only by manual dispatch with a version and a target (`testpypi` or
  `pypi`), for an existing tag `v<version>` whose commit passed CI; it checks that all
  version strings agree, builds once, checks and smoke-tests the artifacts, records
  SHA-256 hashes and publishes those artifacts with Trusted Publishing, core before shim.
  `id-token: write` is granted only to the two upload jobs, which run in the protected
  environment of the target. A tag push does not publish.
- `server.json` (schema 2025-12-11) names `io.github.sdmhdn/failslice` and points at the
  `failslice-mcp` PyPI package with a required `--root` argument; both READMEs carry the
  `mcp-name` ownership marker. Submission is a manual, approved step.
- `docs/releasing.md` is the checklist.

### Demo GIF
- `docs/demo.tape` is rendered by vhs in GitHub Actions (`demo-gif.yml`, manual), from the
  built wheels, with the vhs version pinned. Real time, not sped up (the tape says so on
  screen); the GIF is uploaded as an artifact and committed as `docs/assets/demo.gif`
  after review. Reproducible means the same procedure and behaviour, not identical
  pixels or timing.

## Consequences
`pip install failslice` users start the server with `python -m failslice.mcp_server` or
install `failslice-mcp`. Publication is two uploads that are not atomic: if the shim
upload fails after the core succeeded, re-run only the failed job of that workflow run (it
reuses the same built artifacts); a new dispatch would rebuild and try to upload the core
again, which the index refuses.
