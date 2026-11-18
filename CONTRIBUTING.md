# Contributing to FailSlice

Thanks for your interest. Bug reports with a small reproducer (a `failslice.yaml` and the
data, or the run directory FailSlice wrote) are the most useful contribution. Before a
larger change, please open an issue to agree on the approach.

## Development setup

Requirements: Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/sdmhdn/failslice.git
cd failslice
uv sync                      # the package, the failslice-mcp shim and the dev tools
uv run failslice --help
uv run failslice-mcp --help
```

The repository is a uv workspace: the root project is the `failslice` distribution and
`packages/failslice-mcp/` is the small distribution that only provides the
`failslice-mcp` command (ADR 0013).

## Checks

Every pull request must pass these, as CI does on Linux, macOS and Windows with Python
3.11 to 3.14:

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy                  # strict, on src/failslice
uv run pytest --cov          # coverage of at least 90%
```

The suite includes Hypothesis property tests on random multi-table schemas, a brute-force
oracle for small inputs, the examples as smoke tests and the MCP server through the SDK's
in-memory client. The TPC-H example downloads DuckDB's `tpch` extension and is skipped
when that is not possible (set `FAILSLICE_REQUIRE_TPCH=1` to make it fail instead).

To check the built packages the way a user installs them:

```bash
uv build --no-sources --out-dir dist
uv build --no-sources --package failslice-mcp --out-dir dist
uv venv /tmp/fs-smoke/venv
uv pip install --python /tmp/fs-smoke/venv dist/*.whl pytest
cp scripts/smoke_installed.py /tmp/fs-smoke/
cd /tmp/fs-smoke && venv/bin/python -I smoke_installed.py --expect-version <version> run
```

## Ground rules

- Every guarantee stated in the README or the docs must be enforced by code and covered
  by a test. Say "reduced" unless the 1-minimality check ran to completion; never claim a
  smallest possible result or a root cause; key inference is always a suggestion.
- Benchmark numbers are generated (`uv run python benchmarks/run.py`), never typed in;
  the example READMEs' result sections come from `uv run python examples/run.py all
  --update-readme`.
- `src/failslice/_demo/` must stay byte-identical to `examples/quickstart/` (a test checks
  it).
- Design decisions go into a short ADR in `docs/decisions/`.
- Documentation: the README is a landing page; details live in `docs/*.md` (English) and
  `docs/guide-fa.md` (Persian, where every line starts with a Persian word).
- Add an entry to `CHANGELOG.md` for user-visible changes.

Releasing is described in [docs/releasing.md](docs/releasing.md).
