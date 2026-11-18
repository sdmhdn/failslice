# Releasing

FailSlice is released as two PyPI distributions with the same version:
`failslice` (everything) and `failslice-mcp` (only the `failslice-mcp` command, pinned
to `failslice==<version>`). Publishing uses PyPI Trusted Publishing from
`.github/workflows/release.yml`; there are no API tokens. Every step that publishes
something (pushing the tag, uploading to TestPyPI, uploading to PyPI, submitting to the
MCP Registry) is done only with the maintainer's explicit go-ahead for that step.

## One-time setup

1. GitHub, Settings → Environments: create `testpypi` and `pypi`, each with the
   maintainer as required reviewer and "Allow administrators to bypass" off; restrict
   deployment to tags `v*`.
2. TestPyPI and PyPI, "Publishing" → add a **pending publisher** for each project
   (`failslice` and `failslice-mcp`, so four in total): owner `sdmhdn`, repository
   `failslice`, workflow `release.yml`, environment `testpypi` (TestPyPI) or `pypi` (PyPI).

## Checklist for a release

Record the evidence (links, hashes) in the release notes or the PR.

### Before tagging

- [ ] Version `X.Y.Z` is set in `src/failslice/__init__.py`,
      `packages/failslice-mcp/pyproject.toml` (version and the `failslice==` pin),
      `server.json` (twice), `CITATION.cff` (`version` and `date-released`), and
      `CHANGELOG.md` has a `## X.Y.Z` section with the release date
      (`tests/test_release.py` checks all but the dates).
- [ ] `uv lock` is current; `uv sync --locked` works.
- [ ] CI is green on the commit to be tagged: lint and types, 12 test jobs, 3 package
      jobs (built wheels installed outside the checkout, CLI, demo, generated test, MCP
      over stdio).
- [ ] Benchmarks regenerated on that code (`uv run python benchmarks/run.py`), and
      `benchmarks/run.py --check` passes.
- [ ] Example sections current (`uv run python examples/run.py all --check-readme`).
- [ ] The demo GIF was rendered by the "demo GIF" workflow from this code, reviewed, and
      committed as `docs/assets/demo.gif`.
- [ ] An independent review of claims versus code found nothing open (every finding fixed
      or rejected with a written reason).
- [ ] A clean-machine quickstart following only the README passed in under two minutes:
      fresh `UV_CACHE_DIR` and `UV_TOOL_DIR`, a directory outside the checkout, the wheels
      from this commit standing in for PyPI.
- [ ] `server.json` validates (`tests/test_release.py`; optionally
      `mcp-publisher validate`) and both READMEs contain
      `<!-- mcp-name: io.github.sdmhdn/failslice -->`.

### TestPyPI (with approval)

- [ ] Push the tag: `git tag -a vX.Y.Z -m "FailSlice X.Y.Z" && git push origin vX.Y.Z`.
- [ ] Actions → release → Run workflow: version `X.Y.Z`, target `testpypi`; approve the
      environment when asked. Keep the `SHA256SUMS` from the build job's log.
- [ ] Install from TestPyPI in a fresh environment. FailSlice's own packages come from
      TestPyPI, every dependency from PyPI:

      ```bash
      uvx --index https://test.pypi.org/simple/ --index-strategy unsafe-best-match \
          failslice@X.Y.Z demo shop
      ```

      then the rest of the README quickstart, and `failslice-mcp@X.Y.Z --version` the
      same way.
- [ ] Check the project pages on test.pypi.org: description renders, links work, the
      Persian section is readable, authors and URLs are right.

### PyPI (with approval)

- [ ] Run the release workflow again with target `pypi`; approve the environment.
- [ ] In a fresh environment: `uvx failslice@X.Y.Z demo shop` and the README quickstart;
      `uvx failslice-mcp@X.Y.Z --version`.
- [ ] Create the GitHub release for the tag from the CHANGELOG section, attaching
      `SHA256SUMS`.

### MCP Registry (with approval)

- [ ] `mcp-publisher login github` (as `sdmhdn`), then `mcp-publisher publish` from the
      repository root. The registry checks that the `failslice-mcp` description on PyPI
      contains the `mcp-name` marker.
- [ ] Search the registry for `io.github.sdmhdn/failslice` and check the entry.

### After the release

- [ ] README status line, CHANGELOG (start an "Unreleased" section) and the Persian
      guide's status list.
