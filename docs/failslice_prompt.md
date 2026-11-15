# Build FailSlice: a failure-preserving data reducer (Python library + CLI + MCP server)

You are the lead engineer on a new open-source portfolio project in this empty folder. You will work together with Codex CLI (`codex exec`) as a second engineer and an independent reviewer. Read this whole brief before you do anything. Then start with Phase 0.

## 1. What we are building and why

A data pipeline or SQL query gives a wrong result (an inflated total, a duplicated key, a NULL that breaks a join, a crash) on a dataset with hundreds of thousands of rows across several related tables. Today an engineer cuts the data down by hand until the bug is small enough to understand and turn into a test. FailSlice automates that.

**Input:**

- one or more tables (CSV or Parquet files)
- declared foreign-key relationships between them
- a *failure predicate*: a DuckDB SQL query that returns true while the bug is present

**Output:**

- a much smaller set of tables that still triggers the failure, with referential integrity intact
- independent verification that the reduced data really reproduces the failure
- a manifest (hashes, stats, settings)
- a ready-to-run pytest regression test with fixtures

**For AI agents:** an MCP server exposes the loop *define predicate → reduce → inspect → export test*, so an agent (Claude, Cursor, etc.) can debug a data bug end to end.

**Positioning.** Be honest about this in the README. Delta debugging (Zeller's ddmin) and database subsetting are established ideas. Related tools each cover only one piece:

- `dfshrink`: shrinks a single Polars frame against a validator; it has no relational awareness.
- `dbslice`: extracts an FK-intact Postgres subset from a seed record; it has no failure predicate.
- SQLancer reducers, `sql-reduce` and `SQLreduce`: shrink SQL *statements*, not data rows.
- RATest (SIGMOD 2019): small relational counterexamples, as a research demo.

FailSlice is the first to combine **predicate-guided, multi-table, FK-consistent row reduction on local files, plus verified regression artifacts, plus an MCP interface**. Never claim to have invented minimization. Never claim global minimality.

Inspiration: a small, polished MCP portfolio project (`devinsight-mcp`) set the bar for presentation: tests, CI, PyPI, MCP Registry, a demo GIF and a clear README. It belongs to someone else. Do not copy its code, text or structure. We aim for much higher technical depth and usefulness.

## 2. Working agreement

- **Phases and stop points.** Work in the phases below. At the end of each phase, stop and give me a short summary: what was done, test results, open questions. Wait for my go-ahead before starting the next phase.
- **Git.** Run `git init` in Phase 0 and work on branches. Commit at sensible points with clear messages. Do not create a GitHub remote, push, publish to PyPI or TestPyPI, or submit to the MCP Registry without my explicit approval for that specific step.
- **Network.** Never bypass the configured proxy (see my global instructions).
- **When unsure,** pick the simplest design that keeps correctness guarantees honest, write the decision down in `docs/decisions/` (short ADRs), and move on. Ask me only about decisions that are genuinely mine, such as scope cuts. The name (`failslice`), the license (MIT) and the author attribution (Section 8, Phase 0) are already decided.
- **Before choosing any dependency or tool version,** check the current stable version. Prefer few, well-maintained dependencies.

## 3. Collaboration protocol with Codex

You are the architect and the final owner of quality. Codex is a peer.

- **Its uses:**
  - (a) a design reviewer and debater in Phase 1
  - (b) the implementer of well-specified, self-contained modules when that saves time
  - (c) an independent code reviewer at the end of every phase
- **Session modes:**
  - **Fresh session** (`codex exec ...`) for new tasks and for every review. Independence matters: never let the session that wrote some code review it.
  - **Resume** (`codex exec resume <session-id> ...`) to answer Codex's questions or to have it fix review findings on code it wrote itself. Start fresh instead if the session has become very long or the files changed a lot.
  - **Its instructions** (`~/.codex/AGENTS.md`) tell it to put blocking questions under "Questions" and stop. Answer them by resuming.
- **Invocation details:**
  - Close stdin (`< /dev/null`), or `codex exec` waits for input and hangs.
  - Use `--sandbox read-only` for reviews and debates, and `--sandbox workspace-write` for implementation tasks.
  - Run long tasks in the background, with generous timeouts.
  - Save prompts and outputs under `.handoff/` (git-ignored), e.g. `.handoff/phase2-review.md`, so the history is inspectable.
- **Task briefs.** Give Codex a precise brief each time: goal, files it may touch, interfaces to respect, tests that must pass, and "do not change anything outside this scope".
- **Reviews.**
  - Ask the reviewer for concrete findings with file:line, a failure scenario and a severity. Also ask it to flag any claim in docs or README that the code does not back up.
  - Triage every finding yourself: fix it, or reject it with a written reason. Never apply findings blindly.
  - For high-severity disagreements, run one debate round (resume the reviewer with your counter-argument), then decide.
- **Verification.** Never trust "tests pass" from either agent without running the tests yourself.

## 4. Scope of v1 (keep it tight; depth over breadth)

### In scope

- **Inputs:** local CSV and Parquet files, loaded into an in-process DuckDB database. Optionally, an existing `.duckdb` file opened read-only.
- **Schema config** in a `failslice.yaml` (or `.toml`):
  - the tables and their file paths
  - an optional primary key per table
  - single-column foreign keys (`child.col -> parent.col`) whose graph must be acyclic
  - nullable FKs are allowed (NULL means no reference)
  - detect FK cycles and reject them with a clear error
- **The failure predicate:**
  - a single read-only DuckDB `SELECT` statement returning one boolean, which references the configured tables by name
  - also support a "query + expectation" form, e.g. *this query's result differs from an expected value or file*, or *this query raises an error matching a regex*
  - the crash-reproduction case (the target query errors) is important; support it explicitly
- **Reduction engine:**
  - stable row identity: an internal row id per table, assigned at load time
  - a ddmin-style algorithm (complement-first, with granularity doubling), run over the kept-row sets of the tables
  - a sound FK-integrity strategy (see Section 5)
  - a result cache keyed on a hash of the configuration
  - budgets: max predicate evaluations, wall-clock limit, per-evaluation timeout
  - deterministic results for a given seed
- **Predicate outcome handling:**
  - distinguish TRUE (failure present), FALSE, ERROR (an unexpected error, i.e. a different error) and TIMEOUT
  - only TRUE counts as "still failing"
  - a determinism check before reducing: run the predicate on the full data twice; refuse, or warn loudly, if the results differ
- **Verification:** replay the reduced dataset in a fresh DuckDB connection (ideally a separate process) built only from the exported files, and confirm that the predicate is TRUE and that FK integrity holds.
- **Artifacts:**
  - the reduced tables as Parquet and CSV
  - `manifest.json`: input file hashes, output hashes, row counts before and after per table, predicate text, config, engine version, seed, number of evaluations, elapsed time, and whether 1-minimality was verified
  - a Markdown report
  - a generated pytest file plus fixture data that loads the reduced tables and asserts the failure (or, after a fix, its absence; let the user choose the polarity with a flag)
- **1-minimality:** an optional final pass verifies that removing any single remaining row (under the deletion rules) makes the failure disappear. Report "1-minimal: verified" only when this actually ran to completion. Otherwise report "reduced, minimality not verified".
- **CLI**, built with Typer or plain argparse (choose and write an ADR):
  - `failslice init`: scaffold a config, infer candidate PKs and FKs from data and mark them as *suggestions*
  - `failslice check`: validate the config, FKs and predicate; run the determinism check
  - `failslice reduce`
  - `failslice verify`
  - `failslice export-test`
  - `failslice explain`: summarize the surviving rows, e.g. join fan-out per key, so the human or the agent sees why it fails
  - show rich progress output (shrink ratio, evaluations, elapsed time)
- **MCP server**, built on the official Python MCP SDK. Check its current API, and use structured tool outputs and the progress notifications it supports:
  - **Tools:** `inspect_dataset` (tables, row counts, schema, FK validity), `suggest_relationships` (suggestions only, with evidence and a confidence label), `check_predicate`, `reduce_failure` (long-running: report progress, respect budgets), `verify_reproducer`, `explain_reproducer`, `export_pytest`
  - **Resources:** the latest run's manifest, report, and reduced-table previews
  - **One prompt:** a guided "debug this data bug" workflow
- **Safety:**
  - accept only paths inside an allowed root directory
  - validate predicates with `sqlglot` (or DuckDB's own parser) as a single SELECT, with no DDL/DML, no `COPY`, no `ATTACH`, no `INSTALL`/`LOAD`, and no file-reading table functions inside the predicate
  - disable DuckDB external access where possible (`enable_external_access=false` after loading)
  - document the threat model briefly

### Out of scope for v1 (list these in the README as "Roadmap")

Live warehouse connections; composite or cyclic FKs; value-level simplification (shrinking strings or numbers); column dropping; Polars or pandas predicates; parallel predicate evaluation; a web UI. Keep the architecture open for column dropping and parallel evaluation, but do not build them.

## 5. The hard part: FK-consistent reduction

Design this carefully in Phase 1 and debate it with Codex before writing code. Requirements:

- every configuration ever passed to the predicate must be referentially intact: every non-NULL FK value in a kept child row points to a kept parent row
- the algorithm must terminate, and must respect the budgets
- results must be reproducible

**Two candidate strategies to start the debate.** Evaluate both and pick one, or a hybrid:

1. **Upward closure.**
   - Reduce child-most tables first, in reverse topological order.
   - After any removal, add back every parent row referenced by kept children (closure).
   - Parent tables are then reduced only among rows not required by kept children.
   - Simple and sound, but it may leave extra parent rows that the bug does not need until a later pass.
2. **Cascade deletion.**
   - Removing a parent row also removes all its children.
   - This allows removing large connected chunks at once (fast early progress).
   - It interacts with ddmin's assumptions in non-obvious ways.

**Points to address in the ADR:**

- the order of passes, and iterating to a fixed point across tables
- how 1-minimality is defined under the chosen deletion rules
- the expected number of predicate calls
- the cache key
- performance: materialize configurations as DuckDB temp tables or views filtered by row-id sets, and avoid copying data per evaluation (measure this)

**Required test strategy:**

- property-based tests (Hypothesis) on random small schemas and datasets with planted bugs, checking that:
  - the output still satisfies the predicate
  - FK integrity holds
  - the output is a subset of the input
  - the run is deterministic for a fixed seed
  - the 1-minimality claim is true whenever it is reported
- a brute-force oracle on tiny datasets, to compare the reduction quality

## 6. Demos and benchmarks: this is what makes the project credible

Build an `examples/` folder with at least four realistic and diverse bug scenarios. They must be reproducible from public or generated data with a single command, and committed data must stay small (generate large inputs at runtime):

1. **Join fan-out overcount** on TPC-H data generated with DuckDB's `tpch` extension (`CALL dbgen(sf=...)`), e.g. revenue inflated because of a non-unique join key introduced by a "late-arriving duplicate".
2. **NULL semantics bug**: `NOT IN` with a NULL in the subquery returns no rows; or a filter that silently drops NULL keys.
3. **Crash reproduction**: a query that fails with a specific DuckDB error (e.g. a cast failure on a malformed value), reduced to the 1–3 rows that cause it.
4. **Dedup or window logic bug** in a "latest record per key" transformation (ties on timestamp).

Each scenario has a README with the story, the predicate, the before and after row counts, and the generated test. The demos must not look like "we planted one obviously bad row". Prefer bugs that need a *combination* of rows across tables.

Also add `benchmarks/`, a script that reports, per scenario, the input rows, output rows, shrink ratio, predicate evaluations, wall time, and whether 1-minimality was verified. Put the table in the README and generate it, never hand-write the numbers.

## 7. Engineering quality bar

- **Project setup:**
  - Python 3.10+ (check whether 3.10 is still worth supporting), `src/` layout, `pyproject.toml`, `uv` for env and lock
  - `ruff` (lint and format), and `mypy` or `pyright` in strict mode on the package
  - a small public API (`failslice.reduce(...)`, etc.) with docstrings
- **Tests:** pytest with coverage, aiming for at least 90% on the core engine. Use Hypothesis property tests, golden tests for artifacts and reports, MCP server tests using the SDK's in-memory client, and CLI tests.
- **CI (GitHub Actions):**
  - lint, type check and tests on Linux, macOS and Windows, across the supported Python versions
  - the examples run as smoke tests
  - pin actions by version
- **Release:**
  - PyPI packaging with an entry point, plus `uvx failslice` and `uvx failslice-mcp` working
  - Trusted Publishing workflow prepared, but run it only with my approval
  - `server.json` for the MCP Registry, prepared but not submitted
- **Documentation:**
  - README: a 30-second pitch, the GIF, quickstart in under 60 seconds, a "how it works" diagram (Mermaid), guarantees and non-guarantees, MCP client configuration (Claude Code, Claude Desktop, Cursor), positioning versus related tools, and the roadmap
  - also: `CONTRIBUTING.md`, `CHANGELOG.md`, the MIT license, and docs pages for the predicate language and the FK semantics
- **Demo GIF:** recorded with `vhs` (a tape file committed to the repo), so it can be regenerated. Show the real elapsed time; if you speed it up, label it.
- **Honesty rules for all docs and outputs:** state only guarantees the code actually checks. Use "reduced" vs "1-minimal (verified)". Label PK/FK inference as suggestions. Never claim root-cause detection: FailSlice isolates, the human or agent explains.

## 8. Phases

**Phase 0: setup and name check**

- `git init`, `.gitignore` (including `.handoff/`), a minimal skeleton, and a CLAUDE.md for this repo that summarizes the conventions in this brief.
- The project name is **`failslice`** (PyPI package, CLI command `failslice`, MCP entry point `failslice-mcp`, import package `failslice`, config file `failslice.yaml`). On 2026-11-14 it was free on PyPI and no GitHub repository used it. Re-check quickly with `curl -s -o /dev/null -w '%{http_code}' https://pypi.org/pypi/failslice/json` (404 = free). Stop and tell me only if it has been taken since.
- Note: avoid the earlier working title "reproslice"; several unrelated small GitHub projects use it for patch and JSON reduction.
- **Author and attribution:** the author is **Saeed Mahdian <saeed.mahdian@gmail.com>**. Use exactly this name and email:
  - `LICENSE`: the standard MIT text with `Copyright (c) 2026 Saeed Mahdian <saeed.mahdian@gmail.com>`.
  - `pyproject.toml`: `authors = [{ name = "Saeed Mahdian", email = "saeed.mahdian@gmail.com" }]` (also as maintainer), so it shows on PyPI.
  - `CITATION.cff` (valid CFF 1.2.0, author Saeed Mahdian, with email), so GitHub shows "Cite this repository".
  - A short "Author" section at the end of the README: "Created and maintained by Saeed Mahdian", with the email. Ask me for my GitHub username before adding a profile link; never guess it.
  - Set the repo-local git identity (`git config user.name "Saeed Mahdian"`, `git config user.email "saeed.mahdian@gmail.com"`, without `--global`) so commits carry my name.
- Stop.

**Phase 1: design, debated with Codex**

- Write `docs/design.md`: architecture (engine, config, predicate runner, artifacts, CLI, MCP), the data model, the FK strategy (Section 5), predicate outcome semantics, the cache, budgets, the verification process, the safety model, and the test plan.
- Before reading your draft, a fresh Codex session writes its own short design for the FK strategy and the predicate semantics. Then the two designs are debated (one or two rounds), and the result goes into the ADRs.
- Stop and show me the design summary and any open decisions.

**Phase 2: core engine**

- Config loading and validation, row identity, predicate runner with outcome classes, the reduction algorithm, cache, budgets, verification, and the 1-minimality pass.
- Property tests and the brute-force oracle.
- Fresh Codex review. Stop.

**Phase 3: artifacts and CLI**

- Manifest, report, pytest export, `explain`, and all CLI commands, with tests.
- Codex review. Stop.

**Phase 4: MCP server**

- Tools, resources and the prompt, with progress reporting and structured outputs.
- Tests with the in-memory client.
- Manually try it from Claude Code with a local `.mcp.json`, and record the transcript in `.handoff/`.
- Codex review. Stop.

**Phase 5: examples and benchmarks**

- The four scenarios, the benchmark script, and the generated README table.
- Stop.

**Phase 6: polish and release prep**

- README, docs, the GIF, CI on all three OSes, packaging, `server.json`, CHANGELOG, a TestPyPI dry run *only after my approval*.
- Final full-repo review by a fresh Codex session, focused on correctness claims versus code and on first-time-user experience: a clean-machine quickstart following only the README.
- Stop with a release checklist.

## 9. Definition of done (v1)

- A new user can run `uvx failslice` on an example and get a verified reproducer and a passing generated test in under two minutes, following only the README.
- Every guarantee stated in the README is enforced by code and covered by a test.
- CI is green on three OSes. The benchmarks table is generated. The GIF is reproducible.
- An agent connected via MCP can take a described data bug from "here are my files" to "here is a failing pytest" without human file editing.

Start now with Phase 0.
