# FailSlice documentation

FailSlice shrinks a multi-table dataset (CSV / Parquet / DuckDB files) to a small set of
rows that still triggers a data bug, keeps foreign keys intact, verifies the result in a
fresh process, and hands you a ready-to-run pytest regression test.

## What FailSlice adds (and what it does not claim)

None of the underlying ideas is new: delta debugging (ddmin) is a classic way to shrink a
failing input, and database subsetting tools have long extracted FK-consistent samples.
What FailSlice adds is the combination, applied to a problem the usual tools leave open:

- **Logs and monitoring** tell you *which* job or step failed; the data that caused it is
  still millions of rows.
- **Data-quality checks** flag rows that break a *row-level* rule. The hardest data bugs
  come from a *combination* of rows across tables (a duplicated key plus a join that
  double-counts, a NULL inside a `NOT IN` subquery, a timestamp tie in "latest record per
  key"); no single row is wrong, so there is nothing to flag.
- **Classic reducers** shrink text or code and know nothing about relations between
  tables; deleting rows by hand breaks foreign keys and changes the bug.
- **Subsetting tools** keep foreign keys intact but choose rows by sampling or rules, not
  by whether the bug is still there.

FailSlice asks a different question: *what is the smallest slice of these tables that
still makes this failure happen?* Concretely:

- the search is **guided by your failure predicate**, a DuckDB SQL query that says "the
  bug is present";
- every configuration the predicate sees is **referentially intact**, so the bug you end
  up with comes from real data, not from broken references;
- claims are **checked, not assumed**: the result is replayed in a separate process from
  the exported files only, and it is labelled "1-minimal (verified)" only when that check
  really ran;
- the output is **actionable**: a self-contained pytest bundle (needs only `pytest` and
  `duckdb`) that documents the bug today and guards against its return after the fix;
- an **MCP server**, so an AI agent can go from "here are my files" to "here is a failing
  test" without loading millions of rows into its context.

FailSlice isolates rows; it does not determine the root cause, and it never claims that no
smaller reproducer exists.

## Pages

| page | contents |
|---|---|
| [Configuration](configuration.md) | `failslice.yaml`, settings, `failslice init` and its key suggestions |
| [Predicates](predicates.md) | the three predicate forms, outcomes, comparison rules, what is rejected |
| [Foreign keys and minimality](foreign-keys.md) | FK semantics, input orphans, cascade deletion, what "1-minimal" means |
| [Command line and Python API](cli.md) | commands, options, exit codes, `--json`, the library API |
| [Run artifacts](artifacts.md) | the run directory, manifest, report, CSV, `explain`, the generated test, replay |
| [MCP server](mcp.md) | `failslice-mcp` for AI agents: setup, tools, resources, limits |
| [Design](design.md) | architecture and the reasoning behind it; decisions in [`decisions/`](decisions/) |
| [راهنمای فارسی](guide-fa.md) | the complete user guide in Persian |
