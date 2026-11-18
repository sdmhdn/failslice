# MCP server

`failslice-mcp` exposes the FailSlice loop to AI agents over the
[Model Context Protocol](https://modelcontextprotocol.io/) (stdio transport), so an agent can
go from "here are my files" to "here is a failing pytest test" without loading the data into
its context. Design and limits: [ADR 0011](decisions/0011-mcp-server.md).

## Running it

```bash
uvx failslice-mcp --root /absolute/path/to/your/data          # from PyPI, nothing to install
python -m failslice.mcp_server --root /absolute/path/to/data   # with only `failslice` installed
uv run failslice-mcp --root /absolute/path/to/data             # in a checkout, after `uv sync`
```

The `failslice-mcp` command comes from the small `failslice-mcp` distribution, which
installs `failslice` of the same version ([ADR 0013](decisions/0013-packaging-and-release.md)).
`uvx failslice-mcp@latest` picks up a newer release.

`--root` is required and must be an existing directory. The server can read supported
files only inside it and writes only under `<root>/.failslice/`. Options: `--max-evals`,
`--wall-clock`, `--eval-timeout` (ceilings for what an agent may request; defaults 5,000
evaluations, 900 s, 30 s; positive numbers only). A bad option or root is reported as a
usage error (exit status 2).

## Client configuration

Use absolute paths.

**Claude Code** (writes the entry for you; add `--scope project` to put it in `.mcp.json`
for the whole team):

```bash
claude mcp add failslice -- uvx failslice-mcp --root /absolute/path/to/your/data
```

**Claude Desktop** (`claude_desktop_config.json`) and **Cursor** (`.cursor/mcp.json` in
the project, or `~/.cursor/mcp.json`), same entry:

```json
{
  "mcpServers": {
    "failslice": {
      "command": "uvx",
      "args": ["failslice-mcp", "--root", "/absolute/path/to/your/data"]
    }
  }
}
```

Claude Desktop does not read your shell's `PATH`; if it cannot start `uvx`, give its
absolute path as `command` (`which uvx` shows it).

From a checkout instead of PyPI, use `"command": "uv"` and
`"args": ["run", "--directory", "/path/to/failslice", "failslice-mcp", "--root", "..."]`.

The FailSlice tests exercise the server through the SDK's in-memory client, and CI starts
the installed `failslice-mcp` command and drives it over stdio with the SDK client; a
session from Claude Code was tried by hand. Claude Desktop and Cursor use the same configuration format
but are not tested by the project.

Reductions can take minutes. The server sends progress notifications (elapsed seconds and
a message with rows kept and evaluations), but a client may still apply its own request
timeout; raise it if your client allows.

## Tools

Every tool returns `{format, status, data, error, warnings}`; `error` is
`{code, message, field, retryable}`. Only invocation and internal errors (`status:
"error"`) are marked as MCP tool errors; domain outcomes such as `failed` (for example a
predicate that is never TRUE) or `replay-failed` are ordinary results that may still
carry an `error` object explaining them.

| tool | does |
|---|---|
| `inspect_dataset` | exact row and NULL counts, schemas, declared-FK orphan counts, bounded previews and a config template, for a `directory` or a `tables` mapping |
| `suggest_relationships` | primary/foreign key **suggestions** with evidence and confidence, plus a `failslice.yaml` text with them commented out |
| `check_predicate` | validation and the determinism check for a project |
| `reduce_failure` | the full run: reduce, export, replay, report, manifest, CSV, pytest bundle; progress notifications; cancellable |
| `verify_reproducer` | replay a run in a fresh process |
| `explain_reproducer` | observations on the surviving rows (not a diagnosis) |
| `export_pytest` | re-verify and write a pytest bundle under `<root>/.failslice/exports/<run_id>/` |

Projects are given either as `config_path` (a `failslice.yaml` inside the root) or as an
inline `config` with the same structure (`tables`, `foreign_keys`, `predicate` with one of
`sql` / `expect` / `raises`, optional `settings`); inline paths are relative to `base_dir`
(default: the root). The input schema of each tool describes the full structure.

Runs are addressed by `run_id` (returned by `reduce_failure`) or `latest`.

### Budgets and concurrency

- An explicit budget above the server's ceiling is rejected (`over_ceiling`); defaults that
  nobody set are lowered to the ceiling. The reply reports the effective settings. Replay
  timeouts (`verify_reproducer`, `export_pytest`) may not exceed the wall-clock ceiling.
- One expensive operation runs at a time; a second call gets `busy` (retryable). Resources
  stay readable.
- Cancelling a request kills the work it started (the reduction child process, a replay).
  A cancelled run is not recovered or verified; its manifest and report record the
  cancellation. Artifact steps that are already running (CSV, bundle, explain) finish first;
  a cancel that arrives while artifacts are written still turns the outcome into
  `cancelled`.
- `reduce_failure` reports the run's real outcome as its status: `reduced` only for a
  verified reproducer, otherwise `replay-failed`, `integrity-failed`, `unverified`,
  `no-candidate`, `cancelled` or `failed` (e.g. the predicate is never TRUE). These are
  ordinary results; only invalid input and internal errors are marked as tool errors.

## Resources

| URI | content |
|---|---|
| `failslice://runs` | finalized runs, newest first by finish time, then incomplete ones: 50 per page with a `next` link |
| `failslice://runs/page/{page}` | further pages of the same list |
| `failslice://runs/{run_id}/manifest` | `manifest.json` |
| `failslice://runs/{run_id}/report` | `report.md` |
| `failslice://runs/{run_id}/tables/{table}/preview` | first rows of a reduced table, with exact counts |

`{run_id}` may be `latest`: the most recently finished run with a valid manifest. Only
these artifacts are readable; there is no generic file reader. The manifest and the report
are refused if they are symlinks; a table preview follows a symlink only to a target
inside the same run directory.

## Prompt

`debug-data-bug` (arguments: `bug`, optional `directory`, `config_path`,
`expected_behavior`, `expect`) gives the agent a step-by-step workflow from the bug
description to an exported test.

## Safety and limits

- Every path is resolved (symlinks followed) and must stay inside the root; glob
  characters, FIFOs and devices are refused. Predicates are validated as everywhere else,
  and DuckDB's external file access is switched off after loading.
- Size limits: 64 KiB per config, 16 KiB of predicate SQL, 32 tables, 128 columns per
  inspected table, 64 KiB per tool reply (counting both copies of the JSON), 256 KiB per
  resource; previews at most 20 rows and 50 columns with cells cut in SQL. Replies to
  malformed tool arguments are produced by the MCP SDK's argument validation and are not
  bounded by FailSlice.
- The root is an authorization, not a secret: an agent connected to the server can read
  the supported data files inside it. Data values are untrusted content; the server's
  instructions tell the agent so.
