# Command line and Python API

## Commands

| command | does |
|---|---|
| `failslice init [DIR]` | write `failslice.yaml` for a folder, with key suggestions ([details](configuration.md#starting-from-a-folder-failslice-init)) |
| `failslice check` | validate config and predicate, count input orphans, run the determinism check |
| `failslice reduce` | reduce, export, replay, then write report, manifest, CSV and the pytest bundle |
| `failslice verify RUN_DIR` | replay a run directory in a fresh process |
| `failslice export-test RUN_DIR --out DIR` | re-verify the run, then write a pytest bundle into a new directory |
| `failslice explain RUN_DIR` | observations on the rows that survived reduction |

### Options

- Every command: `--json` (one JSON document on stdout), `--debug` (tracebacks).
- `check`, `reduce`: `-c/--config` (default `failslice.yaml`), `--root` (allowed root,
  default the config's directory), `--isolate/--no-isolate` (default: isolated).
- `reduce`: `--out` (inside the root; default `.failslice/runs/<id>` next to the config),
  `--seed`, `--max-evals`, `--wall-clock`, `--eval-timeout`, `--minimality/--no-minimality`,
  `--allow-nondeterministic`, `--no-progress`.
- `verify`: `--timeout` (seconds, default 300).
- `export-test`: `--out` (required, new directory inside `--root`, default root: the
  current directory), `--name` (default `reproducer_<hash>`), `--expect fails|passes`,
  `--timeout`.
- `explain`: `--limit` (keys shown per foreign key, default 20).
- `init`: `-c/--config`, `--root`, `--sample-rows` (default 10,000), `--full-scan`.

`check` and `reduce` run in a supervised child process by default: a hard deadline of
`wall_clock_s`, and an evaluation that overruns `eval_timeout_s` plus a grace period is
killed. In-process (`--no-isolate`, and the library default), timeouts are cooperative:
they interrupt the SQL query but not Python-side work such as a slow regular expression.
Ctrl-C stops the child and keeps the last checkpoint.

Progress (phase, rows kept, shrink ratio, evaluations, elapsed time) is shown on stderr
when stderr is a terminal.

## Exit codes

| code | meaning |
|---|---|
| 0 | the requested operation succeeded (a budget-stopped but verified reduction is a success) |
| 1 | a requirement failed: predicate never TRUE, nondeterministic, replay failed, integrity failed |
| 2 | invalid usage, config, path or predicate (including parser errors) |
| 3 | inconclusive: budget or deadline before a confirmed result, replay unverified |
| 4 | operational error: I/O, artifacts, a crashed child, an internal error |
| 130 | interrupted |

## JSON output

With `--json`, stdout contains exactly one document:

```json
{"format": 1, "command": "reduce", "status": "reduced", "exit_code": 0,
 "run_dir": "...", "data": {...}, "error": null}
```

Every unsuccessful result carries `error` with `code`, `message` and, where known,
`field`. Parser errors produce the same envelope (exit 2). `--help` and `--version` are
for humans: they print text and exit 0, even with `--json`.

## Python API

```python
import failslice


def main() -> None:
    project = failslice.load_project("failslice.yaml")
    report = failslice.check(project, isolate=True)
    result = failslice.reduce(project, isolate=True, on_progress=print)
    print(result.status, result.minimality_label, result.verification.status)
    print(result.manifest.rows_before, "->", result.manifest.rows_after)
    failslice.verify(result.run_dir)


if __name__ == "__main__":  # required: isolate=True starts a child process
    main()
```

- `load_project(path, root=None) -> Project`; `Project.with_settings(**changes)`.
- `check(project, isolate=False) -> CheckReport` (`ok`, `tables`, `orphans`, `outcomes`,
  `problems`, `stop`).
- `reduce(project, out_dir=None, isolate=False, on_progress=None) -> ReductionResult`
  (`status`, `stats`, `verification`, `notes`, `spec`, `manifest`, `failure`,
  `minimality_label`). In isolated runs `on_progress` runs on its own thread and receives
  only the latest event; keep it non-blocking.
- `verify(run_dir, timeout_s=300) -> VerificationReport`.
- `validate_predicate(project)` raises `PredicateValidationError` with a `reason`.
- Errors: `ConfigError`, `PredicateValidationError`, `PredicateNotFailingError`,
  `DeterminismError`.
