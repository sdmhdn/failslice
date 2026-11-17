# ADR 0010: CLI contract (commands, exit codes, JSON output)

- Status: accepted (Phase 3 design, debated with Codex)
- Date: 2026-11-17

## Decision
- Typer (ADR 0006). Commands: `init`, `check`, `reduce`, `verify`, `export-test`, `explain`.
- `check` and `reduce` run supervised (isolated) by default; the library default stays
  in-process. Ctrl-C kills the child, keeps the checkpoint and exits 130.
- Progress (rich) goes to stderr only, disabled without a TTY or with `--no-progress`.
- `--json`: exactly one versioned JSON document on stdout:
  `{"format": 1, "command", "status", "exit_code", "run_dir", "data", "error"}`.
  Every unsuccessful result carries `error` (`code`, `message` and, where known,
  `field`); tracebacks only with `--debug`. Parser errors (unknown option, bad value,
  missing argument) also produce the envelope (exit 2) when `--json` is given.
  `--help` and `--version` are for humans: they print text and exit 0, even with `--json`.
- Exit codes, mapped from the recorded reason, not from the status alone:
  - 0: the requested operation succeeded (a budget-stopped but verified reduction is a success);
  - 1: the check / predicate / replay requirement failed (not failing, nondeterministic,
    replay-failed, integrity-failed);
  - 2: usage, config or safety rejection;
  - 3: inconclusive (budget or deadline before a confirmed candidate or before the check
    finished, replay unverified);
  - 4: operational, I/O, artifact or internal error;
  - 130: interrupted.
- Runs go to `.failslice/runs/<id>/` next to the config unless `--out` (inside the root).

## Consequences
Scripts and agents can rely on exit codes and the JSON envelope; both are covered by tests.
