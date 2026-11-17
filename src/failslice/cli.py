"""The ``failslice`` command line (ADR 0010).

Commands: ``init``, ``check``, ``reduce``, ``verify``, ``export-test``, ``explain``.
Exit codes: 0 success, 1 a check / predicate / replay requirement failed, 2 usage /
config / safety rejection, 3 inconclusive (budget or deadline), 4 operational or internal
error, 130 interrupted. ``--json`` prints exactly one JSON document on stdout.
"""

from __future__ import annotations

import json
import sys
import traceback
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any, Literal, NoReturn

import typer
from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn
from typer.exceptions import TyperException

from failslice import __version__
from failslice.api import CheckReport, ReductionResult, check, reduce, verify
from failslice.config import ConfigError, Project, load_project, resolve_inside
from failslice.explain import explain
from failslice.inference import discover_tables, render_config, suggest_relationships
from failslice.pytest_export import ExportError, build_bundle
from failslice.reducer import DeterminismError, PredicateNotFailingError, ProgressEvent
from failslice.runspec import RunSpecError
from failslice.safety import PredicateValidationError

__all__ = ["EXIT_CODES", "app", "main"]

EXIT_CODES = {
    "ok": 0,
    "failed": 1,
    "rejected": 2,
    "inconclusive": 3,
    "error": 4,
    "interrupted": 130,
}
JSON_FORMAT = 1

app = typer.Typer(
    name="failslice",
    help="Shrink multi-table data to a small, FK-consistent, verified reproducer of a data bug.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)

_err = Console(stderr=True, soft_wrap=True)
_out = Console(soft_wrap=True, highlight=False)

JsonOpt = Annotated[bool, typer.Option("--json", help="Print one JSON document on stdout.")]
DebugOpt = Annotated[bool, typer.Option("--debug", help="Show tracebacks on errors.")]
ConfigOpt = Annotated[
    Path, typer.Option("--config", "-c", help="Path to failslice.yaml.", show_default=True)
]
RootOpt = Annotated[
    Path | None,
    typer.Option(help="Allowed root for every path (default: the config's directory)."),
]


class _Exit(Exception):
    def __init__(self, kind: str, message: str, *, field: str | None = None) -> None:
        super().__init__(message)
        self.kind = kind
        self.field = field


def _emit(
    command: str,
    *,
    as_json: bool,
    status: str,
    exit_kind: str,
    data: Any = None,
    run_dir: Path | None = None,
    error: dict[str, Any] | None = None,
    text: Callable[[], None] | None = None,
    message: str | None = None,
) -> NoReturn:
    code = EXIT_CODES[exit_kind]
    if error is None and exit_kind != "ok":  # every unsuccessful result explains itself
        error = {"code": status, "message": message or status}
    if as_json:
        doc = {
            "format": JSON_FORMAT,
            "command": command,
            "status": status,
            "exit_code": code,
            "run_dir": str(run_dir) if run_dir is not None else None,
            "data": data,
            "error": error,
        }
        sys.stdout.write(json.dumps(doc, default=str) + "\n")
    elif text is not None:
        text()
    raise typer.Exit(code)


def _guard(command: str, as_json: bool, debug: bool, body: Callable[[], NoReturn]) -> NoReturn:
    """Run a command body and map exceptions to the exit-code contract."""
    try:
        body()
    except (typer.Exit, typer.Abort):
        raise
    except KeyboardInterrupt:
        _fail(command, as_json, "interrupted", "interrupted", "interrupted by the user")
    except _Exit as exc:
        _fail(command, as_json, exc.kind, exc.kind, str(exc), field=exc.field)
    except PredicateValidationError as exc:
        _fail(command, as_json, "rejected", f"predicate_{exc.reason}", str(exc))
    except (ConfigError, RunSpecError, ExportError) as exc:
        kind = "error" if isinstance(exc, ExportError) else "rejected"
        _fail(command, as_json, kind, type(exc).__name__, str(exc))
    except PredicateNotFailingError as exc:
        _fail(command, as_json, "failed", "predicate_not_failing", str(exc))
    except DeterminismError as exc:
        _fail(command, as_json, "failed", "nondeterministic", str(exc))
    except Exception as exc:
        if debug:
            traceback.print_exc()
        _fail(command, as_json, "error", "internal_error", f"{type(exc).__name__}: {exc}")
    raise AssertionError("unreachable")  # pragma: no cover


def _fail(
    command: str,
    as_json: bool,
    kind: str,
    code: str,
    message: str,
    *,
    field: str | None = None,
) -> NoReturn:
    error = {"code": code, "message": message}
    if field:
        error["field"] = field
    _emit(
        command,
        as_json=as_json,
        status="error" if kind != "failed" else "failed",
        exit_kind=kind,
        error=error,
        text=lambda: _err.print(f"[bold red]error:[/] {message}", markup=True, highlight=False),
    )


def _load(config: Path, root: Path | None) -> Project:
    return load_project(config, root=root)


def _progress_enabled(as_json: bool, no_progress: bool) -> bool:
    return not (as_json or no_progress) and sys.stderr.isatty()


# ---------------------------------------------------------------------------- version
def _version(value: bool) -> None:
    if value:
        _out.print(f"failslice {__version__}")
        raise typer.Exit(0)


@app.callback()
def _main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_version, is_eager=True, help="Show the version."),
    ] = False,
) -> None:
    """FailSlice: shrink multi-table data to a verified reproducer of a data bug."""


# ---------------------------------------------------------------------------- init
@app.command()
def init(
    directory: Annotated[Path, typer.Argument(help="Directory with CSV / Parquet files.")] = Path(
        "."
    ),
    config: Annotated[
        Path | None,
        typer.Option("--config", "-c", help="Config to write (default: DIR/failslice.yaml)."),
    ] = None,
    root: RootOpt = None,
    sample_rows: Annotated[
        int, typer.Option(min=1, help="Rows read per file for the suggestions.")
    ] = 10_000,
    full_scan: Annotated[bool, typer.Option(help="Read every row (slower, exact).")] = False,
    as_json: JsonOpt = False,
    debug: DebugOpt = False,
) -> None:
    """Scaffold failslice.yaml from a folder of files, with PK/FK *suggestions* (commented out)."""

    def body() -> NoReturn:
        base = directory.resolve()
        allowed = (root or base).resolve()
        target = (config or base / "failslice.yaml").resolve()
        resolve_inside(base, target, allowed)
        if target.exists():
            raise _Exit("rejected", f"{target} already exists; not overwriting it", field="config")
        tables = discover_tables(base, root=allowed)
        if not tables:
            raise _Exit("rejected", f"no .csv / .parquet files in {base}", field="directory")
        report = suggest_relationships(
            tables,
            root=allowed,
            base_dir=target.parent,
            sample_rows=None if full_scan else sample_rows,
        )
        target.write_text(render_config(report), encoding="utf-8")

        def text() -> None:
            _out.print(f"wrote {target}")
            _out.print(
                f"{len(report.tables)} tables, {len(report.primary_keys)} primary-key and "
                f"{len(report.foreign_keys)} foreign-key suggestions (commented out; review them)"
            )
            for w in report.warnings:
                _err.print(f"warning: {w}")
            _out.print("next: edit the predicate, then run `failslice check`")

        _emit(
            "init",
            as_json=as_json,
            status="ok",
            exit_kind="ok",
            data={"config": str(target), "suggestions": report.model_dump(mode="json")},
            text=text,
        )

    _guard("init", as_json, debug, body)


# ---------------------------------------------------------------------------- check
def _check_data(report: CheckReport) -> dict[str, Any]:
    return {
        "ok": report.ok,
        "deterministic": report.deterministic,
        "tables": report.tables,
        "orphans": report.orphans,
        "outcomes": [asdict(o) for o in report.outcomes],
        "problems": report.problems,
        "stop": report.stop,
    }


@app.command("check")
def check_cmd(
    config: ConfigOpt = Path("failslice.yaml"),
    root: RootOpt = None,
    isolate: Annotated[
        bool, typer.Option(help="Run in a supervised child process with a hard deadline.")
    ] = True,
    as_json: JsonOpt = False,
    debug: DebugOpt = False,
) -> None:
    """Validate the config and predicate, count FK orphans, run the determinism check."""

    def body() -> NoReturn:
        project = _load(config, root)
        report = check(project, isolate=isolate)

        def text() -> None:
            for name, t in report.tables.items():
                _out.print(f"table {name}: {t['rows']:,} rows")
            for fk, n in report.orphans.items():
                note = f"{n} input orphan(s) (kept as exemptions)" if n else "no orphans"
                _out.print(f"fk {fk}: {note}")
            if report.ok:
                _out.print("ok: the predicate is TRUE on the full data, 4 of 4 runs identical")
            for p in report.problems:
                _err.print(f"problem: {p}")

        if report.ok:
            status, kind = "ok", "ok"
        elif report.stop in ("killed", "budget"):
            status, kind = "stopped", "inconclusive"
        elif report.stop == "crashed":
            status, kind = "error", "error"
        else:
            status, kind = "failed", "failed"
        _emit(
            "check",
            as_json=as_json,
            status=status,
            exit_kind=kind,
            message="; ".join(report.problems),
            data=_check_data(report),
            text=text,
        )

    _guard("check", as_json, debug, body)


# ---------------------------------------------------------------------------- reduce
def _reduce_exit(result: ReductionResult) -> tuple[str, str]:
    """(status, exit kind) for a finished reduction, from the recorded reason."""
    if result.status == "cancelled":
        return "cancelled", "interrupted"
    if result.status == "no-candidate":
        if result.failure == "crashed":
            return "error", "error"
        return "no-candidate", "inconclusive"
    v = result.verification
    if v is None or v.status == "unverified":
        return "unverified", "inconclusive"
    if not v.ok:
        return v.status, "failed"
    m = result.manifest
    if m is not None and any(st.status == "failed" for st in m.steps):
        return "artifact-error", "error"
    return "reduced", "ok"


def _result_data(result: ReductionResult) -> dict[str, Any]:
    m = result.manifest
    return {
        "status": result.status,
        "minimality": result.minimality_label,
        "verification": asdict(result.verification) if result.verification else None,
        "manifest": m.model_dump(mode="json") if m is not None else None,
        "notes": result.notes,
    }


@app.command("reduce")
def reduce_cmd(
    config: ConfigOpt = Path("failslice.yaml"),
    root: RootOpt = None,
    out: Annotated[
        Path | None,
        typer.Option(help="Run directory (default: .failslice/runs/<id> next to the config)."),
    ] = None,
    seed: Annotated[int | None, typer.Option(help="Override settings.seed.")] = None,
    max_evals: Annotated[int | None, typer.Option(help="Override settings.max_evals.")] = None,
    wall_clock: Annotated[
        float | None, typer.Option(help="Override settings.wall_clock_s (seconds).")
    ] = None,
    eval_timeout: Annotated[
        float | None, typer.Option(help="Override settings.eval_timeout_s (seconds).")
    ] = None,
    minimality: Annotated[
        bool | None, typer.Option("--minimality/--no-minimality", help="Run certification.")
    ] = None,
    allow_nondeterministic: Annotated[
        bool | None,
        typer.Option("--allow-nondeterministic", help="Reduce an unstable predicate anyway."),
    ] = None,
    isolate: Annotated[
        bool, typer.Option(help="Run in a supervised child process with hard deadlines.")
    ] = True,
    no_progress: Annotated[bool, typer.Option("--no-progress", help="No progress bar.")] = False,
    as_json: JsonOpt = False,
    debug: DebugOpt = False,
) -> None:
    """Reduce the data, export the reproducer, replay it in a fresh process, write the
    report, manifest and pytest bundle."""

    def body() -> NoReturn:
        project = _load(config, root)
        overrides = {
            k: v
            for k, v in {
                "seed": seed,
                "max_evals": max_evals,
                "wall_clock_s": wall_clock,
                "eval_timeout_s": eval_timeout,
                "minimality": minimality,
                "allow_nondeterministic": allow_nondeterministic,
            }.items()
            if v is not None
        }
        if overrides:
            project = project.with_settings(**overrides)
        run_out = None
        if out is not None:
            run_out = resolve_inside(Path.cwd(), out, project.root)
        if _progress_enabled(as_json, no_progress):
            with Progress(
                TextColumn("[bold]{task.fields[phase]:<11}"),
                BarColumn(),
                TextColumn("{task.fields[kept]:>10} rows kept ({task.fields[ratio]})"),
                TextColumn("{task.fields[evals]} evals"),
                TimeElapsedColumn(),
                console=_err,
                transient=True,
            ) as bar:
                task = bar.add_task(
                    "reduce", total=1.0, phase="loading", kept="-", ratio="-", evals=0
                )

                def on_progress(ev: ProgressEvent) -> None:
                    ratio = ev.kept_rows / ev.input_rows if ev.input_rows else 1.0
                    bar.update(
                        task,
                        completed=1.0 - ratio,
                        phase=ev.phase,
                        kept=f"{ev.kept_rows:,}",
                        ratio=f"{ratio:.2%}",
                        evals=ev.evaluations,
                    )

                result = reduce(project, out_dir=run_out, isolate=isolate, on_progress=on_progress)
        else:
            result = reduce(project, out_dir=run_out, isolate=isolate)
        status, kind = _reduce_exit(result)

        def text() -> None:
            stats = result.stats
            if result.status == "reduced" and stats is not None:
                before, after = stats.input_rows, stats.output_rows
                pct = after / before if before else 1.0
                v = result.verification
                _out.print(
                    f"{result.minimality_label}: {before:,} -> {after:,} rows ({pct:.2%}); "
                    f"replay {v.status if v else 'not run'}"
                )
            else:
                _out.print("no reproducer: the run stopped before any configuration was confirmed")
            for n in result.notes:
                _err.print(f"note: {n}")
            _out.print(f"run directory: {result.run_dir}")
            if (result.run_dir / "report.md").is_file():
                _out.print(f"  report: {result.run_dir / 'report.md'}")
            if (result.run_dir / "pytest").is_dir():
                _out.print(f"  test:   pytest {result.run_dir / 'pytest'}")

        _emit(
            "reduce",
            as_json=as_json,
            status=status,
            exit_kind=kind,
            message="; ".join(result.notes)
            or (result.verification.detail if result.verification else ""),
            data=_result_data(result),
            run_dir=result.run_dir,
            text=text,
        )

    _guard("reduce", as_json, debug, body)


# ---------------------------------------------------------------------------- verify
@app.command("verify")
def verify_cmd(
    run_dir: Annotated[Path, typer.Argument(help="Run directory to replay.")],
    timeout: Annotated[float, typer.Option(help="Seconds for the replay.")] = 300.0,
    as_json: JsonOpt = False,
    debug: DebugOpt = False,
) -> None:
    """Replay a run directory in a fresh process from its exported files only."""

    def body() -> NoReturn:
        report = verify(run_dir, timeout_s=timeout)
        kind = "ok" if report.ok else "inconclusive" if report.status == "unverified" else "failed"

        def text() -> None:
            _out.print(f"{report.status}: predicate {report.outcome or '?'}")
            if report.detail:
                _out.print(f"  {report.detail}")
            for fk, n in report.integrity_violations.items():
                _out.print(f"  {fk}: {n} new dangling reference(s)")

        _emit(
            "verify",
            as_json=as_json,
            status=report.status,
            exit_kind=kind,
            message=report.detail,
            data=asdict(report),
            run_dir=run_dir.resolve(),
            text=text,
        )

    _guard("verify", as_json, debug, body)


# ---------------------------------------------------------------------------- export-test
@app.command("export-test")
def export_test(
    run_dir: Annotated[Path, typer.Argument(help="A verified run directory.")],
    out: Annotated[Path, typer.Option(help="New directory for the pytest bundle.")],
    name: Annotated[
        str | None,
        typer.Option(help="Test name ([a-z_][a-z0-9_]*); default reproducer_<hash>."),
    ] = None,
    expect: Annotated[
        str,
        typer.Option(
            help="fails: the test passes while the bug reproduces; passes: once it is fixed."
        ),
    ] = "fails",
    root: Annotated[
        Path | None, typer.Option(help="Allowed root for --out (default: current directory).")
    ] = None,
    timeout: Annotated[float, typer.Option(help="Seconds for the re-verification.")] = 300.0,
    as_json: JsonOpt = False,
    debug: DebugOpt = False,
) -> None:
    """Re-verify a run directory and write a self-contained pytest bundle (pytest + duckdb)."""

    def body() -> NoReturn:
        if expect not in ("fails", "passes"):
            raise _Exit("rejected", "--expect must be 'fails' or 'passes'", field="expect")
        allowed = (root or Path.cwd()).resolve()
        dest = resolve_inside(Path.cwd(), out, allowed)
        report = verify(run_dir, timeout_s=timeout)
        if not report.ok:
            kind = "inconclusive" if report.status == "unverified" else "failed"
            raise _Exit(
                kind, f"the run directory does not verify ({report.status}): {report.detail}"
            )
        polarity: Literal["fails", "passes"] = "fails" if expect == "fails" else "passes"
        bundle = build_bundle(run_dir, dest, name=name, expect=polarity)

        def text() -> None:
            _out.print(f"wrote {bundle.directory}")
            _out.print(f"run it with: pytest {bundle.directory}")

        _emit(
            "export-test",
            as_json=as_json,
            status="ok",
            exit_kind="ok",
            data={
                "directory": str(bundle.directory),
                "test_file": str(bundle.test_file),
                "expect": bundle.expect,
                "files": [str(f.relative_to(bundle.directory)) for f in bundle.files],
            },
            run_dir=Path(run_dir).resolve(),
            text=text,
        )

    _guard("export-test", as_json, debug, body)


# ---------------------------------------------------------------------------- explain
@app.command("explain")
def explain_cmd(
    run_dir: Annotated[Path, typer.Argument(help="Run directory.")],
    limit: Annotated[int, typer.Option(min=1, help="Keys shown per foreign key.")] = 20,
    as_json: JsonOpt = False,
    debug: DebugOpt = False,
) -> None:
    """Observations on the rows that survived reduction (not a diagnosis)."""

    def body() -> NoReturn:
        ex = explain(run_dir, limit=limit)
        _emit(
            "explain",
            as_json=as_json,
            status="ok",
            exit_kind="ok",
            data=ex.model_dump(mode="json"),
            run_dir=Path(run_dir).resolve(),
            text=lambda: _out.print(ex.render_text(), end="", markup=False),
        )

    _guard("explain", as_json, debug, body)


def main(argv: list[str] | None = None) -> None:
    """Console-script entry point. Parser errors honour ``--json`` too (exit code 2);
    ``--help`` and ``--version`` always print text."""
    args = sys.argv[1:] if argv is None else argv
    try:
        rv = app(args=args, prog_name="failslice", standalone_mode=False)
    except typer.Exit as exc:
        raise SystemExit(exc.exit_code) from None
    except typer.Abort:
        _err.print("interrupted")
        raise SystemExit(EXIT_CODES["interrupted"]) from None
    except TyperException as exc:
        if "--json" in args:
            command = next((a for a in args if not a.startswith("-")), None)
            doc = {
                "format": JSON_FORMAT,
                "command": command,
                "status": "error",
                "exit_code": EXIT_CODES["rejected"],
                "run_dir": None,
                "data": None,
                "error": {"code": "usage", "message": _usage_message(exc)},
            }
            sys.stdout.write(json.dumps(doc) + "\n")
        else:
            show = getattr(exc, "show", None)
            if callable(show):
                show()
            else:  # pragma: no cover - every parser error has show()
                _err.print(f"error: {_usage_message(exc)}")
        raise SystemExit(EXIT_CODES["rejected"]) from None
    raise SystemExit(rv if isinstance(rv, int) else 0)


def _usage_message(exc: Exception) -> str:
    fmt = getattr(exc, "format_message", None)
    return str(fmt()) if callable(fmt) else str(exc)


if __name__ == "__main__":  # pragma: no cover
    main()
