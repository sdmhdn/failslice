"""Regression tests for the independent Phase 3 review (one block per finding)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest

import failslice
from failslice import portable
from failslice.cli import main as cli_main
from failslice.config import ConfigError, Project
from failslice.explain import explain
from failslice.pytest_export import build_bundle
from failslice.runspec import RunSpec, RunSpecError
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables


@pytest.fixture
def run(project_factory: Callable[..., Project], tmp_path: Path) -> failslice.ReductionResult:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    return failslice.reduce(p, out_dir=tmp_path / "run")


def _tamper(run_dir: Path, change: Callable[[dict], None]) -> None:  # type: ignore[type-arg]
    spec = json.loads((run_dir / "spec.json").read_text())
    change(spec)
    (run_dir / "spec.json").write_text(json.dumps(spec))


# 1: no Python from the spec reaches the generated test.
def test_version_string_cannot_inject_python(
    run: failslice.ReductionResult, tmp_path: Path
) -> None:
    _tamper(run.run_dir, lambda s: s.update(failslice_version='x"""\nINJECTED = True\n"""'))
    bundle = build_bundle(run.run_dir, tmp_path / "b")
    text = bundle.test_file.read_text()
    assert "INJECTED" not in text and failslice.__version__ in text


# 2: tampered table names are refused before any file is written.
@pytest.mark.parametrize("bad", ["../../victim", "a b", "__fs_x", "t;drop"])
def test_tampered_table_names_are_refused(
    run: failslice.ReductionResult, tmp_path: Path, bad: str
) -> None:
    def rename(spec: dict) -> None:  # type: ignore[type-arg]
        spec["tables"][bad] = spec["tables"].pop("lines")

    _tamper(run.run_dir, rename)
    with pytest.raises(RunSpecError, match="invalid table name"):
        RunSpec.read(run.run_dir)
    victim = tmp_path / "victim.parquet"
    victim.write_text("keep")
    with pytest.raises(RunSpecError):
        build_bundle(run.run_dir, tmp_path / "dest" / "b")
    assert victim.read_text() == "keep" and not (tmp_path / "dest").exists()


def test_foreign_keys_must_refer_to_listed_tables(run: failslice.ReductionResult) -> None:
    _tamper(run.run_dir, lambda s: s.update(foreign_keys=["x.a -> lines.order_id"]))
    with pytest.raises(RunSpecError, match="unknown table"):
        RunSpec.read(run.run_dir)


# 3: bundle restoration expressions run only after external access is disabled.
def test_bundle_metadata_cannot_read_files(run: failslice.ReductionResult, tmp_path: Path) -> None:
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP SECRET")
    bundle = run.run_dir / "pytest"
    data = json.loads((bundle / "reproducer.json").read_text())
    data["tables"][0]["select"].append(f"(SELECT content FROM read_text('{secret}')) AS leak")
    (bundle / "reproducer.json").write_text(json.dumps(data))
    # The expression runs after external access is disabled: DuckDB refuses the read.
    with pytest.raises(duckdb.Error, match=r"(?i)permission|disabled|external"):
        portable.run_bundle(bundle)


# 5: finalization never runs past its deadline (CSV and explain are interruptible).
def test_explain_and_csv_honour_the_deadline(run: failslice.ReductionResult) -> None:
    from failslice.artifacts import write_csv

    assert run.spec is not None
    with pytest.raises(Exception, match=r"(?i)interrupt|deadline"):
        explain(run.run_dir, deadline=time.monotonic() - 1)
    with pytest.raises(Exception, match=r"(?i)interrupt|deadline"):
        write_csv(run.run_dir, run.spec, time.monotonic() - 1)


def test_bundle_is_not_published_after_the_deadline(
    run: failslice.ReductionResult, tmp_path: Path
) -> None:
    from failslice.pytest_export import ExportError

    with pytest.raises(ExportError, match="deadline"):
        build_bundle(run.run_dir, tmp_path / "late", deadline=time.monotonic() - 1)
    assert not (tmp_path / "late").exists()


# 6 + 7: an isolated check has a per-evaluation hard limit and reports why it stopped.
def test_isolated_check_is_killed_per_evaluation(
    project_factory: Callable[..., Project],
) -> None:
    from failslice.supervisor import run_check_supervised

    # Catastrophic backtracking in Python's re: not interruptible by DuckDB's watchdog.
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(1,)])},
        predicate={
            "raises": {"query": "SELECT error(repeat('a', 40) || '!') FROM t", "match": "(a+)+$"}
        },
        settings={"eval_timeout_s": 1, "wall_clock_s": 120},
    )
    start = time.monotonic()
    report = run_check_supervised(p, grace_s=1)
    assert time.monotonic() - start < 30
    assert report.stop == "killed" and "one evaluation exceeded" in report.problems[0]


def test_check_exit_codes_follow_the_stop_reason(
    project: Project, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from failslice import cli
    from failslice.api import CheckReport

    cfg = str(project.base_dir / "failslice.yaml")
    for stop, code in (("killed", 3), ("crashed", 4), (None, 1)):
        monkeypatch.setattr(
            cli, "check", lambda *_a, stop=stop, **_k: CheckReport(ok=False, stop=stop)
        )
        with pytest.raises(SystemExit) as exc:
            cli_main(["check", "-c", cfg, "--json"])
        assert exc.value.code == code
        assert json.loads(capsys.readouterr().out)["exit_code"] == code


@pytest.fixture
def project(project_factory: Callable[..., Project]) -> Project:
    return project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)


# 8: parser errors honour --json; --help and --version print text with exit 0.
@pytest.mark.parametrize(
    "args",
    [
        ["check", "--json", "--bogus"],
        ["reduce", "--json", "--max-evals", "bad"],
        ["verify", "--json"],
    ],
)
def test_usage_errors_in_json_mode(args: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main(args)
    assert exc.value.code == 2
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1
    doc = json.loads(out[0])
    assert doc["exit_code"] == 2 and doc["error"]["code"] == "usage"


def test_help_and_version_exit_zero(capsys: pytest.CaptureFixture[str]) -> None:
    for args in (["check", "--help"], ["--version"]):
        with pytest.raises(SystemExit) as exc:
            cli_main(args)
        assert exc.value.code == 0
    assert "failslice" in capsys.readouterr().out


# 9: the default run directory stays inside the allowed root.
def test_symlinked_default_run_dir_is_refused(project: Project, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (project.base_dir / ".failslice").symlink_to(outside)
    with pytest.raises(ConfigError, match="outside the allowed root"):
        failslice.reduce(project)
    assert list(outside.iterdir()) == []


# 10: a table named "orphans" keeps its own fixture.
def test_table_named_orphans(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory(
        {"orphans": ([("x", "BIGINT")], [(1,), (2,)])},
        predicate={"sql": "SELECT count(*) > 0 FROM orphans WHERE x = 2"},
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    bundle = res.run_dir / "pytest"
    assert (bundle / "fixtures" / "tables" / "orphans.parquet").is_file()
    assert (bundle / "fixtures" / "orphans.parquet").is_file()
    assert portable.run_bundle(bundle).outcome is not None


# 11: two default bundles of different runs are collected together.
def test_two_default_bundles_together(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    suite = tmp_path / "suite"
    for i, wanted in enumerate((2, 3)):
        p = project_factory(
            {"t": ([("x", "BIGINT")], [(1,), (2,), (3,)])},
            predicate={"sql": f"SELECT count(*) > 0 FROM t WHERE x = {wanted}"},
        )
        res = failslice.reduce(p, out_dir=tmp_path / f"run{i}")
        shutil.copytree(res.run_dir / "pytest", suite / f"bundle{i}")
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(suite)],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert proc.returncode == 0 and "2 passed" in proc.stdout, proc.stdout


# 12: statistics reconstructed after a crash are marked incomplete, never zero.
def test_crash_stats_are_marked_unknown(run: failslice.ReductionResult, tmp_path: Path) -> None:
    import dataclasses

    from failslice.artifacts import RunContext, finalize
    from failslice.reducer import ReductionStats, StopReason

    stats = ReductionStats({"t": 5}, evaluations=1, stop_reason=StopReason.CRASHED, complete=False)
    result = dataclasses.replace(
        run, status="no-candidate", stats=stats, spec=None, verification=None, failure="crashed"
    )
    ctx = RunContext("rid", tmp_path, "2026-01-01T00:00:00+00:00", time.monotonic(), {}, {})
    m = finalize(ctx, dataclasses.replace(result, run_dir=tmp_path))
    assert m.stats is not None
    assert m.stats["complete"] is False and m.stats["stop_reason"] == "crashed"
    assert m.stats["outcomes"] is None and m.stats["cache_hits"] is None
    assert "unknown (the reduction process stopped" in (tmp_path / "report.md").read_text()


# 13: the report states integrity only after a verified replay.
def test_report_integrity_wording(run: failslice.ReductionResult, tmp_path: Path) -> None:
    import dataclasses

    from failslice.artifacts import RunContext, finalize
    from failslice.verify import VerificationReport

    assert "found no new dangling references" in (run.run_dir / "report.md").read_text()
    for status, phrase in (
        ("integrity-failed", "integrity check failed"),
        ("unverified", "was not verified"),
    ):
        out = tmp_path / status
        shutil.copytree(run.run_dir, out, ignore=shutil.ignore_patterns("pytest"))
        res = dataclasses.replace(
            run,
            run_dir=out,
            verification=VerificationReport(status),  # type: ignore[arg-type]
        )
        ctx = RunContext(
            "rid",
            out,
            "2026-01-01T00:00:00+00:00",
            time.monotonic(),
            run.manifest.config,
            {},  # type: ignore[union-attr]
        )
        finalize(ctx, res)
        assert phrase in (out / "report.md").read_text()


# 14: duplicate parent keys count only referenced keys.
def test_duplicate_parent_keys_are_referenced_only(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(
        {
            "p": ([("k", "BIGINT")], [(1,), (1,), (2,), (2,)]),
            "c": ([("fk", "BIGINT")], [(1,)]),
        },
        fks=["c.fk -> p.k"],
        predicate={"sql": "SELECT count(*) = 1 FROM c"},
        settings={"minimality": False},
    )
    from failslice.engine import Engine

    out = tmp_path / "run"
    e = Engine(p, allowed_directories=[out])
    try:
        e.export(out)
    finally:
        e.close()
    (fk,) = explain(out).foreign_keys
    assert fk.duplicate_parent_keys == 1


# Recheck: every statement gets its own deadline-armed watchdog.
def test_deadline_connection_rearms_per_statement() -> None:
    raw = duckdb.connect()
    con = portable.DeadlineConnection(raw, time.monotonic() + 0.05)
    assert con.execute("SELECT 1").fetchone() == (1,)
    time.sleep(0.1)
    with pytest.raises(TimeoutError):
        con.execute("SELECT 1")
    slow = portable.DeadlineConnection(raw, time.monotonic() + 0.2)
    with pytest.raises(TimeoutError):
        slow.execute("SELECT count(*) FROM range(10000000000)")
    assert portable.DeadlineConnection(raw, None).executemany("SELECT ?", [[1]]) is not None


def test_run_bundle_honours_deadline(run: failslice.ReductionResult) -> None:
    with pytest.raises(TimeoutError):
        portable.run_bundle(run.run_dir / "pytest", deadline=time.monotonic() - 1)


# Recheck 7: a check stopped by its budget is inconclusive (exit 3), not a crash.
def test_check_budget_stop(project_factory: Callable[..., Project]) -> None:
    p = project_factory(
        fanout_tables(),
        fks=FANOUT_FKS,
        predicate=FANOUT_PREDICATE,
        settings={"wall_clock_s": 1e-6},
    )
    report = failslice.check(p)
    assert not report.ok and report.stop == "budget" and "budget" in report.problems[0]
    with pytest.raises(SystemExit) as exc:
        cli_main(["check", "-c", str(p.base_dir / "failslice.yaml"), "--no-isolate", "--json"])
    assert exc.value.code == 3


# Recheck 11: the default test name also depends on the predicate.
def test_default_names_differ_by_predicate(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    names = set()
    for i, sql in enumerate(("SELECT count(*) > 0 FROM t", "SELECT count(*) >= 1 FROM t")):
        p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": sql})
        res = failslice.reduce(p, out_dir=tmp_path / f"r{i}")
        names |= {f.name for f in (res.run_dir / "pytest").glob("test_*.py")}
    assert len(names) == 2


# Recheck 13: integrity-failed is described with the replay's own detail.
def test_report_integrity_failed_detail(run: failslice.ReductionResult, tmp_path: Path) -> None:
    import dataclasses

    from failslice.artifacts import RunContext, finalize
    from failslice.verify import VerificationReport

    out = tmp_path / "x"
    shutil.copytree(run.run_dir, out, ignore=shutil.ignore_patterns("pytest"))
    v = VerificationReport("integrity-failed", detail="malformed orphans metadata: duplicate")
    res = dataclasses.replace(run, run_dir=out, verification=v)
    assert run.manifest is not None
    finalize(RunContext("rid", out, "t", time.monotonic(), run.manifest.config, {}), res)
    assert "integrity check failed: malformed orphans metadata" in (out / "report.md").read_text()


# Recheck: unsuccessful JSON results always carry an error object.
def test_json_error_is_never_null_on_failure(
    project: Project, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import dataclasses

    from failslice import cli
    from failslice.api import ReductionResult

    crashed = ReductionResult(
        project.base_dir,
        "no-candidate",
        None,
        None,
        ["crash: IOException: bad parquet"],
        failure="crashed",
    )
    monkeypatch.setattr(cli, "reduce", lambda *_a, **_k: dataclasses.replace(crashed))
    with pytest.raises(SystemExit) as exc:
        cli_main(["reduce", "-c", str(project.base_dir / "failslice.yaml"), "--json"])
    doc = json.loads(capsys.readouterr().out)
    assert exc.value.code == 4 and doc["error"]["message"] == "crash: IOException: bad parquet"


def test_fetch_runs_under_the_deadline() -> None:
    raw = duckdb.connect()
    con = portable.DeadlineConnection(raw, time.monotonic() + 0.2)
    assert con.fetchone("SELECT 42") == (42,)
    assert con.fetchall("SELECT 1 WHERE false") == []
    assert con.fetchone("SELECT 1 WHERE false") is None
    with pytest.raises(TimeoutError):
        con.fetchall("SELECT count(*) FROM range(10000000000)")


def test_default_names_differ_by_foreign_keys(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    names = set()
    for i, parent_col in enumerate(("k1", "k2")):
        p = project_factory(
            {
                "p": ([("k1", "BIGINT"), ("k2", "BIGINT")], [(1, 1)]),
                "c": ([("fk", "BIGINT")], [(1,)]),
            },
            fks=[f"c.fk -> p.{parent_col}"],
            predicate={"sql": "SELECT count(*) > 0 FROM c"},
        )
        res = failslice.reduce(p, out_dir=tmp_path / f"r{i}")
        names |= {f.name for f in (res.run_dir / "pytest").glob("test_*.py")}
    assert len(names) == 2


def test_fifo_fixtures_are_refused(run: failslice.ReductionResult, tmp_path: Path) -> None:
    from failslice.pytest_export import ExportError

    bundle = run.run_dir / "pytest"
    target = bundle / "fixtures" / "tables" / "lines.parquet"
    target.unlink()
    os.mkfifo(target)
    with pytest.raises(ValueError, match="not a regular file"):
        portable.run_bundle(bundle)
    (run.run_dir / "tables" / "lines.parquet").unlink()
    os.mkfifo(run.run_dir / "tables" / "lines.parquet")
    with pytest.raises(ExportError, match="not a regular file"):
        build_bundle(run.run_dir, tmp_path / "b")
