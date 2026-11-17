"""The ``failslice`` CLI: commands, exit codes (ADR 0010) and the JSON envelope."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from failslice.cli import EXIT_CODES, app
from failslice.config import Project
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables

runner = CliRunner()


def _json(args: list[str]) -> tuple[int, dict[str, Any]]:
    result = runner.invoke(app, [*args, "--json"])
    lines = result.stdout.strip().splitlines()
    assert len(lines) == 1, result.stdout  # exactly one JSON document on stdout
    doc = json.loads(lines[0])
    assert doc["format"] == 1 and doc["exit_code"] == result.exit_code
    return result.exit_code, doc


@pytest.fixture
def project(project_factory: Callable[..., Project]) -> Project:
    return project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)


def test_exit_code_contract() -> None:
    assert EXIT_CODES == {
        "ok": 0,
        "failed": 1,
        "rejected": 2,
        "inconclusive": 3,
        "error": 4,
        "interrupted": 130,
    }


def test_version_and_help() -> None:
    assert runner.invoke(app, ["--version"]).stdout.startswith("failslice ")
    result = runner.invoke(app, [])
    assert "reduce" in result.stdout and "export-test" in result.stdout


def test_check(project: Project) -> None:
    cfg = str(project.base_dir / "failslice.yaml")
    code, doc = _json(["check", "-c", cfg, "--no-isolate"])
    assert code == 0 and doc["status"] == "ok" and doc["data"]["deterministic"]
    result = runner.invoke(app, ["check", "-c", cfg])  # isolated by default
    assert result.exit_code == 0 and "4 of 4 runs identical" in result.stdout


def test_check_failures(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    code, doc = _json(["check", "-c", str(p.base_dir / "failslice.yaml"), "--no-isolate"])
    assert code == 1 and doc["status"] == "failed"
    code, doc = _json(["check", "-c", str(tmp_path / "missing.yaml")])
    assert code == 2 and doc["error"]["code"] == "ConfigError"
    bad = p.base_dir / "bad.yaml"
    bad.write_text(
        "tables:\n  t: {path: t.parquet}\npredicate:\n  sql: SELECT * FROM read_text('x')\n"
    )
    code, doc = _json(["check", "-c", str(bad)])
    assert code == 2 and doc["error"]["code"].startswith("predicate_")


def test_reduce_verify_explain_export(project: Project, tmp_path: Path) -> None:
    cfg = str(project.base_dir / "failslice.yaml")
    code, doc = _json(["reduce", "-c", cfg, "--no-isolate", "--seed", "1"])
    assert code == 0 and doc["status"] == "reduced"
    assert doc["data"]["minimality"] == "1-minimal (verified)"
    assert doc["data"]["manifest"]["settings"]["seed"] == 1
    run_dir = Path(doc["run_dir"])
    assert run_dir.parent == project.base_dir / ".failslice" / "runs"

    code, doc = _json(["verify", str(run_dir)])
    assert code == 0 and doc["status"] == "verified"
    code, doc = _json(["explain", str(run_dir), "--limit", "1"])
    assert code == 0 and doc["data"]["format"] == 1
    text = runner.invoke(app, ["explain", str(run_dir)]).stdout
    assert "not a diagnosis" in text

    out = project.base_dir / "regression"
    code, doc = _json(
        ["export-test", str(run_dir), "--out", str(out), "--root", str(project.base_dir)]
    )
    assert code == 0 and len(list(out.glob("test_reproducer_*.py"))) == 1
    code, doc = _json(
        ["export-test", str(run_dir), "--out", str(out), "--root", str(project.base_dir)]
    )
    assert code == 4 and "already exists" in doc["error"]["message"]
    code, doc = _json(["export-test", str(run_dir), "--out", str(tmp_path.parent / "x")])
    assert code == 2 and "outside the allowed root" in doc["error"]["message"]
    code, doc = _json(["export-test", str(run_dir), "--out", "y", "--expect", "maybe"])
    assert code == 2 and doc["error"]["field"] == "expect"


def test_reduce_out_must_stay_inside_root(project: Project, tmp_path: Path) -> None:
    cfg = str(project.base_dir / "failslice.yaml")
    code, doc = _json(["reduce", "-c", cfg, "--out", str(tmp_path / "elsewhere")])
    assert code == 2 and "outside the allowed root" in doc["error"]["message"]


def test_reduce_budget_stop_is_inconclusive(project: Project) -> None:
    cfg = str(project.base_dir / "failslice.yaml")
    code, doc = _json(["reduce", "-c", cfg, "--no-isolate", "--max-evals", "2"])
    assert code == 3 and doc["status"] == "no-candidate"
    assert doc["data"]["manifest"]["status"] == "no-candidate"


def test_reduce_never_failing_predicate(project_factory: Callable[..., Project]) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    code, doc = _json(["reduce", "-c", str(p.base_dir / "failslice.yaml")])
    assert code == 1 and doc["error"]["code"] == "predicate_not_failing"


def test_verify_failure_codes(project: Project) -> None:
    cfg = str(project.base_dir / "failslice.yaml")
    _, doc = _json(["reduce", "-c", cfg, "--no-isolate", "--no-minimality"])
    run_dir = Path(doc["run_dir"])
    (run_dir / "tables" / "orders.parquet").unlink()
    code, doc = _json(["verify", str(run_dir)])
    assert code in (1, 3) and doc["status"] in ("replay-failed", "unverified")
    code, doc = _json(
        [
            "export-test",
            str(run_dir),
            "--out",
            str(project.base_dir / "z"),
            "--root",
            str(project.base_dir),
        ]
    )
    assert code in (1, 3) and "does not verify" in doc["error"]["message"]


def test_init(tmp_path: Path) -> None:
    import duckdb

    data = tmp_path / "data"
    data.mkdir()
    con = duckdb.connect()
    con.execute(f"COPY (SELECT range AS id FROM range(5)) TO '{data / 'customers.csv'}'")
    con.execute(
        f"COPY (SELECT range AS order_id, range % 5 AS customer_id FROM range(9)) "
        f"TO '{data / 'orders.parquet'}'"
    )
    code, doc = _json(["init", str(data)])
    assert code == 0
    config = Path(doc["data"]["config"])
    text = config.read_text()
    assert "suggestion" in text and "SELECT false" in text
    code, doc = _json(["init", str(data)])
    assert code == 2 and doc["error"]["field"] == "config"
    code, doc = _json(["init", str(tmp_path / "empty_dir_does_not_exist")])
    assert code in (2, 4)
    # the scaffold loads, and check reports the placeholder predicate as not failing
    code, doc = _json(["check", "-c", str(config), "--no-isolate"])
    assert code == 1


def test_console_script_smoke(project: Project) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "failslice.cli",
            "check",
            "-c",
            str(project.base_dir / "failslice.yaml"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_text_output_and_progress(
    project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from failslice import cli

    monkeypatch.setattr(cli, "_progress_enabled", lambda *_a: True)
    cfg = str(project.base_dir / "failslice.yaml")
    result = runner.invoke(app, ["reduce", "-c", cfg, "--no-isolate"])
    assert result.exit_code == 0, result.output
    assert "1-minimal (verified): 551 -> 4 rows" in result.stdout
    assert "run directory:" in result.stdout and "test:   pytest" in result.stdout
    run_dir = next((project.base_dir / ".failslice" / "runs").iterdir())
    result = runner.invoke(app, ["verify", str(run_dir)])
    assert result.exit_code == 0 and result.stdout.startswith("verified: predicate TRUE")
    out = project.base_dir / "t1"
    result = runner.invoke(
        app, ["export-test", str(run_dir), "--out", str(out), "--root", str(project.base_dir)]
    )
    assert result.exit_code == 0 and "run it with: pytest" in result.stdout
    result = runner.invoke(app, ["reduce", "-c", cfg, "--no-isolate", "--max-evals", "2"])
    assert result.exit_code == 3 and "no reproducer" in result.stdout


def test_text_errors_interrupt_and_internal(
    project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    from failslice import cli

    cfg = str(project.base_dir / "failslice.yaml")

    def interrupted(*_a: Any, **_k: Any) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "reduce", interrupted)
    result = runner.invoke(app, ["reduce", "-c", cfg])
    assert result.exit_code == 130 and "interrupted" in result.stderr

    def boom(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("kaputt")

    monkeypatch.setattr(cli, "reduce", boom)
    code, doc = _json(["reduce", "-c", cfg])
    assert code == 4 and doc["error"] == {
        "code": "internal_error",
        "message": "RuntimeError: kaputt",
    }
    result = runner.invoke(app, ["reduce", "-c", cfg, "--debug"])
    assert result.exit_code == 4 and "Traceback" in result.stderr


def test_reduce_exit_mapping() -> None:
    import dataclasses

    from failslice.api import ReductionResult
    from failslice.cli import _reduce_exit
    from failslice.reducer import ReductionStats
    from failslice.verify import VerificationReport

    base = ReductionResult(Path("x"), "reduced", ReductionStats({}), VerificationReport("verified"))
    assert _reduce_exit(base) == ("reduced", "ok")
    crashed = dataclasses.replace(base, status="no-candidate", failure="crashed")
    assert _reduce_exit(crashed) == ("error", "error")
    killed = dataclasses.replace(base, status="no-candidate", stats=None, failure="killed")
    assert _reduce_exit(killed) == ("no-candidate", "inconclusive")
    unverified = dataclasses.replace(base, verification=VerificationReport("unverified"))
    assert _reduce_exit(unverified) == ("unverified", "inconclusive")
    failed = dataclasses.replace(base, verification=VerificationReport("integrity-failed"))
    assert _reduce_exit(failed) == ("integrity-failed", "failed")


def test_init_text(tmp_path: Path) -> None:
    import duckdb

    data = tmp_path / "d"
    data.mkdir()
    duckdb.connect().execute(f"COPY (SELECT 1 AS id) TO '{data / 'a.csv'}'")
    result = runner.invoke(app, ["init", str(data), "--full-scan"])
    assert result.exit_code == 0 and "failslice check" in result.stdout
    empty = tmp_path / "empty"
    empty.mkdir()
    result = runner.invoke(app, ["init", str(empty)])
    assert result.exit_code == 2 and "no .csv" in result.stderr
