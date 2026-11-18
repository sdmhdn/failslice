"""The quickstart example works as the README describes (the numbers there come from it)."""

from __future__ import annotations

import importlib.util
import json
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

import failslice

EXAMPLE = Path(__file__).parents[1] / "examples" / "quickstart"


def test_quickstart_example(tmp_path: Path) -> None:
    work = tmp_path / "quickstart"
    shutil.copytree(EXAMPLE, work, ignore=shutil.ignore_patterns("generated", ".failslice"))
    runpy.run_path(str(work / "make_data.py"), run_name="__main__")  # writes data/generated

    project = failslice.load_project(work / "failslice.yaml")
    assert failslice.check(project).ok
    result = failslice.reduce(project, isolate=True)
    assert result.status == "reduced"
    assert result.minimality_label == "1-minimal (verified)"
    assert result.verification is not None and result.verification.ok
    assert result.stats is not None
    assert result.stats.sizes_before == {"customers": 1000, "orders": 5001, "lines": 20000}
    assert result.stats.sizes_after == {"customers": 1, "orders": 2, "lines": 1}


def test_quickstart_run_script(tmp_path: Path) -> None:
    # The shipped run.py, as a user runs it (its own process, its own directory).
    work = tmp_path / "quickstart"
    shutil.copytree(EXAMPLE, work, ignore=shutil.ignore_patterns("generated", ".failslice"))
    subprocess.run([sys.executable, "make_data.py"], cwd=work, check=True)
    proc = subprocess.run(
        [sys.executable, "run.py"], cwd=work, capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert "check ok: True" in proc.stdout
    assert "reduced | 1-minimal (verified) | verified" in proc.stdout


def test_quickstart_cli(tmp_path: Path) -> None:
    # The README's command-line quickstart and the output line it shows.
    work = tmp_path / "quickstart"
    shutil.copytree(EXAMPLE, work, ignore=shutil.ignore_patterns("generated", ".failslice"))
    subprocess.run([sys.executable, "make_data.py"], cwd=work, check=True)
    cli = [sys.executable, "-m", "failslice.cli"]
    check = subprocess.run([*cli, "check"], cwd=work, capture_output=True, text=True, check=False)
    assert check.returncode == 0, check.stderr
    proc = subprocess.run([*cli, "reduce"], cwd=work, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert "1-minimal (verified): 26,001 -> 4 rows (0.02%); replay verified" in proc.stdout
    (run_dir,) = (work / ".failslice" / "runs").iterdir()
    tests = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(run_dir / "pytest")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert tests.returncode == 0, tests.stdout


# --------------------------------------------------------------- Phase 5 scenarios
ROOT = Path(__file__).parents[1]


def _load(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


driver = _load(ROOT / "examples" / "run.py", "failslice_examples_driver")
bench = _load(ROOT / "benchmarks" / "run.py", "failslice_benchmarks_driver")
RESULTS = json.loads((ROOT / "benchmarks" / "results.json").read_text(encoding="utf-8"))


def _copy(name: str, tmp_path: Path) -> Path:
    work = tmp_path / name
    shutil.copytree(
        ROOT / "examples" / name, work, ignore=shutil.ignore_patterns("data", ".failslice")
    )
    return work


def _tpch_or_skip() -> None:
    """Provisioning the tpch extension needs network unless it is cached.

    ``FAILSLICE_REQUIRE_TPCH=1`` (set in CI) turns unavailability into a failure.
    """
    make_data = runpy.run_path(
        str(ROOT / "examples" / "tpch_fanout" / "make_data.py"), run_name="m"
    )
    con = duckdb.connect()
    try:
        make_data["load_tpch"](con)
    except make_data["ExtensionUnavailableError"] as err:
        if os.environ.get("FAILSLICE_REQUIRE_TPCH") == "1":
            raise
        pytest.skip(f"tpch extension unavailable: {err}")
    finally:
        con.close()


@pytest.mark.parametrize("name", driver.NAMES)
def test_scenario(name: str, tmp_path: Path) -> None:
    if name == "tpch_fanout":
        _tpch_or_skip()
    run = driver.run_example(name, _copy(name, tmp_path))
    assert run.status == "reduced" and run.verification == "verified" and run.test_passed
    assert run.minimality == "1-minimal (verified)"
    # The README's generated section (counts and surviving rows) is what a fresh run gives.
    assert driver.render(run) == driver.readme_section(ROOT / "examples" / name / "README.md")
    # The benchmark table's deterministic columns agree with the fresh run.
    (recorded,) = [s for s in RESULTS["scenarios"] if s["name"] == name]
    assert (recorded["rows_before"], recorded["rows_after"], recorded["minimality"]) == (
        run.rows_before,
        run.rows_after,
        run.minimality,
    )


def test_quickstart_benchmark_row(tmp_path: Path) -> None:
    work = tmp_path / "quickstart"
    shutil.copytree(EXAMPLE, work, ignore=shutil.ignore_patterns("generated", ".failslice"))
    driver.generate(work)
    run = driver.reduce_example("quickstart", work)
    (recorded,) = [s for s in RESULTS["scenarios"] if s["name"] == "quickstart"]
    assert (recorded["rows_before"], recorded["rows_after"]) == (run.rows_before, run.rows_after)


def test_benchmark_table_is_generated() -> None:
    assert bench.main(["--check"]) == 0
    for scenario in RESULTS["scenarios"]:
        assert scenario["minimality"] == "1-minimal (verified)" and scenario["test_passed"]


def test_tpch_proxy_settings_and_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    make_data = runpy.run_path(
        str(ROOT / "examples" / "tpch_fanout" / "make_data.py"), run_name="m"
    )
    err = RuntimeError("Failed to parse http_proxy 'http://user:p%40ss@10.0.0.1:3128'")
    assert "p%40ss" not in make_data["_redact"](err) and "***@10.0.0.1" in make_data["_redact"](err)
    for key in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://us%40er:se%3Acret@proxy.example:3128")
    con = duckdb.connect()
    try:
        make_data["_proxy"](con)  # sets DuckDB options only; no network access
        settings = con.execute(
            "SELECT current_setting('http_proxy'), current_setting('http_proxy_username')"
        ).fetchone()
        assert settings == ("proxy.example:3128", "us@er")
    finally:
        con.close()
