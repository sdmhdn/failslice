"""The quickstart example works as the README describes (the numbers there come from it)."""

from __future__ import annotations

import runpy
import shutil
import subprocess
import sys
from pathlib import Path

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
