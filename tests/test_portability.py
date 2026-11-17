"""Platform portability: non-ASCII names and values through export, replay and the bundle.

On Windows the default text encoding is often not UTF-8; every metadata file FailSlice
writes or reads must still use UTF-8 explicitly.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import failslice
from failslice import portable
from failslice.config import Project


def test_unicode_names_and_values_round_trip(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    rows = [(1, "مشتری اول"), (2, "Ünïcödé ✓"), (3, "مشتری سوم")]
    p = project_factory(
        {"t": ([("id", "BIGINT"), ("نام", "VARCHAR")], rows)},
        predicate={"sql": "SELECT count(*) > 0 FROM t WHERE \"نام\" = 'Ünïcödé ✓'"},
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok, res.verification
    assert res.stats is not None and res.stats.output_rows == 1
    spec = json.loads((res.run_dir / "spec.json").read_text(encoding="utf-8"))
    assert spec["tables"]["t"]["columns"][1][0] == "نام"
    assert "نام" in (res.run_dir / "report.md").read_text(encoding="utf-8")
    csv = (res.run_dir / "csv" / "t.csv").read_text(encoding="utf-8")
    assert "Ünïcödé ✓" in csv
    result = portable.run_bundle(res.run_dir / "pytest")
    assert result.outcome is not None and result.outcome.is_true, result
