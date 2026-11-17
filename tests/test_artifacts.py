"""Phase 3 artifacts: manifest, CSV, report, explain, the portable pytest bundle, and the
supervised check."""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import shutil
import subprocess
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import failslice
from failslice import portable
from failslice.artifacts import load_manifest
from failslice.config import ConfigError, Project
from failslice.explain import explain
from failslice.pytest_export import ExportError, build_bundle
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables


def _run_pytest(bundle: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    """Run a bundle's test in a fresh interpreter where importing failslice fails, from
    another working directory (the bundle must be self-contained)."""
    code = (
        "import sys; sys.modules['failslice'] = None; import pytest; "
        f"sys.exit(pytest.main(['-q', '-p', 'no:cacheprovider', *{list(extra)!r}, "
        f"{str(bundle)!r}]))"
    )
    return subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        check=False,
        cwd=bundle.parent.parent,
    )


@pytest.fixture
def fanout_run(
    project_factory: Callable[..., Project], tmp_path: Path
) -> failslice.ReductionResult:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    return failslice.reduce(p, out_dir=tmp_path / "run")


# ------------------------------------------------------------------ manifest + report + CSV
def test_manifest_and_inventory(fanout_run: failslice.ReductionResult) -> None:
    res = fanout_run
    m = res.manifest
    assert m is not None and m == load_manifest(res.run_dir)
    assert m.status == "reduced" and m.minimality == "1-minimal (verified)"
    assert m.rows_before == 551 and m.rows_after == 4
    assert m.shrink_ratio == round(4 / 551, 6)
    assert m.tables["orders"] == {
        "rows_before": 121,
        "rows_after": 2,
        "columns": [["order_id", "BIGINT"], ["customer_id", "BIGINT"]],
        "exported_columns": [["order_id", "BIGINT"], ["customer_id", "BIGINT"]],
        "primary_key": None,
    }
    assert set(m.versions) == {"failslice", "duckdb", "python"}
    assert m.settings["seed"] == 0 and m.config["foreign_keys"] == list(FANOUT_FKS)
    assert all(st.status == "done" for st in m.steps), m.steps
    assert m.stats is not None and m.stats["evaluations"] == res.stats.evaluations  # type: ignore[union-attr]
    roles = {a.path: a.role for a in m.artifacts}
    assert roles["tables/orders.parquet"] == "table" and roles["csv/orders.csv"] == "csv"
    test_files = [p for p in roles if p.startswith("pytest/test_reproducer_")]
    assert len(test_files) == 1 and roles[test_files[0]] == "pytest"
    assert roles["report.md"] == "report"
    assert "manifest.json" not in roles and "checkpoint.json" not in roles
    for a in m.artifacts:  # hashes and sizes describe the files on disk
        data = (res.run_dir / a.path).read_bytes()
        assert a.bytes == len(data) and a.sha256 == hashlib.sha256(data).hexdigest()
    assert not (res.run_dir / "result.json").exists()


def test_report_wording(fanout_run: failslice.ReductionResult) -> None:
    text = (fanout_run.run_dir / "report.md").read_text()
    assert "**Result: 1-minimal (verified).**" in text
    assert "not a claim that no smaller reproducer exists" in text
    assert "| orders | 121 | 2 |" in text and "| **total** | **551** | **4** |" in text
    assert "Observations on the rows that survived reduction (not a diagnosis)" in text
    assert "pytest pytest/" in text
    lowered = text.lower()
    for banned in ("root cause is", "caused by", "minimum", "minimal reproducer"):
        assert banned not in lowered


def test_report_and_manifest_without_candidate(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE, settings={"max_evals": 2}
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    m = res.manifest
    assert m is not None and m.status == "no-candidate" and m.minimality == "no reproducer"
    assert m.rows_before == 551 and m.rows_after is None and m.shrink_ratio is None
    assert {st.name: st.status for st in m.steps} == {
        "csv": "skipped",
        "pytest": "skipped",
        "explain": "skipped",
        "report": "done",
    }
    text = (res.run_dir / "report.md").read_text()
    assert "**No reproducer.**" in text and "nothing was exported" in text


def test_minimality_label_needs_verified_lossless_replay(
    fanout_run: failslice.ReductionResult,
) -> None:
    import dataclasses

    from failslice.verify import VerificationReport

    res = fanout_run
    assert res.minimality_label == "1-minimal (verified)"
    failed = dataclasses.replace(res, verification=VerificationReport("replay-failed"))
    assert failed.minimality_label == "reduced, minimality not verified"
    assert res.spec is not None
    lossy = dataclasses.replace(res, spec=res.spec.model_copy(update={"warnings": ("lossy",)}))
    assert lossy.minimality_label == "reduced, minimality not verified"


def test_csv_format(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    rows = [(1, "a,b", None), (2, "", 1.5), (3, "\\N", None), (4, None, 2.0)]
    p = project_factory(
        {"t": ([("id", "BIGINT"), ("s", "VARCHAR"), ("x", "DOUBLE")], rows)},
        predicate={"sql": "SELECT count(*) = 4 FROM t"},
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    text = (res.run_dir / "csv" / "t.csv").read_text()
    assert text.splitlines() == [
        'id,"s",x',  # FORCE_QUOTE also quotes the header of text columns
        '1,"a,b",\\N',
        '2,"",1.5',
        '3,"\\N",\\N',
        "4,\\N,2.0",
    ]


def test_finalize_after_deadline_still_writes_report_and_manifest(
    fanout_run: failslice.ReductionResult, tmp_path: Path
) -> None:
    import time

    from failslice.artifacts import RunContext, finalize

    res = fanout_run
    run_dir = tmp_path / "copy"
    shutil.copytree(res.run_dir, run_dir, ignore=shutil.ignore_patterns("pytest", "csv"))
    (run_dir / "manifest.json").unlink()
    ctx = RunContext("rid", run_dir, "2026-01-01T00:00:00+00:00", time.monotonic(), {}, {})
    import dataclasses

    m = finalize(ctx, dataclasses.replace(res, run_dir=run_dir), deadline=time.monotonic() - 1)
    assert {st.name: st.status for st in m.steps} == {
        "csv": "skipped",
        "pytest": "skipped",
        "explain": "skipped",
        "report": "done",
    }
    assert (run_dir / "manifest.json").is_file() and (run_dir / "report.md").is_file()
    assert "## Incomplete steps" in (run_dir / "report.md").read_text()


# ------------------------------------------------------------------ explain
def test_explain_observations(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory(
        {
            "p": ([("k", "BIGINT")], [(1,), (1,), (2,)]),
            "c": ([("id", "BIGINT"), ("fk", "BIGINT")], [(1, 1), (1, 1), (2, None), (3, 9)]),
        },
        fks=["c.fk -> p.k"],
        predicate={"sql": "SELECT count(*) = 4 FROM c"},
        settings={"minimality": False},
    )
    out = tmp_path / "run"
    from failslice.engine import Engine

    e = Engine(p, allowed_directories=[out])
    try:
        e.export(out)
    finally:
        e.close()
    # declare a primary key in the spec to exercise the duplicate check
    spec = json.loads((out / "spec.json").read_text())
    spec["tables"]["c"]["primary_key"] = "id"
    (out / "spec.json").write_text(json.dumps(spec))
    ex = explain(out, limit=1)
    c = next(t for t in ex.tables if t.name == "c")
    assert (c.rows, c.null_counts, c.pk_duplicate_values, c.pk_excess_rows) == (4, {"fk": 1}, 1, 1)
    (fk,) = ex.foreign_keys
    assert (fk.child_rows, fk.null_references, fk.input_orphans) == (4, 1, 1)
    assert fk.duplicate_parent_keys == 1 and fk.join_pairs == 4  # key 1: 2 children x 2 parents
    assert fk.keys[0].key == "1" and fk.keys[0].pairs == 4 and fk.truncated
    text = ex.render_text()
    assert "not a diagnosis" in text and "produces 4 pairs" in text
    assert "cause" not in text.lower()


# ------------------------------------------------------------------ codec
_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=True, allow_infinity=True),
    st.decimals(allow_nan=False, allow_infinity=False),
    st.text(),
    st.binary(),
    st.dates(),
    st.datetimes(),
    st.datetimes(timezones=st.just(dt.UTC)),
    st.times(),
    st.timedeltas(),
    st.uuids(),
)
_values = st.recursive(
    _scalars,
    lambda inner: st.one_of(
        st.lists(inner, max_size=3),
        st.dictionaries(st.one_of(st.text(), st.integers(), st.booleans()), inner, max_size=3),
    ),
    max_leaves=8,
)


@settings(max_examples=200, deadline=None)
@given(rows=st.lists(st.tuples(_values, _values), max_size=4))
def test_codec_round_trip(rows: list[tuple[Any, ...]]) -> None:
    encoded = json.loads(json.dumps(portable.encode_rows(rows)))
    decoded = portable.decode_rows(encoded)
    assert portable.results_equal(decoded, rows, ordered=True, tol=None)
    for a, b in zip(decoded, rows, strict=True):
        for x, y in zip(a, b, strict=True):
            assert type(x) is type(y) or isinstance(y, tuple)


def test_codec_rejects_inexact_values() -> None:
    with pytest.raises(TypeError, match="cannot encode"):
        portable.encode_value(object())
    with pytest.raises(ValueError, match="unknown value tag"):
        portable.decode_value(["pickle", "x"])
    with pytest.raises(ValueError, match="malformed"):
        portable.decode_value({"$t": "int"})
    assert portable.decode_value(portable.encode_value(decimal.Decimal("1.10"))) == decimal.Decimal(
        "1.10"
    )
    assert portable.decode_value(portable.encode_value(uuid.UUID(int=5))) == uuid.UUID(int=5)


# ------------------------------------------------------------------ pytest bundle
def test_bundle_is_self_contained_and_passes(fanout_run: failslice.ReductionResult) -> None:
    bundle = fanout_run.run_dir / "pytest"
    files = sorted(p.relative_to(bundle).as_posix() for p in bundle.rglob("*") if p.is_file())
    (test_name,) = [f for f in files if f.startswith("test_reproducer_")]
    files.remove(test_name)
    assert files == [
        "_failslice_portable.py",
        "fixtures/orphans.parquet",
        "fixtures/tables/customers.parquet",
        "fixtures/tables/lines.parquet",
        "fixtures/tables/orders.parquet",
        "query.sql",
        "reproducer.json",
    ]
    assert (bundle / "_failslice_portable.py").read_bytes() == Path(portable.__file__).read_bytes()
    text = "".join((bundle / f).read_text() for f in (test_name, "reproducer.json"))
    assert str(fanout_run.run_dir) not in text and "/tmp" not in text  # no absolute paths
    assert "import failslice" not in (bundle / test_name).read_text()
    proc = _run_pytest(bundle)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_bundle_moves_and_detects_tampering(
    fanout_run: failslice.ReductionResult, tmp_path: Path
) -> None:
    moved = tmp_path / "elsewhere" / "bundle"
    shutil.copytree(fanout_run.run_dir / "pytest", moved)
    shutil.rmtree(fanout_run.run_dir)  # originals gone: the bundle stands alone
    assert _run_pytest(moved).returncode == 0
    with (moved / "fixtures" / "tables" / "lines.parquet").open("ab") as fh:
        fh.write(b"x")
    proc = _run_pytest(moved)
    assert proc.returncode == 1 and "sha256 mismatch" in proc.stdout


def test_bundle_polarity_and_edited_query(
    fanout_run: failslice.ReductionResult, tmp_path: Path
) -> None:
    passes = build_bundle(fanout_run.run_dir, tmp_path / "passes", expect="passes", name="fixed")
    assert 'EXPECT = "passes"' in passes.test_file.read_text()
    proc = _run_pytest(passes.directory)
    assert proc.returncode == 1 and "predicate is TRUE" in proc.stdout  # bug still present
    # The user fixes the pipeline: join on distinct orders.
    (passes.directory / "query.sql").write_text(
        "SELECT (SELECT sum(l.amount) FROM lines l JOIN (SELECT DISTINCT * FROM orders) o "
        "USING (order_id)) <> (SELECT sum(amount) FROM lines)\n"
    )
    assert _run_pytest(passes.directory).returncode == 0
    # An unrelated error fails under either polarity.
    (passes.directory / "query.sql").write_text("SELECT no_such_column FROM lines\n")
    proc = _run_pytest(passes.directory)
    assert proc.returncode == 1 and "predicate is ERROR" in proc.stdout


def test_two_bundles_in_one_session(fanout_run: failslice.ReductionResult, tmp_path: Path) -> None:
    a = build_bundle(fanout_run.run_dir, tmp_path / "suite" / "a", name="first")
    build_bundle(fanout_run.run_dir, tmp_path / "suite" / "b", expect="passes", name="second")
    proc = _run_pytest(a.directory.parent)
    # each bundle uses its own companion module: one passes, the other fails as expected
    assert "1 failed, 1 passed" in proc.stdout, proc.stdout


def test_bundle_refusals(fanout_run: failslice.ReductionResult, tmp_path: Path) -> None:
    with pytest.raises(ExportError, match="already exists"):
        build_bundle(fanout_run.run_dir, fanout_run.run_dir / "pytest")
    with pytest.raises(ExportError, match="must match"):
        build_bundle(fanout_run.run_dir, tmp_path / "x", name="Bad-Name")
    with pytest.raises(ExportError, match="'fails' or 'passes'"):
        build_bundle(fanout_run.run_dir, tmp_path / "x", expect="maybe")  # type: ignore[arg-type]
    with pytest.raises(ConfigError, match="glob character"):
        build_bundle(fanout_run.run_dir, tmp_path / "x[1]")
    assert not (tmp_path / "x").exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".")]  # no staging left


@pytest.mark.parametrize(
    ("predicate", "rows"),
    [
        (
            {"expect": {"query": "SELECT sum(x) FROM t", "expected": 6}},
            [(1,), (2,), (3,), (4,)],
        ),
        (
            {"expect": {"query": "SELECT x, s FROM t", "expected": [[1, "a"], [2, "z"]]}},
            [(1,), (2,)],
        ),
        (
            {
                "expect": {
                    "query": "SELECT x / 3.0 FROM t ORDER BY x",
                    "expected": [[0.3333], [0.6666]],
                    "float_tolerance": 0.001,
                    "ordered": True,
                    "mode": "equals",
                }
            },
            [(1,), (2,)],
        ),
        (
            {"raises": {"query": "SELECT CAST(s AS INTEGER) FROM t", "match": "Could not convert"}},
            [(1,), (2,)],
        ),
    ],
)
def test_bundle_forms(
    project_factory: Callable[..., Project],
    tmp_path: Path,
    predicate: dict[str, Any],
    rows: list[tuple[int]],
) -> None:
    data = [(x, "abcd"[x - 1]) for (x,) in rows]
    p = project_factory(
        {"t": ([("x", "BIGINT"), ("s", "VARCHAR")], data)},
        predicate=predicate,
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok, res.verification
    proc = _run_pytest(res.run_dir / "pytest")
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_bundle_with_expectation_file_and_enum(tmp_path: Path) -> None:
    db = tmp_path / "data.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TYPE mood AS ENUM ('b', 'a')")
    con.execute(
        "CREATE TABLE t AS SELECT i::INTEGER AS id, "
        "(CASE WHEN i % 2 = 0 THEN 'a' ELSE 'b' END)::mood AS m FROM range(6) r(i)"
    )
    con.execute("COPY (SELECT 0 AS code) TO '" + str(tmp_path / "expected.parquet") + "'")
    con.close()
    (tmp_path / "failslice.yaml").write_text(
        "tables:\n  t: {path: data.duckdb}\npredicate:\n  expect:\n"
        "    query: SELECT min(enum_code(m)) FROM t\n"
        "    expected_file: expected.parquet\n    mode: equals\n"
    )
    res = failslice.reduce(
        failslice.load_project(tmp_path / "failslice.yaml"), out_dir=tmp_path / "r"
    )
    assert res.verification is not None and res.verification.ok
    proc = _run_pytest(tmp_path / "r" / "pytest")
    assert proc.returncode == 0, proc.stdout + proc.stderr


# ------------------------------------------------------------------ supervised check
def test_check_isolated(project_factory: Callable[..., Project]) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    report = failslice.check(p, isolate=True)
    assert report.ok and report.deterministic and len(report.outcomes) == 4
    assert report.orphans == dict.fromkeys(FANOUT_FKS, 0)


def test_check_isolated_errors(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    report = failslice.check(p, isolate=True)
    assert not report.ok and "nothing to reduce" in report.problems[0]
    (tmp_path / "missing.yaml").write_text(
        "tables:\n  t: {path: nope.csv}\npredicate:\n  sql: SELECT true\n"
    )
    with pytest.raises(ConfigError, match="file not found"):
        failslice.check(failslice.load_project(tmp_path / "missing.yaml"), isolate=True)
