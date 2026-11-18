"""DuckDB engine: loading, row identity, configurations, cascade repair, cache, export.

Layout inside one in-memory DuckDB database (``docs/design.md`` §4, §8):

- ``__fs_base.<t>``: the loaded rows plus ``__fs_rid`` (0..n-1 in file read order);
- ``__fs_meta.orphans(edge, rid)``: input-orphan exemptions per FK edge;
- ``__fs_state.cur_<t>(rid, pos)``: the current configuration; ``pos`` is the seeded
  order the reducer partitions;
- ``__fs_view.<t>``: what the predicate sees during reduction (original columns of the
  kept rows); ``__fs_plain.<t>``: all original rows (determinism baseline).

A candidate deletes position ranges from one table; :meth:`Engine.attempt` builds the
greatest intact subset (cascade, parents first) inside a transaction, evaluates the
predicate and commits only on TRUE.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import random
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import duckdb
import yaml

from failslice import __version__
from failslice.config import (
    RESERVED_PREFIX,
    ConfigError,
    ExpectPredicate,
    ForeignKey,
    Project,
    literal_path,
    resolve_inside,
)
from failslice.predicate import Outcome, OutcomeKind, PredicateRunner, Row
from failslice.runspec import InputFile, ReplaySettings, RunSpec, TableEntry, restoring_select

__all__ = ["AttemptResult", "Candidate", "Engine", "TableInfo", "q"]

FINGERPRINT_VERSION = 1


def q(name: str) -> str:
    """Quote a SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def file_sha256(path: Path) -> str:
    """SHA-256 of a file's bytes."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _require_unchanged(path: Path, sha_before: str) -> None:
    """The file must have the same bytes after DuckDB read it as before, so the
    recorded hash describes exactly the loaded rows."""
    if file_sha256(path) != sha_before:
        raise ConfigError(f"input file {path} changed while it was being loaded")


@dataclass(frozen=True)
class TableInfo:
    """A loaded table: resolved schema, input row count and source identity."""

    name: str
    columns: tuple[tuple[str, str], ...]
    row_count: int
    source: str
    source_sha256: str


@dataclass(frozen=True)
class Candidate:
    """Delete the rows at the given (inclusive) position ranges of ``table``."""

    table: str
    exclude: tuple[tuple[int, int], ...]

    def deleted_count(self) -> int:
        """Number of rows of ``table`` the candidate deletes directly."""
        return sum(hi - lo + 1 for lo, hi in self.exclude)


@dataclass(frozen=True)
class AttemptResult:
    """Outcome of one candidate. ``accepted`` is true iff the outcome was TRUE."""

    outcome: Outcome
    accepted: bool
    cached: bool
    sizes: Mapping[str, int] = field(default_factory=dict)
    """Table sizes after the attempt (the new configuration if accepted)."""


Fingerprint = tuple[int, int, int]


class EngineBrokenError(RuntimeError):
    """The DuckDB connection is unusable after a rollback; the run must stop (an
    isolated run then recovers its last checkpoint)."""


class Engine:
    """Holds the loaded data and the current configuration for one run."""

    def __init__(
        self,
        project: Project,
        *,
        allowed_directories: Sequence[Path] = (),
        expected_rows: Sequence[Row] | None = None,
    ) -> None:
        self.project = project
        # No spill files: DuckDB would otherwise write to ".tmp" in the working directory,
        # outside the allowed root (data is held in memory; see "Limits").
        self.con = duckdb.connect(":memory:", config={"temp_directory": ""})
        self.tables: dict[str, TableInfo] = {}
        self.orphan_counts: dict[str, int] = {}
        self._sizes: dict[str, int] = {}
        self._fingerprints: dict[str, Fingerprint] = {}
        self._cache: dict[str, Outcome] = {}
        self.cache_hits = 0
        self.expected_file: Path | None = None
        self.expected_sha256: str | None = None
        try:
            self._load()
            self.expected_rows = self._load_expected(expected_rows)
            self._compute_orphans()
            self._create_views()
            self._lock_down(allowed_directories)
        except BaseException:
            self.con.close()
            raise
        self.runner = PredicateRunner(project.predicate, self.expected_rows)
        self._cache_namespace = self._compute_cache_namespace()
        self.reset_state()

    # ------------------------------------------------------------------ loading
    def _load(self) -> None:
        con = self.con
        con.execute("SET enable_progress_bar = false")
        for schema in ("__fs_base", "__fs_meta", "__fs_state", "__fs_view", "__fs_plain"):
            con.execute(f"CREATE SCHEMA {schema}")
        con.execute("SET preserve_insertion_order = true")
        for name in self.project.topo_order:
            spec = self.project.tables[name]
            path = self.project.table_path(name)
            if not path.is_file():
                raise ConfigError(f"table {name!r}: file not found: {path}")
            fmt = spec.resolved_format()
            sha_before = file_sha256(path)
            stage = f"__fs_state.{q('stage_' + name)}"
            if fmt == "csv":
                con.execute(f"CREATE TABLE {stage} AS SELECT * FROM read_csv({_lit(str(path))})")
            elif fmt == "parquet":
                con.execute(
                    f"CREATE TABLE {stage} AS SELECT * FROM read_parquet({_lit(str(path))})"
                )
            else:
                self._load_duckdb_table(name, spec.source_table or name, path, stage)
            cols = con.execute(f"DESCRIBE {stage}").fetchall()
            columns = tuple((str(c[0]), str(c[1])) for c in cols)
            for col, _ in columns:
                if col.lower().startswith(RESERVED_PREFIX):
                    raise ConfigError(
                        f"table {name!r}: column {col!r} uses the reserved prefix "
                        f"{RESERVED_PREFIX!r}"
                    )
            # A user column named "rowid" shadows DuckDB's row-id pseudo column, so it is
            # renamed while the row ids are taken, then renamed back.
            user_rowid = [c for c, _ in columns if c.lower() == "rowid"]
            if user_rowid:
                tmp = f"__fs_state.{q('stage2_' + name)}"
                con.execute(
                    f"CREATE TABLE {tmp} AS SELECT * RENAME ({q(user_rowid[0])} AS "
                    f"__fs_user_rowid) FROM {stage}"
                )
                con.execute(f"DROP TABLE {stage}")
                stage = tmp
                star = f"* RENAME (__fs_user_rowid AS {q(user_rowid[0])})"
            else:
                star = "*"
            con.execute(
                f"CREATE TABLE __fs_base.{q(name)} AS "
                f"SELECT rowid::BIGINT AS __fs_rid, {star} FROM {stage} ORDER BY __fs_rid"
            )
            con.execute(f"DROP TABLE {stage}")
            (count,) = self._one(f"SELECT count(*) FROM __fs_base.{q(name)}")
            _require_unchanged(path, sha_before)
            self.tables[name] = TableInfo(name, columns, int(count), str(path), sha_before)
        for fk in self.project.foreign_keys:
            for table, col in ((fk.child, fk.child_column), (fk.parent, fk.parent_column)):
                if col not in {c for c, _ in self.tables[table].columns}:
                    raise ConfigError(f"foreign key {fk}: table {table!r} has no column {col!r}")

    def _load_duckdb_table(self, name: str, source: str, path: Path, stage: str) -> None:
        con = self.con
        alias = q("__fs_src_" + name)
        con.execute(f"ATTACH {_lit(str(path))} AS {alias} (READ_ONLY)")
        try:
            views = self._one(
                "SELECT count(*) FROM duckdb_views() WHERE database_name = ? AND view_name = ?",
                ["__fs_src_" + name, source],
            )[0]
            if views:
                raise ConfigError(
                    f"table {name!r}: {source!r} in {path.name} is a view; only physical "
                    "tables are accepted (a view could read files outside the allowed root)"
                )
            found = self._one(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name = ? AND table_name = ?",
                ["__fs_src_" + name, source],
            )[0]
            if not found:
                raise ConfigError(f"table {name!r}: no table {source!r} in {path.name}")
            con.execute(f"CREATE TABLE {stage} AS SELECT * FROM {alias}.{q(source)}")
        finally:
            con.execute(f"DETACH {alias}")

    def _load_expected(self, given: Sequence[Row] | None) -> list[Row] | None:
        spec = self.project.predicate
        if not isinstance(spec, ExpectPredicate):
            return None
        if given is not None:
            return list(given)
        if spec.expected_file is None:
            return None
        path = resolve_inside(self.project.base_dir, spec.expected_file, self.project.root)
        if not path.is_file():  # also refuses FIFOs and devices, which could block reading
            raise ConfigError(f"predicate.expected_file is not a regular file: {path}")
        self.expected_file = path
        self.expected_sha256 = file_sha256(path)
        reader = "read_parquet" if path.suffix.lower() in {".parquet", ".pq"} else "read_csv"
        # Snapshot once: this table is what is evaluated and exported (never re-read).
        self.con.execute(
            f"CREATE TABLE __fs_meta.expected AS SELECT * FROM {reader}({_lit(str(path))})"
        )
        _require_unchanged(path, self.expected_sha256)
        return [tuple(r) for r in self.con.execute("SELECT * FROM __fs_meta.expected").fetchall()]

    def _compute_orphans(self) -> None:
        con = self.con
        con.execute("CREATE TABLE __fs_meta.orphans(edge INTEGER, rid BIGINT)")
        for i, fk in enumerate(self.project.foreign_keys):
            try:
                con.execute(
                    f"INSERT INTO __fs_meta.orphans SELECT {i}, c.__fs_rid "
                    f"FROM __fs_base.{q(fk.child)} c WHERE c.{q(fk.child_column)} IS NOT NULL "
                    f"AND NOT EXISTS (SELECT 1 FROM __fs_base.{q(fk.parent)} p "
                    f"WHERE p.{q(fk.parent_column)} = c.{q(fk.child_column)})"
                )
            except duckdb.Error as exc:
                raise ConfigError(f"foreign key {fk}: columns cannot be compared: {exc}") from exc
            (n,) = self._one("SELECT count(*) FROM __fs_meta.orphans WHERE edge = ?", [i])
            self.orphan_counts[str(fk)] = int(n)

    def _create_views(self) -> None:
        for name, info in self.tables.items():
            cols = ", ".join(q(c) for c, _ in info.columns)
            self.con.execute(
                f"CREATE VIEW __fs_plain.{q(name)} AS SELECT {cols} FROM __fs_base.{q(name)}"
            )

    def _lock_down(self, allowed_directories: Sequence[Path]) -> None:
        con = self.con
        if allowed_directories:
            dirs = ", ".join(_lit(str(Path(d).resolve()) + "/") for d in allowed_directories)
            con.execute(f"SET allowed_directories = [{dirs}]")
        con.execute("SET enable_external_access = false")
        con.execute("SET enable_progress_bar = false")
        con.execute(f"SET threads = {int(self.project.settings.threads)}")

    # ------------------------------------------------------------------ state
    def _one(self, sql: str, params: Sequence[Any] | None = None) -> tuple[Any, ...]:
        row = self.con.execute(sql, params).fetchone()
        assert row is not None
        return tuple(row)

    def _view_sql(self, table: str, source: str) -> str:
        cols = ", ".join(f"b.{q(c)}" for c, _ in self.tables[table].columns)
        return (
            f"CREATE OR REPLACE VIEW __fs_view.{q(table)} AS SELECT {cols} "
            f"FROM __fs_base.{q(table)} b SEMI JOIN {source} k ON b.__fs_rid = k.rid"
        )

    @staticmethod
    def _cur(table: str) -> str:
        return f"__fs_state.{q('cur_' + table)}"

    @staticmethod
    def _cand(table: str) -> str:
        return f"__fs_state.{q('cand_' + table)}"

    def reset_state(self, kept: Mapping[str, Iterable[int]] | None = None) -> None:
        """(Re)initialise the configuration: all rows, or the given kept row ids.

        Positions follow a permutation of the row ids seeded by ``settings.seed``
        (Python's ``random.Random``, stable across platforms and versions).
        """
        rng = random.Random(self.project.settings.seed)
        for name, info in self.tables.items():
            if kept is None:
                rids = list(range(info.row_count))
            else:
                wanted = set(kept.get(name, ()))
                if any(r < 0 or r >= info.row_count for r in wanted):
                    raise ValueError(f"row id out of range for table {name!r}")
                rids = [r for r in range(info.row_count) if r in wanted]
            rng_t = random.Random(f"{rng.random()}:{name}")
            rng_t.shuffle(rids)
            self.con.execute(
                f"CREATE OR REPLACE TABLE {self._cur(name)} AS "
                "SELECT unnest($1::BIGINT[]) AS rid, unnest(range($2::BIGINT)) AS pos",
                [rids, len(rids)],
            )
            self.con.execute(self._view_sql(name, self._cur(name)))
            self._sizes[name] = len(rids)
            self._fingerprints[name] = self._fingerprint(self._cur(name))
        if kept is not None:
            self._check_intact_current()

    def _check_intact_current(self) -> None:
        for i, fk in enumerate(self.project.foreign_keys):
            dangling = self._dangling(fk, i, self._cur(fk.child), self._cur(fk.parent))
            if dangling:
                raise ValueError(f"configuration is not intact on {fk}: {dangling} dangling rows")

    def _dangling(self, fk: ForeignKey, edge: int, child_src: str, parent_src: str) -> int:
        (n,) = self._one(
            f"SELECT count(*) FROM __fs_base.{q(fk.child)} c SEMI JOIN {child_src} k "
            f"ON c.__fs_rid = k.rid WHERE NOT {self._supported(fk, edge, parent_src)}"
        )
        return int(n)

    def _supported(self, fk: ForeignKey, edge: int, parent_src: str, alias: str = "c") -> str:
        col = f"{alias}.{q(fk.child_column)}"
        return (
            f"({col} IS NULL OR EXISTS (SELECT 1 FROM __fs_base.{q(fk.parent)} p "
            f"SEMI JOIN {parent_src} s ON p.__fs_rid = s.rid "
            f"WHERE p.{q(fk.parent_column)} = {col}) "
            f"OR {alias}.__fs_rid IN (SELECT rid FROM __fs_meta.orphans WHERE edge = {edge}))"
        )

    def _fingerprint(self, source: str) -> Fingerprint:
        n, s, x = self._one(
            f"SELECT count(*), coalesce(sum(hash(rid)::HUGEINT), 0), "
            f"coalesce(bit_xor(hash(rid)), 0) FROM {source}"
        )
        return int(n), int(s), int(x)

    def input_identity(self) -> dict[str, str]:
        """Hashes of every input file (tables and expectation file), for checkpoints."""
        ident = {f"table:{n}": t.source_sha256 for n, t in self.tables.items()}
        if self.expected_sha256 is not None:
            ident["expected"] = self.expected_sha256
        return ident

    @property
    def sizes(self) -> dict[str, int]:
        """Current kept-row count per table."""
        return dict(self._sizes)

    @property
    def total(self) -> int:
        """Current total number of kept rows."""
        return sum(self._sizes.values())

    def kept_rids(self) -> dict[str, list[int]]:
        """The current configuration as sorted original row ids per table."""
        return {
            name: [
                int(r[0])
                for r in self.con.execute(
                    f"SELECT rid FROM {self._cur(name)} ORDER BY rid"
                ).fetchall()
            ]
            for name in self.tables
        }

    # ------------------------------------------------------------------ evaluation
    def _set_search_path(self, schema: str) -> None:
        self.con.execute(f"SET search_path = {_lit(schema)}")

    def evaluate_current(self, path: Literal["view", "plain"] = "view") -> Outcome:
        """Evaluate the predicate on the current configuration (``view``) or on all
        original rows through plain views (``plain``). Never cached."""
        self._set_search_path("__fs_view" if path == "view" else "__fs_plain")
        try:
            return self.runner.run(self.con, self.project.settings.eval_timeout_s)
        finally:
            self._reset_connection_state()

    def _reset_connection_state(self) -> None:
        with contextlib.suppress(duckdb.Error):  # aborted transaction; caller rolls back
            self.con.execute("SET search_path = 'main'")

    def _compute_cache_namespace(self) -> str:
        """Hash of everything except the configuration that determines an outcome
        (fixed for the engine's lifetime, so computed once)."""
        payload = {
            "fp_version": FINGERPRINT_VERSION,
            "fs": __version__,
            "duckdb": duckdb.__version__,
            "inputs": {n: t.source_sha256 for n, t in self.tables.items()},
            "schema": {n: t.columns for n, t in self.tables.items()},
            "fks": [str(f) for f in self.project.foreign_keys],
            "predicate": self.project.predicate.model_dump(mode="json"),
            "expected": repr(self.expected_rows),
            "threads": self.project.settings.threads,
            "seed": self.project.settings.seed,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()

    def _cache_key(self, fps: Mapping[str, Fingerprint]) -> str:
        config = json.dumps({n: list(fps[n]) for n in sorted(fps)})
        return hashlib.sha256(f"{self._cache_namespace}:{config}".encode()).hexdigest()

    def attempt(
        self,
        candidate: Candidate,
        *,
        use_cache: bool = True,
        before_eval: Callable[[], None] | None = None,
    ) -> AttemptResult:
        """Evaluate ``G(K \\ D)`` for the candidate's deletion set ``D`` and commit it
        iff the predicate is TRUE. Non-TRUE outcomes are cached (search hints only)."""
        target = candidate.table
        if target not in self.tables:
            raise KeyError(target)
        con = self.con
        con.execute("BEGIN TRANSACTION")
        committed = False
        try:
            changed = self._build_candidate(candidate)
            fps = {
                name: (
                    self._fingerprint(self._cand(name))
                    if name in changed
                    else self._fingerprints[name]
                )
                for name in self.tables
            }
            sizes = {n: fps[n][0] for n in self.tables}
            key = self._cache_key(fps)
            if use_cache and key in self._cache:
                self.cache_hits += 1
                return AttemptResult(self._cache[key], False, True, self.sizes)
            for name in changed:
                con.execute(self._view_sql(name, self._cand(name)))
            self._set_search_path("__fs_view")
            if before_eval is not None:
                before_eval()
            outcome = self.runner.run(con, self.project.settings.eval_timeout_s)
            if not outcome.is_true:
                if outcome.kind is not OutcomeKind.TIMEOUT:
                    self._cache[key] = outcome
                return AttemptResult(outcome, False, False, self.sizes)
            try:
                con.execute("SET search_path = 'main'")
            except duckdb.TransactionException:
                # A TRUE outcome from an error (``raises`` form) aborts the transaction.
                # Rebuild the same candidate (deterministic) in a fresh transaction.
                con.execute("ROLLBACK")
                con.execute("BEGIN TRANSACTION")
                rebuilt = self._build_candidate(candidate)
                if rebuilt != changed:  # pragma: no cover - defensive
                    raise RuntimeError("candidate rebuild differs from the evaluated one") from None
            self._commit_candidate(changed)
            committed = True
            for name in changed:
                self._fingerprints[name] = fps[name]
                self._sizes[name] = sizes[name]
            return AttemptResult(outcome, True, False, self.sizes)
        finally:
            if not committed:
                self._rollback()

    def _commit_candidate(self, changed: Sequence[str]) -> None:
        con = self.con
        for name in changed:
            con.execute(
                f"CREATE OR REPLACE TABLE {self._cur(name)} AS SELECT rid, "
                f"(row_number() OVER (ORDER BY pos) - 1)::BIGINT AS pos FROM {self._cand(name)}"
            )
            con.execute(f"DROP TABLE {self._cand(name)}")
            con.execute(self._view_sql(name, self._cur(name)))
        con.execute("COMMIT")

    def _rollback(self) -> None:
        with contextlib.suppress(duckdb.Error):
            self.con.execute("ROLLBACK")
        self._reset_connection_state()
        try:
            self.con.execute("SELECT 1").fetchall()
        except duckdb.Error as exc:  # pragma: no cover - DuckDB left in a broken state
            raise EngineBrokenError(f"connection unusable after rollback: {exc}") from exc

    def _build_candidate(self, cand: Candidate) -> list[str]:
        """Create ``cand_<t>`` tables for every table whose kept set changes."""
        con = self.con
        changed: list[str] = []
        excl = " OR ".join(f"pos BETWEEN {int(lo)} AND {int(hi)}" for lo, hi in cand.exclude)
        affected = {cand.table} | self.project.descendants(cand.table)
        for name in self.project.topo_order:
            if name not in affected:
                continue
            if name == cand.table:
                where = f"NOT ({excl})" if excl else "true"
                con.execute(
                    f"CREATE TABLE {self._cand(name)} AS SELECT rid, pos FROM {self._cur(name)} "
                    f"WHERE {where}"
                )
                changed.append(name)
                continue
            edges = [
                (i, fk)
                for i, fk in enumerate(self.project.foreign_keys)
                if fk.child == name and fk.parent in changed
            ]
            if not edges:
                continue
            conds = " AND ".join(self._supported(fk, i, self._cand(fk.parent)) for i, fk in edges)
            con.execute(
                f"CREATE TABLE {self._cand(name)} AS SELECT k.rid, k.pos FROM {self._cur(name)} k "
                f"JOIN __fs_base.{q(name)} c ON c.__fs_rid = k.rid WHERE {conds}"
            )
            (n,) = self._one(f"SELECT count(*) FROM {self._cand(name)}")
            if int(n) == self._sizes[name]:
                con.execute(f"DROP TABLE {self._cand(name)}")
            else:
                changed.append(name)
        return changed

    # ------------------------------------------------------------------ export
    def export(self, out_dir: Path) -> RunSpec:
        """Write the current configuration: ``tables/<t>.parquet``, ``rowmap.parquet``,
        ``orphans.parquet``, the expectation snapshot and ``spec.json`` (a
        :class:`~failslice.runspec.RunSpec`; everything replay needs).

        ``out_dir`` must be inside one of the engine's allowed directories. Each table
        file is read back and compared with the kept rows (after restoring source types);
        a difference is recorded in ``RunSpec.warnings``.
        """
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "tables").mkdir(exist_ok=True)
        con = self.con
        entries: dict[str, TableEntry] = {}
        warnings: list[str] = []
        for name, info in self.tables.items():
            cols = ", ".join(f"b.{q(c)}" for c, _ in info.columns)
            kept = (
                f"SELECT {cols} FROM __fs_base.{q(name)} b SEMI JOIN {self._cur(name)} k "
                f"ON b.__fs_rid = k.rid"
            )
            rel = f"tables/{name}.parquet"
            target = literal_path(out_dir / rel)
            con.execute(
                f"COPY ({kept} ORDER BY b.__fs_rid) TO {_lit(str(target))} (FORMAT parquet)"
            )
            reader = f"read_parquet({_lit(str(target))})"
            exported = tuple(
                (str(r[0]), str(r[1]))
                for r in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()
            )
            restored = restoring_select(reader, exported, info.columns)
            try:
                (diff,) = self._one(
                    f"SELECT (SELECT count(*) FROM (({kept}) EXCEPT ALL ({restored}))) + "
                    f"(SELECT count(*) FROM (({restored}) EXCEPT ALL ({kept})))"
                )
            except duckdb.Error as exc:  # e.g. a value that cannot be cast back
                warnings.append(
                    f"table {name!r}: the Parquet round trip cannot restore the source "
                    f"types {list(info.columns)} from {list(exported)}: {exc}"
                )
                diff = 0
            if diff:
                warnings.append(
                    f"table {name!r}: {diff} rows differ after the Parquet round trip "
                    f"(types {list(info.columns)} exported as {list(exported)})"
                )
            entries[name] = TableEntry(
                file=rel,
                rows=self._sizes[name],
                columns=exported,
                source_columns=info.columns,
                primary_key=self.project.tables[name].primary_key,
            )
        rowmap_parts = [
            f"SELECT {_lit(name)} AS table_name, "
            f"(row_number() OVER (ORDER BY rid) - 1)::BIGINT AS row_index, rid AS source_rid "
            f"FROM {self._cur(name)}"
            for name in self.tables
        ]
        con.execute(
            f"COPY ({' UNION ALL '.join(rowmap_parts)} ORDER BY table_name, row_index) "
            f"TO {_lit(str(literal_path(out_dir / 'rowmap.parquet')))} (FORMAT parquet)"
        )
        orphan_parts = []
        for i, fk in enumerate(self.project.foreign_keys):
            orphan_parts.append(
                f"SELECT {i} AS edge, {_lit(str(fk))} AS fk, {_lit(fk.child)} AS table_name, "
                f"m.row_index, m.rid AS source_rid FROM (SELECT rid, "
                f"(row_number() OVER (ORDER BY rid) - 1)::BIGINT AS row_index "
                f"FROM {self._cur(fk.child)}) m "
                f"SEMI JOIN (SELECT rid FROM __fs_meta.orphans WHERE edge = {i}) o ON o.rid = m.rid"
            )
        orphan_sql = (
            " UNION ALL ".join(orphan_parts)
            if orphan_parts
            else "SELECT 0 AS edge, '' AS fk, '' AS table_name, 0::BIGINT AS row_index, "
            "0::BIGINT AS source_rid WHERE false"
        )
        con.execute(
            f"COPY ({orphan_sql}) TO {_lit(str(literal_path(out_dir / 'orphans.parquet')))} "
            "(FORMAT parquet)"
        )
        expected_file = None
        if self.expected_file is not None:
            (out_dir / "expected").mkdir(exist_ok=True)
            dest = literal_path(out_dir / "expected" / "expected.parquet")
            con.execute(
                f"COPY (SELECT * FROM __fs_meta.expected) TO {_lit(str(dest))} (FORMAT parquet)"
            )
            expected_file = "expected/expected.parquet"
        predicate = self.project.predicate
        inputs = {
            f"table:{n}": InputFile(path=t.source, sha256=t.source_sha256, rows=t.row_count)
            for n, t in self.tables.items()
        }
        if self.expected_file is not None and self.expected_sha256 is not None:
            inputs["expected"] = InputFile(
                path=str(self.expected_file), sha256=self.expected_sha256
            )
        spec = RunSpec(
            failslice_version=__version__,
            duckdb_version=duckdb.__version__,
            tables=entries,
            foreign_keys=tuple(str(fk) for fk in self.project.foreign_keys),
            predicate=predicate,
            expected_file=expected_file,
            expected_yaml=(
                yaml.safe_dump(predicate.expected)
                if isinstance(predicate, ExpectPredicate) and predicate.inline
                else None
            ),
            inputs=inputs,
            settings=ReplaySettings(
                threads=self.project.settings.threads,
                eval_timeout_s=self.project.settings.eval_timeout_s,
            ),
            warnings=tuple(warnings),
            run_settings=self.project.settings.model_dump(mode="json"),
            input_orphans=dict(self.orphan_counts),
        )
        spec.write(out_dir)
        return spec

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.con.close()
