"""Core business logic of the source snapshot service."""
from __future__ import annotations

import hashlib
import json
import threading
import zlib

from . import parsers
from .diff import diff_fields
from .errors import (
    JobInterrupted,
    NotFoundError,
    SnapshotCorruptError,
    ValidationError,
)
from .storage import SNAPSHOT_COLS, Storage, rowdict, utcnow

RETENTION_STRATEGIES = ("oldest_first", "largest_first")
REF_TYPES = ("audit", "replay")
RESUMABLE_JOB_STATES = ("pending", "running", "interrupted", "failed")


class SnapshotService:
    def __init__(self, db_path: str = "snapshots.db"):
        self.db = Storage(db_path)
        self._seq_lock = threading.Lock()

    # ------------------------------------------------------------------
    # sources
    # ------------------------------------------------------------------
    def create_source(
        self,
        name: str,
        parser_version: str = parsers.DEFAULT_VERSION,
        retention_max: int = 100,
        retention_strategy: str = "oldest_first",
        protect_corrupt: bool = False,
    ) -> dict:
        if not name or not name.strip():
            raise ValidationError("source name is required")
        parsers.ensure_version(parser_version)
        if retention_strategy not in RETENTION_STRATEGIES:
            raise ValidationError(f"unknown retention strategy: {retention_strategy}")
        if retention_max < 1:
            raise ValidationError("retention_max must be >= 1")
        now = utcnow()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "INSERT INTO sources(name, parser_version, retention_max, retention_strategy,"
                " protect_corrupt, created_at) VALUES (?,?,?,?,?,?)",
                (name.strip(), parser_version, retention_max, retention_strategy,
                 int(protect_corrupt), now),
            )
            source_id = cur.lastrowid
            conn.execute(
                "INSERT INTO parser_history(source_id, kind, from_version, to_version, reason,"
                " created_at) VALUES (?,?,?,?,?,?)",
                (source_id, "init", None, parser_version, "source created", now),
            )
        return self.get_source(source_id)

    def get_source(self, source_id: int) -> dict:
        row = self.db.query_one("SELECT * FROM sources WHERE id=?", (source_id,))
        if not row:
            raise NotFoundError(f"source {source_id} not found")
        data = rowdict(row)
        data["protect_corrupt"] = bool(data["protect_corrupt"])
        return data

    def list_sources(self) -> list[dict]:
        rows = self.db.query(
            "SELECT s.*, (SELECT COUNT(*) FROM snapshots sp WHERE sp.source_id = s.id)"
            " AS snapshot_count FROM sources s ORDER BY s.id"
        )
        out = []
        for row in rows:
            data = rowdict(row)
            data["protect_corrupt"] = bool(data["protect_corrupt"])
            out.append(data)
        return out

    # ------------------------------------------------------------------
    # ingest: hash + compress + parse
    # ------------------------------------------------------------------
    def ingest(self, source_id: int, payload: str | bytes) -> dict:
        src = self.get_source(source_id)
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        digest = hashlib.sha256(raw).hexdigest()
        blob = zlib.compress(raw, 9)

        status, result_json = "ok", None
        try:
            result = parsers.parse(src["parser_version"], raw.decode("utf-8"))
            result_json = json.dumps(result, ensure_ascii=False, sort_keys=True)
        except (UnicodeDecodeError, parsers.ParseError):
            status = "parse_error"

        now = utcnow()
        with self._seq_lock, self.db.transaction() as conn:
            seq = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM snapshots WHERE source_id=?",
                (source_id,),
            ).fetchone()["seq"]
            cur = conn.execute(
                "INSERT INTO snapshots(source_id, seq, content_hash, codec, payload, raw_size,"
                " compressed_size, parser_version, parse_result, status, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (source_id, seq, digest, "zlib", blob, len(raw), len(blob),
                 src["parser_version"], result_json, status, now),
            )
            snapshot_id = cur.lastrowid
        return self.get_snapshot(snapshot_id)

    # ------------------------------------------------------------------
    # snapshot reads
    # ------------------------------------------------------------------
    def _snapshot_dict(self, row) -> dict:
        data = rowdict(row)
        data["parse_result"] = json.loads(data["parse_result"]) if data["parse_result"] else None
        data["content_hash_short"] = data["content_hash"][:12]
        return data

    def get_snapshot(self, snapshot_id: int) -> dict:
        row = self.db.query_one(
            f"SELECT {SNAPSHOT_COLS}, (SELECT COUNT(*) FROM snapshot_refs r"
            " WHERE r.snapshot_id = snapshots.id AND r.active = 1) AS active_refs"
            " FROM snapshots WHERE id=?",
            (snapshot_id,),
        )
        if not row:
            raise NotFoundError(f"snapshot {snapshot_id} not found")
        return self._snapshot_dict(row)

    def list_snapshots(self, source_id: int) -> list[dict]:
        self.get_source(source_id)
        rows = self.db.query(
            f"SELECT {SNAPSHOT_COLS}, (SELECT COUNT(*) FROM snapshot_refs r"
            " WHERE r.snapshot_id = snapshots.id AND r.active = 1) AS active_refs"
            " FROM snapshots WHERE source_id=? ORDER BY seq DESC",
            (source_id,),
        )
        return [self._snapshot_dict(r) for r in rows]

    def read_payload(self, snapshot_id: int) -> str:
        """Decompress the stored payload, verifying integrity (坏快照检测)."""
        row = self.db.query_one(
            "SELECT id, content_hash, payload FROM snapshots WHERE id=?", (snapshot_id,)
        )
        if not row:
            raise NotFoundError(f"snapshot {snapshot_id} not found")
        try:
            raw = zlib.decompress(row["payload"])
        except zlib.error as exc:
            self._mark_corrupt(snapshot_id)
            raise SnapshotCorruptError(
                f"snapshot {snapshot_id}: payload decompress failed"
            ) from exc
        if hashlib.sha256(raw).hexdigest() != row["content_hash"]:
            self._mark_corrupt(snapshot_id)
            raise SnapshotCorruptError(f"snapshot {snapshot_id}: content hash mismatch")
        return raw.decode("utf-8", errors="replace")

    def _mark_corrupt(self, snapshot_id: int) -> None:
        self.db.execute("UPDATE snapshots SET status='corrupt' WHERE id=?", (snapshot_id,))

    # ------------------------------------------------------------------
    # re-parse + diff
    # ------------------------------------------------------------------
    def reparse(self, snapshot_id: int, parser_version: str | None = None,
                job_id: int | None = None) -> dict:
        """Re-run the parser against a snapshot's stored payload and diff the
        result against the parse result saved at ingest time."""
        snap = self.get_snapshot(snapshot_id)
        version = parser_version or self.get_source(snap["source_id"])["parser_version"]
        parsers.ensure_version(version)

        try:
            text = self.read_payload(snapshot_id)
        except SnapshotCorruptError:
            self._record_run(job_id, snapshot_id, version, None, None, "corrupt")
            raise

        status, result, result_json = "ok", None, None
        try:
            result = parsers.parse(version, text)
            result_json = json.dumps(result, ensure_ascii=False, sort_keys=True)
        except parsers.ParseError:
            status = "parse_error"

        diff = None
        if status == "ok":
            diff = diff_fields(snap["parse_result"] or {}, result)
        diff_json = json.dumps(diff, ensure_ascii=False) if diff is not None else None
        self._record_run(job_id, snapshot_id, version, result_json, diff_json, status)
        return {
            "snapshot_id": snapshot_id,
            "parser_version": version,
            "status": status,
            "result": result,
            "diff": diff,
        }

    def _record_run(self, job_id, snapshot_id, version, result_json, diff_json, status):
        # UNIQUE(job_id, snapshot_id) makes job-driven runs idempotent on resume;
        # manual runs (job_id NULL) never conflict and accumulate as history.
        self.db.execute(
            "INSERT OR REPLACE INTO reparse_runs(job_id, snapshot_id, parser_version, result,"
            " diff, status, created_at) VALUES (?,?,?,?,?,?,?)",
            (job_id, snapshot_id, version, result_json, diff_json, status, utcnow()),
        )

    def list_runs(self, snapshot_id: int) -> list[dict]:
        self.get_snapshot(snapshot_id)
        rows = self.db.query(
            "SELECT * FROM reparse_runs WHERE snapshot_id=? ORDER BY id DESC", (snapshot_id,)
        )
        out = []
        for row in rows:
            data = rowdict(row)
            data["result"] = json.loads(data["result"]) if data["result"] else None
            data["diff"] = json.loads(data["diff"]) if data["diff"] else None
            out.append(data)
        return out

    # ------------------------------------------------------------------
    # references (audit / replay pins)
    # ------------------------------------------------------------------
    def add_ref(self, snapshot_id: int, ref_type: str, ref_key: str,
                note: str | None = None) -> dict:
        self.get_snapshot(snapshot_id)
        if ref_type not in REF_TYPES:
            raise ValidationError(f"ref_type must be one of {REF_TYPES}")
        if not ref_key or not ref_key.strip():
            raise ValidationError("ref_key is required")
        ref_id = self.db.execute(
            "INSERT INTO snapshot_refs(snapshot_id, ref_type, ref_key, note, active, created_at)"
            " VALUES (?,?,?,?,1,?)",
            (snapshot_id, ref_type, ref_key.strip(), note, utcnow()),
        )
        return self.get_ref(ref_id)

    def get_ref(self, ref_id: int) -> dict:
        row = self.db.query_one("SELECT * FROM snapshot_refs WHERE id=?", (ref_id,))
        if not row:
            raise NotFoundError(f"ref {ref_id} not found")
        data = rowdict(row)
        data["active"] = bool(data["active"])
        return data

    def release_ref(self, ref_id: int) -> dict:
        self.get_ref(ref_id)
        self.db.execute("UPDATE snapshot_refs SET active=0 WHERE id=?", (ref_id,))
        return self.get_ref(ref_id)

    def list_refs(self, snapshot_id: int | None = None, source_id: int | None = None,
                  active_only: bool = False) -> list[dict]:
        clauses, params = [], []
        if snapshot_id is not None:
            clauses.append("r.snapshot_id=?")
            params.append(snapshot_id)
        if source_id is not None:
            clauses.append("s.source_id=?")
            params.append(source_id)
        if active_only:
            clauses.append("r.active=1")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.db.query(
            "SELECT r.*, s.seq AS snapshot_seq, s.source_id FROM snapshot_refs r"
            f" JOIN snapshots s ON s.id = r.snapshot_id {where} ORDER BY r.id DESC",
            tuple(params),
        )
        out = []
        for row in rows:
            data = rowdict(row)
            data["active"] = bool(data["active"])
            out.append(data)
        return out

    # ------------------------------------------------------------------
    # retention: policy-driven cleanup that never deletes referenced versions
    # ------------------------------------------------------------------
    def set_retention(self, source_id: int, max_snapshots: int | None = None,
                      strategy: str | None = None, protect_corrupt: bool | None = None) -> dict:
        src = self.get_source(source_id)
        max_snapshots = src["retention_max"] if max_snapshots is None else max_snapshots
        strategy = src["retention_strategy"] if strategy is None else strategy
        protect_corrupt = src["protect_corrupt"] if protect_corrupt is None else protect_corrupt
        if max_snapshots < 1:
            raise ValidationError("max_snapshots must be >= 1")
        if strategy not in RETENTION_STRATEGIES:
            raise ValidationError(f"unknown retention strategy: {strategy}")
        self.db.execute(
            "UPDATE sources SET retention_max=?, retention_strategy=?, protect_corrupt=? WHERE id=?",
            (max_snapshots, strategy, int(protect_corrupt), source_id),
        )
        return self.get_source(source_id)

    def cleanup_candidates(self, source_id: int) -> dict:
        """Preview which snapshots retention would delete, and which are
        protected by active audit/replay references (or corrupt-protection)."""
        src = self.get_source(source_id)
        rows = self.db.query(
            f"SELECT {SNAPSHOT_COLS} FROM snapshots WHERE source_id=? ORDER BY seq ASC",
            (source_id,),
        )
        snaps = [self._snapshot_dict(r) for r in rows]
        total = len(snaps)
        excess = max(0, total - src["retention_max"])

        ref_rows = self.db.query(
            "SELECT r.snapshot_id, r.ref_type, r.ref_key FROM snapshot_refs r"
            " JOIN snapshots s ON s.id = r.snapshot_id WHERE s.source_id=? AND r.active=1",
            (source_id,),
        )
        ref_map: dict[int, list[str]] = {}
        for r in ref_rows:
            ref_map.setdefault(r["snapshot_id"], []).append(f"{r['ref_type']}:{r['ref_key']}")

        protected, pool = [], []
        for snap in snaps:
            reasons = list(ref_map.get(snap["id"], []))
            if src["protect_corrupt"] and snap["status"] == "corrupt":
                reasons.append("corrupt-protected")
            if reasons:
                protected.append({
                    "id": snap["id"], "seq": snap["seq"], "status": snap["status"],
                    "content_hash_short": snap["content_hash_short"], "reasons": reasons,
                })
            else:
                pool.append(snap)

        if src["retention_strategy"] == "largest_first":
            pool.sort(key=lambda s: (-s["compressed_size"], s["seq"]))
        else:
            pool.sort(key=lambda s: s["seq"])

        candidates = pool[:excess]
        for c in candidates:
            c["reason"] = src["retention_strategy"]
        return {
            "source_id": source_id,
            "strategy": src["retention_strategy"],
            "retention_max": src["retention_max"],
            "total": total,
            "excess": excess,
            "candidates": candidates,
            "protected": protected,
            "blocked": excess - len(candidates),
        }

    def apply_cleanup(self, source_id: int) -> dict:
        plan = self.cleanup_candidates(source_id)
        deleted = []
        for cand in plan["candidates"]:
            with self.db.transaction() as conn:
                # Released refs are history and go with the snapshot; active refs
                # are excluded above and additionally guarded by ON DELETE RESTRICT.
                conn.execute(
                    "DELETE FROM snapshot_refs WHERE snapshot_id=? AND active=0", (cand["id"],)
                )
                conn.execute("DELETE FROM snapshots WHERE id=?", (cand["id"],))
            deleted.append(cand["id"])
        return {
            "deleted": deleted,
            "blocked": plan["blocked"],
            "protected": plan["protected"],
        }

    # ------------------------------------------------------------------
    # parser upgrade / rollback
    # ------------------------------------------------------------------
    def upgrade_parser(self, source_id: int, to_version: str,
                       reason: str | None = None) -> dict:
        src = self.get_source(source_id)
        if not parsers.is_upgrade(src["parser_version"], to_version):
            raise ValidationError(
                f"cannot upgrade from {src['parser_version']} to {to_version}:"
                " target is not a newer version"
            )
        self._change_parser(source_id, "upgrade", src["parser_version"], to_version, reason)
        return self.get_source(source_id)

    def rollback_parser(self, source_id: int, to_version: str,
                        reason: str | None = None) -> dict:
        src = self.get_source(source_id)
        parsers.ensure_version(to_version)
        if to_version == src["parser_version"]:
            raise ValidationError(f"source is already on parser {to_version}")
        if to_version not in self._used_versions(source_id):
            raise ValidationError(
                f"cannot roll back to {to_version}: version was never used by this source"
            )
        self._change_parser(source_id, "rollback", src["parser_version"], to_version, reason)
        return self.get_source(source_id)

    def _used_versions(self, source_id: int) -> set[str]:
        rows = self.db.query(
            "SELECT from_version, to_version FROM parser_history WHERE source_id=?", (source_id,)
        )
        used = set()
        for row in rows:
            used.add(row["to_version"])
            if row["from_version"]:
                used.add(row["from_version"])
        return used

    def _change_parser(self, source_id, kind, from_version, to_version, reason):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE sources SET parser_version=? WHERE id=?", (to_version, source_id)
            )
            conn.execute(
                "INSERT INTO parser_history(source_id, kind, from_version, to_version, reason,"
                " created_at) VALUES (?,?,?,?,?,?)",
                (source_id, kind, from_version, to_version, reason, utcnow()),
            )

    def parser_history(self, source_id: int) -> list[dict]:
        self.get_source(source_id)
        return [
            rowdict(r)
            for r in self.db.query(
                "SELECT * FROM parser_history WHERE source_id=? ORDER BY id DESC", (source_id,)
            )
        ]

    # ------------------------------------------------------------------
    # batch re-parse jobs with checkpoints (断点续跑)
    # ------------------------------------------------------------------
    def create_reparse_job(self, source_id: int, parser_version: str | None = None,
                           batch_size: int = 50) -> dict:
        src = self.get_source(source_id)
        version = parser_version or src["parser_version"]
        parsers.ensure_version(version)
        if batch_size < 1:
            raise ValidationError("batch_size must be >= 1")
        total = self.db.query_one(
            "SELECT COUNT(*) AS n FROM snapshots WHERE source_id=?", (source_id,)
        )["n"]
        now = utcnow()
        job_id = self.db.execute(
            "INSERT INTO reparse_jobs(source_id, parser_version, status, checkpoint_id,"
            " batch_size, processed, errors, total, created_at, updated_at)"
            " VALUES (?,?, 'pending', 0, ?, 0, 0, ?, ?, ?)",
            (source_id, version, batch_size, total, now, now),
        )
        return self.get_job(job_id)

    def get_job(self, job_id: int) -> dict:
        row = self.db.query_one("SELECT * FROM reparse_jobs WHERE id=?", (job_id,))
        if not row:
            raise NotFoundError(f"reparse job {job_id} not found")
        return rowdict(row)

    def list_jobs(self, source_id: int) -> list[dict]:
        self.get_source(source_id)
        return [
            rowdict(r)
            for r in self.db.query(
                "SELECT * FROM reparse_jobs WHERE source_id=? ORDER BY id DESC", (source_id,)
            )
        ]

    def run_reparse_job(self, job_id: int, interrupt_after: int | None = None) -> dict:
        """Process a job from its checkpoint. `interrupt_after` simulates a
        crash after N items this run (chaos hook used by tests and the UI)."""
        job = self.get_job(job_id)
        if job["status"] == "completed":
            return job
        self.db.execute(
            "UPDATE reparse_jobs SET status='running', updated_at=? WHERE id=?",
            (utcnow(), job_id),
        )
        processed_this_run = 0
        try:
            while True:
                job = self.get_job(job_id)
                rows = self.db.query(
                    "SELECT id FROM snapshots WHERE source_id=? AND id>? ORDER BY id ASC LIMIT ?",
                    (job["source_id"], job["checkpoint_id"], job["batch_size"]),
                )
                if not rows:
                    break
                for row in rows:
                    try:
                        self.reparse(row["id"], parser_version=job["parser_version"],
                                     job_id=job_id)
                    except SnapshotCorruptError:
                        pass  # recorded as a corrupt run; job keeps going
                    self.db.execute(
                        "UPDATE reparse_jobs SET checkpoint_id=?, updated_at=? WHERE id=?",
                        (row["id"], utcnow(), job_id),
                    )
                    processed_this_run += 1
                    if interrupt_after is not None and processed_this_run >= interrupt_after:
                        self._interrupt_job(job_id)
                        raise JobInterrupted(job_id)
                self._sync_job_counts(job_id)
            self._sync_job_counts(job_id)
            self.db.execute(
                "UPDATE reparse_jobs SET status='completed', updated_at=? WHERE id=?",
                (utcnow(), job_id),
            )
        except JobInterrupted:
            raise
        except Exception:
            self.db.execute(
                "UPDATE reparse_jobs SET status='failed', updated_at=? WHERE id=?",
                (utcnow(), job_id),
            )
            raise
        return self.get_job(job_id)

    def resume_reparse_job(self, job_id: int, interrupt_after: int | None = None) -> dict:
        job = self.get_job(job_id)
        if job["status"] not in RESUMABLE_JOB_STATES:
            raise ValidationError(
                f"job {job_id} is {job['status']}; only "
                f"{'/'.join(RESUMABLE_JOB_STATES)} jobs can be resumed"
            )
        return self.run_reparse_job(job_id, interrupt_after=interrupt_after)

    def _interrupt_job(self, job_id: int) -> None:
        self._sync_job_counts(job_id)
        self.db.execute(
            "UPDATE reparse_jobs SET status='interrupted', updated_at=? WHERE id=?",
            (utcnow(), job_id),
        )

    def _sync_job_counts(self, job_id: int) -> None:
        """Reconcile processed/error counters from actual runs, so a crash
        between a run and its checkpoint update never double-counts."""
        row = self.db.query_one(
            "SELECT COUNT(*) AS n,"
            " COALESCE(SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END), 0) AS errs"
            " FROM reparse_runs WHERE job_id=?",
            (job_id,),
        )
        self.db.execute(
            "UPDATE reparse_jobs SET processed=?, errors=?, updated_at=? WHERE id=?",
            (row["n"], row["errs"], utcnow(), job_id),
        )
