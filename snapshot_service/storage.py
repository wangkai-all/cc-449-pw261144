"""SQLite storage layer: schema and connection management."""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    retention_max INTEGER NOT NULL DEFAULT 100,
    retention_strategy TEXT NOT NULL DEFAULT 'oldest_first',
    protect_corrupt INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    codec TEXT NOT NULL DEFAULT 'zlib',
    payload BLOB NOT NULL,
    raw_size INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL,
    parser_version TEXT NOT NULL,
    parse_result TEXT,
    status TEXT NOT NULL DEFAULT 'ok',
    created_at TEXT NOT NULL,
    UNIQUE (source_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots(source_id, id);

CREATE TABLE IF NOT EXISTS snapshot_refs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE RESTRICT,
    ref_type TEXT NOT NULL CHECK (ref_type IN ('audit', 'replay')),
    ref_key TEXT NOT NULL,
    note TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_refs_snapshot ON snapshot_refs(snapshot_id, active);

CREATE TABLE IF NOT EXISTS reparse_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    checkpoint_id INTEGER NOT NULL DEFAULT 0,
    batch_size INTEGER NOT NULL DEFAULT 50,
    processed INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reparse_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER REFERENCES reparse_jobs(id) ON DELETE CASCADE,
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    parser_version TEXT NOT NULL,
    result TEXT,
    diff TEXT,
    status TEXT NOT NULL DEFAULT 'ok',
    created_at TEXT NOT NULL,
    UNIQUE (job_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS parser_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('init', 'upgrade', 'rollback')),
    from_version TEXT,
    to_version TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);
"""

# Snapshot columns safe to return to clients (payload BLOB excluded).
SNAPSHOT_COLS = (
    "id, source_id, seq, content_hash, codec, raw_size, compressed_size, "
    "parser_version, parse_result, status, created_at"
)


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def rowdict(row: sqlite3.Row | None) -> dict | None:
    return {k: row[k] for k in row.keys()} if row is not None else None


class Storage:
    """Thin SQLite helper.

    File-backed databases use one connection per operation; in-memory
    databases share a single connection. All access is serialized through
    a re-entrant lock so the service is safe under a threaded ASGI server.
    """

    def __init__(self, path: str = "snapshots.db"):
        self.path = path
        self._lock = threading.RLock()
        self._mem: sqlite3.Connection | None = None
        if path == ":memory:":
            self._mem = self._new_conn()
            self._mem.executescript(SCHEMA)
        else:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with self.transaction() as conn:
                conn.executescript(SCHEMA)

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def transaction(self):
        """Yield a connection; commit on success, roll back on error."""
        with self._lock:
            if self._mem is not None:
                try:
                    yield self._mem
                    self._mem.commit()
                except Exception:
                    self._mem.rollback()
                    raise
            else:
                conn = self._new_conn()
                try:
                    yield conn
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.close()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.transaction() as conn:
            return conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: tuple = ()) -> int:
        with self.transaction() as conn:
            return conn.execute(sql, params).lastrowid
