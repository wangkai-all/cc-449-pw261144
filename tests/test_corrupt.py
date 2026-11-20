"""Corrupt snapshots (坏快照): detection, marking, and safe handling."""
import zlib

import pytest

from conftest import SAMPLE_PAYLOAD, ingest_n
from snapshot_service.errors import SnapshotCorruptError


def _tamper(service, snapshot_id, blob):
    service.db.execute("UPDATE snapshots SET payload=? WHERE id=?", (blob, snapshot_id))


def test_undecompressible_payload_detected_and_marked(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    _tamper(service, snap["id"], b"\xff\xfe not zlib")
    with pytest.raises(SnapshotCorruptError, match="decompress"):
        service.read_payload(snap["id"])
    assert service.get_snapshot(snap["id"])["status"] == "corrupt"


def test_hash_mismatch_detected(service, source):
    """Payload decompresses fine but no longer matches the stored hash."""
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    _tamper(service, snap["id"], zlib.compress('{"title":"被篡改","price":"¥0.01"}'.encode()))
    with pytest.raises(SnapshotCorruptError, match="hash mismatch"):
        service.read_payload(snap["id"])
    assert service.get_snapshot(snap["id"])["status"] == "corrupt"


def test_reparse_corrupt_snapshot_raises_and_records_run(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    _tamper(service, snap["id"], b"garbage")
    job = service.create_reparse_job(source["id"], parser_version="v2")
    with pytest.raises(SnapshotCorruptError):
        service.reparse(snap["id"], parser_version="v2", job_id=job["id"])
    runs = service.list_runs(snap["id"])
    assert runs[0]["status"] == "corrupt"
    assert runs[0]["result"] is None


def test_corrupt_snapshot_does_not_break_siblings(service, source):
    ids = ingest_n(service, source["id"], 3)
    _tamper(service, ids[1], b"garbage")
    # siblings still read and reparse fine
    assert service.read_payload(ids[0])
    assert service.reparse(ids[2], parser_version="v2")["status"] == "ok"


def test_corrupt_snapshot_cleanup_unprotected_by_default(service, source):
    ids = ingest_n(service, source["id"], 3)
    _tamper(service, ids[0], b"garbage")
    with pytest.raises(SnapshotCorruptError):
        service.read_payload(ids[0])
    service.set_retention(source["id"], max_snapshots=2)
    plan = service.cleanup_candidates(source["id"])
    # corrupt snapshot is a normal deletion candidate unless protect_corrupt is on
    assert [c["id"] for c in plan["candidates"]] == [ids[0]]
