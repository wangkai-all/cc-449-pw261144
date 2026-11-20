"""Retention cleanup: policy-driven, never deletes audit/replay-referenced versions."""
import json

import pytest

from conftest import ingest_n


def test_no_candidates_under_limit(service, source):
    ingest_n(service, source["id"], 3)
    plan = service.cleanup_candidates(source["id"])
    assert plan["excess"] == 0
    assert plan["candidates"] == []
    assert plan["blocked"] == 0


def test_oldest_first_candidates(service, source):
    ids = ingest_n(service, source["id"], 5)
    service.set_retention(source["id"], max_snapshots=3)
    plan = service.cleanup_candidates(source["id"])
    assert plan["excess"] == 2
    assert [c["id"] for c in plan["candidates"]] == ids[:2]  # oldest two


def test_largest_first_strategy(service, source):
    import hashlib
    small = ingest_n(service, source["id"], 2)
    # incompressible payload so compressed size is genuinely largest
    noise = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(8))
    big_payload = json.dumps({"title": noise, "price": "¥1.00", "sku": "big"})
    big = service.ingest(source["id"], big_payload)["id"]
    service.set_retention(source["id"], max_snapshots=2, strategy="largest_first")
    plan = service.cleanup_candidates(source["id"])
    assert plan["excess"] == 1
    assert [c["id"] for c in plan["candidates"]] == [big]
    result = service.apply_cleanup(source["id"])
    assert result["deleted"] == [big]
    assert {s["id"] for s in service.list_snapshots(source["id"])} == set(small)


def test_audit_reference_blocks_deletion(service, source):
    ids = ingest_n(service, source["id"], 4)
    service.set_retention(source["id"], max_snapshots=2)
    service.add_ref(ids[0], "audit", "AUDIT-1")
    plan = service.cleanup_candidates(source["id"])
    # excess=2; the pinned oldest is exempt, cleanup takes the next-oldest
    # unprotected snapshots instead and reports it as protected
    assert [c["id"] for c in plan["candidates"]] == [ids[1], ids[2]]
    assert plan["blocked"] == 0
    assert plan["protected"][0]["id"] == ids[0]
    assert "audit:AUDIT-1" in plan["protected"][0]["reasons"]

    result = service.apply_cleanup(source["id"])
    assert result["deleted"] == [ids[1], ids[2]]
    remaining = {s["id"] for s in service.list_snapshots(source["id"])}
    assert remaining == {ids[0], ids[3]}  # pinned oldest survives


def test_replay_reference_blocks_deletion(service, source):
    ids = ingest_n(service, source["id"], 3)
    service.set_retention(source["id"], max_snapshots=1)
    service.add_ref(ids[0], "replay", "session-9")
    service.add_ref(ids[1], "replay", "session-10")
    result = service.apply_cleanup(source["id"])
    assert result["deleted"] == [ids[2]]
    assert result["blocked"] == 1  # second excess snapshot is pinned
    remaining = {s["id"] for s in service.list_snapshots(source["id"])}
    assert remaining == {ids[0], ids[1]}


def test_all_referenced_nothing_deleted(service, source):
    ids = ingest_n(service, source["id"], 3)
    service.set_retention(source["id"], max_snapshots=1)
    for i, sid in enumerate(ids):
        service.add_ref(sid, "audit", f"AUDIT-{i}")
    result = service.apply_cleanup(source["id"])
    assert result["deleted"] == []
    assert result["blocked"] == 2
    assert len(service.list_snapshots(source["id"])) == 3


def test_released_reference_allows_cleanup(service, source):
    ids = ingest_n(service, source["id"], 3)
    service.set_retention(source["id"], max_snapshots=2)
    ref = service.add_ref(ids[0], "audit", "AUDIT-1")
    assert service.cleanup_candidates(source["id"])["candidates"][0]["id"] == ids[1]
    service.release_ref(ref["id"])
    plan = service.cleanup_candidates(source["id"])
    assert [c["id"] for c in plan["candidates"]] == [ids[0]]


def test_cleanup_cascades_reparse_runs(service, source):
    ids = ingest_n(service, source["id"], 3)
    service.reparse(ids[0], parser_version="v2")
    assert len(service.list_runs(ids[0])) == 1
    service.set_retention(source["id"], max_snapshots=2)
    service.apply_cleanup(source["id"])
    rows = service.db.query("SELECT * FROM reparse_runs WHERE snapshot_id=?", (ids[0],))
    assert rows == []


def test_protect_corrupt_policy(service, source):
    ids = ingest_n(service, source["id"], 3)
    # corrupt the oldest snapshot
    service.db.execute("UPDATE snapshots SET payload=? WHERE id=?", (b"garbage", ids[0]))
    with pytest.raises(Exception):
        service.read_payload(ids[0])
    service.set_retention(source["id"], max_snapshots=1, protect_corrupt=True)
    plan = service.cleanup_candidates(source["id"])
    protected_ids = [p["id"] for p in plan["protected"]]
    assert ids[0] in protected_ids
    assert "corrupt-protected" in plan["protected"][0]["reasons"]
    # corrupt one is protected, so the two deletable excess are ids[1], ids[2]
    assert [c["id"] for c in plan["candidates"]] == [ids[1], ids[2]]


def test_apply_cleanup_is_idempotent(service, source):
    ingest_n(service, source["id"], 4)
    service.set_retention(source["id"], max_snapshots=2)
    first = service.apply_cleanup(source["id"])
    second = service.apply_cleanup(source["id"])
    assert len(first["deleted"]) == 2
    assert second["deleted"] == []
    assert len(service.list_snapshots(source["id"])) == 2


def test_invalid_strategy_rejected(service, source):
    from snapshot_service.errors import ValidationError
    with pytest.raises(ValidationError):
        service.set_retention(source["id"], strategy="random_first")
