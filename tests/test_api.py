"""End-to-end API smoke tests via FastAPI TestClient."""
import json

import pytest
from fastapi.testclient import TestClient

from conftest import SAMPLE_PAYLOAD
from snapshot_service.app import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "api.db"))
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def source_id(client):
    resp = client.post("/api/sources", json={"name": "api-demo", "retention_max": 3})
    assert resp.status_code == 201
    return resp.json()["id"]


def test_full_flow(client, source_id):
    # ingest
    resp = client.post(f"/api/sources/{source_id}/responses",
                       json={"payload": SAMPLE_PAYLOAD})
    assert resp.status_code == 201
    snap = resp.json()
    assert snap["content_hash"]
    assert snap["parse_result"]["price"] == "¥199.00"

    # timeline
    timeline = client.get(f"/api/sources/{source_id}/snapshots").json()
    assert len(timeline) == 1

    # payload roundtrip
    payload = client.get(f"/api/snapshots/{snap['id']}/payload").json()
    assert payload["payload"] == SAMPLE_PAYLOAD

    # reparse with diff
    out = client.post(f"/api/snapshots/{snap['id']}/reparse",
                      json={"parser_version": "v2"}).json()
    assert out["status"] == "ok"
    fields = {d["field"] for d in out["diff"]}
    assert {"price", "price_value", "currency"} <= fields

    # refs
    ref = client.post(f"/api/snapshots/{snap['id']}/refs",
                      json={"ref_type": "audit", "ref_key": "AUDIT-1"}).json()
    assert ref["active"] is True
    refs = client.get(f"/api/snapshots/{snap['id']}/refs").json()
    assert len(refs) == 1
    released = client.delete(f"/api/refs/{ref['id']}").json()
    assert released["active"] is False


def test_retention_endpoints(client, source_id):
    for i in range(5):
        payload = json.dumps({"title": f"商品{i}", "price": "¥1.00", "sku": f"s-{i}"})
        client.post(f"/api/sources/{source_id}/responses", json={"payload": payload})
    plan = client.get(f"/api/sources/{source_id}/cleanup-candidates").json()
    assert plan["excess"] == 2
    assert len(plan["candidates"]) == 2
    result = client.post(f"/api/sources/{source_id}/cleanup").json()
    assert len(result["deleted"]) == 2
    assert len(client.get(f"/api/sources/{source_id}/snapshots").json()) == 3


def test_parser_upgrade_rollback_endpoints(client, source_id):
    resp = client.post(f"/api/sources/{source_id}/parser/upgrade",
                       json={"to_version": "v2", "reason": "test"})
    assert resp.json()["parser_version"] == "v2"
    bad = client.post(f"/api/sources/{source_id}/parser/upgrade",
                      json={"to_version": "v1"})
    assert bad.status_code == 400
    resp = client.post(f"/api/sources/{source_id}/parser/rollback",
                       json={"to_version": "v1"})
    assert resp.json()["parser_version"] == "v1"
    history = client.get(f"/api/sources/{source_id}/parser/history").json()
    assert [h["kind"] for h in history] == ["rollback", "upgrade", "init"]


def test_job_interrupt_resume_via_api(client, source_id):
    for i in range(5):
        payload = json.dumps({"title": f"商品{i}", "price": "¥2.00", "sku": f"j-{i}"})
        client.post(f"/api/sources/{source_id}/responses", json={"payload": payload})
    job = client.post(f"/api/sources/{source_id}/reparse-jobs",
                      json={"parser_version": "v2", "batch_size": 2,
                            "simulate_interrupt_after": 3}).json()
    assert job["status"] == "interrupted"
    assert job["processed"] == 3
    done = client.post(f"/api/reparse-jobs/{job['id']}/resume", json={}).json()
    assert done["status"] == "completed"
    assert done["processed"] == 5


def test_corrupt_snapshot_returns_409(client, source_id):
    resp = client.post(f"/api/sources/{source_id}/responses",
                       json={"payload": SAMPLE_PAYLOAD})
    snap_id = resp.json()["id"]
    app_service = client.app.state.service
    app_service.db.execute("UPDATE snapshots SET payload=? WHERE id=?",
                           (b"garbage", snap_id))
    resp = client.get(f"/api/snapshots/{snap_id}/payload")
    assert resp.status_code == 409
    assert "decompress" in resp.json()["detail"]


def test_404s(client):
    assert client.get("/api/sources/999").status_code == 404
    assert client.get("/api/snapshots/999").status_code == 404
    assert client.delete("/api/refs/999").status_code == 404


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "来源快照服务" in resp.text
