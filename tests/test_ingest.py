"""Ingest: hash, compressed payload and parse result are stored per response."""
import hashlib
import json
import zlib

from conftest import SAMPLE_PAYLOAD


def test_ingest_stores_hash_of_raw_payload(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    assert snap["content_hash"] == hashlib.sha256(SAMPLE_PAYLOAD.encode()).hexdigest()


def test_ingest_compresses_payload_and_roundtrips(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    assert snap["codec"] == "zlib"
    assert snap["raw_size"] == len(SAMPLE_PAYLOAD.encode())
    assert snap["compressed_size"] < snap["raw_size"]
    assert service.read_payload(snap["id"]) == SAMPLE_PAYLOAD


def test_repetitive_payload_compresses_well(service, source):
    payload = json.dumps({"title": "耳机" * 200, "price": "¥1.00"})
    snap = service.ingest(source["id"], payload)
    assert snap["compressed_size"] < snap["raw_size"] // 2


def test_ingest_parses_with_current_parser(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    assert snap["status"] == "ok"
    assert snap["parser_version"] == "v1"
    assert snap["parse_result"] == {
        "title": "无线  耳机",
        "price": "¥199.00",
        "tags": ["Audio", " electronics ", "AUDIO"],
        "sku": "a-123",
        "stock": "17",
    }


def test_seq_increments_per_source(service, source):
    ids = [service.ingest(source["id"], SAMPLE_PAYLOAD)["seq"] for _ in range(3)]
    assert ids == [1, 2, 3]
    other = service.create_source("other")
    assert service.ingest(other["id"], SAMPLE_PAYLOAD)["seq"] == 1


def test_invalid_json_stored_as_parse_error(service, source):
    snap = service.ingest(source["id"], "这不是 JSON")
    assert snap["status"] == "parse_error"
    assert snap["parse_result"] is None
    # payload is still retrievable for forensics
    assert service.read_payload(snap["id"]) == "这不是 JSON"


def test_compressed_blob_is_actually_zlib(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    row = service.db.query_one("SELECT payload FROM snapshots WHERE id=?", (snap["id"],))
    assert zlib.decompress(row["payload"]).decode() == SAMPLE_PAYLOAD
