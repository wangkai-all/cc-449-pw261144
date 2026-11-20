"""Re-parse by snapshot and compare field differences."""
from conftest import SAMPLE_PAYLOAD


def test_reparse_same_version_has_empty_diff(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    out = service.reparse(snap["id"], parser_version="v1")
    assert out["status"] == "ok"
    assert out["diff"] == []


def test_reparse_defaults_to_source_current_parser(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    service.upgrade_parser(source["id"], "v2")
    out = service.reparse(snap["id"])  # no explicit version -> current (v2)
    assert out["parser_version"] == "v2"
    assert len(out["diff"]) > 0


def test_reparse_with_v2_shows_field_diffs(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    out = service.reparse(snap["id"], parser_version="v2")
    by_field = {d["field"]: d for d in out["diff"]}

    assert by_field["price"] == {"field": "price", "change": "removed",
                                 "old": "¥199.00", "new": None}
    assert by_field["price_value"]["change"] == "added"
    assert by_field["price_value"]["new"] == 199.0
    assert by_field["currency"] == {"field": "currency", "change": "added",
                                    "old": None, "new": "CNY"}
    assert by_field["in_stock"]["new"] is True
    assert by_field["tags"]["change"] == "changed"
    assert by_field["tags"]["new"] == ["audio", "electronics"]
    assert by_field["stock"] == {"field": "stock", "change": "changed",
                                 "old": "17", "new": 17}
    assert by_field["sku"]["new"] == "A-123"
    assert by_field["title"]["new"] == "无线 耳机"


def test_reparse_records_runs(service, source):
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    service.reparse(snap["id"], parser_version="v2")
    service.reparse(snap["id"], parser_version="v1")
    runs = service.list_runs(snap["id"])
    assert len(runs) == 2
    assert {r["parser_version"] for r in runs} == {"v1", "v2"}
    assert all(r["job_id"] is None for r in runs)


def test_reparse_parse_error_snapshot_diffs_against_empty(service, source):
    snap = service.ingest(source["id"], "not json")
    assert snap["status"] == "parse_error"
    # payload itself is fine, only unparseable -> reparse with v1 also fails
    out = service.reparse(snap["id"], parser_version="v1")
    assert out["status"] == "parse_error"
    assert out["diff"] is None


def test_reparse_unknown_version_rejected(service, source):
    import pytest

    from snapshot_service.errors import ValidationError
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    with pytest.raises(ValidationError):
        service.reparse(snap["id"], parser_version="v9")
