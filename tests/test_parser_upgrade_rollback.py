"""Parser upgrades and rollbacks (解析器升级和回滚)."""
import json

import pytest

from conftest import SAMPLE, SAMPLE_PAYLOAD
from snapshot_service.errors import ValidationError


def test_upgrade_changes_current_version_and_records_history(service, source):
    service.upgrade_parser(source["id"], "v2", reason="字段类型规范化")
    assert service.get_source(source["id"])["parser_version"] == "v2"
    history = service.parser_history(source["id"])
    assert history[0]["kind"] == "upgrade"
    assert history[0]["from_version"] == "v1"
    assert history[0]["to_version"] == "v2"
    assert history[0]["reason"] == "字段类型规范化"
    assert history[1]["kind"] == "init"


def test_new_ingests_use_upgraded_parser(service, source):
    service.upgrade_parser(source["id"], "v2")
    snap = service.ingest(source["id"], SAMPLE_PAYLOAD)
    assert snap["parser_version"] == "v2"
    assert snap["parse_result"]["price_value"] == 199.0
    assert snap["parse_result"]["currency"] == "CNY"


def test_old_snapshots_keep_original_parser_version(service, source):
    old = service.ingest(source["id"], SAMPLE_PAYLOAD)
    service.upgrade_parser(source["id"], "v2")
    old_after = service.get_snapshot(old["id"])
    assert old_after["parser_version"] == "v1"
    assert old_after["parse_result"]["price"] == "¥199.00"  # stored result untouched


def test_upgrade_validates_direction(service, source):
    service.upgrade_parser(source["id"], "v2")
    with pytest.raises(ValidationError, match="not a newer version"):
        service.upgrade_parser(source["id"], "v2")  # same version
    with pytest.raises(ValidationError, match="not a newer version"):
        service.upgrade_parser(source["id"], "v1")  # backwards is a rollback


def test_upgrade_unknown_version_rejected(service, source):
    with pytest.raises(ValidationError, match="unknown parser version"):
        service.upgrade_parser(source["id"], "v9")


def test_rollback_to_previous_version(service, source):
    service.upgrade_parser(source["id"], "v2")
    service.rollback_parser(source["id"], "v1", reason="v2 回归问题")
    assert service.get_source(source["id"])["parser_version"] == "v1"
    history = service.parser_history(source["id"])
    assert history[0]["kind"] == "rollback"
    assert history[0]["from_version"] == "v2"
    assert history[0]["to_version"] == "v1"


def test_rollback_to_never_used_version_rejected(service, source):
    # source has only ever used v1; rolling back to v2 (never used) must fail
    with pytest.raises(ValidationError, match="never used"):
        service.rollback_parser(source["id"], "v2")


def test_rollback_to_current_version_rejected(service, source):
    with pytest.raises(ValidationError, match="already on"):
        service.rollback_parser(source["id"], "v1")


def test_rollback_restores_original_parse_results(service, source):
    """Full cycle: v1 ingest -> upgrade -> diff visible -> rollback -> diff gone."""
    old = service.ingest(source["id"], SAMPLE_PAYLOAD)

    service.upgrade_parser(source["id"], "v2")
    upgraded = service.reparse(old["id"])  # uses current v2
    assert len(upgraded["diff"]) > 0

    service.rollback_parser(source["id"], "v1")
    rolled_back = service.reparse(old["id"])  # uses current v1 again
    assert rolled_back["parser_version"] == "v1"
    assert rolled_back["diff"] == []

    # and new ingests are parsed with v1 again
    fresh = service.ingest(source["id"], json.dumps(SAMPLE))
    assert fresh["parser_version"] == "v1"
    assert fresh["parse_result"]["price"] == "¥199.00"


def test_upgrade_then_batch_reparse_migrates_all_snapshots(service, source):
    from conftest import ingest_n
    ids = ingest_n(service, source["id"], 4)
    service.upgrade_parser(source["id"], "v2")
    job = service.create_reparse_job(source["id"])  # defaults to current v2
    done = service.run_reparse_job(job["id"])
    assert done["status"] == "completed"
    for sid in ids:
        runs = [r for r in service.list_runs(sid) if r["job_id"] == job["id"]]
        assert runs[0]["parser_version"] == "v2"
        assert runs[0]["diff"]  # every snapshot shows the v1->v2 field changes
