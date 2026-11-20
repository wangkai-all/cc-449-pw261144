"""Batch re-parse jobs: checkpoints, interruption and resume (断点续跑)."""
import pytest

from conftest import SAMPLE_PAYLOAD, ingest_n
from snapshot_service.errors import JobInterrupted, ValidationError


def test_job_completes_all_snapshots(service, source):
    ids = ingest_n(service, source["id"], 7)
    job = service.create_reparse_job(source["id"], parser_version="v2", batch_size=3)
    assert job["total"] == 7
    done = service.run_reparse_job(job["id"])
    assert done["status"] == "completed"
    assert done["processed"] == 7
    assert done["errors"] == 0
    assert done["checkpoint_id"] == ids[-1]
    for sid in ids:
        runs = [r for r in service.list_runs(sid) if r["job_id"] == job["id"]]
        assert len(runs) == 1
        assert runs[0]["parser_version"] == "v2"


def test_interrupt_and_resume_from_checkpoint(service, source):
    ids = ingest_n(service, source["id"], 10)
    job = service.create_reparse_job(source["id"], parser_version="v2", batch_size=3)

    with pytest.raises(JobInterrupted):
        service.run_reparse_job(job["id"], interrupt_after=4)

    interrupted = service.get_job(job["id"])
    assert interrupted["status"] == "interrupted"
    assert interrupted["checkpoint_id"] == ids[3]  # stopped exactly after the 4th
    assert interrupted["processed"] == 4

    done = service.resume_reparse_job(job["id"])
    assert done["status"] == "completed"
    assert done["processed"] == 10
    assert done["checkpoint_id"] == ids[-1]

    # exactly-once: every snapshot has exactly one run for this job
    for sid in ids:
        runs = [r for r in service.list_runs(sid) if r["job_id"] == job["id"]]
        assert len(runs) == 1


def test_multiple_interrupts_accumulate_no_duplicates(service, source):
    ids = ingest_n(service, source["id"], 6)
    job = service.create_reparse_job(source["id"], parser_version="v2", batch_size=2)
    for _ in range(3):
        with pytest.raises(JobInterrupted):
            service.run_reparse_job(job["id"], interrupt_after=1)
        service.get_job(job["id"])  # checkpoint persists between runs
    done = service.resume_reparse_job(job["id"])
    assert done["status"] == "completed"
    total_runs = sum(
        len([r for r in service.list_runs(sid) if r["job_id"] == job["id"]]) for sid in ids
    )
    assert total_runs == 6


def test_job_skips_corrupt_snapshot_and_counts_error(service, source):
    ids = ingest_n(service, source["id"], 5)
    service.db.execute("UPDATE snapshots SET payload=? WHERE id=?", (b"\x00bad", ids[2]))
    job = service.create_reparse_job(source["id"], parser_version="v2", batch_size=2)
    done = service.run_reparse_job(job["id"])
    assert done["status"] == "completed"
    assert done["processed"] == 5
    assert done["errors"] == 1
    corrupt_runs = [r for r in service.list_runs(ids[2]) if r["job_id"] == job["id"]]
    assert corrupt_runs[0]["status"] == "corrupt"
    # the corrupt snapshot is flagged on the timeline too
    assert service.get_snapshot(ids[2])["status"] == "corrupt"


def test_completed_job_rerun_is_noop(service, source):
    ids = ingest_n(service, source["id"], 3)
    job = service.create_reparse_job(source["id"], parser_version="v2")
    service.run_reparse_job(job["id"])
    again = service.run_reparse_job(job["id"])
    assert again["status"] == "completed"
    assert again["processed"] == 3
    for sid in ids:
        assert len([r for r in service.list_runs(sid) if r["job_id"] == job["id"]]) == 1


def test_resume_completed_job_rejected(service, source):
    ingest_n(service, source["id"], 2)
    job = service.create_reparse_job(source["id"], parser_version="v2")
    service.run_reparse_job(job["id"])
    with pytest.raises(ValidationError):
        service.resume_reparse_job(job["id"])


def test_empty_source_job_completes_immediately(service, source):
    job = service.create_reparse_job(source["id"], parser_version="v2")
    done = service.run_reparse_job(job["id"])
    assert done["status"] == "completed"
    assert done["total"] == 0
    assert done["processed"] == 0
