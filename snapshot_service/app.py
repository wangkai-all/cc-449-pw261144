"""HTTP API + static UI for the source snapshot service."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import parsers
from .errors import JobInterrupted, NotFoundError, SnapshotCorruptError, ValidationError
from .service import RETENTION_STRATEGIES, SnapshotService

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


class SourceIn(BaseModel):
    name: str
    parser_version: str = parsers.DEFAULT_VERSION
    retention_max: int = 100
    retention_strategy: str = "oldest_first"
    protect_corrupt: bool = False


class IngestIn(BaseModel):
    payload: str


class ReparseIn(BaseModel):
    parser_version: str | None = None


class RefIn(BaseModel):
    ref_type: str
    ref_key: str
    note: str | None = None


class RetentionIn(BaseModel):
    max_snapshots: int | None = None
    strategy: str | None = None
    protect_corrupt: bool | None = None


class ParserChangeIn(BaseModel):
    to_version: str
    reason: str | None = None


class JobIn(BaseModel):
    parser_version: str | None = None
    batch_size: int = Field(default=50, ge=1)
    simulate_interrupt_after: int | None = Field(
        default=None, description="chaos hook: interrupt the run after N items"
    )


class ResumeIn(BaseModel):
    simulate_interrupt_after: int | None = None


def create_app(db_path: str | None = None) -> FastAPI:
    service = SnapshotService(db_path or os.environ.get("SNAPSHOT_DB", "snapshots.db"))
    app = FastAPI(title="来源快照服务", version="1.0.0")
    app.state.service = service

    @app.exception_handler(NotFoundError)
    async def _404(_: Request, exc: NotFoundError):
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(ValidationError)
    async def _400(_: Request, exc: ValidationError):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.exception_handler(SnapshotCorruptError)
    async def _409(_: Request, exc: SnapshotCorruptError):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    # ------------------------------ UI ------------------------------
    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    # --------------------------- metadata ---------------------------
    @app.get("/api/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/parsers")
    def parser_registry():
        return {
            "versions": parsers.PARSER_VERSIONS,
            "default": parsers.DEFAULT_VERSION,
            "fields": parsers.PARSER_FIELDS,
            "retention_strategies": list(RETENTION_STRATEGIES),
        }

    # ---------------------------- sources ----------------------------
    @app.post("/api/sources", status_code=201)
    def create_source(body: SourceIn):
        return service.create_source(
            body.name, body.parser_version, body.retention_max,
            body.retention_strategy, body.protect_corrupt,
        )

    @app.get("/api/sources")
    def list_sources():
        return service.list_sources()

    @app.get("/api/sources/{source_id}")
    def get_source(source_id: int):
        return service.get_source(source_id)

    # ---------------------------- ingest -----------------------------
    @app.post("/api/sources/{source_id}/responses", status_code=201)
    def ingest(source_id: int, body: IngestIn):
        return service.ingest(source_id, body.payload)

    @app.get("/api/sources/{source_id}/snapshots")
    def list_snapshots(source_id: int):
        return service.list_snapshots(source_id)

    # --------------------------- snapshots ---------------------------
    @app.get("/api/snapshots/{snapshot_id}")
    def get_snapshot(snapshot_id: int):
        return service.get_snapshot(snapshot_id)

    @app.get("/api/snapshots/{snapshot_id}/payload")
    def get_payload(snapshot_id: int):
        return {"snapshot_id": snapshot_id, "payload": service.read_payload(snapshot_id)}

    @app.post("/api/snapshots/{snapshot_id}/reparse")
    def reparse(snapshot_id: int, body: ReparseIn | None = None):
        version = body.parser_version if body else None
        return service.reparse(snapshot_id, parser_version=version)

    @app.get("/api/snapshots/{snapshot_id}/runs")
    def list_runs(snapshot_id: int):
        return service.list_runs(snapshot_id)

    # ------------------------------ refs -----------------------------
    @app.get("/api/snapshots/{snapshot_id}/refs")
    def list_snapshot_refs(snapshot_id: int):
        return service.list_refs(snapshot_id=snapshot_id)

    @app.post("/api/snapshots/{snapshot_id}/refs", status_code=201)
    def add_ref(snapshot_id: int, body: RefIn):
        return service.add_ref(snapshot_id, body.ref_type, body.ref_key, body.note)

    @app.delete("/api/refs/{ref_id}")
    def release_ref(ref_id: int):
        return service.release_ref(ref_id)

    @app.get("/api/sources/{source_id}/refs")
    def list_source_refs(source_id: int):
        return service.list_refs(source_id=source_id)

    # ---------------------------- retention --------------------------
    @app.put("/api/sources/{source_id}/retention")
    def set_retention(source_id: int, body: RetentionIn):
        return service.set_retention(
            source_id, body.max_snapshots, body.strategy, body.protect_corrupt
        )

    @app.get("/api/sources/{source_id}/cleanup-candidates")
    def cleanup_candidates(source_id: int):
        return service.cleanup_candidates(source_id)

    @app.post("/api/sources/{source_id}/cleanup")
    def apply_cleanup(source_id: int):
        return service.apply_cleanup(source_id)

    # ------------------------- parser versions -----------------------
    @app.post("/api/sources/{source_id}/parser/upgrade")
    def upgrade_parser(source_id: int, body: ParserChangeIn):
        return service.upgrade_parser(source_id, body.to_version, body.reason)

    @app.post("/api/sources/{source_id}/parser/rollback")
    def rollback_parser(source_id: int, body: ParserChangeIn):
        return service.rollback_parser(source_id, body.to_version, body.reason)

    @app.get("/api/sources/{source_id}/parser/history")
    def parser_history(source_id: int):
        return service.parser_history(source_id)

    # --------------------------- reparse jobs ------------------------
    @app.post("/api/sources/{source_id}/reparse-jobs", status_code=201)
    def create_job(source_id: int, body: JobIn):
        job = service.create_reparse_job(source_id, body.parser_version, body.batch_size)
        try:
            return service.run_reparse_job(job["id"], body.simulate_interrupt_after)
        except JobInterrupted:
            return service.get_job(job["id"])

    @app.get("/api/sources/{source_id}/reparse-jobs")
    def list_jobs(source_id: int):
        return service.list_jobs(source_id)

    @app.get("/api/reparse-jobs/{job_id}")
    def get_job(job_id: int):
        return service.get_job(job_id)

    @app.post("/api/reparse-jobs/{job_id}/resume")
    def resume_job(job_id: int, body: ResumeIn | None = None):
        interrupt = body.simulate_interrupt_after if body else None
        try:
            return service.resume_reparse_job(job_id, interrupt_after=interrupt)
        except JobInterrupted:
            return service.get_job(job_id)

    return app


app = create_app()
