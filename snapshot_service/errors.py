"""Shared exception types for the snapshot service."""


class NotFoundError(Exception):
    """Requested entity does not exist."""


class ValidationError(Exception):
    """Request parameters are invalid."""


class SnapshotCorruptError(Exception):
    """Snapshot payload failed decompression or hash verification (坏快照)."""


class JobInterrupted(Exception):
    """Batch re-parse job was interrupted; it can be resumed from its checkpoint."""

    def __init__(self, job_id: int):
        super().__init__(f"reparse job {job_id} interrupted at checkpoint")
        self.job_id = job_id
