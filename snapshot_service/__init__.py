"""来源快照服务 — source snapshot service.

Stores a hash, compressed payload and parse result for every upstream
response, supports re-parsing snapshots with field-level diffs, retention
cleanup that never touches audit/replay-referenced versions, and resumable
batch re-parse jobs (断点续跑).
"""

__version__ = "1.0.0"
