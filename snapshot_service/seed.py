"""Seed a demo database: `python -m snapshot_service.seed [db_path]`."""
from __future__ import annotations

import json
import sys

from .service import SnapshotService

SAMPLES = [
    {"title": "  无线 耳机  ", "price": "¥199.00", "tags": "Audio, electronics ,AUDIO",
     "sku": "a-123", "stock": "17"},
    {"title": "机械键盘", "price": "¥349.50", "tags": "Keyboard, Computers",
     "sku": "kb-77", "stock": "0"},
    {"title": "USB-C 数据线", "price": "$9.99", "tags": "cable,usb", "sku": "cb-01",
     "stock": "230"},
    {"title": "降噪耳机 Pro", "price": "¥899.00", "tags": "Audio, Flagship",
     "sku": "a-999", "stock": "5"},
    "这不是 JSON，会进入 parse_error 状态",
    {"title": "显示器 27寸", "price": "¥1299.00", "tags": "Monitor, Computers",
     "sku": "mn-27", "stock": "42"},
]


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else "snapshots.db"
    svc = SnapshotService(db_path)

    src = svc.create_source("demo-shop", parser_version="v1", retention_max=20)
    sid = src["id"]
    snap_ids = []
    for sample in SAMPLES:
        payload = sample if isinstance(sample, str) else json.dumps(sample, ensure_ascii=False)
        snap_ids.append(svc.ingest(sid, payload)["id"])

    # pin the first snapshot for an audit case and a replay session
    svc.add_ref(snap_ids[0], "audit", "AUDIT-2026-041", note="季度合规审计")
    svc.add_ref(snap_ids[0], "replay", "replay-session-7", note="故障复盘重放")

    # a second source already on v2 for comparison
    svc.create_source("partner-api", parser_version="v2", retention_max=50)

    print(f"seeded {db_path}: source {sid} with {len(snap_ids)} snapshots "
          f"(1 pinned by audit+replay)")


if __name__ == "__main__":
    main()
