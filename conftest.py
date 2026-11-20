import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from snapshot_service.service import SnapshotService  # noqa: E402

SAMPLE = {
    "title": "  无线  耳机  ",
    "price": "¥199.00",
    "tags": "Audio, electronics ,AUDIO",
    "sku": "a-123",
    "stock": "17",
}
SAMPLE_PAYLOAD = json.dumps(SAMPLE, ensure_ascii=False)


@pytest.fixture()
def service(tmp_path):
    return SnapshotService(str(tmp_path / "test.db"))


@pytest.fixture()
def source(service):
    return service.create_source("demo", parser_version="v1", retention_max=100)


def ingest_n(service, source_id, n, start=0):
    """Ingest n distinct valid payloads; return their snapshot ids."""
    ids = []
    for i in range(start, start + n):
        payload = json.dumps({**SAMPLE, "sku": f"sku-{i}", "title": f"商品 {i}"},
                             ensure_ascii=False)
        ids.append(service.ingest(source_id, payload)["id"])
    return ids
