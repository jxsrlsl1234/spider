"""离线存储 sink：种子采集全链路元数据持久化。

记录类型：
    - collection_trace : 每次 URL 采集的全链路 CollectionTrace
    - seed_event       : 种子生命周期事件
    - resource         : 资源元数据（html 字段 = 对象存储 URL）
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Protocol

from src.domain.models import CollectionTrace


class OfflineStore(Protocol):
    def emit(self, record_type: str, payload: Dict[str, Any]) -> None: ...
    def record_trace(self, trace: CollectionTrace) -> None: ...
    def close(self) -> None: ...


class JsonlOfflineStore:
    def __init__(self, base_dir: Path) -> None:
        self._base = Path(base_dir)
        self._lock = threading.Lock()
        self._counts: Dict[str, int] = {}

    def _partition_path(self, record_type: str) -> Path:
        dt = datetime.now().strftime("%Y-%m-%d")
        d = self._base / record_type / f"dt={dt}"
        d.mkdir(parents=True, exist_ok=True)
        return d / "part-000.jsonl"

    def emit(self, record_type: str, payload: Dict[str, Any]) -> None:
        line = json.dumps(
            {"_type": record_type, "_ingested_at": time.time(), **payload},
            ensure_ascii=False,
        )
        path = self._partition_path(record_type)
        with self._lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
            self._counts[record_type] = self._counts.get(record_type, 0) + 1

    def record_trace(self, trace: CollectionTrace) -> None:
        self.emit("collection_trace", trace.to_dict())

    def record_seed_event(self, domain: str, event_type: str, **payload: Any) -> None:
        self.emit("seed_event", {"domain": domain, "event_type": event_type, **payload})

    def record_resource(self, payload: Dict[str, Any]) -> None:
        self.emit("resource", payload)

    @property
    def counts(self) -> Dict[str, int]:
        return dict(self._counts)

    def close(self) -> None:
        return None
