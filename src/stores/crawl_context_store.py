"""HBase crawl_context 表存储接口。

全链路上下文统一持久化至此，分配唯一 context_id，支持全流程持续更新。
MVP：LocalCrawlContextStore（JSON 文件模拟 HBase 行）。
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, Optional, Protocol

from src.domain.context import CrawlContext


class CrawlContextStore(Protocol):
    """生产实现：对接 HBase crawl_context 表。"""

    def create(self, context: CrawlContext) -> None: ...

    def get(self, context_id: str) -> Optional[CrawlContext]: ...

    def update(self, context: CrawlContext) -> None: ...

    def save(self, context: CrawlContext) -> None:
        """create 或 update（upsert）。"""
        ...


class LocalCrawlContextStore:
    """MVP：本地 JSON 行存储，模拟 HBase crawl_context。"""

    def __init__(self, base_dir: Path) -> None:
        self._dir = Path(base_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, context_id: str) -> Path:
        return self._dir / f"{context_id}.json"

    def create(self, context: CrawlContext) -> None:
        with self._lock:
            p = self._path(context.context_id)
            if p.exists():
                raise ValueError(f"context already exists: {context.context_id}")
            p.write_text(json.dumps(context.to_row(), ensure_ascii=False), encoding="utf-8")

    def get(self, context_id: str) -> Optional[CrawlContext]:
        p = self._path(context_id)
        if not p.exists():
            return None
        return CrawlContext.from_row(json.loads(p.read_text(encoding="utf-8")))

    def update(self, context: CrawlContext) -> None:
        with self._lock:
            self._path(context.context_id).write_text(
                json.dumps(context.to_row(), ensure_ascii=False), encoding="utf-8"
            )

    def save(self, context: CrawlContext) -> None:
        if self.get(context.context_id) is None:
            self.create(context)
        else:
            self.update(context)
