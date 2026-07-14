"""HBase 消息持久化层：按 RowKey 存储消息主体。

MVP 使用 LocalHBaseMessageStore（JSON 文件模拟 Region）；生产环境替换为 HBase 客户端。
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import List, Optional, Protocol, Tuple

from src.stores.data_queue.models import QueueMessage, SubQueueType, build_row_key
from src.stores.data_queue.topic import topic_from_storage_key, topic_to_storage_key


class HBaseMessageStore(Protocol):
    """HBase 消息表协议。"""

    def put(self, message: QueueMessage) -> None: ...

    def scan_partition(
        self,
        topic: str,
        partition: int,
        sub_queue: SubQueueType,
        after_offset: int,
        limit: int,
    ) -> List[QueueMessage]: ...

    def delete(self, topic: str, row_key: str) -> None: ...

    def pending_count(self, topic: str) -> int: ...

    def list_topics(self, topic_prefix: str) -> List[str]: ...


class LocalHBaseMessageStore:
    """MVP：本地 JSON 文件模拟 HBase 预分区表。"""

    def __init__(self, base_dir: Path, *, partition_width: int = 2, offset_width: int = 10) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)
        self._partition_width = partition_width
        self._offset_width = offset_width
        self._lock = threading.RLock()

    def _topic_dir(self, topic: str) -> Path:
        d = self._base / topic_to_storage_key(topic)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _path(self, topic: str, row_key: str) -> Path:
        return self._topic_dir(topic) / f"{row_key}.json"

    def put(self, message: QueueMessage) -> None:
        with self._lock:
            self._path(message.topic, message.row_key).write_text(
                json.dumps(message.to_row(), ensure_ascii=False), encoding="utf-8",
            )

    def scan_partition(
        self,
        topic: str,
        partition: int,
        sub_queue: SubQueueType,
        after_offset: int,
        limit: int,
    ) -> List[QueueMessage]:
        prefix = build_row_key(
            sub_queue, partition, 0,
            partition_width=self._partition_width,
            offset_width=self._offset_width,
        )[: len(sub_queue.value) + self._partition_width]
        rows: List[QueueMessage] = []
        topic_dir = self._topic_dir(topic)
        if not topic_dir.exists():
            return rows
        candidates: List[Tuple[str, QueueMessage]] = []
        with self._lock:
            for p in topic_dir.glob(f"{prefix}*.json"):
                msg = QueueMessage.from_row(json.loads(p.read_text(encoding="utf-8")))
                if msg.offset > after_offset:
                    candidates.append((msg.row_key, msg))
        candidates.sort(key=lambda x: x[0])
        for _, msg in candidates[:limit]:
            rows.append(msg)
        return rows

    def delete(self, topic: str, row_key: str) -> None:
        with self._lock:
            p = self._path(topic, row_key)
            if p.exists():
                p.unlink()

    def pending_count(self, topic: str) -> int:
        topic_dir = self._topic_dir(topic)
        if not topic_dir.exists():
            return 0
        return len(list(topic_dir.glob("*.json")))

    def list_topics(self, topic_prefix: str) -> List[str]:
        enc_prefix = topic_to_storage_key(topic_prefix)
        topics: List[str] = []
        if not self._base.exists():
            return topics
        for p in self._base.iterdir():
            if not p.is_dir() or not p.name.startswith(f"{enc_prefix}_"):
                continue
            try:
                topics.append(topic_from_storage_key(p.name, topic_prefix))
            except ValueError:
                continue
        return sorted(topics)
