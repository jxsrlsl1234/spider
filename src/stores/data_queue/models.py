"""DataQueue 领域模型：RowKey、子队列类型、消息体。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

from src.domain.link import Link


class SubQueueType(str, Enum):
    """子队列标识。消费时 retry 优先于 normal。"""

    RETRY = "retry"
    NORMAL = "normal"

    @classmethod
    def consume_order(cls) -> list["SubQueueType"]:
        return [cls.RETRY, cls.NORMAL]


def build_row_key(
    sub_queue: SubQueueType,
    partition: int,
    offset: int,
    *,
    partition_width: int = 2,
    offset_width: int = 10,
) -> str:
    """RowKey = 子队列标识 + 分区编号 + 全局递增偏移量。"""
    return f"{sub_queue.value}{partition:0{partition_width}d}{offset:0{offset_width}d}"


def parse_row_key(
    row_key: str,
    *,
    partition_width: int = 2,
    offset_width: int = 10,
) -> tuple[SubQueueType, int, int]:
    if row_key.startswith(SubQueueType.RETRY.value):
        sub = SubQueueType.RETRY
        sub_len = len(SubQueueType.RETRY.value)
    elif row_key.startswith(SubQueueType.NORMAL.value):
        sub = SubQueueType.NORMAL
        sub_len = len(SubQueueType.NORMAL.value)
    else:
        raise ValueError(f"unknown row_key prefix: {row_key!r}")
    part = int(row_key[sub_len : sub_len + partition_width])
    off = int(row_key[sub_len + partition_width :])
    return sub, part, off


@dataclass
class LinkQueueItem:
    """DataQueue 中的标准任务单元。"""

    context_id: str
    link: Link
    priority: float = 0.0
    enqueued_at: float = field(default_factory=time.time)
    row_key: Optional[str] = None
    topic: Optional[str] = None

    def to_payload(self) -> Dict[str, Any]:
        return {
            "context_id": self.context_id,
            "link": self.link.to_dict(),
            "priority": self.priority,
            "enqueued_at": self.enqueued_at,
            "topic": self.topic,
        }

    @classmethod
    def from_payload(
        cls,
        data: Dict[str, Any],
        *,
        row_key: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> "LinkQueueItem":
        return cls(
            context_id=data["context_id"],
            link=Link.from_dict(data["link"]),
            priority=float(data.get("priority", 0.0)),
            enqueued_at=float(data.get("enqueued_at", time.time())),
            row_key=row_key,
            topic=topic or data.get("topic"),
        )


@dataclass
class QueueMessage:
    """HBase 中持久化的消息行。"""

    row_key: str
    topic: str
    sub_queue: SubQueueType
    partition: int
    offset: int
    payload: Dict[str, Any]
    created_at: float = field(default_factory=time.time)

    def to_row(self) -> Dict[str, Any]:
        return {
            "row_key": self.row_key,
            "topic": self.topic,
            "sub_queue": self.sub_queue.value,
            "partition": self.partition,
            "offset": self.offset,
            "payload": self.payload,
            "created_at": self.created_at,
        }

    @classmethod
    def from_row(cls, data: Dict[str, Any]) -> "QueueMessage":
        return cls(
            row_key=data["row_key"],
            topic=data["topic"],
            sub_queue=SubQueueType(data["sub_queue"]),
            partition=int(data["partition"]),
            offset=int(data["offset"]),
            payload=dict(data["payload"]),
            created_at=float(data.get("created_at", time.time())),
        )
