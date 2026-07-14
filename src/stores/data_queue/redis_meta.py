"""Redis 元数据层：分布式锁、偏移量、分区分配、内存 Buffer、消费限速。

偏移量、分区轮询、消费位点均按 Topic（domain 独立）隔离。
MVP 使用 LocalRedisMetaStore（内存模拟）；生产环境替换为真实 Redis 客户端。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Protocol, Set, Tuple

from src.stores.data_queue.models import QueueMessage, SubQueueType


@dataclass
class BufferEntry:
    row_key: str
    message: QueueMessage


class RedisMetaStore(Protocol):
    """Redis 元数据管控协议（元数据按 Topic/domain 隔离）。"""

    def acquire_lock(self, key: str, timeout: float = 5.0) -> bool: ...

    def release_lock(self, key: str) -> None: ...

    def next_partition(self, topic: str) -> int: ...

    def next_offset(self, topic: str) -> int: ...

    def get_max_offset(self, topic: str) -> int: ...

    def get_consume_offset(self, topic: str, partition: int, sub_queue: SubQueueType) -> int: ...

    def set_consume_offset(self, topic: str, partition: int, sub_queue: SubQueueType, offset: int) -> None: ...

    def register_topic(self, topic: str) -> None: ...

    def list_topics(self) -> List[str]: ...

    def push_buffer(self, topic: str, entry: BufferEntry) -> bool: ...

    def pop_buffer(self, topic: str, max_items: int) -> List[BufferEntry]: ...

    def buffer_size(self, topic: str) -> int: ...

    def buffer_capacity(self) -> int: ...

    def is_buffered(self, topic: str, row_key: str) -> bool: ...

    def release_buffered(self, topic: str, row_key: str) -> None: ...


class LocalRedisMetaStore:
    """MVP：内存模拟 Redis；partition / produce offset / consume offset 均 per-topic。"""

    def __init__(
        self,
        *,
        partition_count: int = 16,
        buffer_capacity: int = 1000,
        consume_rate_per_second: float = 200.0,
    ) -> None:
        self._partition_count = partition_count
        self._buffer_capacity = buffer_capacity
        self._rate = consume_rate_per_second
        self._lock = threading.RLock()
        self._locks: Dict[str, float] = {}
        self._partition_rr: Dict[str, int] = {}
        self._produce_offset: Dict[str, int] = {}
        self._consume_offsets: Dict[Tuple[str, int, str], int] = {}
        self._active_topics: Set[str] = set()
        self._buffers: Dict[str, Deque[BufferEntry]] = {}
        self._buffered_keys: Dict[str, Set[str]] = {}
        self._tokens = consume_rate_per_second
        self._last_refill = time.monotonic()

    @property
    def partition_count(self) -> int:
        return self._partition_count

    def acquire_lock(self, key: str, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if key not in self._locks:
                    self._locks[key] = time.monotonic()
                    return True
            time.sleep(0.001)
        return False

    def release_lock(self, key: str) -> None:
        with self._lock:
            self._locks.pop(key, None)

    def next_partition(self, topic: str) -> int:
        with self._lock:
            cur = self._partition_rr.get(topic, 0)
            p = cur % self._partition_count
            self._partition_rr[topic] = cur + 1
            return p

    def next_offset(self, topic: str) -> int:
        """生成该 domain（Topic）下的下一个生产偏移量。"""
        with self._lock:
            nxt = self._produce_offset.get(topic, 0) + 1
            self._produce_offset[topic] = nxt
            return nxt

    def get_max_offset(self, topic: str) -> int:
        """该 domain 当前已分配的最大生产偏移量。"""
        with self._lock:
            return self._produce_offset.get(topic, 0)

    def get_consume_offset(self, topic: str, partition: int, sub_queue: SubQueueType) -> int:
        with self._lock:
            return self._consume_offsets.get((topic, partition, sub_queue.value), 0)

    def set_consume_offset(self, topic: str, partition: int, sub_queue: SubQueueType, offset: int) -> None:
        with self._lock:
            self._consume_offsets[(topic, partition, sub_queue.value)] = offset

    def register_topic(self, topic: str) -> None:
        with self._lock:
            self._active_topics.add(topic)

    def list_topics(self) -> List[str]:
        with self._lock:
            return sorted(self._active_topics)

    def push_buffer(self, topic: str, entry: BufferEntry) -> bool:
        with self._lock:
            buf = self._buffers.setdefault(topic, deque())
            keys = self._buffered_keys.setdefault(topic, set())
            if entry.row_key in keys:
                return True
            if len(buf) >= self._buffer_capacity:
                return False
            buf.append(entry)
            keys.add(entry.row_key)
            return True

    def _refill_tokens(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last_refill = now

    def pop_buffer(self, topic: str, max_items: int) -> List[BufferEntry]:
        out: List[BufferEntry] = []
        with self._lock:
            self._refill_tokens()
            buf = self._buffers.get(topic)
            if not buf:
                return out
            while buf and len(out) < max_items and self._tokens >= 1.0:
                out.append(buf.popleft())
                self._tokens -= 1.0
        return out

    def buffer_size(self, topic: str) -> int:
        with self._lock:
            return len(self._buffers.get(topic, []))

    def buffer_capacity(self) -> int:
        return self._buffer_capacity

    def is_buffered(self, topic: str, row_key: str) -> bool:
        with self._lock:
            return row_key in self._buffered_keys.get(topic, set())

    def release_buffered(self, topic: str, row_key: str) -> None:
        with self._lock:
            self._buffered_keys.get(topic, set()).discard(row_key)
