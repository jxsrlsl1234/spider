"""Redis 元数据层：分布式锁、偏移量、分区分配、内存 Buffer、按域名消费限速。

偏移量、分区轮询、消费位点均按 Topic（domain 独立）隔离。
消费令牌桶亦按 Topic 独立（默认 QPS + domain_qps 覆盖）。
MVP 使用 LocalRedisMetaStore（内存模拟）；生产环境替换为真实 Redis 客户端。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Mapping, Optional, Protocol, Set, Tuple

from src.stores.data_queue.models import QueueMessage, SubQueueType
from src.stores.data_queue.topic import build_topic, parse_domain


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

    def rate_for_topic(self, topic: str) -> float: ...

    def set_domain_qps(self, domain: str, qps: float) -> None: ...

    def set_domain_qps_map(self, mapping: Mapping[str, float]) -> None: ...


class LocalRedisMetaStore:
    """MVP：内存模拟 Redis；partition / produce offset / consume offset / 令牌桶均 per-topic。"""

    def __init__(
        self,
        *,
        partition_count: int = 16,
        buffer_capacity: int = 1000,
        consume_rate_per_second: float = 2.0,
        domain_qps: Optional[Mapping[str, float]] = None,
        topic_prefix: str = "crawl.link.task",
    ) -> None:
        self._partition_count = partition_count
        self._buffer_capacity = buffer_capacity
        self._default_rate = float(consume_rate_per_second)
        self._domain_qps: Dict[str, float] = {
            k.lower().strip(): float(v) for k, v in (domain_qps or {}).items()
        }
        self._topic_prefix = topic_prefix
        self._lock = threading.RLock()
        self._locks: Dict[str, float] = {}
        self._partition_rr: Dict[str, int] = {}
        self._produce_offset: Dict[str, int] = {}
        self._consume_offsets: Dict[Tuple[str, int, str], int] = {}
        self._active_topics: Set[str] = set()
        self._buffers: Dict[str, Deque[BufferEntry]] = {}
        self._buffered_keys: Dict[str, Set[str]] = {}
        self._tokens: Dict[str, float] = {}
        self._last_refill: Dict[str, float] = {}

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

    def rate_for_topic(self, topic: str) -> float:
        try:
            domain = parse_domain(topic, self._topic_prefix).lower()
        except ValueError:
            return self._default_rate
        if domain in self._domain_qps:
            return float(self._domain_qps[domain])
        return self._default_rate

    def set_domain_qps(self, domain: str, qps: float) -> None:
        key = domain.lower().strip()
        with self._lock:
            self._domain_qps[key] = float(qps)
            topic = build_topic(key, self._topic_prefix)
            if topic in self._tokens:
                rate = self.rate_for_topic(topic)
                if rate <= 0:
                    self._tokens[topic] = 0.0
                else:
                    self._tokens[topic] = min(rate, self._tokens[topic])

    def set_domain_qps_map(self, mapping: Mapping[str, float]) -> None:
        for domain, qps in mapping.items():
            self.set_domain_qps(domain, qps)

    def _refill_tokens(self, topic: str) -> None:
        rate = self.rate_for_topic(topic)
        now = time.monotonic()
        if topic not in self._tokens:
            # 懒创建：初始填满一桶；qps<=0 表示暂停出队
            self._tokens[topic] = 0.0 if rate <= 0 else rate
            self._last_refill[topic] = now
            return
        if rate <= 0:
            self._tokens[topic] = 0.0
            self._last_refill[topic] = now
            return
        elapsed = now - self._last_refill.get(topic, now)
        if elapsed > 0:
            self._tokens[topic] = min(rate, self._tokens[topic] + elapsed * rate)
            self._last_refill[topic] = now

    def pop_buffer(self, topic: str, max_items: int) -> List[BufferEntry]:
        out: List[BufferEntry] = []
        with self._lock:
            self._refill_tokens(topic)
            buf = self._buffers.get(topic)
            if not buf:
                return out
            while buf and len(out) < max_items and self._tokens.get(topic, 0.0) >= 1.0:
                out.append(buf.popleft())
                self._tokens[topic] = self._tokens.get(topic, 0.0) - 1.0
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
