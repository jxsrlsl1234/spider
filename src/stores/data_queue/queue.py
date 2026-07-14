"""DataQueue：平台自研消息队列（Redis + HBase）。

Topic 由 domain 拼接（如 crawl.link.task.example_com），偏移量/分区/消费位点均 per-domain 隔离。
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Protocol

from src.util.logging_conf import get_logger, log
from src.stores.data_queue.file_redis_meta import FileRedisMetaStore
from src.stores.data_queue.hbase_store import HBaseMessageStore, LocalHBaseMessageStore
from src.stores.data_queue.models import (
    LinkQueueItem,
    QueueMessage,
    SubQueueType,
    build_row_key,
    parse_row_key,
)
from src.stores.data_queue.redis_meta import BufferEntry, LocalRedisMetaStore, RedisMetaStore
from src.stores.data_queue.topic import build_topic, parse_domain

logger = get_logger("data_queue")


class DataQueue(Protocol):
    """自研 DataQueue 协议。"""

    def topic_for_domain(self, domain: str) -> str: ...

    def publish(
        self,
        item: LinkQueueItem,
        *,
        topic: Optional[str] = None,
        sub_queue: SubQueueType = SubQueueType.NORMAL,
    ) -> str: ...

    def consume(self, max_items: int = 1, *, topic: str) -> List[LinkQueueItem]: ...

    def consume_any(self, max_items: int = 1, *, topics: Optional[List[str]] = None) -> List[LinkQueueItem]: ...

    def retry(self, item: LinkQueueItem, *, topic: Optional[str] = None) -> str: ...

    def ack(self, row_key: str, *, topic: str) -> None: ...

    def pending(self, *, topic: Optional[str] = None) -> int: ...

    def list_topics(self) -> List[str]: ...


class MvpDataQueue:
    """MVP DataQueue：FileRedisMetaStore + LocalHBaseMessageStore（多进程共享）。"""

    def __init__(
        self,
        *,
        base_dir: Path,
        topic_prefix: str = "crawl.link.task",
        partition_count: int = 16,
        partition_width: int = 2,
        offset_width: int = 10,
        buffer_capacity: int = 1000,
        consume_rate_per_second: float = 200.0,
        redis_meta: Optional[RedisMetaStore] = None,
        hbase_store: Optional[HBaseMessageStore] = None,
    ) -> None:
        self._topic_prefix = topic_prefix
        self._partition_width = partition_width
        self._offset_width = offset_width
        base = Path(base_dir)
        self._redis: RedisMetaStore = redis_meta or FileRedisMetaStore(
            base / "_redis_meta",
            partition_count=partition_count,
            buffer_capacity=buffer_capacity,
            consume_rate_per_second=consume_rate_per_second,
        )
        self._hbase: HBaseMessageStore = hbase_store or LocalHBaseMessageStore(
            base, partition_width=partition_width, offset_width=offset_width,
        )
        self.published = 0
        self.consumed = 0
        self.retried = 0
        self.acked = 0

    def topic_for_domain(self, domain: str) -> str:
        return build_topic(domain, self._topic_prefix)

    def _resolve_topic(self, item: LinkQueueItem, topic: Optional[str]) -> str:
        if topic:
            return topic
        if item.topic:
            return item.topic
        return self.topic_for_domain(item.link.domain)

    def publish(
        self,
        item: LinkQueueItem,
        *,
        topic: Optional[str] = None,
        sub_queue: SubQueueType = SubQueueType.NORMAL,
    ) -> str:
        topic = self._resolve_topic(item, topic)
        item.topic = topic
        lock_key = f"dq:publish:{topic}"
        if not self._redis.acquire_lock(lock_key):
            raise RuntimeError(f"failed to acquire publish lock: {topic}")

        try:
            self._redis.register_topic(topic)
            partition = self._redis.next_partition(topic)
            offset = self._redis.next_offset(topic)
            row_key = build_row_key(
                sub_queue, partition, offset,
                partition_width=self._partition_width,
                offset_width=self._offset_width,
            )
            message = QueueMessage(
                row_key=row_key,
                topic=topic,
                sub_queue=sub_queue,
                partition=partition,
                offset=offset,
                payload=item.to_payload(),
            )
            self._hbase.put(message)
            item.row_key = row_key
            self.published += 1
            domain = parse_domain(topic, self._topic_prefix)
            log(
                logger, 20, "dq_publish",
                topic=topic, domain=domain, row_key=row_key,
                sub_queue=sub_queue.value, offset=offset,
                max_offset=self._redis.get_max_offset(topic),
            )
            return row_key
        finally:
            self._redis.release_lock(lock_key)

    def consume(self, max_items: int = 1, *, topic: str) -> List[LinkQueueItem]:
        lock_key = f"dq:consume:{topic}"
        if not self._redis.acquire_lock(lock_key):
            return []

        try:
            self._refill_buffer(topic)
            entries = self._redis.pop_buffer(topic, max_items)
            items: List[LinkQueueItem] = []
            for entry in entries:
                item = LinkQueueItem.from_payload(
                    entry.message.payload, row_key=entry.row_key, topic=topic,
                )
                items.append(item)
                self.consumed += 1
            return items
        finally:
            self._redis.release_lock(lock_key)

    def consume_any(self, max_items: int = 1, *, topics: Optional[List[str]] = None) -> List[LinkQueueItem]:
        """跨 domain 拉取：轮询各 Topic，每个 domain 偏移量独立。"""
        candidates = topics or self.list_topics()
        if not candidates:
            return []

        items: List[LinkQueueItem] = []
        rounds = 0
        max_rounds = len(candidates) * max(max_items, 1) + 1
        idx = 0
        while len(items) < max_items and rounds < max_rounds:
            topic = candidates[idx % len(candidates)]
            got = self.consume(max_items=1, topic=topic)
            if got:
                items.extend(got)
            idx += 1
            rounds += 1
        return items[:max_items]

    def _refill_buffer(self, topic: str) -> None:
        if self._redis.buffer_size(topic) >= self._redis.buffer_capacity() // 2:
            return

        partition_count = getattr(self._redis, "partition_count", 16)
        for partition in range(partition_count):
            for sub_queue in SubQueueType.consume_order():
                if self._redis.buffer_size(topic) >= self._redis.buffer_capacity():
                    return
                after = self._redis.get_consume_offset(topic, partition, sub_queue)
                messages = self._hbase.scan_partition(
                    topic, partition, sub_queue, after_offset=after, limit=32,
                )
                for msg in messages:
                    if self._redis.is_buffered(topic, msg.row_key):
                        continue
                    if not self._redis.push_buffer(topic, BufferEntry(row_key=msg.row_key, message=msg)):
                        return

    def retry(self, item: LinkQueueItem, *, topic: Optional[str] = None) -> str:
        topic = self._resolve_topic(item, topic)
        if item.row_key:
            self.ack(item.row_key, topic=topic)
        row_key = self.publish(item, topic=topic, sub_queue=SubQueueType.RETRY)
        self.retried += 1
        log(logger, 20, "dq_retry", topic=topic, row_key=row_key, context_id=item.context_id)
        return row_key

    def ack(self, row_key: str, *, topic: str) -> None:
        self._hbase.delete(topic, row_key)
        self._redis.release_buffered(topic, row_key)
        sub, partition, offset = parse_row_key(
            row_key,
            partition_width=self._partition_width,
            offset_width=self._offset_width,
        )
        current = self._redis.get_consume_offset(topic, partition, sub)
        if offset > current:
            self._redis.set_consume_offset(topic, partition, sub, offset)
        self.acked += 1

    def list_topics(self) -> List[str]:
        redis_topics = set(self._redis.list_topics())
        hbase_topics = set(self._hbase.list_topics(self._topic_prefix))
        return sorted(redis_topics | hbase_topics)

    def pending(self, *, topic: Optional[str] = None) -> int:
        if topic:
            return self._hbase.pending_count(topic) + self._redis.buffer_size(topic)
        return sum(
            self._hbase.pending_count(t) + self._redis.buffer_size(t)
            for t in self.list_topics()
        )
