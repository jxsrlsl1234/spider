"""文件锁 + JSON 持久化的 Redis 元数据，供多进程 DataQueue 共享。"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Deque, Dict, List, Set

from src.util.logging_conf import get_logger, log
from src.stores.data_queue.models import SubQueueType
from src.stores.data_queue.redis_meta import BufferEntry

logger = get_logger("file_redis_meta")


class FileRedisMetaStore:
    """MVP 多进程：偏移量/Topic/锁文件持久化到 base_dir。"""

    def __init__(
        self,
        base_dir: Path,
        *,
        partition_count: int = 16,
        buffer_capacity: int = 1000,
        consume_rate_per_second: float = 200.0,
    ) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)
        self._state_path = self._base / "state.json"
        self._lock_dir = self._base / "locks"
        self._lock_dir.mkdir(parents=True, exist_ok=True)
        self._partition_count = partition_count
        self._buffer_capacity = buffer_capacity
        self._rate = consume_rate_per_second
        self._buffers: Dict[str, Deque[BufferEntry]] = {}
        self._buffered_keys: Dict[str, Set[str]] = {}
        self._tokens = consume_rate_per_second
        self._last_refill = time.monotonic()
        # 进程内并发 publish/ack 会同时改 state.json，必须串行
        self._state_mu = threading.Lock()

    @property
    def partition_count(self) -> int:
        return self._partition_count

    def _load_state(self) -> dict:
        if not self._state_path.exists():
            return {
                "partition_rr": {},
                "produce_offset": {},
                "consume_offsets": {},
                "active_topics": [],
            }
        try:
            return json.loads(self._state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            log(logger, 40, "redis_meta_load_error", error=repr(exc))
            return {
                "partition_rr": {},
                "produce_offset": {},
                "consume_offsets": {},
                "active_topics": [],
            }

    def _save_state(self, state: dict) -> None:
        """原子写：唯一 tmp → replace，避免并发写同一 state.tmp 导致 FileNotFoundError。"""
        self._base.mkdir(parents=True, exist_ok=True)
        tmp = self._base / f"state.{os.getpid()}.{uuid.uuid4().hex}.tmp"
        try:
            tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self._state_path)
        except OSError:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _mutate_state(self, fn):  # noqa: ANN001
        with self._state_mu:
            state = self._load_state()
            result = fn(state)
            self._save_state(state)
            return result

    def acquire_lock(self, key: str, timeout: float = 5.0) -> bool:
        safe = key.replace("/", "_").replace(":", "_")
        lock_path = self._lock_dir / f"{safe}.lock"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                fd = open(lock_path, "x", encoding="utf-8")
                fd.write(str(time.time()))
                fd.close()
                return True
            except FileExistsError:
                try:
                    age = time.time() - lock_path.stat().st_mtime
                    if age > timeout * 2:
                        lock_path.unlink(missing_ok=True)
                        continue
                except OSError:
                    pass
                time.sleep(0.01)
        return False

    def release_lock(self, key: str) -> None:
        safe = key.replace("/", "_").replace(":", "_")
        lock_path = self._lock_dir / f"{safe}.lock"
        lock_path.unlink(missing_ok=True)

    def next_partition(self, topic: str) -> int:
        def _fn(state: dict) -> int:
            rr = state.setdefault("partition_rr", {})
            cur = int(rr.get(topic, 0))
            p = cur % self._partition_count
            rr[topic] = cur + 1
            return p

        return int(self._mutate_state(_fn))

    def next_offset(self, topic: str) -> int:
        def _fn(state: dict) -> int:
            offs = state.setdefault("produce_offset", {})
            nxt = int(offs.get(topic, 0)) + 1
            offs[topic] = nxt
            return nxt

        return int(self._mutate_state(_fn))

    def get_max_offset(self, topic: str) -> int:
        with self._state_mu:
            state = self._load_state()
            return int(state.get("produce_offset", {}).get(topic, 0))

    def get_consume_offset(self, topic: str, partition: int, sub_queue: SubQueueType) -> int:
        with self._state_mu:
            state = self._load_state()
            key = f"{topic}|{partition}|{sub_queue.value}"
            return int(state.get("consume_offsets", {}).get(key, 0))

    def set_consume_offset(self, topic: str, partition: int, sub_queue: SubQueueType, offset: int) -> None:
        def _fn(state: dict) -> None:
            offs = state.setdefault("consume_offsets", {})
            key = f"{topic}|{partition}|{sub_queue.value}"
            offs[key] = offset

        self._mutate_state(_fn)

    def register_topic(self, topic: str) -> None:
        def _fn(state: dict) -> None:
            topics = set(state.get("active_topics") or [])
            topics.add(topic)
            state["active_topics"] = sorted(topics)

        self._mutate_state(_fn)

    def list_topics(self) -> List[str]:
        with self._state_mu:
            state = self._load_state()
            return sorted(state.get("active_topics") or [])

    def push_buffer(self, topic: str, entry: BufferEntry) -> bool:
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
        self._refill_tokens()
        buf = self._buffers.get(topic)
        if not buf:
            return out
        while buf and len(out) < max_items and self._tokens >= 1.0:
            out.append(buf.popleft())
            self._tokens -= 1.0
        return out

    def buffer_size(self, topic: str) -> int:
        return len(self._buffers.get(topic, []))

    def buffer_capacity(self) -> int:
        return self._buffer_capacity

    def is_buffered(self, topic: str, row_key: str) -> bool:
        return row_key in self._buffered_keys.get(topic, set())

    def release_buffered(self, topic: str, row_key: str) -> None:
        self._buffered_keys.get(topic, set()).discard(row_key)
