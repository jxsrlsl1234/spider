"""通用消息队列抽象。

- FileMessageQueue：跨进程 SeedCollectMq（本地目录）
- 抓取任务使用独立的 DataQueue（src/stores/data_queue/）
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Protocol


class MessageQueue(Protocol):
    def publish(self, topic: str, message: Dict[str, Any]) -> None: ...

    def consume(self, topic: str, max_messages: int = 100) -> List[Dict[str, Any]]: ...

    def pending(self, topic: str) -> int: ...


class FileMessageQueue:
    """每个消息一个 JSON 文件；按文件名排序消费（跨进程）。"""

    def __init__(self, base_dir: Path) -> None:
        self._base = Path(base_dir)
        self._base.mkdir(parents=True, exist_ok=True)
        self.published = 0
        self.consumed = 0

    def _topic_dir(self, topic: str) -> Path:
        safe = topic.replace("/", "_").replace(":", "_")
        d = self._base / safe
        d.mkdir(parents=True, exist_ok=True)
        return d

    def publish(self, topic: str, message: Dict[str, Any]) -> None:
        d = self._topic_dir(topic)
        name = f"{time.time_ns():020d}_{uuid.uuid4().hex[:8]}.json"
        path = d / name
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(message, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
        self.published += 1

    def consume(self, topic: str, max_messages: int = 100) -> List[Dict[str, Any]]:
        d = self._topic_dir(topic)
        out: List[Dict[str, Any]] = []
        files = sorted(p for p in d.glob("*.json") if not p.name.endswith(".tmp"))
        for path in files:
            if len(out) >= max_messages:
                break
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                path.unlink(missing_ok=True)
                out.append(data)
                self.consumed += 1
            except (json.JSONDecodeError, OSError):
                continue
        return out

    def pending(self, topic: str) -> int:
        d = self._topic_dir(topic)
        if not d.exists():
            return 0
        return len([p for p in d.glob("*.json") if not p.name.endswith(".tmp")])
