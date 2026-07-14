"""爬取上下文 CrawlContext（持久化至 HBase crawl_context 表）。"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ContextStatus(str, Enum):
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


def new_context_id() -> str:
    return uuid.uuid4().hex


def _now() -> float:
    return time.time()


@dataclass
class ContextNode:
    """全链路单节点记录：入参、出参、状态。"""

    node: str
    status: str
    input: Dict[str, Any] = field(default_factory=dict)
    output: Dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=_now)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node": self.node,
            "status": self.status,
            "input": self.input,
            "output": self.output,
            "ts": self.ts,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ContextNode":
        return cls(
            node=data["node"],
            status=data["status"],
            input=dict(data.get("input") or {}),
            output=dict(data.get("output") or {}),
            ts=float(data.get("ts", _now())),
            error=data.get("error"),
        )


@dataclass
class CrawlContext:
    """爬取全链路上下文。不做内存透传，统一落 HBase，全流程可持续 update。"""

    context_id: str
    link: Dict[str, Any]
    seed_meta: Dict[str, Any] = field(default_factory=dict)
    status: ContextStatus = ContextStatus.CREATED
    nodes: List[ContextNode] = field(default_factory=list)
    business_data: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    def touch(self) -> None:
        self.updated_at = _now()

    def add_node(
        self, node: str, status: str, *,
        input: Optional[Dict[str, Any]] = None,
        output: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        self.nodes.append(
            ContextNode(
                node=node, status=status,
                input=input or {}, output=output or {},
                error=error,
            )
        )
        self.touch()

    def set_business(self, **kwargs: Any) -> None:
        self.business_data.update(kwargs)
        self.touch()

    def to_row(self) -> Dict[str, Any]:
        """对应 HBase crawl_context 表一行。"""
        return {
            "context_id": self.context_id,
            "link": self.link,
            "seed_meta": self.seed_meta,
            "status": self.status.value,
            "nodes": [n.to_dict() for n in self.nodes],
            "business_data": self.business_data,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "CrawlContext":
        return cls(
            context_id=row["context_id"],
            link=dict(row.get("link") or {}),
            seed_meta=dict(row.get("seed_meta") or {}),
            status=ContextStatus(row.get("status", ContextStatus.CREATED.value)),
            nodes=[ContextNode.from_dict(n) for n in row.get("nodes") or []],
            business_data=dict(row.get("business_data") or {}),
            created_at=float(row.get("created_at", _now())),
            updated_at=float(row.get("updated_at", _now())),
        )
