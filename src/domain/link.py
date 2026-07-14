"""标准化 Link 对象。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class LinkSourceType(str, Enum):
    SITEMAP = "sitemap"            # 种子 sitemap 优先扩链入口
    SEED = "seed"
    SAME_DOMAIN = "same_domain"
    CROSS_DOMAIN = "cross_domain"
    ADMITTED_DOMAIN = "admitted_domain"


@dataclass
class Link:
    """LinkScheduler 输出的标准化链接对象。"""

    url: str
    domain: str
    depth: int = 0
    source_domain: Optional[str] = None
    source_url: Optional[str] = None
    anchor: str = ""
    position: str = "content"
    source_type: LinkSourceType = LinkSourceType.SEED
    priority: float = 0.5
    seed_meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "domain": self.domain,
            "depth": self.depth,
            "source_domain": self.source_domain,
            "source_url": self.source_url,
            "anchor": self.anchor,
            "position": self.position,
            "source_type": self.source_type.value,
            "priority": self.priority,
            "seed_meta": self.seed_meta,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Link":
        return cls(
            url=data["url"],
            domain=data["domain"],
            depth=int(data.get("depth", 0)),
            source_domain=data.get("source_domain"),
            source_url=data.get("source_url"),
            anchor=data.get("anchor", ""),
            position=data.get("position", "content"),
            source_type=LinkSourceType(data.get("source_type", LinkSourceType.SEED.value)),
            priority=float(data.get("priority", 0.5)),
            seed_meta=dict(data.get("seed_meta") or {}),
        )


@dataclass
class LinkSubmitResult:
    """LinkScheduler 提交结果。"""

    accepted: bool
    context_id: Optional[str] = None
    link: Optional[Link] = None
    reason: str = ""
