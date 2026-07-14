"""领域模型：Seed / Link / CrawlContext / URL 工具。"""

from src.domain.context import ContextNode, ContextStatus, CrawlContext, new_context_id
from src.domain.link import Link, LinkSourceType, LinkSubmitResult

__all__ = [
    "Link",
    "LinkSourceType",
    "LinkSubmitResult",
    "CrawlContext",
    "ContextNode",
    "ContextStatus",
    "new_context_id",
]
