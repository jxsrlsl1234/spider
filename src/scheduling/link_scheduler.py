"""LinkScheduler：URL 预处理 → 标准化 Link → CrawlContext → DataQueue。"""
from __future__ import annotations

from typing import Any, Dict, Optional

from config import Config
from src.crawl.dedup import UrlDedup
from src.domain.context import ContextStatus, CrawlContext, new_context_id
from src.domain.link import Link, LinkSourceType, LinkSubmitResult
from src.util.logging_conf import get_logger, log
from src.stores.seed_store import SeedStore
from src.stores.crawl_context_store import CrawlContextStore
from src.stores.data_queue import DataQueue, LinkQueueItem, build_topic
from src.domain.urls import domain_of_url, normalize_url, url_health_score

logger = get_logger("link_scheduler")


class LinkScheduler:
    """Link 加工模块：去重、过滤、校验 → Link → HBase Context → DataQueue。"""

    def __init__(
        self,
        config: Config,
        seed_store: SeedStore,
        context_store: CrawlContextStore,
        data_queue: DataQueue,
        url_dedup: Optional[UrlDedup] = None,
    ) -> None:
        self._config = config
        self._seeds = seed_store
        self._context_store = context_store
        self._data_queue = data_queue
        self._url_dedup = url_dedup or UrlDedup()
        self._topic_prefix = config.data_queue.topic_prefix
        self.submitted = 0
        self.rejected = 0

    def submit_raw(
        self,
        raw_url: str,
        *,
        source_type: LinkSourceType = LinkSourceType.SEED,
        source_domain: Optional[str] = None,
        source_url: Optional[str] = None,
        depth: int = 0,
        anchor: str = "",
        position: str = "content",
        seed_meta: Optional[Dict[str, Any]] = None,
        parent_context_id: Optional[str] = None,
        priority: Optional[float] = None,
    ) -> LinkSubmitResult:
        """将原始 URL 加工为 Link 并推入 DataQueue。"""
        url = normalize_url(raw_url)
        if not url:
            self.rejected += 1
            return LinkSubmitResult(accepted=False, reason="invalid_url")

        if depth > self._config.schedule.max_depth:
            self.rejected += 1
            return LinkSubmitResult(accepted=False, reason="max_depth")

        if url_health_score(url) < 0.1:
            self.rejected += 1
            return LinkSubmitResult(accepted=False, reason="low_url_health")

        if self._url_dedup.check_and_add(url):
            self.rejected += 1
            return LinkSubmitResult(accepted=False, reason="duplicate_url")

        domain = domain_of_url(url)
        base_priority = self._seeds.seed_weight(domain)
        if priority is not None:
            base_priority = max(base_priority, priority)

        meta = dict(seed_meta or {})
        seed = self._seeds.get_seed(domain)
        if seed:
            meta.setdefault("seed_weight", seed.weight)
            meta.setdefault("seed_status", seed.status.value)

        link = Link(
            url=url,
            domain=domain,
            depth=depth,
            source_domain=source_domain,
            source_url=source_url,
            anchor=anchor,
            position=position,
            source_type=source_type,
            priority=base_priority,
            seed_meta=meta,
        )

        context_id = new_context_id()
        context = CrawlContext(
            context_id=context_id,
            link=link.to_dict(),
            seed_meta=meta,
            status=ContextStatus.CREATED,
        )
        context.add_node(
            "link_scheduler",
            status="success",
            input={"raw_url": raw_url, "parent_context_id": parent_context_id},
            output={"link": link.to_dict()},
        )
        context.status = ContextStatus.QUEUED
        topic = build_topic(domain, self._topic_prefix)
        context.add_node(
            "data_queue",
            status="pending",
            input={"context_id": context_id, "priority": base_priority, "topic": topic, "domain": domain},
        )
        self._context_store.create(context)

        queue_item = LinkQueueItem(context_id=context_id, link=link, priority=base_priority, topic=topic)
        row_key = self._data_queue.publish(queue_item, topic=topic)

        context.nodes[-1].status = "success"
        context.nodes[-1].output = {"topic": topic, "domain": domain, "row_key": row_key}
        context.set_business(queue_item=queue_item.context_id, row_key=row_key, topic=topic, domain=domain)
        self._context_store.update(context)

        self.submitted += 1
        log(logger, 20, "link_submitted", context_id=context_id, url=url, domain=domain, topic=topic, row_key=row_key)
        return LinkSubmitResult(accepted=True, context_id=context_id, link=link)
