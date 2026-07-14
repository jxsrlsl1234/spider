"""TaskScheduler：DataQueue 消费者 — 抓取 URL、存储 HTML、扩链。

职责边界：
- **消费** DataQueue（LinkScheduler 投递的抓取任务）
- **同域扩链** → 回投 LinkScheduler → DataQueue
- **跨域扩链** → 发布到 SeedCollectMq（未经质量验证，由 SeedCollectConsumer 处理）
"""
from __future__ import annotations

from typing import List, Optional

from config import Config
from src.crawl.dedup import ContentDedup
from src.domain.context import ContextStatus, CrawlContext
from src.domain.link import LinkSourceType
from src.seed.evaluator import HeuristicQualityModel
from src.crawl.fetcher import Fetcher
from src.scheduling.link_scheduler import LinkScheduler
from src.util.logging_conf import get_logger, log
from src.domain.models import DiscoveredLink, HtmlPage, RenderMode
from src.crawl.parser import Parser
from src.crawl.robots import RobotsCache
from src.mq.seed_collect import SeedCollectPublisher
from src.stores.seed_store import SeedStore
from src.stores.result_store import ResultStore
from src.stores.crawl_context_store import CrawlContextStore
from src.stores.data_queue import DataQueue, LinkQueueItem

logger = get_logger("task_scheduler")


class TaskScheduler:
    """DataQueue 消费侧：抓取 → 解析 → 存储 → 扩链。"""

    def __init__(
        self,
        config: Config,
        fetcher: Fetcher,
        seed_store: SeedStore,
        link_scheduler: LinkScheduler,
        context_store: CrawlContextStore,
        data_queue: DataQueue,
        *,
        seed_collect_publisher: SeedCollectPublisher,
        result_store: Optional[ResultStore] = None,
        content_dedup: Optional[ContentDedup] = None,
    ) -> None:
        self._config = config
        self._fetcher = fetcher
        self._seeds = seed_store
        self._link_scheduler = link_scheduler
        self._context_store = context_store
        self._data_queue = data_queue
        self._parser = Parser()
        self._robots = RobotsCache(config)
        self._quality = HeuristicQualityModel()
        self._store = result_store
        self._seed_collect_publisher = seed_collect_publisher
        self._pages_done = 0
        self._fetch_ok = 0
        self._fetch_fail = 0

    @property
    def pages_done(self) -> int:
        return self._pages_done

    def poll_tasks(self, max_tasks: int = 1) -> List[LinkQueueItem]:
        """从 DataQueue 拉取抓取任务（retry 优先）。"""
        return self._data_queue.consume_any(max_items=max_tasks)

    async def execute(self, item: LinkQueueItem) -> None:
        """执行单条 DataQueue 任务：抓取全链路。"""
        context = self._context_store.get(item.context_id)
        if context is None:
            log(logger, 30, "context_missing", context_id=item.context_id)
            return

        link = item.link
        self._pages_done += 1
        context.status = ContextStatus.RUNNING
        context.add_node(
            "task_scheduler",
            status="running",
            input={"context_id": item.context_id, "row_key": item.row_key},
        )
        self._context_store.update(context)

        try:
            if not self._robots.allowed(link.domain, link.url):
                context.add_node("fetch", status="skipped", output={"reason": "robots_disallow"})
                context.status = ContextStatus.SKIPPED
                self._context_store.update(context)
                if item.row_key and item.topic:
                    self._data_queue.ack(item.row_key, topic=item.topic)
                return

            seed = self._seeds.get_seed(link.domain)
            render_mode = seed.render_mode if seed else RenderMode.STATIC
            result = await self._fetcher.fetch(link.url, render_mode)
            self._seeds.record_reachability(link.domain, result.ok, result.elapsed)

            context.add_node(
                "fetch",
                status="success" if result.ok else "failed",
                input={"url": link.url, "render_mode": render_mode.value},
                output={
                    "ok": result.ok,
                    "status": result.status,
                    "elapsed": round(result.elapsed, 4),
                    "render_mode": result.render_mode.value,
                    "error": result.error,
                },
            )

            if not result.ok or not result.html:
                self._fetch_fail += 1
                self._seeds.update_weight(link.domain, reward=0.0)
                context.status = ContextStatus.FAILED
                context.set_business(fetch_ok=False)
                self._context_store.update(context)
                self._data_queue.retry(item, topic=item.topic)
                return

            self._fetch_ok += 1
            if result.render_mode != render_mode:
                self._seeds.set_render_mode(link.domain, result.render_mode)

            doc = self._parser.parse(link.url, result.html)
            discovered = self._parser.discovered_links(doc, link.domain)
            context.add_node(
                "parse",
                status="success",
                output={"html_len": len(result.html), "links": len(discovered)},
            )

            self._expand_links(link, discovered, context)

            page = HtmlPage(
                url=link.url, html=result.html, status=result.status,
                content_type=result.content_type,
            )
            page.quality = self._quality.score_html(result.html)
            meta = None
            if self._store is not None:
                meta = self._store.store_html(link.domain, page, item.context_id)
            is_dup = meta is None

            self._seeds.record_production(
                link.domain,
                size=len(result.html.encode("utf-8")),
                content_len=len(result.html),
                quality=page.quality,
                dup=bool(is_dup and self._store is not None),
            )
            reward = 0.2 + 0.8 * page.quality if not is_dup else 0.1
            self._seeds.update_weight(link.domain, reward=reward)

            context.add_node(
                "store",
                status="success",
                output={"quality": page.quality, "reward": reward, "is_dup": is_dup},
            )
            seed_rec = self._seeds.get_seed(link.domain)
            context.set_business(
                fetch_ok=True,
                html_object_url=meta.get("html") if meta else None,
                content_hash=meta.get("content_hash") if meta else None,
                seed_weight=seed_rec.weight if seed_rec else None,
                seed_status=seed_rec.status.value if seed_rec else None,
            )
            context.status = ContextStatus.SUCCESS
            self._context_store.update(context)
            if item.row_key and item.topic:
                self._data_queue.ack(item.row_key, topic=item.topic)

        except Exception as exc:  # noqa: BLE001
            context.add_node("error", status="failed", error=repr(exc))
            context.status = ContextStatus.FAILED
            self._context_store.update(context)
            self._data_queue.retry(item, topic=item.topic)
            log(logger, 40, "task_execute_error", context_id=item.context_id, error=repr(exc))

    def _expand_links(
        self, parent_link, discovered: List[DiscoveredLink], context: CrawlContext,
    ) -> None:
        same_count, cross_count = 0, 0
        mq_published: List[str] = []

        for dl in discovered:
            if dl.same_domain:
                res = self._link_scheduler.submit_raw(
                    dl.url,
                    source_type=LinkSourceType.SAME_DOMAIN,
                    source_domain=parent_link.domain,
                    source_url=parent_link.url,
                    depth=parent_link.depth + 1,
                    anchor=dl.anchor,
                    position=dl.position,
                    parent_context_id=context.context_id,
                )
                if res.accepted:
                    same_count += 1
            else:
                cross_count += 1

        if cross_count:
            from src.domain.models import CollectionTrace, Stage

            trace = CollectionTrace(
                trace_id=context.context_id, url=parent_link.url, domain=parent_link.domain,
                source_domain=parent_link.source_domain, depth=parent_link.depth,
            )
            mq_published = self._seed_collect_publisher.publish_runtime_links(
                source_domain=parent_link.domain,
                source_url=parent_link.url,
                depth=parent_link.depth,
                trace_id=context.context_id,
                links=discovered,
            )
            for d in mq_published:
                if d not in trace.mq_published_domains:
                    trace.mq_published_domains.append(d)
            trace.add(
                Stage.MQ_PUBLISH,
                published=len(mq_published),
                topic=self._seed_collect_publisher.topic,
                mq="SeedCollectMq",
            )

        context.add_node(
            "discovery",
            status="success",
            output={
                "same_domain_submitted": same_count,
                "cross_domain_to_seed_collect_mq": len(mq_published),
                "seed_collect_domains": mq_published,
            },
        )

    @property
    def stats(self) -> dict:
        return {
            "pages_processed": self._pages_done,
            "fetch_ok": self._fetch_ok,
            "fetch_fail": self._fetch_fail,
            "data_queue_pending": self._data_queue.pending(),
        }
