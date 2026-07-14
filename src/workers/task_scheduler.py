"""TaskScheduler 常驻进程：持续消费 DataQueue 抓取与扩链。

用法::

    python -m src.workers.task_scheduler
    python -m src.workers.task_scheduler --self-test --max-pages 50
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time
from typing import Dict, Optional, Set

from config import Config
from src.crawl.dedup import ContentDedup, UrlDedup
from src.crawl.fetcher import AiohttpFetcher, Fetcher, MockFetcher
from src.scheduling.link_scheduler import LinkScheduler
from src.util.logging_conf import get_logger, log, setup_logging
from src.stores.offline_store import JsonlOfflineStore
from src.runtime import build_data_queue, build_seed_collect_publisher, build_seed_store
from src.stores.result_store import ResultStore
from src.stores.crawl_context_store import LocalCrawlContextStore
from src.scheduling.task_scheduler import TaskScheduler
from src.stores.data_queue.models import LinkQueueItem

logger = get_logger("worker.task_scheduler")

_STOP = False


def _handle_stop(signum, frame) -> None:  # noqa: ANN001, ARG001
    global _STOP
    _STOP = True
    log(logger, 20, "worker_stop_signal", signum=signum)


def _build_mock_pages() -> dict:
    hn = (
        "<html><body><nav><a href='/newest'>new</a></nav>"
        "<main><h1>Hacker News</h1>"
        "<a href='https://docs.python.org/3/'>python docs</a>"
        "<a href='https://github.com/'>github</a>"
        "<a href='https://arxiv.org/'>arxiv</a>"
        "<p>" + ("讨论内容 discussion text " * 40) + "</p>"
        "</main></body></html>"
    )
    py = (
        "<html><body><main><h1>Python Docs</h1>"
        "<a href='https://github.com/'>github</a>"
        "<a href='https://realpython.com/'>realpython</a>"
        "<p>" + ("Python documentation content. " * 40) + "</p>"
        "</main></body></html>"
    )
    gh = (
        "<html><body><main><h1>GitHub</h1>"
        "<a href='https://docs.python.org/3/'>docs</a>"
        "<a href='https://arxiv.org/'>arxiv</a>"
        "<p>" + ("Where the world builds software. " * 40) + "</p>"
        "</main></body></html>"
    )
    return {
        "https://news.ycombinator.com/": hn,
        "https://news.ycombinator.com/news": hn,
        "https://docs.python.org/3/": py,
        "https://github.com/": gh,
        "https://arxiv.org/": "<html><body><main><p>" + ("papers " * 100) + "</p></main></body></html>",
        "https://realpython.com/": "<html><body><main><p>" + ("tutorials " * 100) + "</p></main></body></html>",
    }


class TaskSchedulerWorker:
    """常驻：poll DataQueue → execute；同域扩链回 DataQueue；跨域发 SeedCollectMq。"""

    def __init__(self, config: Config, fetcher: Fetcher) -> None:
        self._config = config
        self._fetcher = fetcher
        self._offline = JsonlOfflineStore(config.offline_dir)
        self._seed_store = build_seed_store(config, self._offline)
        self._seed_store.load()
        self._context_store = LocalCrawlContextStore(config.hbase_context_dir)
        self._data_queue = build_data_queue(config)
        self._link_scheduler = LinkScheduler(
            config, self._seed_store, self._context_store,
            self._data_queue, url_dedup=UrlDedup(),
        )
        publisher = build_seed_collect_publisher(config)
        self._task_scheduler = TaskScheduler(
            config, fetcher, self._seed_store, self._link_scheduler,
            self._context_store, self._data_queue,
            seed_collect_publisher=publisher,
            result_store=ResultStore(config, ContentDedup(), offline=self._offline),
        )
        self._max_pages = config.worker.task_scheduler_max_pages
        self._host_inflight: Dict[str, int] = {}

    def _soft_requeue(self, item) -> None:
        """单域并发已满：ACK 后重新入 normal 队列（不计失败、不进 retry）。"""
        try:
            topic = item.topic or self._data_queue.topic_for_domain(item.link.domain)
            if item.row_key and topic:
                self._data_queue.ack(item.row_key, topic=topic)
            fresh = LinkQueueItem(
                context_id=item.context_id,
                link=item.link,
                priority=item.priority,
                topic=topic,
            )
            self._data_queue.publish(fresh, topic=topic)
            log(logger, 10, "host_concurrency_deferred", domain=item.link.domain, context_id=item.context_id)
        except Exception as exc:  # noqa: BLE001
            log(
                logger, 40, "soft_requeue_failed",
                domain=getattr(item.link, "domain", None),
                context_id=getattr(item, "context_id", None),
                error=repr(exc),
            )

    async def _execute_tracked(self, item) -> None:
        domain = item.link.domain
        try:
            await self._task_scheduler.execute(item)
        finally:
            self._host_inflight[domain] = max(0, self._host_inflight.get(domain, 1) - 1)

    async def run_forever(self) -> None:
        idle = self._config.worker.task_scheduler_idle_sleep_seconds
        concurrency = max(1, self._config.fetch.concurrency)
        per_host = max(1, self._config.fetch.max_concurrency_per_host)
        log(
            logger, 20, "task_scheduler_worker_start",
            concurrency=concurrency,
            max_concurrency_per_host=per_host,
            timeout_seconds=self._config.fetch.timeout_seconds,
            connect_timeout_seconds=self._config.fetch.connect_timeout_seconds,
            max_retries=self._config.fetch.max_retries,
            connector_limit=self._config.fetch.connector_limit,
            connector_limit_per_host=self._config.fetch.connector_limit_per_host,
            max_pages=self._max_pages or "unlimited",
            idle_sleep=idle,
        )
        inflight: Set[asyncio.Task] = set()
        deferred_busy = False
        while not _STOP:
            if self._max_pages and self._task_scheduler.pages_done >= self._max_pages:
                log(logger, 20, "task_scheduler_max_pages_reached", pages=self._task_scheduler.pages_done)
                break

            if self._task_scheduler.pages_done % 20 == 0:
                self._seed_store.load(replace=False)

            slots = concurrency - len(inflight)
            items = self._task_scheduler.poll_tasks(max_tasks=max(0, slots)) if slots > 0 else []
            deferred_busy = False

            if items:
                started = 0
                for item in items:
                    domain = item.link.domain
                    if self._host_inflight.get(domain, 0) >= per_host:
                        self._soft_requeue(item)
                        deferred_busy = True
                        continue
                    self._host_inflight[domain] = self._host_inflight.get(domain, 0) + 1
                    task = asyncio.create_task(self._execute_tracked(item))
                    inflight.add(task)
                    task.add_done_callback(inflight.discard)
                    started += 1
                # 同站任务过多被延后：等已有 in-flight 完成释放槽位，避免 busy requeue
                if started == 0 and deferred_busy:
                    if inflight:
                        await asyncio.wait(
                            inflight, timeout=0.5, return_when=asyncio.FIRST_COMPLETED,
                        )
                    else:
                        self._host_inflight.clear()
                        await asyncio.sleep(idle)
                    continue
                if started:
                    continue

            if inflight:
                await asyncio.wait(inflight, timeout=0.2, return_when=asyncio.FIRST_COMPLETED)
                continue

            await asyncio.sleep(idle)

        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        self._seed_store.save()
        await self._fetcher.close()
        log(logger, 20, "task_scheduler_worker_exit", **self._task_scheduler.stats)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="TaskScheduler 常驻：消费 DataQueue 抓取/扩链")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--max-pages", type=int, default=None, help=">0 时达页数退出（默认常驻）")
    parser.add_argument(
        "--concurrency", type=int, default=None,
        help=f"并发抓取数（默认 config.fetch.concurrency）",
    )
    parser.add_argument(
        "--timeout", type=float, default=None,
        help="单次请求总超时秒数（默认 config.fetch.timeout_seconds）",
    )
    parser.add_argument(
        "--connect-timeout", type=float, default=None,
        help="建连超时秒数（默认 config.fetch.connect_timeout_seconds）",
    )
    parser.add_argument(
        "--max-retries", type=int, default=None,
        help="失败重试次数（总尝试 = max_retries+1；默认 config.fetch.max_retries）",
    )
    parser.add_argument(
        "--retry-backoff", type=float, default=None,
        help="重试基础退避秒数，实际为 backoff*2^attempt（默认 config.fetch.retry_backoff_seconds）",
    )
    parser.add_argument(
        "--max-concurrency-per-host", type=int, default=None,
        help="单域同时抓取上限（默认 config.fetch.max_concurrency_per_host，防 ConnectionTimeout）",
    )
    parser.add_argument(
        "--connector-limit", type=int, default=None,
        help="连接池总连接数上限（默认 config.fetch.connector_limit）",
    )
    parser.add_argument(
        "--connector-limit-per-host", type=int, default=None,
        help="单 host 连接数上限（默认 config.fetch.connector_limit_per_host）",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = Config()
    if args.max_pages is not None:
        config.worker.task_scheduler_max_pages = args.max_pages
    if args.concurrency is not None:
        config.fetch.concurrency = max(1, args.concurrency)
    if args.max_concurrency_per_host is not None:
        config.fetch.max_concurrency_per_host = max(1, args.max_concurrency_per_host)
    if args.timeout is not None:
        config.fetch.timeout_seconds = args.timeout
    if args.connect_timeout is not None:
        config.fetch.connect_timeout_seconds = args.connect_timeout
    if args.max_retries is not None:
        config.fetch.max_retries = max(0, args.max_retries)
    if args.retry_backoff is not None:
        config.fetch.retry_backoff_seconds = max(0.0, args.retry_backoff)
    if args.connector_limit is not None:
        config.fetch.connector_limit = max(1, args.connector_limit)
    if args.connector_limit_per_host is not None:
        config.fetch.connector_limit_per_host = max(1, args.connector_limit_per_host)

    # 连接池 per-host 不得小于单域任务并发，否则池内排队会变成 ConnectionTimeout
    if config.fetch.connector_limit_per_host < config.fetch.max_concurrency_per_host:
        config.fetch.connector_limit_per_host = config.fetch.max_concurrency_per_host

    fetcher: Fetcher
    if args.self_test:
        fetcher = MockFetcher(_build_mock_pages())
        config.schedule.per_domain_min_interval = 0.0
    else:
        fetcher = AiohttpFetcher(config)

    log(
        logger, 20, "fetch_config",
        concurrency=config.fetch.concurrency,
        max_concurrency_per_host=config.fetch.max_concurrency_per_host,
        timeout_seconds=config.fetch.timeout_seconds,
        connect_timeout_seconds=config.fetch.connect_timeout_seconds,
        sock_read_timeout_seconds=config.fetch.sock_read_timeout_seconds,
        max_retries=config.fetch.max_retries,
        retry_backoff_seconds=config.fetch.retry_backoff_seconds,
        connector_limit=config.fetch.connector_limit,
        connector_limit_per_host=config.fetch.connector_limit_per_host,
        self_test=bool(args.self_test),
    )

    worker = TaskSchedulerWorker(config, fetcher)
    asyncio.run(worker.run_forever())
    return 0


if __name__ == "__main__":
    sys.exit(main())
