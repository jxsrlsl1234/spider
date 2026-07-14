"""LinkScheduler 常驻进程：周期扫描 SeedStore，将 ACTIVE 未投递种子写入 DataQueue。

用法::

    python -m src.workers.link_scheduler
    python -m src.workers.link_scheduler --interval 3
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from typing import Optional

from config import Config
from src.crawl.dedup import UrlDedup
from src.domain.link import LinkSourceType
from src.scheduling.link_scheduler import LinkScheduler
from src.util.logging_conf import get_logger, log, setup_logging
from src.stores.offline_store import JsonlOfflineStore
from src.runtime import build_data_queue, build_seed_store
from src.stores.crawl_context_store import LocalCrawlContextStore

logger = get_logger("worker.link_scheduler")

_STOP = False


def _handle_stop(signum, frame) -> None:  # noqa: ANN001, ARG001
    global _STOP
    _STOP = True
    log(logger, 20, "worker_stop_signal", signum=signum)


class LinkSchedulerWorker:
    """周期：reload SeedStore → 未 scheduled 的 ACTIVE → DataQueue → mark_scheduled → save。"""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._offline = JsonlOfflineStore(config.offline_dir)
        self._seed_store = build_seed_store(config, self._offline)
        self._context_store = LocalCrawlContextStore(config.hbase_context_dir)
        self._data_queue = build_data_queue(config)
        self._link_scheduler = LinkScheduler(
            config, self._seed_store, self._context_store,
            self._data_queue, url_dedup=UrlDedup(),
        )
        self.cycles = 0
        self.total_enqueued = 0

    def _enqueue_seed(self, domain: str) -> int:
        seed = self._seed_store.get_seed(domain)
        if seed is None:
            return 0
        n = 0
        seed_meta = seed.to_dict()
        for url, prio, role in self._seed_store.iter_seed_crawl_plan(seed):
            st = LinkSourceType.SITEMAP if role == "sitemap" else LinkSourceType.SEED
            res = self._link_scheduler.submit_raw(
                url, source_type=st, depth=0, seed_meta=seed_meta, priority=prio,
            )
            if res.accepted:
                n += 1
        return n

    def run_once(self) -> int:
        self._seed_store.load()
        enqueued = 0
        domains = [rec.domain for rec in self._seed_store.iter_unscheduled_active()]
        for domain in domains:
            n = self._enqueue_seed(domain)
            if n > 0:
                self._seed_store.mark_scheduled(domain)
                enqueued += n
                log(logger, 20, "seed_enqueued", domain=domain, urls=n)
        if domains:
            self._seed_store.save()
        self.cycles += 1
        self.total_enqueued += enqueued
        log(
            logger, 20, "link_scheduler_cycle",
            cycle=self.cycles,
            candidates=len(domains),
            enqueued=enqueued,
            total_enqueued=self.total_enqueued,
            data_queue_pending=self._data_queue.pending(),
            seed_store=self._seed_store.stats(),
        )
        return enqueued

    def run_forever(self, *, interval: float) -> None:
        log(
            logger, 20, "link_scheduler_worker_start",
            interval=interval,
            seed_store_path=str(self._config.seed_store_path),
        )
        while not _STOP:
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                log(logger, 40, "link_scheduler_cycle_error", error=repr(exc))
            # 可中断 sleep
            end = time.time() + interval
            while not _STOP and time.time() < end:
                time.sleep(min(0.5, end - time.time()))
        log(logger, 20, "link_scheduler_worker_exit", cycles=self.cycles, total_enqueued=self.total_enqueued)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="LinkScheduler 常驻：ACTIVE 未投递种子 → DataQueue")
    parser.add_argument("--interval", type=float, default=None, help="扫描间隔秒（默认 config.worker）")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = Config()
    interval = args.interval if args.interval is not None else config.worker.link_scheduler_interval_seconds
    LinkSchedulerWorker(config).run_forever(interval=interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
