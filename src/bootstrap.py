"""初始种子冷启动：寻源 → SeedCollectMq → 评估 → SeedStore 持久化。

::

    python -m src.bootstrap
    python -m src.bootstrap --log-level DEBUG
    python -m src.bootstrap --seeds seeds.txt
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from config import Config
from src.etl.domain_merge import DomainMergeRecord, RawPageRecord, merge_domain_pages
from src.seed.evaluator import SeedAdmissionEvaluator
from src.util.logging_conf import get_logger, log, setup_logging
from src.stores.offline_store import JsonlOfflineStore
from src.mq.seed_collect import (
    SeedCollectConsumer,
    SeedCollectMq,
    SeedCollectPublisher,
    publish_static_seeds,
)
from src.stores.seed_store import SeedStore
from src.domain.urls import domain_of_url, normalize_url

logger = get_logger("bootstrap")


def load_static_seeds(config: Config) -> List[Tuple[str, str]]:
    """解析 seeds.txt，返回 [(url, domain)]。路径相对当前工作目录。"""
    path = config.resolve_path(config.seeds_file)
    out: List[Tuple[str, str]] = []
    if not path.exists():
        log(
            logger, 30, "seeds_file_missing",
            path=str(path),
            cwd=str(Path.cwd()),
            hint="请在项目根目录启动，或配置 IDE cwd=${workspaceFolder}，或使用 --seeds 指定路径",
        )
        return out

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        norm = normalize_url(line.split()[0])
        if not norm:
            continue
        out.append((norm, domain_of_url(norm)))
    log(logger, 20, "static_seeds_loaded", count=len(out), path=str(path), cwd=str(Path.cwd()))
    return out


def merge_static_seeds_as_pages(config: Config) -> List[DomainMergeRecord]:
    """将 seeds.txt 各行视为单页记录，走 ETL Merge 归并。"""
    pages = [RawPageRecord(url=url, title="", status_code=200) for url, _ in load_static_seeds(config)]
    return merge_domain_pages(pages)


class AutoEtlPipeline:
    """自动寻源 ETL：Ingest → Clean → Merge → 发布 SeedCollectMq。"""

    def __init__(self, config: Config, publisher: SeedCollectPublisher) -> None:
        self._config = config
        self._publisher = publisher

    def run(
        self,
        pages: List[RawPageRecord],
        *,
        target_count: int = 1000,
        trace_id: str = "",
    ) -> List[DomainMergeRecord]:
        merged = merge_domain_pages(pages)
        for rec in merged[:target_count]:
            self._publisher.publish_auto_etl(rec, trace_id=trace_id)
        log(logger, 20, "auto_etl_published", domains=len(merged[:target_count]), topic=self._publisher.topic)
        return merged[:target_count]

    def run_from_urls(self, urls: List[str], **kwargs) -> List[DomainMergeRecord]:
        pages = [RawPageRecord(url=u) for u in urls]
        return self.run(pages, **kwargs)


class BootstrapRunner:
    """冷启动编排：人工/自动寻源 → SeedCollectMq → Evaluator → SeedStore.save。"""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._offline = JsonlOfflineStore(config.offline_dir)
        self._mq = SeedCollectMq(config)
        self._publisher = SeedCollectPublisher(config, self._mq)
        self._store = SeedStore(config, self._offline)
        self._consumer = SeedCollectConsumer(
            config,
            self._store,
            self._mq,
            offline=self._offline,
            evaluator=SeedAdmissionEvaluator(config),
        )

    @property
    def seed_store(self) -> SeedStore:
        return self._store

    def run(self, *, source: str = "manual") -> Path:
        """
        source:
          - manual: seeds.txt → SeedCollectMq
          - auto_etl: Merge(seeds.txt 演示) → SeedCollectMq（AUTO_ETL 评估）
        """
        log(
            logger, 20, "bootstrap_start",
            source=source,
            seeds_file=str(self._config.resolve_path(self._config.seeds_file)),
            seed_store_path=str(self._config.resolve_path(self._config.seed_store_path)),
            topic=self._mq.topic,
            cwd=str(Path.cwd()),
        )

        if source == "auto_etl":
            pages = [
                RawPageRecord(url=url, title="", status_code=200)
                for url, _ in load_static_seeds(self._config)
            ]
            if not pages:
                log(logger, 40, "bootstrap_aborted", reason="no_input_pages")
                raise SystemExit(1)
            published = len(AutoEtlPipeline(self._config, self._publisher).run(pages))
        else:
            published = publish_static_seeds(self._config, self._publisher)
            if published == 0:
                log(logger, 40, "bootstrap_aborted", reason="no_seeds_published")
                raise SystemExit(1)

        log(logger, 20, "bootstrap_published", published=published, mq_pending=self._mq.pending())

        admissions = self._consumer.drain()
        log(
            logger, 20, "bootstrap_evaluated",
            messages_processed=self._consumer.messages_processed,
            newly_admitted=self._consumer.newly_admitted,
            updated_existing=self._consumer.updated_existing,
            rejected=self._consumer.rejected,
            admissions=len(admissions),
            seed_store=self._store.stats(),
        )

        for adm in admissions:
            log(
                logger, 20, "bootstrap_seed_admitted",
                domain=adm.domain,
                entry_url=adm.entry_url,
                status=adm.status.value,
            )

        snapshot = self._store.save()
        log(
            logger, 20, "bootstrap_done",
            snapshot=str(snapshot),
            seed_store=self._store.stats(),
            active=self._store.count_active(),
        )
        print(f"\n===== Bootstrap 完成 =====")
        print(f"snapshot: {snapshot}")
        print(f"seed_store: {self._store.stats()}")
        print(f"ACTIVE 种子数: {self._store.count_active()}")
        print("下一步（三个常驻进程）:")
        print("  python -m src.workers.link_scheduler")
        print("  python -m src.workers.task_scheduler [--self-test]")
        print("  python -m src.workers.seed_collect")
        return snapshot


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="初始种子冷启动：寻源评估后写入 SeedStore（独立于爬取主控）",
    )
    parser.add_argument("--seeds", type=Path, default=None, help="种子文件路径（默认 seeds.txt）")
    parser.add_argument(
        "--source",
        choices=("manual", "auto_etl"),
        default="manual",
        help="manual=人工 seeds.txt；auto_etl=走 ETL Merge + AUTO_ETL 评分",
    )
    parser.add_argument("--seed-store", type=Path, default=None, help="SeedStore 快照输出路径")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    config = Config()
    if args.seeds is not None:
        config.seeds_file = config.resolve_path(args.seeds)
    if args.seed_store is not None:
        config.seed_store_path = config.resolve_path(args.seed_store)

    log(
        logger, 20, "bootstrap_cli",
        args=vars(args),
        seeds_file=str(config.seeds_file),
        seed_store_path=str(config.seed_store_path),
        cwd=str(Path.cwd()),
    )
    runner = BootstrapRunner(config)
    runner.run(source=args.source)
    return 0


if __name__ == "__main__":
    sys.exit(main())
