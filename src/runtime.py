"""多进程 Worker 共享的运行时组装。"""
from __future__ import annotations

from config import Config
from src.stores.offline_store import JsonlOfflineStore
from src.mq.seed_collect import SeedCollectMq, SeedCollectPublisher
from src.stores.seed_store import SeedStore
from src.stores.data_queue import MvpDataQueue


def build_data_queue(config: Config) -> MvpDataQueue:
    dq = config.data_queue
    return MvpDataQueue(
        base_dir=dq.base_dir,
        topic_prefix=dq.topic_prefix,
        partition_count=dq.partition_count,
        partition_width=dq.partition_width,
        offset_width=dq.offset_width,
        buffer_capacity=dq.buffer_capacity,
        consume_rate_per_second=dq.consume_rate_per_second,
        domain_qps=dq.domain_qps,
    )


def build_seed_store(config: Config, offline: JsonlOfflineStore | None = None) -> SeedStore:
    return SeedStore(config, offline)


def build_seed_collect_mq(config: Config) -> SeedCollectMq:
    return SeedCollectMq(config)


def build_seed_collect_publisher(config: Config, mq: SeedCollectMq | None = None) -> SeedCollectPublisher:
    return SeedCollectPublisher(config, mq or build_seed_collect_mq(config))
