"""治理模块：老化淘汰 + 周期审计。"""
from __future__ import annotations

from typing import Dict, Protocol

from config import Config
from src.util.logging_conf import get_logger, log
from src.domain.models import SeedStatus
from src.stores.seed_store import SeedStore

logger = get_logger("governance")

_MIN_QUALITY = 0.15
_MIN_PRODUCED_FOR_JUDGE = 3


class DiscoveryConsumerLike(Protocol):
    def reset_cycle(self) -> None: ...
    @property
    def candidate_pool_size(self) -> int: ...


class Governance:
    def __init__(self, config: Config, seed_store: SeedStore, consumer: DiscoveryConsumerLike) -> None:
        self._config = config
        self._store = seed_store
        self._consumer = consumer

    def run_cycle(self) -> Dict[str, int]:
        self._consumer.reset_cycle()
        promoted, evicted, demoted = 0, 0, 0

        for seed in list(self._store.iter_active_seeds()):
            if seed.status == SeedStatus.PROBATION and seed.produced_count >= _MIN_PRODUCED_FOR_JUDGE:
                if seed.avg_quality >= _MIN_QUALITY:
                    self._store.promote(seed.domain)
                    promoted += 1
                else:
                    self._store.evict(seed.domain)
                    evicted += 1
                continue

            if seed.status == SeedStatus.ACTIVE and seed.produced_count >= _MIN_PRODUCED_FOR_JUDGE:
                if seed.avg_quality < _MIN_QUALITY:
                    if seed.weight <= 0.1:
                        self._store.evict(seed.domain)
                        evicted += 1
                    else:
                        self._store.update_weight(seed.domain, reward=0.0)
                        demoted += 1

        stats = {"promoted": promoted, "evicted": evicted, "demoted": demoted,
                 "candidate_pool": self._consumer.candidate_pool_size}
        log(logger, 20, "governance_cycle", **stats)
        return stats

    def detect_link_farm(self) -> None:
        return None
