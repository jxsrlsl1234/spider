"""SeedCollect 常驻进程：消费 SeedCollectMq → 评估 → SeedStore（不投递 DataQueue）。

扩链新域由 TaskScheduler 写入 SeedCollectMq；本进程评估入库后
``scheduled=False``，由 LinkScheduler 进程按 ACTIVE 未投递规则入队。

用法::

    python -m src.workers.seed_collect
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from typing import Optional

from config import Config
from src.seed.evaluator import SeedAdmissionEvaluator
from src.seed.governance import Governance
from src.util.logging_conf import get_logger, log, setup_logging
from src.stores.offline_store import JsonlOfflineStore
from src.runtime import build_seed_collect_mq, build_seed_store
from src.mq.seed_collect import SeedCollectConsumer

logger = get_logger("worker.seed_collect")

_STOP = False


def _handle_stop(signum, frame) -> None:  # noqa: ANN001, ARG001
    global _STOP
    _STOP = True
    log(logger, 20, "worker_stop_signal", signum=signum)


class SeedCollectWorker:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._offline = JsonlOfflineStore(config.offline_dir)
        self._seed_store = build_seed_store(config, self._offline)
        self._mq = build_seed_collect_mq(config)
        self._consumer = SeedCollectConsumer(
            config, self._seed_store, self._mq,
            offline=self._offline,
            evaluator=SeedAdmissionEvaluator(config),
        )
        self._governance = Governance(config, self._seed_store, self._consumer)
        self.cycles = 0

    def run_once(self) -> int:
        self._seed_store.load()
        admissions = self._consumer.consume_batch()
        # 新种子保持 scheduled=False，交给 LinkScheduler
        for adm in admissions:
            seed = self._seed_store.get_seed(adm.domain)
            if seed and seed.scheduled and adm.is_new:
                seed.scheduled = False
            log(
                logger, 20, "seed_collect_admitted",
                domain=adm.domain,
                status=adm.status.value,
                scheduled=False if seed else None,
            )
        if admissions or self.cycles % 10 == 0:
            self._governance.run_cycle()
            self._seed_store.save()
        self.cycles += 1
        log(
            logger, 20, "seed_collect_cycle",
            cycle=self.cycles,
            admitted=len(admissions),
            mq_pending=self._mq.pending(),
            seed_store=self._seed_store.stats(),
        )
        return len(admissions)

    def run_forever(self, *, interval: float) -> None:
        log(logger, 20, "seed_collect_worker_start", interval=interval, topic=self._mq.topic)
        while not _STOP:
            try:
                self.run_once()
            except Exception as exc:  # noqa: BLE001
                log(logger, 40, "seed_collect_cycle_error", error=repr(exc))
            end = time.time() + interval
            while not _STOP and time.time() < end:
                time.sleep(min(0.5, end - time.time()))
        self._seed_store.save()
        log(logger, 20, "seed_collect_worker_exit", cycles=self.cycles)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="SeedCollect 常驻：MQ → 评估 → SeedStore")
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument(
        "--min-in-degree", type=int, default=None,
        help="覆盖准入闸门（演示扩链入库可设 1；生产建议 2）",
    )
    parser.add_argument("--quick-score-threshold", type=float, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = Config()
    if args.min_in_degree is not None:
        config.admission.min_in_degree = args.min_in_degree
    if args.quick_score_threshold is not None:
        config.admission.quick_score_threshold = args.quick_score_threshold
    log(
        logger, 20, "admission_gates",
        min_in_degree=config.admission.min_in_degree,
        quick_score_threshold=config.admission.quick_score_threshold,
    )
    interval = args.interval if args.interval is not None else config.worker.seed_collect_interval_seconds
    SeedCollectWorker(config).run_forever(interval=interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
