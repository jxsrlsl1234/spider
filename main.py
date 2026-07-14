"""入口说明：冷启动 + 三个常驻 Worker。

::

    # 1) 一次性冷启动（写入 SeedStore）
    python -m src.bootstrap

    # 2) 三个常驻进程（分三个终端）
    python -m src.workers.link_scheduler
    python -m src.workers.task_scheduler [--self-test] [--max-pages N]
    python -m src.workers.seed_collect
"""
from __future__ import annotations

import argparse
import sys

from src.util.logging_conf import setup_logging


def main() -> int:
    parser = argparse.ArgumentParser(
        description="请使用 bootstrap + workers，本入口仅打印说明",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.parse_args()
    setup_logging("INFO")
    print(
        """
用法（工作目录 = 项目根，需含 seeds.txt）:

  # 冷启动一次
  python -m src.bootstrap

  # 三个常驻进程（分别开终端 / IDE Launch）
  python -m src.workers.link_scheduler
  python -m src.workers.task_scheduler --self-test --max-pages 50
  python -m src.workers.seed_collect

说明:
  LinkScheduler  — 周期将 SeedStore 中 ACTIVE/PROBATION 且未投递的种子写入 DataQueue
  TaskScheduler  — 消费 DataQueue 抓取；同域扩链回 DataQueue；跨域发 SeedCollectMq
  SeedCollect    — 消费 SeedCollectMq 评估后写入 SeedStore（不直接入 DataQueue）
""".strip()
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
