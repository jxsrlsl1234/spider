#!/usr/bin/env python3
"""项目根目录入口：无论从何处调用，都能找到 seeds.txt。

用法::

    python run_bootstrap.py
    python run_bootstrap.py --log-level DEBUG
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bootstrap import main

if __name__ == "__main__":
    raise SystemExit(main())
