"""robots.txt 合规（MVP 简化实现）。

MVP：域级缓存 + 允许优先的宽松策略；提供 urllib.robotparser 接入点。
生产化（见 DESIGN.md §3.2）：分布式 robots 缓存(TTL)、sitemap 发现、UA 精细规则、
crawl-delay 自适应退避。
"""
from __future__ import annotations

from typing import Dict, Optional
from urllib.robotparser import RobotFileParser

from config import Config


class RobotsCache:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._cache: Dict[str, Optional[RobotFileParser]] = {}
        self.ua = config.fetch.user_agent

    def set_rules(self, domain: str, robots_txt: str) -> None:
        """外部（抓取到 robots.txt 后）注入规则。"""
        rp = RobotFileParser()
        rp.parse(robots_txt.splitlines())
        self._cache[domain] = rp

    def allowed(self, domain: str, url: str) -> bool:
        """MVP：未获取到 robots 规则时默认允许（宽松）。"""
        if not self._config.respect_robots:
            return True
        rp = self._cache.get(domain)
        if rp is None:
            # TODO(生产化): 首次遇到域时异步抓取 robots.txt 并缓存；此处默认放行
            return True
        return rp.can_fetch(self.ua, url)

    def crawl_delay(self, domain: str) -> Optional[float]:
        rp = self._cache.get(domain)
        if rp is None:
            return None
        try:
            d = rp.crawl_delay(self.ua)
            return float(d) if d is not None else None
        except Exception:  # noqa: BLE001
            return None
