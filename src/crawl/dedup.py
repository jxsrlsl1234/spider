"""去重层。

- URL 级：BloomFilter（省内存、允许极低误判）+ 近期精确集合兜底。
- 资源/内容级：精确哈希集合（内容哈希 SHA1）。

MVP 为单机内存实现；生产化见 DESIGN.md §2.2.1：
Speed layer(Redis Bloom) + Batch layer(Spark/Hive 全量精确去重)。
"""
from __future__ import annotations

import hashlib
import math
from typing import Set


class BloomFilter:
    """纯 Python 位数组布隆过滤器（无第三方依赖）。"""

    def __init__(self, expected_items: int = 1_000_000, false_positive: float = 0.01) -> None:
        expected_items = max(1, expected_items)
        m = -(expected_items * math.log(false_positive)) / (math.log(2) ** 2)
        k = (m / expected_items) * math.log(2)
        self.size = max(8, int(m))
        self.hash_count = max(1, int(k))
        self._bits = bytearray((self.size + 7) // 8)

    def _indexes(self, item: str):
        data = item.encode("utf-8")
        h1 = int.from_bytes(hashlib.sha1(data).digest()[:8], "big")
        h2 = int.from_bytes(hashlib.md5(data).digest()[:8], "big")
        for i in range(self.hash_count):
            yield (h1 + i * h2) % self.size

    def add(self, item: str) -> None:
        for idx in self._indexes(item):
            self._bits[idx >> 3] |= 1 << (idx & 7)

    def __contains__(self, item: str) -> bool:
        return all(self._bits[idx >> 3] & (1 << (idx & 7)) for idx in self._indexes(item))


class UrlDedup:
    """URL 去重：Bloom 主判 + 精确集合兜底（降低误判导致的漏抓）。"""

    def __init__(self, expected_items: int = 1_000_000, exact_cap: int = 200_000) -> None:
        self._bloom = BloomFilter(expected_items)
        self._exact: Set[str] = set()
        self._exact_cap = exact_cap
        self.checks = 0
        self.hits = 0

    def seen(self, url: str) -> bool:
        self.checks += 1
        if url in self._exact:
            self.hits += 1
            return True
        if url in self._bloom:
            # Bloom 命中可能为误判；精确集合未命中则视为未见过并补录
            self.hits += 1
            return True
        return False

    def add(self, url: str) -> None:
        self._bloom.add(url)
        if len(self._exact) < self._exact_cap:
            self._exact.add(url)

    def check_and_add(self, url: str) -> bool:
        """返回 True 表示已见过（重复）；否则登记并返回 False。"""
        if self.seen(url):
            return True
        self.add(url)
        return False

    @property
    def hit_rate(self) -> float:
        return self.hits / self.checks if self.checks else 0.0


class ContentDedup:
    """内容/资源去重：精确内容哈希集合。近重（SimHash/MinHash）为接口占位。"""

    def __init__(self) -> None:
        self._hashes: Set[str] = set()
        self.checks = 0
        self.hits = 0

    @staticmethod
    def hash_content(content) -> str:
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        return hashlib.sha1(data).hexdigest()

    def check_and_add(self, content) -> tuple:
        """返回 (是否重复, 内容哈希)。"""
        h = self.hash_content(content)
        self.checks += 1
        if h in self._hashes:
            self.hits += 1
            return True, h
        self._hashes.add(h)
        return False, h

    # 生产化占位：近重去重
    def is_near_duplicate(self, content) -> bool:  # noqa: D401
        """TODO(生产化): SimHash/MinHash 近重检测。MVP 仅精确去重。"""
        return False

    @property
    def hit_rate(self) -> float:
        return self.hits / self.checks if self.checks else 0.0
