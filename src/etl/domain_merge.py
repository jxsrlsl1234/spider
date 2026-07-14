"""ETL 域名归并（Merge）：publicsuffix eTLD+1 + 代表页聚合。

同一注册域下保留：
- 首页（homepage）
- Sitemap 地址（扩链最佳入口）
- 一个随机内容页（探测质量样本）

并将全组页面的标题、状态码聚合为域级元特征，供离线评分使用。
"""
from __future__ import annotations

import random
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlparse

from src.domain.urls import domain_of_url, normalize_url, registrable_domain

_SITEMAP_RE = re.compile(r"sitemap[^/]*\.xml", re.I)


@dataclass
class RawPageRecord:
    """Ingest 阶段单条页面记录。"""

    url: str
    title: str = ""
    status_code: int = 0
    source_id: str = ""


@dataclass
class DomainMergeRecord:
    """Merge 阶段单域产出。"""

    domain: str
    homepage_url: str
    entry_url: str
    sitemap_url: str = ""
    sample_content_url: str = ""
    page_aggregate: Dict[str, Any] = field(default_factory=dict)

    def to_hints(self) -> Dict[str, Any]:
        """写入 SeedCollectMessage.hints / 评估特征。"""
        return {
            "homepage_url": self.homepage_url,
            "sitemap_url": self.sitemap_url,
            "sample_content_url": self.sample_content_url,
            "page_aggregate": self.page_aggregate,
            **{k: v for k, v in self.page_aggregate.items() if k.endswith("_ratio") or k.startswith("avg_")},
        }


def is_sitemap_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return bool(_SITEMAP_RE.search(path))


def is_homepage_url(url: str, domain: str) -> bool:
    p = urlparse(url)
    if domain_of_url(url) != domain:
        return False
    path = p.path or "/"
    return path in ("/", "") and not p.query


def _path_depth(url: str) -> int:
    return len([s for s in urlparse(url).path.split("/") if s])


def merge_domain_pages(
    pages: Sequence[RawPageRecord],
    *,
    rng: Optional[random.Random] = None,
) -> List[DomainMergeRecord]:
    """将原始页面流按注册域归并，每域输出一条 ``DomainMergeRecord``。"""
    rng = rng or random.Random(42)
    by_domain: Dict[str, List[RawPageRecord]] = {}

    for page in pages:
        norm = normalize_url(page.url)
        if not norm:
            continue
        domain = registrable_domain(urlparse(norm).hostname or "")
        if not domain:
            continue
        by_domain.setdefault(domain, []).append(
            RawPageRecord(url=norm, title=page.title, status_code=page.status_code, source_id=page.source_id)
        )

    results: List[DomainMergeRecord] = []
    for domain in sorted(by_domain.keys()):
        group = by_domain[domain]
        results.append(_merge_one_domain(domain, group, rng))
    return results


def _merge_one_domain(domain: str, group: List[RawPageRecord], rng: random.Random) -> DomainMergeRecord:
    titles = [p.title for p in group if p.title]
    status_codes = [p.status_code for p in group if p.status_code > 0]
    ok_count = sum(1 for c in status_codes if 200 <= c < 400)
    status_ok_ratio = ok_count / len(status_codes) if status_codes else 0.0

    page_aggregate: Dict[str, Any] = {
        "titles": titles[:50],
        "status_codes": status_codes[:50],
        "page_count": len(group),
        "status_ok_ratio": round(status_ok_ratio, 4),
        "avg_status_code": round(sum(status_codes) / len(status_codes), 2) if status_codes else 0.0,
        "title_count": len(titles),
    }

    homepage_candidates = [p.url for p in group if is_homepage_url(p.url, domain)]
    homepage_url = homepage_candidates[0] if homepage_candidates else f"https://{domain}/"

    sitemap_candidates = [p.url for p in group if is_sitemap_url(p.url)]
    sitemap_url = sitemap_candidates[0] if sitemap_candidates else ""

    content_pool = [
        p.url for p in group
        if p.url not in {homepage_url, sitemap_url}
        and not is_sitemap_url(p.url)
    ]
    sample_content_url = ""
    if content_pool:
        sample_content_url = rng.choice(sorted(content_pool))

    return DomainMergeRecord(
        domain=domain,
        homepage_url=homepage_url,
        entry_url=homepage_url,
        sitemap_url=sitemap_url,
        sample_content_url=sample_content_url,
        page_aggregate=page_aggregate,
    )


def merge_urls_only(urls: Iterable[str], **kwargs) -> List[DomainMergeRecord]:
    """仅 URL 列表的简化归并（无标题/状态码时聚合为空）。"""
    pages = [RawPageRecord(url=u) for u in urls]
    return merge_domain_pages(pages, **kwargs)
