"""价值评估引擎。

- 在线快速评分 quick_score：不发网络请求，用特征继承对新域打分。
- HTML 质量评分：抓取后对 HTML 内容打分，用于权重更新。
- 统一种子准入评估 evaluate_discovery：人工/ETL/运行时扩链共用。
"""
from __future__ import annotations

from typing import Dict, Optional, Protocol, TYPE_CHECKING

from config import Config
from src.domain.models import DiscoveredLink, SeedCollectMessage, SeedEvaluationResult, SeedSourceType, SeedStatus

if TYPE_CHECKING:
    from src.stores.seed_store import SeedStore
from src.domain.urls import tld_of, url_health_score

_TLD_BONUS: Dict[str, float] = {
    "edu": 1.0, "gov": 1.0, "org": 0.8, "ac": 0.9,
    "com": 0.5, "net": 0.4, "io": 0.6, "dev": 0.6,
}
_POSITION_WEIGHT: Dict[str, float] = {"content": 1.0, "nav": 0.4, "footer": 0.3, "ad": 0.0}
_SPAM_TOKENS = ("casino", "porn", "loan", "replica", "viagra", "seo-", "free-download")


class OnlineEvaluator:
    def __init__(self, config: Config) -> None:
        self._w = config.weights

    def anchor_quality(self, anchor: str) -> float:
        if not anchor:
            return 0.2
        n = len(anchor.strip())
        if n < 2:
            return 0.2
        if n > 80:
            return 0.5
        return min(1.0, 0.4 + n / 100.0)

    def tld_bonus(self, domain: str) -> float:
        return _TLD_BONUS.get(tld_of(domain), 0.3)

    def spam_signal(self, domain: str, anchor: str) -> float:
        blob = f"{domain} {anchor}".lower()
        return 1.0 if any(tok in blob for tok in _SPAM_TOKENS) else 0.0

    def quick_score(
        self,
        *,
        link: DiscoveredLink,
        src_seed_weight: float,
        in_degree: int,
        min_in_degree: int,
    ) -> float:
        w = self._w
        in_degree_norm = min(1.0, in_degree / max(1, min_in_degree))
        pos = _POSITION_WEIGHT.get(link.position, 0.5)
        score = (
            w.src_domain_weight * src_seed_weight * pos
            + w.anchor_quality * self.anchor_quality(link.anchor)
            + w.url_health * url_health_score(link.url)
            + w.tld_bonus * self.tld_bonus(link.domain)
            + w.in_degree_norm * in_degree_norm
            - w.spam_penalty * self.spam_signal(link.domain, link.anchor)
        )
        return max(0.0, min(1.0, score))


class SeedAdmissionEvaluator:
    """统一种子发现 MQ 消费侧价值评估。"""

    EVAL_VERSION = "mvp-1"
    _MANUAL_TRUST = 0.85
    _AUTO_ETL_TRUST = 0.75

    def __init__(self, config: Config) -> None:
        self._config = config
        self._online = OnlineEvaluator(config)

    def evaluate_discovery(
        self,
        msg: SeedCollectMessage,
        *,
        store: "SeedStore",
        in_degree: int = 1,
    ) -> SeedEvaluationResult:
        domain = msg.domain
        entry_url = msg.entry_url or f"https://{domain}/"
        features: Dict[str, float] = {}
        metadata: Dict[str, object] = {
            "anchor": msg.anchor,
            "position": msg.position,
            "source_domain": msg.source_domain,
            "source_url": msg.source_url,
            "depth": msg.depth,
            "hints": msg.hints,
        }

        if msg.source_type == SeedSourceType.MANUAL:
            features["source_trust"] = self._MANUAL_TRUST
            features["tld_bonus"] = self._online.tld_bonus(domain)
            features["url_health"] = url_health_score(entry_url)
            features["rank_prior"] = float(msg.hints.get("rank_prior", 0.6))
            score = (
                0.40 * features["source_trust"]
                + 0.20 * features["tld_bonus"]
                + 0.20 * features["url_health"]
                + 0.20 * features["rank_prior"]
            )
            score = max(0.0, min(1.0, score))
            return SeedEvaluationResult(
                domain=domain,
                entry_url=entry_url,
                quality_score=score,
                weight=max(score, 0.7),
                status=SeedStatus.ACTIVE,
                features=features,
                action="admitted_manual",
                discovery_source=msg.source_type.value,
                discovery_trace_id=msg.trace_id,
                metadata=metadata,
            )

        if msg.source_type == SeedSourceType.AUTO_ETL:
            agg = dict(msg.hints.get("page_aggregate") or {})
            features["source_trust"] = self._AUTO_ETL_TRUST
            features["tld_bonus"] = self._online.tld_bonus(domain)
            features["url_health"] = url_health_score(entry_url)
            features["rank_prior"] = float(msg.hints.get("rank_prior", 0.5))
            features["ref_indegree"] = float(msg.hints.get("ref_indegree", 0.0))
            features["html_quality"] = float(msg.hints.get("html_quality", 0.5))
            features["lang_fit"] = float(msg.hints.get("lang_fit", 0.8))
            features["status_ok_ratio"] = float(agg.get("status_ok_ratio", msg.hints.get("status_ok_ratio", 0.5)))
            features["title_richness"] = min(1.0, float(agg.get("title_count", 0)) / 3.0)
            score = (
                0.20 * features["rank_prior"]
                + 0.12 * features["tld_bonus"]
                + 0.15 * features["ref_indegree"]
                + 0.08 * features["url_health"]
                + 0.15 * features["html_quality"]
                + 0.08 * features["lang_fit"]
                + 0.12 * features["status_ok_ratio"]
                + 0.10 * features["title_richness"]
            )
            spam = self._online.spam_signal(domain, msg.anchor)
            if spam:
                score -= self._config.weights.spam_penalty * spam
            score = max(0.0, min(1.0, score))
            status = SeedStatus.ACTIVE if score >= 0.55 else SeedStatus.PROBATION
            sitemap_url = str(msg.hints.get("sitemap_url", ""))
            homepage_url = str(msg.hints.get("homepage_url", entry_url))
            sample_content_url = str(msg.hints.get("sample_content_url", ""))
            return SeedEvaluationResult(
                domain=domain,
                entry_url=homepage_url or entry_url,
                quality_score=score,
                weight=score,
                status=status,
                features=features,
                action="admitted_auto_etl" if status == SeedStatus.ACTIVE else "admitted_auto_etl_probation",
                discovery_source=msg.source_type.value,
                discovery_trace_id=msg.trace_id,
                sitemap_url=sitemap_url,
                homepage_url=homepage_url,
                sample_content_url=sample_content_url,
                page_aggregate=agg,
                metadata=metadata,
            )

        # 运行时跨域扩链：复用 quick_score 特征继承
        link = DiscoveredLink(
            url=entry_url,
            domain=domain,
            anchor=msg.anchor,
            position=msg.position,
            same_domain=False,
        )
        src = store.get_seed(msg.source_domain) if msg.source_domain else None
        src_weight = src.weight if src else self._config.weight_update.initial_weight
        min_in_degree = self._config.admission.min_in_degree
        features["src_domain_weight"] = src_weight
        features["anchor_quality"] = self._online.anchor_quality(msg.anchor)
        features["url_health"] = url_health_score(entry_url)
        features["tld_bonus"] = self._online.tld_bonus(domain)
        features["in_degree_norm"] = min(1.0, in_degree / max(1, min_in_degree))
        features["spam_signal"] = self._online.spam_signal(domain, msg.anchor)
        score = self._online.quick_score(
            link=link,
            src_seed_weight=src_weight,
            in_degree=in_degree,
            min_in_degree=min_in_degree,
        )
        return SeedEvaluationResult(
            domain=domain,
            entry_url=entry_url,
            quality_score=score,
            weight=score,
            status=SeedStatus.PROBATION,
            features=features,
            action="evaluated_runtime",
            discovery_source=msg.source_type.value,
            discovery_trace_id=msg.trace_id,
            metadata=metadata,
        )


class QualityModel(Protocol):
    def score_html(self, html: str, meta: Optional[Dict] = None) -> float: ...


class HeuristicQualityModel:
    """MVP 轻量 HTML 质量启发式：长度 + 基本结构标签占比。"""

    def score_html(self, html: str, meta: Optional[Dict] = None) -> float:
        if not html:
            return 0.0
        length = len(html)
        len_score = min(1.0, length / 5000.0)
        low = html.lower()
        has_body = "<body" in low
        has_content = any(tag in low for tag in ("<p", "<article", "<main", "<div"))
        structure = 0.5 * (1.0 if has_body else 0.0) + 0.5 * (1.0 if has_content else 0.0)
        return round(0.6 * len_score + 0.4 * structure, 4)
