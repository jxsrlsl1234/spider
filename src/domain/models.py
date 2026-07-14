"""核心数据模型。

包含：
- 枚举（种子状态、抓取层级、阶段）
- 种子实体 SeedRecord（以可注册域为主键）
- 抓取任务与结果、HTML 页面记录
- 全链路透传对象 CollectionTrace / StageEvent
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class SeedStatus(str, Enum):
    """种子在 SeedStore 中的生命周期状态。

    未准入的新域暂存在 ``CandidatePool``（落盘缓冲），**不入库**，无对应枚举值。

    状态流转（MVP 已实现）::

        [准入] ──人工/高分ETL──► ACTIVE ◄──治理晋升── PROBATION ◄──运行时扩链/低分ETL── [MQ准入]
                                  │                    │
                    连续抓取失败   │                    └──低质──► EVICTED
                                  ▼
                             SUSPENDED

    详见 ``DIAGRAMS.md`` 种子状态图。
    """

    PROBATION = "probation"    # 观察期：运行时扩链/低分 ETL 经 MQ 准入；可调度，治理据 HTML 产出晋升或淘汰
    ACTIVE = "active"          # 正式种子：人工/高分 ETL 准入，或 PROBATION 试用达标后 promote；正常调度
    SUSPENDED = "suspended"    # 暂停：record_reachability 连续抓取失败达阈值；不参与调度
    EVICTED = "evicted"        # 淘汰：治理低质 evict；永久不参与调度，记录保留审计


class SeedSourceType(str, Enum):
    """种子发现来源（统一 MQ 入口）。"""

    MANUAL = "manual"                      # 人工策展 seeds.txt
    AUTO_ETL = "auto_etl"                  # 离线自动寻源管线
    RUNTIME_CROSS_DOMAIN = "runtime_cross_domain"  # 运行时页面扩链


class RenderMode(str, Enum):
    """域级记忆的抓取层级（见 DESIGN.md §3.4）。"""

    STATIC = "static"
    FINGERPRINT = "fingerprint"
    BROWSER = "browser"
    PROTECTED = "protected"


class Stage(str, Enum):
    """采集链路阶段（用于全链路透传的事件打点）。"""

    ENQUEUE = "enqueue"
    SCHEDULE = "schedule"
    FETCH = "fetch"
    PARSE = "parse"
    STORE = "store"
    DISCOVERY = "discovery"
    ADMISSION = "admission"
    MQ_PUBLISH = "mq_publish"
    WEIGHT_UPDATE = "weight_update"
    ERROR = "error"


def _now() -> float:
    return time.time()


def new_trace_id() -> str:
    return uuid.uuid4().hex


@dataclass
class SeedRecord:
    """种子实体（以可注册域为主键，见 DESIGN.md §2.2）。"""

    domain: str
    weight: float = 0.5
    status: SeedStatus = SeedStatus.ACTIVE
    render_mode: RenderMode = RenderMode.STATIC
    robots_meta: Dict[str, Any] = field(default_factory=dict)
    rate_limit: Optional[float] = None
    features: Dict[str, float] = field(default_factory=dict)
    # 可达性统计
    success_count: int = 0
    fail_count: int = 0
    consecutive_fail: int = 0
    avg_latency: float = 0.0
    # HTML 产出统计
    produced_count: int = 0
    total_bytes: int = 0
    avg_content_len: float = 0.0
    avg_quality: float = 0.0
    dup_rate: float = 0.0
    # 入度（按来源可注册域去重）
    in_degree: int = 0
    source_domains: Set[str] = field(default_factory=set)
    # 标签（多样性配额）
    tld: str = ""
    lang: str = ""
    asn: str = ""
    topic_tags: Set[str] = field(default_factory=set)
    first_seen: float = field(default_factory=_now)
    last_crawled: Optional[float] = None
    last_success: Optional[float] = None
    last_produced: Optional[float] = None
    # 寻源与评估元数据（统一 MQ 准入后写入）
    entry_url: str = ""
    homepage_url: str = ""
    sitemap_url: str = ""           # 扩链最佳入口；采集时优先抓取
    sample_content_url: str = ""    # ETL 随机内容页样本
    page_aggregate: Dict[str, Any] = field(default_factory=dict)  # 标题/状态码等域级聚合
    discovery_source: str = ""
    discovery_trace_id: str = ""
    quality_score: float = 0.0
    admission_score: float = 0.0
    evaluation_version: str = "mvp-1"
    last_evaluated_at: Optional[float] = None
    # LinkScheduler：是否已将入口 URL 投递过 DataQueue（未开始抓取=False）
    scheduled: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "entry_url": self.entry_url,
            "homepage_url": self.homepage_url,
            "sitemap_url": self.sitemap_url,
            "sample_content_url": self.sample_content_url,
            "page_aggregate": self.page_aggregate,
            "weight": round(self.weight, 4),
            "status": self.status.value,
            "render_mode": self.render_mode.value,
            "quality_score": round(self.quality_score, 4),
            "admission_score": round(self.admission_score, 4),
            "evaluation_version": self.evaluation_version,
            "last_evaluated_at": self.last_evaluated_at,
            "scheduled": self.scheduled,
            "discovery_source": self.discovery_source,
            "discovery_trace_id": self.discovery_trace_id,
            "features": {k: round(v, 4) for k, v in self.features.items()},
            "metadata": self.metadata,
            "robots_meta": self.robots_meta,
            "rate_limit": self.rate_limit,
            "reachability": {
                "success_count": self.success_count,
                "fail_count": self.fail_count,
                "consecutive_fail": self.consecutive_fail,
                "avg_latency": round(self.avg_latency, 4),
                "last_crawled": self.last_crawled,
                "last_success": self.last_success,
            },
            "production": {
                "produced_count": self.produced_count,
                "total_bytes": self.total_bytes,
                "avg_content_len": round(self.avg_content_len, 2),
                "avg_quality": round(self.avg_quality, 4),
                "dup_rate": round(self.dup_rate, 4),
                "last_produced": self.last_produced,
            },
            "graph": {
                "in_degree": self.in_degree,
                "source_domains": sorted(self.source_domains),
            },
            "taxonomy": {
                "tld": self.tld,
                "lang": self.lang,
                "asn": self.asn,
                "topic_tags": sorted(self.topic_tags),
            },
            "timestamps": {
                "first_seen": self.first_seen,
                "last_crawled": self.last_crawled,
                "last_success": self.last_success,
                "last_produced": self.last_produced,
                "last_evaluated_at": self.last_evaluated_at,
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SeedRecord":
        """从快照 JSON 还原 SeedRecord。"""
        reach = data.get("reachability") or {}
        prod = data.get("production") or {}
        graph = data.get("graph") or {}
        tax = data.get("taxonomy") or {}
        ts = data.get("timestamps") or {}
        return cls(
            domain=data["domain"],
            weight=float(data.get("weight", 0.5)),
            status=SeedStatus(data.get("status", SeedStatus.ACTIVE.value)),
            render_mode=RenderMode(data.get("render_mode", RenderMode.STATIC.value)),
            robots_meta=dict(data.get("robots_meta") or {}),
            rate_limit=data.get("rate_limit"),
            features={k: float(v) for k, v in (data.get("features") or {}).items()},
            success_count=int(reach.get("success_count", 0)),
            fail_count=int(reach.get("fail_count", 0)),
            consecutive_fail=int(reach.get("consecutive_fail", 0)),
            avg_latency=float(reach.get("avg_latency", 0.0)),
            produced_count=int(prod.get("produced_count", 0)),
            total_bytes=int(prod.get("total_bytes", 0)),
            avg_content_len=float(prod.get("avg_content_len", 0.0)),
            avg_quality=float(prod.get("avg_quality", 0.0)),
            dup_rate=float(prod.get("dup_rate", 0.0)),
            in_degree=int(graph.get("in_degree", 0)),
            source_domains=set(graph.get("source_domains") or []),
            tld=str(tax.get("tld", "")),
            lang=str(tax.get("lang", "")),
            asn=str(tax.get("asn", "")),
            topic_tags=set(tax.get("topic_tags") or []),
            first_seen=float(ts.get("first_seen") or data.get("first_seen") or _now()),
            last_crawled=ts.get("last_crawled") or reach.get("last_crawled"),
            last_success=ts.get("last_success") or reach.get("last_success"),
            last_produced=ts.get("last_produced") or prod.get("last_produced"),
            entry_url=str(data.get("entry_url", "")),
            homepage_url=str(data.get("homepage_url", "")),
            sitemap_url=str(data.get("sitemap_url", "")),
            sample_content_url=str(data.get("sample_content_url", "")),
            page_aggregate=dict(data.get("page_aggregate") or {}),
            discovery_source=str(data.get("discovery_source", "")),
            discovery_trace_id=str(data.get("discovery_trace_id", "")),
            quality_score=float(data.get("quality_score", 0.0)),
            admission_score=float(data.get("admission_score", 0.0)),
            evaluation_version=str(data.get("evaluation_version", "mvp-1")),
            last_evaluated_at=data.get("last_evaluated_at") or ts.get("last_evaluated_at"),
            scheduled=bool(data.get("scheduled", False)),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass
class CandidateRecord:
    """候选缓冲池条目（未写入 SeedStore 的新域）。

    与 ``SeedStatus`` 无关：未过三闸门的域名只存在此池，不入库。
    详见 ``seed_collect_mq.CandidatePool``。
    """

    domain: str
    quick_score: float = 0.0
    in_degree: int = 0
    source_domains: Set[str] = field(default_factory=set)
    best_anchor: str = ""
    best_src_weight: float = 0.0
    features: Dict[str, float] = field(default_factory=dict)
    first_seen: float = field(default_factory=_now)
    last_seen: float = field(default_factory=_now)
    entry_url: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "quick_score": self.quick_score,
            "in_degree": self.in_degree,
            "source_domains": sorted(self.source_domains),
            "best_anchor": self.best_anchor,
            "best_src_weight": self.best_src_weight,
            "features": self.features,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "entry_url": self.entry_url,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CandidateRecord":
        sources = set(data.get("source_domains") or [])
        return cls(
            domain=data["domain"],
            quick_score=float(data.get("quick_score") or 0.0),
            in_degree=int(data.get("in_degree") or len(sources)),
            source_domains=sources,
            best_anchor=str(data.get("best_anchor") or ""),
            best_src_weight=float(data.get("best_src_weight") or 0.0),
            features=dict(data.get("features") or {}),
            first_seen=float(data.get("first_seen") or _now()),
            last_seen=float(data.get("last_seen") or _now()),
            entry_url=str(data.get("entry_url") or ""),
        )


@dataclass
class Task:
    """抓取任务：调度单元 = URL。"""

    url: str
    domain: str
    priority: float = 0.0
    depth: int = 0
    source_domain: Optional[str] = None
    source_url: Optional[str] = None
    ready_time: float = field(default_factory=_now)
    trace_id: str = field(default_factory=new_trace_id)

    def __lt__(self, other: "Task") -> bool:
        return (-self.priority, self.ready_time) < (-other.priority, other.ready_time)


@dataclass
class FetchResult:
    url: str
    ok: bool
    status: int = 0
    html: Optional[str] = None
    content_type: str = ""
    elapsed: float = 0.0
    render_mode: RenderMode = RenderMode.STATIC
    error: Optional[str] = None
    from_challenge: bool = False


@dataclass
class HtmlPage:
    """抓取到的 HTML 页面（核心采集产物）。"""

    url: str
    html: str
    content_hash: str = ""
    size: int = 0
    quality: float = 0.0
    status: int = 200
    content_type: str = "text/html"


@dataclass
class SeedCollectMessage:
    """SeedCollectMq 消息体：未经质量验证的候选种子站点。"""

    domain: str
    entry_url: str
    source_type: SeedSourceType
    source_domain: str = ""
    source_url: str = ""
    anchor: str = ""
    position: str = "content"
    trace_id: str = ""
    depth: int = 0
    discovered_at: float = field(default_factory=_now)
    hints: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "entry_url": self.entry_url,
            "source_type": self.source_type.value,
            "source_domain": self.source_domain,
            "source_url": self.source_url,
            "anchor": self.anchor,
            "position": self.position,
            "trace_id": self.trace_id,
            "depth": self.depth,
            "discovered_at": self.discovered_at,
            "hints": self.hints,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SeedCollectMessage":
        st = data.get("source_type", SeedSourceType.RUNTIME_CROSS_DOMAIN.value)
        return cls(
            domain=data["domain"],
            entry_url=data.get("entry_url", f"https://{data['domain']}/"),
            source_type=SeedSourceType(st),
            source_domain=data.get("source_domain", ""),
            source_url=data.get("source_url", ""),
            anchor=data.get("anchor", ""),
            position=data.get("position", "content"),
            trace_id=data.get("trace_id", ""),
            depth=int(data.get("depth", 0)),
            discovered_at=float(data.get("discovered_at", _now())),
            hints=dict(data.get("hints") or {}),
        )


@dataclass
class SeedEvaluationResult:
    """价值评估产出，供 SeedStore 写入。"""

    domain: str
    entry_url: str
    quality_score: float
    weight: float
    status: SeedStatus
    features: Dict[str, float]
    action: str
    discovery_source: str
    discovery_trace_id: str = ""
    sitemap_url: str = ""
    homepage_url: str = ""
    sample_content_url: str = ""
    page_aggregate: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "entry_url": self.entry_url,
            "quality_score": round(self.quality_score, 4),
            "weight": round(self.weight, 4),
            "status": self.status.value,
            "features": {k: round(v, 4) for k, v in self.features.items()},
            "action": self.action,
            "discovery_source": self.discovery_source,
            "discovery_trace_id": self.discovery_trace_id,
            "sitemap_url": self.sitemap_url,
            "homepage_url": self.homepage_url,
            "sample_content_url": self.sample_content_url,
            "page_aggregate": self.page_aggregate,
            "metadata": self.metadata,
        }


@dataclass
class DiscoveredLink:
    """从页面发现的一条出链（已规范化）。"""

    url: str
    domain: str
    anchor: str = ""
    position: str = "content"  # content | nav | footer | ad
    same_domain: bool = False


@dataclass
class StageEvent:
    stage: Stage
    ts: float = field(default_factory=_now)
    ok: bool = True
    detail: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {"stage": self.stage.value, "ts": self.ts, "ok": self.ok, "detail": self.detail}


@dataclass
class CollectionTrace:
    """一条 URL 采集的全链路记录，最终落离线存储用于分析与导出。"""

    trace_id: str
    url: str
    domain: str
    source_domain: Optional[str] = None
    depth: int = 0
    started_at: float = field(default_factory=_now)
    finished_at: Optional[float] = None
    events: List[StageEvent] = field(default_factory=list)
    stored_objects: List[Dict[str, Any]] = field(default_factory=list)
    mq_published_domains: List[str] = field(default_factory=list)
    admitted_domains: List[str] = field(default_factory=list)  # 由 MQ 消费者异步写入离线事件

    def add(self, stage: Stage, ok: bool = True, **detail: Any) -> None:
        self.events.append(StageEvent(stage=stage, ok=ok, detail=detail))

    def finish(self) -> None:
        self.finished_at = _now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "url": self.url,
            "domain": self.domain,
            "source_domain": self.source_domain,
            "depth": self.depth,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration": (self.finished_at - self.started_at) if self.finished_at else None,
            "events": [e.to_dict() for e in self.events],
            "stored_objects": self.stored_objects,
            "mq_published_domains": self.mq_published_domains,
            "discovered_domains": self.mq_published_domains,  # 兼容旧字段名
            "admitted_domains": self.admitted_domains,
        }
