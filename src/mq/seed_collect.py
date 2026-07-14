"""SeedCollectMq：寻源收集消息队列。

职责：承接**未经质量验证**的候选种子站点（人工 / 自动 ETL / 运行时跨域扩链）。
消费者 ``SeedCollectConsumer`` 经 Evaluator 评估后写入 ``SeedStore``。

与 ``DataQueue``（抓取任务队列）严格分离：
- SeedCollectMq  → 种子准入管线（寻源 → 评估 → SeedStore）
- DataQueue      → 抓取任务管线（LinkScheduler → TaskScheduler）
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

from config import Config
from src.seed.evaluator import SeedAdmissionEvaluator
from src.etl.domain_merge import DomainMergeRecord
from src.util.logging_conf import get_logger, log
from src.domain.models import (
    CandidateRecord,
    DiscoveredLink,
    SeedCollectMessage,
    SeedSourceType,
    SeedStatus,
)
from src.mq.file_queue import FileMessageQueue
from src.stores.seed_store import SeedStore
from src.domain.urls import tld_of

logger = get_logger("seed_collect_mq")


@dataclass
class SeedCollectAdmission:
    """SeedCollectConsumer 准入结果；新 ACTIVE 由 LinkScheduler 周期投递 DataQueue。"""

    domain: str
    entry_url: str
    depth: int
    status: SeedStatus
    is_new: bool


class SeedCollectMq:
    """寻源收集 MQ（跨进程：本地目录 FileMessageQueue）。"""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._topic = config.seed_collect_mq.topic
        self._backend = FileMessageQueue(Path(config.seed_collect_mq.base_dir))
        self.published = 0
        self.consumed = 0

    @property
    def topic(self) -> str:
        return self._topic

    def publish(self, message: Dict) -> None:
        self._backend.publish(self._topic, message)
        self.published += 1

    def consume(self, max_messages: int) -> List[Dict]:
        batch = self._backend.consume(self._topic, max_messages=max_messages)
        self.consumed += len(batch)
        return batch

    def pending(self) -> int:
        return self._backend.pending(self._topic)


class CandidatePool:
    """运行时跨域候选缓冲（未写入 SeedStore 的新域；跨进程落盘）。"""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._pool: Dict[str, CandidateRecord] = {}
        self._path = config.resolve_path(config.admission.candidate_pool_path)
        self.load()

    def upsert(
        self,
        domain: str,
        source_domain: str,
        quick_score: float,
        anchor: str,
        src_weight: float,
        features: Dict[str, float],
        *,
        entry_url: str = "",
    ) -> CandidateRecord:
        rec = self._pool.get(domain)
        if rec is None:
            rec = CandidateRecord(domain=domain)
            self._pool[domain] = rec
        if source_domain and source_domain != domain:
            rec.source_domains.add(source_domain)
        rec.in_degree = len(rec.source_domains)
        if entry_url and not rec.entry_url:
            rec.entry_url = entry_url
        if quick_score >= rec.quick_score:
            rec.quick_score = quick_score
            rec.best_anchor = anchor
            rec.best_src_weight = src_weight
            rec.features = features
            if entry_url:
                rec.entry_url = entry_url
        rec.last_seen = time.time()
        return rec

    def remove(self, domain: str) -> None:
        self._pool.pop(domain, None)

    def evict_expired(self) -> int:
        now = time.time()
        ttl = self._config.admission.candidate_ttl_seconds
        expired = [d for d, r in self._pool.items() if now - r.first_seen > ttl]
        for d in expired:
            self._pool.pop(d, None)
        return len(expired)

    def enforce_capacity(self) -> int:
        cap = self._config.admission.candidate_pool_capacity
        if len(self._pool) <= cap:
            return 0
        ordered = sorted(self._pool.items(), key=lambda kv: kv[1].quick_score)
        drop = len(self._pool) - cap
        for d, _ in ordered[:drop]:
            self._pool.pop(d, None)
        return drop

    def size(self) -> int:
        return len(self._pool)

    def items(self) -> List[CandidateRecord]:
        return list(self._pool.values())

    def load(self) -> int:
        if not self._path.exists():
            return 0
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log(logger, 30, "candidate_pool_load_failed", path=str(self._path), error=repr(exc))
            return 0
        rows = data.get("candidates") if isinstance(data, dict) else data
        if not isinstance(rows, list):
            return 0
        for row in rows:
            if not isinstance(row, dict) or not row.get("domain"):
                continue
            rec = CandidateRecord.from_dict(row)
            self._pool[rec.domain] = rec
        log(logger, 20, "candidate_pool_loaded", path=str(self._path), size=len(self._pool))
        return len(self._pool)

    def save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": time.time(),
            "candidates": [r.to_dict() for r in self._pool.values()],
        }
        self._path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class SeedCollectPublisher:
    """寻源发布侧：将未验证候选种子写入 SeedCollectMq。"""

    def __init__(self, config: Config, mq: SeedCollectMq) -> None:
        self._config = config
        self._mq = mq
        self.published_count = 0

    @property
    def topic(self) -> str:
        return self._mq.topic

    def publish(self, msg: SeedCollectMessage) -> None:
        self._mq.publish(msg.to_dict())
        self.published_count += 1

    def publish_manual(self, url: str, domain: str, *, hints: Optional[Dict] = None) -> None:
        self.publish(SeedCollectMessage(
            domain=domain,
            entry_url=url,
            source_type=SeedSourceType.MANUAL,
            hints=hints or {"curator": "seeds.txt"},
        ))

    def publish_auto_etl(
        self,
        record: DomainMergeRecord,
        *,
        extra_hints: Optional[Dict] = None,
        trace_id: str = "",
    ) -> None:
        """自动 ETL 归并后的单域记录 → SeedCollectMq。"""
        hints = {**record.to_hints(), **(extra_hints or {})}
        self.publish(SeedCollectMessage(
            domain=record.domain,
            entry_url=record.entry_url,
            source_type=SeedSourceType.AUTO_ETL,
            trace_id=trace_id,
            hints=hints,
        ))

    def publish_auto_etl_url(
        self, url: str, domain: str, *, hints: Optional[Dict] = None, trace_id: str = "",
    ) -> None:
        """兼容：仅 URL 发布（无 Merge 元数据）。"""
        self.publish(SeedCollectMessage(
            domain=domain,
            entry_url=url,
            source_type=SeedSourceType.AUTO_ETL,
            trace_id=trace_id,
            hints=hints or {},
        ))

    def publish_runtime_links(
        self,
        *,
        source_domain: str,
        source_url: str,
        depth: int,
        trace_id: str,
        links: List[DiscoveredLink],
    ) -> List[str]:
        """TaskScheduler 跨域扩链：发布未验证新域到 SeedCollectMq。"""
        published: Set[str] = set()
        for link in links:
            if link.same_domain or not link.domain or link.domain == source_domain:
                continue
            if link.domain in published:
                continue
            self.publish(SeedCollectMessage(
                domain=link.domain,
                entry_url=link.url,
                source_type=SeedSourceType.RUNTIME_CROSS_DOMAIN,
                source_domain=source_domain,
                source_url=source_url,
                anchor=link.anchor,
                position=link.position,
                trace_id=trace_id,
                depth=depth + 1,
            ))
            published.add(link.domain)
        return sorted(published)


class SeedCollectConsumer:
    """寻源消费侧：Evaluator 质量验证 → 写入 SeedStore（不直接操作 DataQueue）。"""

    def __init__(
        self,
        config: Config,
        seed_store: SeedStore,
        mq: SeedCollectMq,
        *,
        offline=None,
        evaluator: Optional[SeedAdmissionEvaluator] = None,
    ) -> None:
        self._config = config
        self._store = seed_store
        self._mq = mq
        self._evaluator = evaluator or SeedAdmissionEvaluator(config)
        self._offline = offline
        self._pool = CandidatePool(config)
        self._promotions_this_cycle = 0
        self.messages_processed = 0
        self.updated_existing = 0
        self.newly_admitted = 0
        self.rejected = 0

    def reset_cycle(self) -> None:
        self._promotions_this_cycle = 0
        self._pool.evict_expired()
        self._pool.enforce_capacity()

    @property
    def candidate_pool_size(self) -> int:
        return self._pool.size()

    def consume_batch(self, max_messages: Optional[int] = None) -> List[SeedCollectAdmission]:
        n = max_messages or self._config.seed_collect_mq.batch_consume_size
        messages = self._mq.consume(n)
        admissions: List[SeedCollectAdmission] = []
        for raw in messages:
            adm = self._handle_message(raw)
            if adm is not None:
                admissions.append(adm)
        admissions.extend(self.promote_ready_candidates())
        self._pool.save()
        return admissions

    def drain(self) -> List[SeedCollectAdmission]:
        all_admissions: List[SeedCollectAdmission] = []
        while self._mq.pending() > 0:
            all_admissions.extend(self.consume_batch())
        return all_admissions

    def promote_ready_candidates(self) -> List[SeedCollectAdmission]:
        """按当前闸门重试候选池（演示下调 min_in_degree 后可立刻入库）。"""
        admissions: List[SeedCollectAdmission] = []
        for rec in list(self._pool.items()):
            if self._store.has_seed(rec.domain) or self._store.is_blacklisted(rec.domain):
                self._pool.remove(rec.domain)
                continue
            if not self._passes_runtime_gates(rec.domain, rec):
                continue
            entry_url = rec.entry_url or f"https://{rec.domain}/"
            source_domain = next(iter(sorted(rec.source_domains)), "")
            msg = SeedCollectMessage(
                domain=rec.domain,
                entry_url=entry_url,
                source_type=SeedSourceType.RUNTIME_CROSS_DOMAIN,
                source_domain=source_domain,
                anchor=rec.best_anchor,
            )
            admit_result = self._evaluator.evaluate_discovery(msg, store=self._store)
            admit_result.status = SeedStatus.PROBATION
            admit_result.action = "admitted_runtime"
            admit_result.weight = rec.quick_score
            admit_result.quality_score = rec.quick_score
            seed = self._store.apply_evaluation(admit_result, source_domain=source_domain or None)
            if seed:
                seed.source_domains |= rec.source_domains
                seed.in_degree = len(seed.source_domains)
                seed.scheduled = False
            self._pool.remove(rec.domain)
            self._promotions_this_cycle += 1
            self.newly_admitted += 1
            self._emit_event(msg, action="admitted_runtime", evaluation=admit_result.to_dict(), seed=seed.to_dict() if seed else {})
            log(
                logger, 20, "seed_admitted",
                domain=rec.domain, source_type=msg.source_type.value,
                quick_score=round(rec.quick_score, 4), in_degree=rec.in_degree,
            )
            admissions.append(SeedCollectAdmission(
                domain=rec.domain,
                entry_url=entry_url,
                depth=msg.depth,
                status=SeedStatus.PROBATION,
                is_new=True,
            ))
        return admissions

    def _handle_message(self, raw: dict) -> Optional[SeedCollectAdmission]:
        msg = SeedCollectMessage.from_dict(raw)
        self.messages_processed += 1
        domain = msg.domain

        if self._store.is_blacklisted(domain):
            self._emit_event(msg, action="skipped_blacklist")
            self.rejected += 1
            return None

        if self._store.has_seed(domain):
            result = self._evaluator.evaluate_discovery(msg, store=self._store)
            self._store.apply_evaluation(result, source_domain=msg.source_domain or None)
            self.updated_existing += 1
            self._emit_event(msg, action="updated_existing", evaluation=result.to_dict())
            log(logger, 20, "seed_updated", domain=domain, source=msg.source_domain)
            return None

        if msg.source_type in (SeedSourceType.MANUAL, SeedSourceType.AUTO_ETL):
            return self._admit_curated(msg)

        return self._admit_runtime(msg)

    def _admit_curated(self, msg: SeedCollectMessage) -> SeedCollectAdmission:
        result = self._evaluator.evaluate_discovery(msg, store=self._store)
        rec = self._store.apply_evaluation(result, source_domain=msg.source_domain or None)
        self.newly_admitted += 1
        self._emit_event(msg, action=result.action, evaluation=result.to_dict(), seed=rec.to_dict())
        log(logger, 20, "seed_admitted", domain=msg.domain, source_type=msg.source_type.value,
            quality_score=round(result.quality_score, 4), status=rec.status.value)
        return SeedCollectAdmission(
            domain=msg.domain,
            entry_url=msg.entry_url,
            depth=msg.depth,
            status=rec.status,
            is_new=True,
        )

    def _admit_runtime(self, msg: SeedCollectMessage) -> Optional[SeedCollectAdmission]:
        result = self._evaluator.evaluate_discovery(msg, store=self._store)
        rec = self._pool.upsert(
            msg.domain,
            msg.source_domain,
            quick_score=result.quality_score,
            anchor=msg.anchor,
            src_weight=result.features.get("src_domain_weight", 0.5),
            features=result.features,
            entry_url=msg.entry_url,
        )
        rec.quick_score = max(rec.quick_score, result.quality_score)

        if not self._passes_runtime_gates(msg.domain, rec):
            self._emit_event(
                msg, action="candidate_pending",
                evaluation=result.to_dict(),
                in_degree=rec.in_degree,
                quick_score=rec.quick_score,
            )
            return None

        admit_result = result
        admit_result.status = SeedStatus.PROBATION
        admit_result.action = "admitted_runtime"
        admit_result.weight = rec.quick_score
        admit_result.quality_score = rec.quick_score
        seed = self._store.apply_evaluation(admit_result, source_domain=msg.source_domain)
        if seed:
            seed.source_domains |= rec.source_domains
            seed.in_degree = len(seed.source_domains)
            seed.scheduled = False
        self._pool.remove(msg.domain)
        self._promotions_this_cycle += 1
        self.newly_admitted += 1
        self._emit_event(msg, action="admitted_runtime", evaluation=admit_result.to_dict(), seed=seed.to_dict())
        log(logger, 20, "seed_admitted", domain=msg.domain, source_type=msg.source_type.value,
            quick_score=round(rec.quick_score, 4), in_degree=rec.in_degree, trace_id=msg.trace_id)
        return SeedCollectAdmission(
            domain=msg.domain,
            entry_url=msg.entry_url,
            depth=msg.depth,
            status=SeedStatus.PROBATION,
            is_new=True,
        )

    def _passes_runtime_gates(self, domain: str, rec: CandidateRecord) -> bool:
        adm = self._config.admission
        if rec.in_degree < adm.min_in_degree:
            return False
        if rec.quick_score < adm.quick_score_threshold:
            return False
        if self._promotions_this_cycle >= adm.promotions_per_cycle:
            return False
        if self._store.count_active() >= self._config.diversity.active_capacity:
            return False
        if self._store.count_active_by_tld(tld_of(domain)) >= self._config.diversity.max_active_per_tld:
            return False
        return True

    def _emit_event(self, msg: SeedCollectMessage, action: str, **extra) -> None:
        if self._offline is not None:
            self._offline.emit(
                "seed_collect_consumer",
                {"action": action, **msg.to_dict(), **extra},
            )


def publish_static_seeds(config: Config, publisher: SeedCollectPublisher) -> int:
    """人工寻源：seeds.txt → SeedCollectMq（未经质量验证）。"""
    from src.bootstrap import load_static_seeds

    count = 0
    for url, domain in load_static_seeds(config):
        publisher.publish_manual(url, domain)
        count += 1
    log(logger, 20, "manual_seeds_published", count=count, topic=publisher.topic)
    return count
