"""种子库管理器（Seed Store）。

以可注册域为主键的 SeedRecord，管理注册、权重、状态流转、淘汰。
MVP：内存 + JSONL 快照；生产：MySQL。
bootstrap 写出快照；workers 启动/周期 load/save。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, Iterator, Optional, Union

from config import Config
from src.util.logging_conf import get_logger, log
from src.domain.models import RenderMode, SeedEvaluationResult, SeedRecord, SeedStatus
from src.stores.offline_store import JsonlOfflineStore
from src.domain.urls import tld_of

logger = get_logger("seed_store")


class SeedStore:
    def __init__(self, config: Config, offline: Optional[JsonlOfflineStore] = None) -> None:
        self._config = config
        self._offline = offline
        self._seeds: Dict[str, SeedRecord] = {}
        self._blacklist: set = set()
        self._path = Path(config.seed_store_path)

    def has_seed(self, domain: str) -> bool:
        return domain in self._seeds

    def get_seed(self, domain: str) -> Optional[SeedRecord]:
        return self._seeds.get(domain)

    def is_blacklisted(self, domain: str) -> bool:
        return domain in self._blacklist

    def register(
        self,
        domain: str,
        *,
        weight: Optional[float] = None,
        status: SeedStatus = SeedStatus.ACTIVE,
        features: Optional[Dict[str, float]] = None,
        source_domain: Optional[str] = None,
    ) -> SeedRecord:
        """注册/合并种子（幂等）。"""
        rec = self._seeds.get(domain)
        if rec is None:
            rec = SeedRecord(
                domain=domain,
                weight=weight if weight is not None else self._config.weight_update.initial_weight,
                status=status,
                features=features or {},
                tld=tld_of(domain),
            )
            self._seeds[domain] = rec
            self._emit_event(domain, "seed_registered", status=status.value, weight=rec.weight)
        if source_domain and source_domain != domain:
            if source_domain not in rec.source_domains:
                rec.source_domains.add(source_domain)
                rec.in_degree = len(rec.source_domains)
        return rec

    def apply_evaluation(
        self,
        result: SeedEvaluationResult,
        *,
        source_domain: Optional[str] = None,
    ) -> SeedRecord:
        """将 MQ 消费侧评估结果写入种子库（幂等合并元数据）。"""
        rec = self._seeds.get(result.domain)
        is_new = rec is None
        if rec is None:
            rec = SeedRecord(
                domain=result.domain,
                weight=result.weight,
                status=result.status,
                features=dict(result.features),
                tld=tld_of(result.domain),
            )
            self._seeds[result.domain] = rec
        else:
            rec.weight = max(rec.weight, result.weight)
            # 已收录：仅允许晋升为 ACTIVE，不因运行时评估降级
            if result.status == SeedStatus.ACTIVE:
                rec.status = SeedStatus.ACTIVE
            rec.features.update(result.features)

        rec.entry_url = result.entry_url or rec.entry_url
        if result.homepage_url:
            rec.homepage_url = result.homepage_url
        if result.sitemap_url:
            rec.sitemap_url = result.sitemap_url
        if result.sample_content_url:
            rec.sample_content_url = result.sample_content_url
        if result.page_aggregate:
            rec.page_aggregate.update(result.page_aggregate)
        rec.discovery_source = result.discovery_source or rec.discovery_source
        rec.discovery_trace_id = result.discovery_trace_id or rec.discovery_trace_id
        rec.quality_score = result.quality_score
        rec.admission_score = result.quality_score if is_new else max(rec.admission_score, result.quality_score)
        rec.evaluation_version = "mvp-1"
        rec.last_evaluated_at = time.time()
        rec.metadata.update(result.metadata)
        rec.metadata["last_action"] = result.action

        if source_domain and source_domain != result.domain:
            if source_domain not in rec.source_domains:
                rec.source_domains.add(source_domain)
                rec.in_degree = len(rec.source_domains)

        event = "seed_evaluated" if not is_new else "seed_registered"
        self._emit_event(
            result.domain,
            event,
            action=result.action,
            quality_score=result.quality_score,
            weight=rec.weight,
            status=rec.status.value,
            discovery_source=result.discovery_source,
            features=result.features,
        )
        return rec

    def export_snapshot(self) -> list[Dict]:
        """导出当前种子库全量快照（用于审计/调试）。"""
        return [rec.to_dict() for rec in sorted(self._seeds.values(), key=lambda r: r.domain)]

    def save(self, path: Optional[Union[str, Path]] = None) -> Path:
        """持久化种子库到 JSONL。"""
        out = Path(path) if path else self._path
        out.parent.mkdir(parents=True, exist_ok=True)
        lock_path = out.with_suffix(out.suffix + ".lock")
        rows = self.export_snapshot()
        # 简易文件锁，降低多进程写冲突
        for _ in range(50):
            try:
                fd = open(lock_path, "x", encoding="utf-8")
                fd.write("1")
                fd.close()
                break
            except FileExistsError:
                time.sleep(0.02)
        else:
            log(logger, 30, "seed_store_save_lock_timeout", path=str(out))
        try:
            tmp = out.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            tmp.replace(out)
        finally:
            lock_path.unlink(missing_ok=True)
        log(logger, 20, "seed_store_saved", path=str(out), seeds=len(rows), stats=self.stats())
        return out

    def load(self, path: Optional[Union[str, Path]] = None, *, replace: bool = True) -> int:
        """从 JSONL 快照加载种子。"""
        src = Path(path) if path else self._path
        if not src.exists():
            log(logger, 30, "seed_store_missing", path=str(src))
            return 0
        if replace:
            self._seeds.clear()
        loaded = 0
        with src.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    rec = SeedRecord.from_dict(data)
                    self._seeds[rec.domain] = rec
                    loaded += 1
                except (json.JSONDecodeError, KeyError, ValueError) as exc:
                    log(logger, 40, "seed_store_load_error", path=str(src), line=line_no, error=repr(exc))
        log(logger, 20, "seed_store_loaded", path=str(src), loaded=loaded, stats=self.stats())
        return loaded

    def add_to_blacklist(self, domain: str) -> None:
        self._blacklist.add(domain)
        self._emit_event(domain, "blacklisted")

    def update_weight(self, domain: str, reward: float) -> Optional[SeedRecord]:
        rec = self.get_seed(domain)
        if rec is None:
            return None
        alpha = self._config.weight_update.ewma_alpha
        rec.weight = alpha * rec.weight + (1 - alpha) * reward
        rec.weight = max(0.0, min(1.0, rec.weight))
        self._emit_event(domain, "weight_update", weight=rec.weight, reward=reward)
        return rec

    def set_status(self, domain: str, status: SeedStatus) -> None:
        rec = self.get_seed(domain)
        if rec is None:
            return
        old = rec.status
        rec.status = status
        self._emit_event(domain, "status_change", old=old.value, new=status.value)

    def promote(self, domain: str) -> None:
        """观察期 → 正式种子（治理周期调用）。"""
        rec = self.get_seed(domain)
        if rec is None:
            return
        if rec.status == SeedStatus.PROBATION:
            self.set_status(domain, SeedStatus.ACTIVE)

    def evict(self, domain: str) -> None:
        self.set_status(domain, SeedStatus.EVICTED)
        self._emit_event(domain, "evicted")

    def set_render_mode(self, domain: str, mode: RenderMode) -> None:
        rec = self._seeds.get(domain)
        if rec and rec.render_mode != mode:
            rec.render_mode = mode
            self._emit_event(domain, "render_mode_change", mode=mode.value)

    def record_reachability(self, domain: str, ok: bool, latency: float) -> SeedRecord:
        rec = self.register(domain)
        rec.last_crawled = time.time()
        if ok:
            rec.success_count += 1
            rec.consecutive_fail = 0
            rec.last_success = rec.last_crawled
            rec.avg_latency = (rec.avg_latency * (rec.success_count - 1) + latency) / rec.success_count
        else:
            rec.fail_count += 1
            rec.consecutive_fail += 1
            if rec.consecutive_fail >= self._config.weight_update.max_consecutive_fail:
                rec.status = SeedStatus.SUSPENDED
                self._emit_event(domain, "suspended", consecutive_fail=rec.consecutive_fail)
        return rec

    def record_production(
        self, domain: str, *, size: int, content_len: int, quality: float, dup: bool,
    ) -> None:
        rec = self.get_seed(domain)
        if rec is None:
            return
        n = rec.produced_count + 1
        rec.produced_count = n
        rec.total_bytes += size
        rec.avg_content_len = (rec.avg_content_len * (n - 1) + content_len) / n
        rec.avg_quality = (rec.avg_quality * (n - 1) + quality) / n
        rec.dup_rate = (rec.dup_rate * (n - 1) + (1.0 if dup else 0.0)) / n
        rec.last_produced = time.time()

    def iter_crawl_roots(self) -> Iterator[SeedRecord]:
        """返回可调度种子入口（ACTIVE / PROBATION 等，由调用方过滤）。"""
        for rec in self._seeds.values():
            if rec.status == SeedStatus.ACTIVE:
                yield rec

    def iter_unscheduled_active(self) -> Iterator[SeedRecord]:
        """ACTIVE/PROBATION 且尚未投递 DataQueue 的种子（LinkScheduler 周期扫描）。

        PROBATION 需要抓取才能晋升，故一并投递试用。
        """
        for rec in self._seeds.values():
            if rec.status in (SeedStatus.ACTIVE, SeedStatus.PROBATION) and not rec.scheduled:
                yield rec

    def mark_scheduled(self, domain: str) -> None:
        rec = self.get_seed(domain)
        if rec is None:
            return
        if not rec.scheduled:
            rec.scheduled = True
            self._emit_event(domain, "seed_scheduled")

    def iter_seed_crawl_plan(self, rec: SeedRecord) -> Iterator[tuple[str, float, str]]:
        """生成种子入队计划：(url, priority, role)。sitemap 优先、优先级最高。"""
        seen: set[str] = set()
        if rec.sitemap_url:
            seen.add(rec.sitemap_url)
            yield rec.sitemap_url, min(1.0, rec.weight + 0.5), "sitemap"
        home = rec.homepage_url or rec.entry_url or f"https://{rec.domain}/"
        if home not in seen:
            seen.add(home)
            yield home, rec.weight, "homepage"
        if rec.sample_content_url and rec.sample_content_url not in seen:
            yield rec.sample_content_url, rec.weight * 0.85, "sample_content"

    def iter_active_seeds(self) -> Iterator[SeedRecord]:
        for rec in self._seeds.values():
            if rec.status in (SeedStatus.ACTIVE, SeedStatus.PROBATION):
                yield rec

    def seed_weight(self, domain: str) -> float:
        rec = self.get_seed(domain)
        return rec.weight if rec else 0.0

    def stats(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for rec in self._seeds.values():
            counts[rec.status.value] = counts.get(rec.status.value, 0) + 1
        counts["seeds"] = len(self._seeds)
        return counts

    def count_active_by_tld(self, tld: str) -> int:
        return sum(1 for r in self._seeds.values() if r.status == SeedStatus.ACTIVE and r.tld == tld)

    def count_active(self) -> int:
        return sum(1 for r in self._seeds.values() if r.status == SeedStatus.ACTIVE)

    def _emit_event(self, domain: str, event_type: str, **payload) -> None:
        if self._offline is not None:
            self._offline.record_seed_event(domain, event_type, **payload)
        log(logger, 10, "seed_event", domain=domain, event=event_type, **payload)
