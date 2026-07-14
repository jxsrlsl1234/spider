"""查看扩链真实情况：SeedStore / 候选池 / 离线 pending。

用法::

    python -m src.tools.seed_discovery_stats
    python -m src.tools.seed_discovery_stats --admit-pending --min-in-degree 1
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import Config
from src.util.logging_conf import get_logger, log, setup_logging
from src.stores.offline_store import JsonlOfflineStore
from src.runtime import build_seed_collect_mq, build_seed_store
from src.mq.seed_collect import CandidatePool, SeedCollectConsumer
from src.seed.evaluator import SeedAdmissionEvaluator

logger = get_logger("tools.seed_discovery_stats")


def _load_offline_pending(offline_dir: Path) -> List[Dict[str, Any]]:
    root = offline_dir / "seed_collect_consumer"
    if not root.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for part in sorted(root.glob("dt=*/part-*.jsonl")):
        for line in part.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("action") == "candidate_pending":
                rows.append(row)
    return rows


def _merge_pending_into_pool(pool: CandidatePool, rows: List[Dict[str, Any]]) -> int:
    before = pool.size()
    by_domain: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        domain = row.get("domain")
        if domain:
            by_domain[str(domain)].append(row)
    for domain, hits in by_domain.items():
        for hit in hits:
            qs = float(hit.get("quick_score") or 0.0)
            features = {}
            evaluation = hit.get("evaluation") or {}
            if isinstance(evaluation, dict):
                features = dict(evaluation.get("features") or {})
                qs = max(qs, float(evaluation.get("quality_score") or qs))
            pool.upsert(
                domain,
                str(hit.get("source_domain") or ""),
                quick_score=qs,
                anchor=str(hit.get("anchor") or ""),
                src_weight=float(features.get("src_domain_weight", 0.5)),
                features=features,
                entry_url=str(hit.get("entry_url") or ""),
            )
    pool.save()
    return pool.size() - before


def print_stats(config: Config) -> None:
    store_path = config.resolve_path(config.seed_store_path)
    pool_path = config.resolve_path(config.admission.candidate_pool_path)
    offline_dir = config.resolve_path(config.offline_dir)

    status_counter: Counter = Counter()
    sched_false = 0
    seeds = 0
    if store_path.exists():
        for line in store_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            seeds += 1
            status_counter[row.get("status", "?")] += 1
            if not row.get("scheduled", True):
                sched_false += 1

    pool = CandidatePool(config)
    pending_rows = _load_offline_pending(offline_dir)
    pending_domains = Counter(r.get("domain") for r in pending_rows)

    print("=== SeedStore ===")
    print(f"path={store_path} count={seeds} by_status={dict(status_counter)} scheduled_false={sched_false}")
    print()
    print("=== CandidatePool (disk) ===")
    print(f"path={pool_path} size={pool.size()}")
    for rec in sorted(pool.items(), key=lambda r: (-r.in_degree, -r.quick_score))[:20]:
        gate_ok = (
            rec.in_degree >= config.admission.min_in_degree
            and rec.quick_score >= config.admission.quick_score_threshold
        )
        print(
            f"  {rec.domain}: in_degree={rec.in_degree} qs={rec.quick_score:.4f} "
            f"sources={sorted(rec.source_domains)} gate={'PASS' if gate_ok else 'WAIT'}"
        )
    print()
    print("=== Offline candidate_pending ===")
    print(f"events={len(pending_rows)} unique_domains={len(pending_domains)}")
    for domain, hits in pending_domains.most_common(20):
        sample = next(r for r in pending_rows if r.get("domain") == domain)
        print(
            f"  {domain}: hits={hits} in_degree={sample.get('in_degree')} "
            f"qs={sample.get('quick_score')} source={sample.get('source_domain')}"
        )
    print()
    print(
        f"当前闸门: min_in_degree={config.admission.min_in_degree} "
        f"quick_score_threshold={config.admission.quick_score_threshold}"
    )
    print("说明: WAIT 表示扩链已发现但未入库；多源引用或下调 --min-in-degree 后可 PASS。")


def admit_pending(config: Config) -> int:
    offline = JsonlOfflineStore(config.offline_dir)
    store = build_seed_store(config, offline)
    store.load()
    mq = build_seed_collect_mq(config)
    consumer = SeedCollectConsumer(
        config, store, mq, offline=offline, evaluator=SeedAdmissionEvaluator(config),
    )
    added = _merge_pending_into_pool(consumer._pool, _load_offline_pending(config.resolve_path(config.offline_dir)))
    log(logger, 20, "rehydrated_pending", merged_or_updated=added, pool_size=consumer.candidate_pool_size)
    consumer.reset_cycle()
    admissions = consumer.promote_ready_candidates()
    consumer._pool.save()
    store.save()
    for adm in admissions:
        log(logger, 20, "demo_admitted", domain=adm.domain, status=adm.status.value)
    print(f"admitted={len(admissions)} domains={[a.domain for a in admissions]}")
    return len(admissions)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="扩链发现统计 / 演示准入 pending")
    parser.add_argument("--min-in-degree", type=int, default=None, help="覆盖 admission.min_in_degree")
    parser.add_argument("--quick-score-threshold", type=float, default=None)
    parser.add_argument(
        "--admit-pending",
        action="store_true",
        help="把 offline candidate_pending 回灌候选池并按当前闸门尝试入库",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    config = Config()
    if args.min_in_degree is not None:
        config.admission.min_in_degree = args.min_in_degree
    if args.quick_score_threshold is not None:
        config.admission.quick_score_threshold = args.quick_score_threshold

    print_stats(config)
    if args.admit_pending:
        print()
        print("=== Admit pending ===")
        admit_pending(config)
        print()
        print_stats(config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
