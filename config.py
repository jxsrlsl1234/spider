"""集中配置。

路径默认相对**当前工作目录（cwd）**。在 IDE 中请将 Run/Debug 的
cwd 设为项目根（见 `.vscode/launch.json`），否则相对路径会解析失败。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class EvaluatorWeights:
    src_domain_weight: float = 0.35
    anchor_quality: float = 0.15
    url_health: float = 0.15
    tld_bonus: float = 0.15
    in_degree_norm: float = 0.20
    spam_penalty: float = 0.30


@dataclass
class AdmissionConfig:
    quick_score_threshold: float = 0.45
    # 生产建议 ≥2（多源互证）；本地演示扩链入库可临时设为 1
    min_in_degree: int = 2
    promotions_per_cycle: int = 50
    candidate_pool_capacity: int = 50_000
    candidate_ttl_seconds: float = 7 * 24 * 3600
    candidate_pool_path: Path = Path("output/candidate_pool.json")


@dataclass
class DiversityQuota:
    active_capacity: int = 20_000
    max_active_per_tld: int = 5_000
    max_active_per_asn: int = 3_000


@dataclass
class FetchConfig:
    """异步抓取引擎（L1 aiohttp）参数。

    Worker 侧按 ``concurrency`` 控制全局 in-flight，并按 ``max_concurrency_per_host``
    限制单域并发，避免同站任务挤满连接池后触发 ConnectionTimeout。
    """

    # 并发
    concurrency: int = 16
    # 单注册域同时 in-flight 上限（防同站打满 connector_limit_per_host）
    max_concurrency_per_host: int = 2
    # 超时（秒）
    timeout_seconds: float = 10.0
    connect_timeout_seconds: float = 10.0
    sock_read_timeout_seconds: float = 10.0
    # 重试：网络异常 / 挑战类状态码；共 max_retries+1 次尝试
    max_retries: int = 2
    retry_backoff_seconds: float = 0.5
    # aiohttp 连接池 / 会话复用（limit_per_host 应 >= max_concurrency_per_host）
    connector_limit: int = 100
    connector_limit_per_host: int = 4
    keepalive_timeout: float = 30.0
    dns_cache_ttl: int = 300
    # HTTP 头与内容上限
    user_agent: str = "SeedSpiderMVP/0.1 (+https://example.com/bot)"
    accept_language: str = "zh-CN,zh;q=0.9,en;q=0.8"
    max_content_bytes: int = 5_000_000
    enable_browser_fallback: bool = False


@dataclass
class ScheduleConfig:
    per_domain_min_interval: float = 2.0
    max_depth: int = 3


@dataclass
class WeightUpdateConfig:
    ewma_alpha: float = 0.8
    max_consecutive_fail: int = 5
    initial_weight: float = 0.5


@dataclass
class ObjectStorageConfig:
    """对象存储配置。离线表 html 字段存 put() 返回的对象 URL。"""

    scheme: str = "s3"
    bucket: str = "crawl-html"
    prefix: str = "html"
    endpoint: str = ""
    mirror_dir: Path = Path("output/object_store")


@dataclass
class DataQueueConfig:
    """自研 DataQueue（Redis + HBase）配置。"""

    topic_prefix: str = "crawl.link.task"
    base_dir: Path = Path("output/data_queue")
    partition_count: int = 16
    partition_width: int = 2
    offset_width: int = 10
    buffer_capacity: int = 1000
    consume_rate_per_second: float = 200.0


@dataclass
class SeedCollectMqConfig:
    """寻源收集 MQ（SeedCollectMq）：存放未经质量验证的候选种子。"""

    topic: str = "seed.collect"
    batch_consume_size: int = 50
    base_dir: Path = Path("output/seed_collect_mq")


@dataclass
class WorkerConfig:
    """独立常驻进程配置。"""

    link_scheduler_interval_seconds: float = 5.0
    task_scheduler_idle_sleep_seconds: float = 0.5
    seed_collect_interval_seconds: float = 2.0
    # 0 = 常驻不退出；>0 时达到页数后退出（自测用）
    task_scheduler_max_pages: int = 0


@dataclass
class Config:
    max_pages: int = 200
    idle_exit_seconds: float = 5.0
    respect_robots: bool = True

    seeds_file: Path = Path("seeds.txt")
    output_dir: Path = Path("output")
    offline_dir: Path = Path("output/offline")
    hbase_context_dir: Path = Path("output/hbase/crawl_context")
    seed_store_path: Path = Path("output/seed_store_snapshot.jsonl")

    object_storage: ObjectStorageConfig = field(default_factory=ObjectStorageConfig)
    data_queue: DataQueueConfig = field(default_factory=DataQueueConfig)
    seed_collect_mq: SeedCollectMqConfig = field(default_factory=SeedCollectMqConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)

    weights: EvaluatorWeights = field(default_factory=EvaluatorWeights)
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    diversity: DiversityQuota = field(default_factory=DiversityQuota)
    fetch: FetchConfig = field(default_factory=FetchConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    weight_update: WeightUpdateConfig = field(default_factory=WeightUpdateConfig)

    def resolve_path(self, path: Path) -> Path:
        """相对路径相对 cwd；绝对路径原样返回。"""
        p = Path(path)
        return p if p.is_absolute() else (Path.cwd() / p)
