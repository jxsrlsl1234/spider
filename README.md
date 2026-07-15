# LLM 语料种子发现与迭代引擎（MVP）

为 LLM 预训练构建可自举的**种子发现与全生命周期治理**原型。

- **种子主键**：可注册域（eTLD+1）
- **采集产物**：原始 HTML（上传对象存储；离线表 `html` 字段存对象 URL）
- **运行形态**：冷启动一次性进程 + 三个常驻 Worker 进程

设计文档：[DESIGN.md](./DESIGN.md)（主设计） · [DETAILED_DESIGN.md](./DETAILED_DESIGN.md)（实现细节） · [DIAGRAMS.md](./DIAGRAMS.md)（架构与流程图）

---

## 运行说明

### 环境

```bash
pip install -r requirements.txt
cd /path/to/spider          # cwd 必须为项目根（含 seeds.txt、config.py）
```

- Python 3.9+
- 真实抓取依赖 `aiohttp`；`--self-test` 使用 MockFetcher，无需网络
- IDE 调试请将工作目录 `cwd` 设为项目根目录

### 启动顺序

```bash
# ① 一次性冷启动：seeds.txt → SeedCollectMq → 评估 → SeedStore
python -m src.bootstrap

# ② 三个常驻进程（各开一个终端，或 IDE Compound「全套常驻」）
python -m src.workers.link_scheduler
# [--self-test] 为 MockFetcher；真实抓取不要加
# 抓取过慢导致 DataQueue pending 堆积时，可调 --concurrency / --timeout 等（见「异步抓取引擎」）
python -m src.workers.task_scheduler [--self-test] [--max-pages N] [--concurrency 32]
# 本地想尽快扩链出新种子时加 --min-in-degree 1（默认 2，见下节）
python -m src.workers.seed_collect [--min-in-degree 1]
```

| 进程 | 命令 | 职责 |
|------|------|------|
| Bootstrap | `python -m src.bootstrap` | 写入初始 SeedStore 快照（跑一次即可，改种子后重跑） |
| LinkScheduler | `python -m src.workers.link_scheduler` | `ACTIVE/PROBATION` 且 `scheduled=false` → DataQueue |
| TaskScheduler | `python -m src.workers.task_scheduler` | 消费 DataQueue：抓取 / 同域回投 / 跨域发 SeedCollectMq |
| SeedCollect | `python -m src.workers.seed_collect` | SeedCollectMq → 评估 → SeedStore（新域 `scheduled=false`）；联调建议 `--min-in-degree 1` |

常用参数：

| 命令 | 参数 | 说明 |
|------|------|------|
| `task_scheduler` | `--self-test` | Mock 页面，本地联通全链路（真实抓取不要加） |
| `task_scheduler` | `--max-pages N` | 抓满 N 页后退出（自测） |
| `task_scheduler` | `--concurrency` / `--timeout` / `--max-retries` 等 | 覆盖抓取引擎参数（见下节） |
| `task_scheduler` | `--default-domain-qps` / `--domain-qps` | DataQueue 按域名出队 QPS（默认 / 覆盖） |
| `seed_collect` | `--min-in-degree K` | 覆盖运行时准入入度门槛（默认 `2`，见下节） |
| `bootstrap` | `--seeds PATH` | 指定种子文件 |

### 异步抓取引擎（并发 / 超时 / 重试 / 会话复用）

TaskScheduler 消费侧用 `AiohttpFetcher`（`src/crawl/fetcher.py`）做 L1 异步抓取：

- **全局并发**：Worker 按 `concurrency` 控制 in-flight
- **单域并发**：`max_concurrency_per_host` 限制同一注册域同时抓取数（默认 2），避免同站任务打满连接池
- **域名级出队 QPS**：DataQueue 按 topic（域名）独立令牌桶；默认 `consume_rate_per_second`，可用 `domain_qps` 覆盖
- **会话复用**：`ClientSession` + `TCPConnector`（Keep-Alive / DNS 缓存）
- **超时**：`total` / `connect` / `sock_read` 分段超时
- **重试**：网络异常与可重试状态码指数退避；404 等不重试

**默认参数**（`config.FetchConfig`）：

| 参数 | 默认 | 含义 |
|------|------|------|
| `concurrency` | `16` | 全局同时抓取任务数 |
| `max_concurrency_per_host` | `2` | 单域同时抓取上限 |
| `timeout_seconds` | `10` | 单次请求总超时（秒） |
| `connect_timeout_seconds` | `10` | 建连超时（秒） |
| `sock_read_timeout_seconds` | `10` | 读超时（秒） |
| `max_retries` | `2` | 失败重试次数（总尝试 = 3） |
| `retry_backoff_seconds` | `0.5` | 退避基数；实际 `0.5 * 2^attempt` 秒 |
| `connector_limit` | `100` | 连接池总连接上限 |
| `connector_limit_per_host` | `4` | 单 host 连接上限（≥ 单域并发） |
| `keepalive_timeout` | `30` | Keep-Alive 空闲秒数 |
| `dns_cache_ttl` | `300` | DNS 缓存 TTL（秒） |

**域名级出队 QPS**（`config.DataQueueConfig`）：

| 参数 | 默认 | 含义 |
|------|------|------|
| `consume_rate_per_second` | `2.0` | 未单独配置的域名，每秒最多出队条数 |
| `domain_qps` | `{}` | 按可注册域覆盖；`<=0` 暂停该域出队 |

两种配置方式：

1. **改代码默认值**：编辑根目录 [`config.py`](./config.py) 中 `FetchConfig` / `DataQueueConfig`
2. **启动时覆盖**：

```bash
python -m src.workers.task_scheduler \
  --concurrency 16 \
  --max-concurrency-per-host 2 \
  --timeout 10 \
  --connect-timeout 10 \
  --connector-limit-per-host 4 \
  --default-domain-qps 2 \
  --domain-qps arxiv.org=0.5,python.org=1
```

**关于 `ConnectionTimeoutError`**：同站（如 arxiv.org）扩链后，若全局并发远大于 `connector_limit_per_host`，多余请求会在连接池排队，**排队时间会计入 `connect` 超时**，表现为 `Connection timeout to host ...`（elapsed≈16.5s 常对应 5s×3 次重试 + 退避）。现已用 `max_concurrency_per_host` 限制单域 in-flight；请保持 `connector_limit_per_host >= max_concurrency_per_host`。

### 尽快看到扩链新种子（建议）

运行时跨域新域默认需 **`min_in_degree >= 2`**（至少被 2 个不同源域引用）才入库；单源发现只会进 CandidatePool / `candidate_pending`，SeedStore 短期不会增长，LinkScheduler 也就没有新站可投。

本地/联调想**尽快扩链、看见新增 seed**时，启动 SeedCollect 时降门槛：

```bash
python -m src.workers.seed_collect --min-in-degree 1
```

含义：任意一个已收录源域首次链到新域且 `quick_score` 过线，即可写入 SeedStore（`PROBATION`，`scheduled=false`），随后 LinkScheduler 即可入队抓取。

建议：

| 场景 | 建议 |
|------|------|
| 本地演示 / 尽快验证扩链闭环 | `--min-in-degree 1`，并保证三 Worker 同时跑 |
| 更接近生产的质量闸门 | 保持默认 `2`（多源互证，减少噪声域） |
| 已有 pending、想一次回灌入库 | `python -m src.tools.seed_discovery_stats --admit-pending --min-in-degree 1` |

高出链 hub 种子可参考 [`seeds_hub.txt`](./seeds_hub.txt)。观测候选池与 pending：`python -m src.tools.seed_discovery_stats`。

### 运行时闭环

```
seeds.txt
    │ bootstrap（一次性）
    ▼
SeedCollectMq ──► SeedCollect ──► SeedStore
                                     │
                          link_scheduler（scheduled=false）
                                     ▼
                                 DataQueue
                                     │
                              task_scheduler
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
              同域 → DataQueue              跨域 → SeedCollectMq
```

两条 MQ 职责分离，勿混用：

| MQ | 内容 | 生产者 | 消费者 | 产出 |
|----|------|--------|--------|------|
| **SeedCollectMq** | 未验证候选种子域 | Bootstrap / ETL / 跨域扩链 | SeedCollect | SeedStore |
| **DataQueue** | 待抓 URL 任务 | LinkScheduler（含同域扩链） | TaskScheduler | HTML + 扩链 |

跨域新域**不会**直接进 DataQueue：须经 SeedCollect 闸门写入 SeedStore，再由 LinkScheduler 投递。

### 运行产物

```
output/
├── seed_store_snapshot.jsonl   # SeedStore 快照
├── candidate_pool.json         # 未过闸门的运行时候选
├── seed_collect_mq/            # SeedCollectMq 文件队列
├── data_queue/                 # DataQueue（按 domain Topic）
├── hbase/crawl_context/        # CrawlContext
├── object_store/html/          # HTML 本地 mirror（权威字段为对象 URL）
├── metadata.jsonl
└── offline/                    # seed_event / resource / seed_collect_consumer …
```

---

## 初始种子生成方案

种子主键始终为 **eTLD+1（可注册域）**。冷启动经统一入口写入 SeedStore：

```
寻源产出 → SeedCollectMq → Evaluator → SeedStore（scheduled=false）
                ↑
         python -m src.bootstrap
```

### MVP：静态文件（当前实现）

| 项 | 说明 |
|----|------|
| 文件 | 默认 [`seeds.txt`](./seeds.txt)；高出链演示可用 [`seeds_hub.txt`](./seeds_hub.txt) |
| 格式 | 每行一条入口 URL；`#` 开头为注释 |
| 命令 | `python -m src.bootstrap`（或 `--seeds PATH`） |
| 评估 | 默认 `--source manual`：高信任启发式 → 多为 `ACTIVE` |
| 备选 | `--source auto_etl`：将文件行当作页面流，走 `etl/domain_merge` 归并后以 `AUTO_ETL` 评分入库 |

```bash
python -m src.bootstrap
python -m src.bootstrap --seeds seeds_hub.txt
python -m src.bootstrap --source auto_etl --seeds seeds.txt
```

MVP 用静态列表保证可复现、零外部依赖；**不代表生产寻源方式**。上线前应替换为下述离线自动化管线，再周期性导出/对接 SeedCollectMq（或直接产出 `seeds.txt` 快照）。

### 理想自动化管线（生产目标）

目标：从多数据源定期产出约 **K≈1000** 量级初始种子（可配置），可复现、可审计。

```text
多源 Ingest → Clean → Merge(eTLD+1) → Dedup → Gate → Score → Diversity Cut(K)
                                                              ↓
                                              SeedCollectMq / seeds 快照 / audit.jsonl
```

| 阶段 | 职责 | 要点 |
|------|------|------|
| **Ingest** | 多源拉取原始 URL/页元数据 | Common Crawl 索引、开放目录、学术站点列表、站点地图批量、人工策展包等；写入带 `source_id` 的对象存储/分区表 |
| **Clean** | 规范化与去噪 | URL 规范化、畸形行丢弃、编码修复；可选受限并发探测（可达性 / 状态码 / robots） |
| **Merge** | 按 eTLD+1 归并 | 同域保留 **首页 + sitemap + 随机内容页**；标题/状态码聚合为 `page_aggregate`（已有骨架：`src/etl/domain_merge.py`） |
| **Dedup** | 域级去重 | 精确域去重；可选镜像/跳转近重复合并 |
| **Gate** | 硬门槛 | 可达、非空壳、语言/TLD 白名单、排除明显垃圾站等 |
| **Score** | 软评分排序 | 来源信任、TLD、可达、入度先验、内容页信号等（与运行时 Evaluator 特征可对齐） |
| **Diversity Cut** | 配额截断 | per-TLD / 主题多样性；截断到目标 K；不足时从候补池回填 |
| **Export** | 对接运行时 | 发布 `SeedCollectMessage(source_type=auto_etl)` → SeedCollectMq，或落盘版本化 `seeds_vN.txt` + `bootstrap_audit.jsonl` |

调度建议：Airflow / Cron **周更**，固定 `etl_version` 与源快照 hash，支持回滚。运行时跨域扩链（`runtime_cross_domain`）只负责**迭代增广**，不替代冷启动规模化寻源。

| MVP 现状 | 理想态 |
|----------|--------|
| 人工维护 `seeds.txt`（约数条～数十条演示） | 多源定时 ETL → 约 1000 域 |
| `AutoEtlPipeline` 仅用静态行演示 Merge→发布 | 完整 Ingest/探测/打分/多样性截断 |
| 一次 `bootstrap` 写快照 | 版本化导出 + 审计表，可灰度替换 SeedStore 冷启动集 |

---

## 模块结构

```
config.py / seeds.txt / main.py
src/
  bootstrap.py / runtime.py     # 冷启动入口与依赖组装
  workers/                      # 常驻进程 CLI
    link_scheduler.py
    task_scheduler.py
    seed_collect.py
  scheduling/                   # 调度核心实现
    link_scheduler.py           # URL → Context → DataQueue
    task_scheduler.py           # 抓取 / 解析 / 存储 / 扩链
  mq/                           # 消息队列
    file_queue.py               # FileMessageQueue
    seed_collect.py             # SeedCollectMq + CandidatePool + Consumer
  stores/                       # 存储
    seed_store.py
    offline_store.py
    object_store.py
    result_store.py             # HTML 上传 + metadata
    crawl_context_store.py
    data_queue/                 # DataQueue（Redis meta + HBase body）
  crawl/                        # 抓取
    fetcher.py / parser.py / robots.py / dedup.py
  seed/                         # 种子准入与治理
    evaluator.py / governance.py
  domain/                       # 领域模型
    models.py / link.py / context.py / urls.py
  etl/                          # 离线域归并寻源
  util/logging_conf.py
  tools/seed_discovery_stats.py
```

| 包 | 职责 |
|----|------|
| `workers/` | 进程入口（信号、循环、参数） |
| `scheduling/` | Link / Task 调度业务实现 |
| `mq/` | 寻源队列与通用文件 MQ |
| `stores/` | SeedStore、对象/离线存储、CrawlContext、DataQueue |
| `crawl/` | HTTP 抓取、解析出链、robots、去重 |
| `seed/` | 准入评分、治理周期 |
| `domain/` | SeedRecord / Link / CrawlContext / URL 工具 |
| `etl/` | 页面流按 eTLD+1 归并 |
| `tools/` | 运维/观测 CLI |

---

## 核心设计要点

- **种子状态**：`PROBATION` → `ACTIVE`；连续抓取失败 → `SUSPENDED`；低质 → `EVICTED`
- **准入来源**：`manual` / `auto_etl` / `runtime_cross_domain`（后者需过 `min_in_degree`、`quick_score` 等闸门）
- **权重**：EWMA，`reward` 由 HTML 质量合成；失败 `reward=0`
- **全链路状态**：写在 `CrawlContext`，不做内存透传
- **配置中心**：根目录 `config.py`（Admission / DataQueue / Fetch / Worker 间隔等）

---

## 刻意简化项（MVP）

| 维度 | 当前实现 |
|------|----------|
| 初始寻源 | 静态 `seeds.txt` + 可选域归并 ETL 骨架 |
| SeedStore | 进程内内存 + JSONL 快照 |
| SeedCollectMq | 本地目录 `FileMessageQueue` |
| DataQueue | 本地文件模拟 Redis 元数据 + HBase 消息体 |
| 对象存储 | `LocalObjectStore` mirror；URL 形如 `s3://…` |
| CrawlContext | 本地 JSON 文件 |
| 抓取 | L1 `aiohttp`：可配并发/超时/重试/连接池会话复用；Playwright/代理为接口预留 |
| 质量评估 | 启发式 HTML / quick_score，无独立模型服务 |
| robots | 规则未拉取时默认放行 |
| 进程编排 | 手工启动三 Worker（无 K8s/服务发现） |

---

## 生产化演进方向

| 维度 | 演进目标 |
|------|----------|
| 寻源 | 多源定时 ETL、采样探测、审计与版本回滚 |
| SeedStore | MySQL（种子 / 来源边 / 事件）+ 服务化读写 |
| SeedCollectMq | Kafka / Redis Streams / RabbitMQ |
| DataQueue | 真实 Redis 集群 + HBase 预分区 Region 路由；多 Consumer |
| 对象存储 | S3 / OSS / HDFS 真实客户端 |
| CrawlContext | HBase `crawl_context` 表 |
| 抓取 | L1.5 指纹伪装 → L2 Playwright → L3 代理池；多机多 Worker 分片 |
| 质量 / 治理 | 独立质量模型；采集农场检测；更严多样性与配额 |
| 部署 | 无状态 Worker、按域分片、独立 SeedCollect 扩缩容 |

接口边界已按上述方向拆分（协议类 + MVP 本地实现），替换存储与 MQ 后端时尽量不改调度业务层。

---

## 相关命令速查

```bash
python -m src.bootstrap
python -m src.workers.link_scheduler
python -m src.workers.task_scheduler --concurrency 32 --timeout 8 --max-retries 1
python -m src.workers.task_scheduler --self-test --max-pages 50
python -m src.workers.seed_collect --min-in-degree 1   # 联调尽快扩链；生产保持默认 2
python -m src.tools.seed_discovery_stats
python main.py          # 打印用法说明
```
