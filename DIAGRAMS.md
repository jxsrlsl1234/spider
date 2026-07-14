# 流程图（DIAGRAMS）

> 架构 / 时序 / 状态图。设计论述见 [`DESIGN.md`](./DESIGN.md)；实现细节见 [`DETAILED_DESIGN.md`](./DETAILED_DESIGN.md)。

---

## 1. 总览

```mermaid
flowchart LR
  subgraph Cold[冷启动]
    S1[seeds.txt] --> S2[bootstrap]
    S2 --> S3[(SeedStore)]
  end
  subgraph Runtime[常驻]
    S3 --> LS[link_scheduler]
    LS --> DQ[(DataQueue)]
    DQ --> TS[task_scheduler]
    TS -->|同域| LS
    TS -->|跨域| MQ[(SeedCollectMq)]
    MQ --> SC[seed_collect]
    SC --> S3
  end
```

---

## 2. 组件与存储

```mermaid
flowchart TB
  LS[LinkScheduler] --> CTX[(crawl_context)]
  LS --> DQ[(data_queue)]
  TS[TaskScheduler] --> DQ
  TS --> CTX
  TS --> OBJ[(object_store)]
  TS --> OFF[(offline jsonl)]
  TS --> SCMQ[(seed_collect_mq)]
  SC[SeedCollect] --> SCMQ
  SC --> SS[(seed_store_snapshot)]
  SC --> CP[(candidate_pool.json)]
  LS --> SS
```

---

## 3. 冷启动

```mermaid
sequenceDiagram
  participant B as bootstrap
  participant MQ as SeedCollectMq
  participant C as SeedCollectConsumer
  participant SS as SeedStore
  B->>MQ: publish_manual / auto_etl
  B->>C: drain / evaluate
  C->>SS: ACTIVE / PROBATION
  B->>SS: save snapshot
```

---

## 4. LinkScheduler 周期

```mermaid
flowchart TD
  A[reload SeedStore] --> B[iter scheduled=false ACTIVE/PROBATION]
  B --> C[submit_raw → Context + DataQueue]
  C --> D[mark_scheduled]
  D --> E[save SeedStore]
```

---

## 5. TaskScheduler 单任务

```mermaid
flowchart TD
  A[consume DataQueue] --> B{robots?}
  B -->|否| Skip[SKIPPED ack]
  B -->|是| F[fetch]
  F -->|失败| R[reachability fail + weight 0 + retry]
  F -->|成功| P[parse]
  P --> ST[store HTML]
  P --> SD{出链}
  SD -->|同域| LQ[LinkScheduler → DataQueue]
  SD -->|跨域| MQ[SeedCollectMq]
  ST --> ACK[ack + weight reward]
```

---

## 6. 跨域准入

```mermaid
sequenceDiagram
  participant TS as TaskScheduler
  participant MQ as SeedCollectMq
  participant SC as SeedCollect
  participant Pool as CandidatePool
  participant SS as SeedStore
  participant LS as LinkScheduler
  TS->>MQ: publish_runtime_links
  SC->>MQ: consume
  alt 已在 SeedStore
    SC->>SS: 更新来源/入度
  else 新域
    SC->>Pool: upsert source_domains
    alt 过闸门
      SC->>SS: PROBATION scheduled=false
      LS->>SS: 下轮投递 DataQueue
    else 未过
      Note over Pool: 等待更多源域
    end
  end
```

---

## 7. 种子状态

```mermaid
stateDiagram-v2
  [*] --> ACTIVE: manual / 高分 ETL
  [*] --> PROBATION: runtime / 低分 ETL
  PROBATION --> ACTIVE: governance promote
  PROBATION --> EVICTED: 低质
  ACTIVE --> SUSPENDED: consecutive_fail 达阈值
  SUSPENDED --> ACTIVE: 人工/治理恢复（扩展）
```

---

## 8. DataQueue 生产消费

```mermaid
flowchart LR
  Pub[publish] --> Lock[domain lock]
  Lock --> RK[分配 RowKey]
  RK --> HB[(HBase body)]
  Cons[consume_any] --> Buf[Buffer + rate]
  Buf --> HB
  Buf -->|ok| Ack[ack offset]
  Buf -->|fail| Ret[retry 子队列]
```

---

## 9. 权重与治理

```mermaid
flowchart TD
  F[fetch 结果] -->|ok| W1[reward = f质量]
  F -->|fail| W0[reward = 0]
  W1 --> EWMA[EWMA 更新 weight]
  W0 --> EWMA
  W0 --> CF[consecutive_fail++]
  CF -->|≥ max| SUS[SUSPENDED]
  G[Governance 周期] --> P[promote / evict / 配额]
```
