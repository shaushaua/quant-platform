# qlib_factor_platform K8s 改造完整方案

> 基于代码库深度分析，针对实际代码结构输出的改造文档

---

## 目录

1. [选型决策：ACK vs ACS](#1-选型决策)
2. [MemoryStore 跨 Pod 方案：为什么不用 Redis](#2-memorystore-跨-pod-方案)
3. [调度器 Go 改造方案](#3-调度器-go-改造方案)
4. [整体架构设计](#4-整体架构设计)
5. [节点池规划](#5-节点池规划)
6. [各服务 K8s 资源配置](#6-各服务-k8s-资源配置)
7. [Go Operator 实现](#7-go-operator-实现)
8. [KEDA 弹性伸缩配置](#8-keda-弹性伸缩配置)
9. [MemoryStore Arrow IPC 改造代码](#9-memorystore-arrow-ipc-改造代码)
10. [Spot 中断优雅退出](#10-spot-中断优雅退出)
11. [镜像构建改造](#11-镜像构建改造)
12. [实施步骤](#12-实施步骤)
13. [成本分析](#13-成本分析)
14. [关键风险与规避](#14-关键风险与规避)

---

## 1. 选型决策

**结论：ACK 标准托管版**

| 维度 | ACK 托管版 | ACS Serverless |
|------|-----------|----------------|
| 抢占式 ECS 节点 | **原生支持**，节点池直接配置 spot | 不管理 ECS，无法使用 spot |
| 回测弹性伸缩 | KEDA + Cluster Autoscaler，精细控制 | 冷启动慢，按 pod vCPU 计费 |
| 行情服务稳定性 | 固定节点，延迟稳定，无调度抖动 | Serverless 调度存在抖动风险 |
| 32GB MemoryStore | 节点本地内存，共享内存卷 | 无法跨容器共享内存 |
| 成本 | Spot 节省 70-90% | 短任务贵，长连接行情服务更贵 |
| 现有代码迁移量 | 最小，docker-compose → K8s YAML | 需要更多改造 |

ACS 唯一优势是零节点运维，但你的场景（长连接行情 + 32GB 实时内存 + 批量计算）决定了 ACK 是正确选择。

---

## 2. MemoryStore 跨 Pod 方案

### 2.1 为什么 Redis 不适合

通过分析 `qlib_factor_platform/data/memory_store.py`，你的数据规模如下：

```
tick：全市场 5000 只 × ~2000 条/只 = 1000万行  ≈ 8GB
order：规模类似                               ≈ 10GB
deal：                                        ≈ 6GB
kline（1min）：5000 × 240 = 120万行           ≈ 1GB
daily_basic：5000 行                          ≈ 可忽略
quotes（最新快照）：5000 个 dict              ≈ <100MB
总计                                          ≈ 25-32GB
```

Redis 的问题：
- DataFrame 需要序列化（pickle/msgpack），32GB 序列化耗时严重
- `update_tick(code, df)` 是追加写入，高频 L2 数据（每秒万条）下会造成严重排队
- `get_tick()` 无 code 参数时全量 concat，Redis 需先传输 8GB 才能拼接
- Redis 单线程，tick/order/deal 并发写入相互阻塞

### 2.2 为什么 Fluid 不适合

Fluid 是 OSS/HDFS **批量文件读取**加速框架，不支持毫秒级 DataFrame 原地追加写入语义，与实时采集场景不匹配。

### 2.3 正确方案：同进程 MemoryStore + Apache Arrow Plasma（按需淘汰）

**核心前提（已确认）**：研究院策略通过 `import DataAPI` 同进程加载，`collector` 和 `live_engine` + 策略运行在**同一个容器进程**中。

这意味着**不需要跨容器共享内存**，MemoryStore 单例模式完全保留，只需解决内存总量问题。

**数据访问热度分析**：

```
热数据（策略每分钟必读，全市场）：
  kline_1min    5000只 × 当日累积    ~240MB   常驻内存
  daily_basic   5000只              ~3MB     常驻内存
  quotes        最新快照             ~2MB     常驻内存
  小计                              ~250MB

冷数据（按股票代码按需查询，研究院策略不会同时查全市场）：
  tick[code]    单只全天             ~1.6MB
  order[code]   单只全天             ~2MB
  deal[code]    单只全天             ~1.2MB
  全市场峰值                         ~24GB    但实际同时活跃 <100只
  实际常驻                           ~200MB
```

**结论：实际常驻内存 < 500MB，峰值（全市场全量）< 8GB。**

研究院策略查 tick 时按 `get_tick(code)` 逐只查询，不存在一次性加载全市场 5000 只 tick 的场景。将 tick/order/deal 改为 **LRU 缓存**（最多缓存 N 只，超出淘汰），冷数据从 OSS 按需加载。

```
+----------------------------------------------------------+
|  live-engine Pod（单 Container，同进程）                   |
|                                                          |
|  collector 线程 ──写──> MemoryStore (LRU)                |
|  live_engine 线程       │                               |
|  研究院策略（import）    │                               |
|    ↓ DataAPI.get_tick() │                               |
|    ↓ DataAPI.get_kline()│                               |
|                         ↓                               |
|  +----------------------------------------------------+  |
|  |  内存布局（进程内，无 shm）                          |  |
|  |  kline_1min    ~240MB  常驻                        |  |
|  |  daily_basic   ~3MB    常驻                        |  |
|  |  quotes        ~2MB    常驻（同步写 Redis Pub/Sub） |  |
|  |  tick LRU      最多缓存200只  ~320MB               |  |
|  |  order LRU     最多缓存200只  ~400MB               |  |
|  |  deal LRU      最多缓存200只  ~240MB               |  |
|  |  合计                  ~1.2GB                      |  |
|  +----------------------------------------------------+  |
|                                                          |
|  冷 tick/order/deal（LRU 淘汰后）-> OSS 归档存储         |
+----------------------------------------------------------+
```

**访问延迟对比**：

| 方案 | 访问延迟 | 备注 |
|------|---------|------|
| 进程内单例 MemoryStore（原方案保留） | ~1μs | 无需改动 |
| LRU miss 从 OSS 加载单只 tick | ~50-200ms | 仅首次，之后命中缓存 |
| Redis 序列化大 DataFrame | ~50-500ms | 不适用 |

**Redis 的正确用途**（仅 quotes 实时推送）：

```
update_quote(code, dict)  ->  HSET market:quotes {code} {json}    写
get_quote(code)           ->  HGET market:quotes {code}           读
行情变更通知              ->  PUBLISH market:tick {code}          推送给监控/告警
```

### 2.4 节点内存需求（修订）

| 组件 | 内存需求 |
|------|--------|
| MemoryStore 热数据常驻 | ~250MB |
| tick/order/deal LRU（200只） | ~1GB |
| collector/live_engine 进程开销 | ~2GB |
| 研究院策略运行时（因子计算） | ~4GB |
| OS + K8s 开销 | ~2GB |
| **合计** | **~10GB** |

推荐机型：`ecs.c8i.4xlarge`（**16核32GB**，按量付费 ~¥1.2/小时，月成本 ~¥864）。
原来估算需要 128GB 节点（月成本 ~¥3240），**节省约 73%**。

行情服务不能用 Spot（断线即断行情），必须按量付费。

---

## 3. 调度器 Go 改造方案

### 3.1 为什么 Go 更合适

当前 `scheduler/controller.py` 的核心问题：

```python
# controller.py — 任务状态只存进程内存，API Pod 重启全丢
self._tasks: Dict[str, TaskInfo] = {}

# controller.py — 直接调用阿里云 ECS API 创建实例
instances = self.aliyun_manager.create_spot_instances(...)

# controller.py — HTTP callback 触发，无主动监控
all_completed = all(s in ("completed", "failed") for s in ...)
```

改为 Go Operator 的收益：

| 问题 | Go Operator 方案 |
|------|------------------|
| 任务状态进程内存，重启丢失 | 状态写入 K8s CRD Status（etcd），永久持久化 |
| 手动调用 ECS API 创建/销毁实例 | 直接创建 K8s Job，平台调度到 Spot 节点池 |
| HTTP callback 被动触发聚合 | Watch K8s Job 状态变更，事件驱动 |
| GIL 限制并发，threading 管理复杂 | goroutine 真并发，Reconcile 模式简洁 |
| aliyun_manager.py 手动维护 ECS 生命周期 | K8s 自动管理 Pod/Job 生命周期，含 Spot 中断重试 |

### 3.2 Python vs Go 边界划分

```
Python 保留：
  - backtest/engine.py        回测计算逻辑（不动）
  - realtime/collector.py     通联数据采集（不动）
  - realtime/live_engine.py   实盘策略执行（不动）
  - data/                     数据层（改造 memory_store）
  - strategy/                 策略层（不动）

Go 接管：
  - scheduler/controller.py   -> Go Operator Reconcile
  - scheduler/aliyun_manager.py -> 废弃，改为 kubectl
---

## 4. 整体架构设计

```
+---------------------------------------------------------------------+
|                        ACK 集群（华东2）                              |
|                                                                     |
|  +---------------------------+  +--------------------------------+  |
|  |  system-pool（按量付费）    |  |  backtest-spot-pool（Spot）    |  |
|  |  ecs.r8i.4xlarge x2       |  |  ecs.c8i.8xlarge 主选          |  |
|  |                           |  |  + 5种 fallback 机型            |  |
|  |  +---------------------+  |  |  0 ~ 50 pod 弹性伸缩            |  |
|  |  | live-engine Pod      |  |  |                               |  |
|  |  |  - collector         |  |  |  +----------+  +----------+   |  |
|  |  |  - live_engine       |  |  |  | backtest |  | backtest |   |  |
|  |  |  共享 /dev/shm 35GB  |  |  |  | worker   |  | worker   |   |  |
|  |  +---------------------+  |  |  +----------+  +----------+   |  |
|  |  +---------------------+  |  +--------------------------------+  |
|  |  | api-deployment       |  |                                     |
|  |  |  FastAPI 2副本        |  |  +--------------------------------+  |
|  |  +---------------------+  |  |  infra-pool（按量付费）           |  |
|  |  +---------------------+  |  |  ecs.c8i.2xlarge x1              |  |
|  |  | backtest-operator    |  |  |                               |  |
|  |  |  Go Operator 1副本   |  |  |  +----------+                |  |
|  |  +---------------------+  |  |  | aggregator|                |  |
|  +---------------------------+  |  | job（临时） |                |  |
|                                 |  +----------+                |  |
|                                 +--------------------------------+  |
|                                                                     |
|  外部托管服务（同 VPC）：                                              |
|  - 阿里云 Redis 企业版（quotes 快照 + Pub/Sub）                       |
|  - 阿里云 RDS MySQL（涨跌停价格，mysql_loader.py）                    |
|  - 阿里云 OSS（历史数据 + 回测结果）                                  |
+---------------------------------------------------------------------+

数据流：
通联 L2 行情 -> collector -> /dev/shm Arrow IPC -> live_engine -> 信号
                          -> Redis HSET         -> API 查询

OSS 历史数据 -> oss_loader.py -> backtest worker -> 结果 -> OSS
```

---

## 5. 节点池规划

### system-pool（行情 + 实盘 + API）

```yaml
机型：ecs.r8i.4xlarge（16C 128GB）
付费：按量付费（不可中断）
数量：2（1主1备，PodAntiAffinity 分散）
Taint：无（系统服务正常调度）
磁盘：100GB ESSD PL1
理由：live-engine Pod 需要 35GB shm + 进程内存，128GB 足够两副本
```

### backtest-spot-pool（回测计算）

```yaml
机型优先级（ECS 抢占式多机型配置）：
  1. ecs.c8i.8xlarge    （32C 64GB）  # 主选，性价比最高
  2. ecs.c8i.4xlarge    （16C 32GB）
  3. ecs.c7.8xlarge     （32C 64GB）
  4. ecs.c7a.8xlarge    （32C 64GB）  # AMD，更便宜
  5. ecs.c6.8xlarge     （32C 64GB）
  6. ecs.c8a.8xlarge    （32C 64GB）
付费：抢占式（Spot），设置出价为按量价格的 70%
数量：0 ~ 50（Cluster Autoscaler 弹性管理）
Taint：dedicated=backtest:NoSchedule
标签：node-role=backtest
磁盘：50GB ESSD（临时计算，不需要大盘）
理由：回测是幂等任务，Spot 中断后 K8s Job 自动重试，成本节省 70-90%
```

### infra-pool（聚合任务）

```yaml
机型：ecs.c8i.2xlarge（8C 16GB）
付费：按量付费
数量：1（常驻，Aggregator Job 跑完自动释放 Pod）
Taint：dedicated=infra:NoSchedule
```

---

## 6. 各服务 K8s 资源配置

### 6.1 live-engine StatefulSet（含 collector sidecar）

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: live-engine
  namespace: qlib
spec:
  replicas: 1
  selector:
    matchLabels:
      app: live-engine
  template:
    metadata:
      labels:
        app: live-engine
    spec:
      nodeSelector:
        node-role: system
      # 行情服务必须固定节点，不允许漂移
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: node-role
                operator: In
                values: [system]
      volumes:
      - name: shm-store
        emptyDir:
          medium: Memory
          sizeLimit: 35Gi
      containers:
      # --- Container 1: 通联数据采集器 ---
      - name: collector
        image: registry-vpc.cn-shanghai.aliyuncs.com/yourrepo/qlib-collector:latest
        command: ["python", "-m", "qlib_factor_platform.realtime.collector"]
        env:
        - name: SHM_STORE_PATH
          value: /dev/shm/store
        - name: USE_SHM_STORE
          value: "true"
        envFrom:
        - secretRef:
            name: qlib-secrets
        volumeMounts:
        - name: shm-store
          mountPath: /dev/shm/store
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
      # --- Container 2: 实盘策略引擎 ---
      - name: live-engine
        image: registry-vpc.cn-shanghai.aliyuncs.com/yourrepo/qlib-live:latest
        command: ["python", "-m", "qlib_factor_platform.realtime.live_engine"]
        env:
        - name: SHM_STORE_PATH
          value: /dev/shm/store
        - name: USE_SHM_STORE
          value: "true"
        envFrom:
        - secretRef:
            name: qlib-secrets
        volumeMounts:
        - name: shm-store
          mountPath: /dev/shm/store
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
          limits:
            memory: "16Gi"
            cpu: "8"
        livenessProbe:
          httpGet:
            path: /health
            port: 8001
          initialDelaySeconds: 30
          periodSeconds: 10
```

### 6.2 API Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: qlib
spec:
  replicas: 2
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      nodeSelector:
        node-role: system
      containers:
      - name: api
        image: registry-vpc.cn-shanghai.aliyuncs.com/yourrepo/qlib-api:latest
        command: ["python", "-m", "uvicorn", "qlib_factor_platform.api.main:app",
                  "--host", "0.0.0.0", "--port", "8000"]
        envFrom:
        - secretRef:
            name: qlib-secrets
        ports:
        - containerPort: 8000
        resources:
          requests:
            memory: "1Gi"
            cpu: "0.5"
          limits:
            memory: "2Gi"
            cpu: "2"
---
apiVersion: v1
kind: Service
metadata:
  name: api-svc
  namespace: qlib
spec:
  selector:
    app: api
  ports:
  - port: 80
    targetPort: 8000
  type: LoadBalancer  # 或 ClusterIP + Ingress
```

### 6.3 Backtest Worker Job 模板（由 Go Operator 动态创建）

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: backtest-{taskId}-{shardIndex}
  namespace: qlib
  labels:
    task-id: "{taskId}"
    shard-index: "{shardIndex}"
spec:
  backoffLimit: 3          # Spot 中断最多重试3次
  activeDeadlineSeconds: 7200
  template:
    spec:
      restartPolicy: OnFailure
      tolerations:
      - key: dedicated
        operator: Equal
        value: backtest
        effect: NoSchedule
      nodeSelector:
        node-role: backtest
      containers:
      - name: backtest
        image: registry-vpc.cn-shanghai.aliyuncs.com/yourrepo/qlib-backtest:{imageTag}
        command:
        - python
        - -m
        - qlib_factor_platform.backtest.engine
        env:
        - name: TASK_ID
          value: "{taskId}"
        - name: SHARD_INDEX
          value: "{shardIndex}"
        - name: START_DATE
          value: "{startDate}"
        - name: END_DATE
          value: "{endDate}"
        - name: STRATEGY_NAME
          value: "{strategyName}"
        - name: RESULT_OSS_PREFIX
          value: "backtest-results/{taskId}/shard-{shardIndex}/"
        envFrom:
        - secretRef:
            name: qlib-secrets
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
          limits:
            memory: "16Gi"
            cpu: "8"
```

---

## 7. Go Operator 实现

### 7.1 项目初始化

```bash
# 在项目根目录旁边新建 Go 项目
mkdir backtest-operator && cd backtest-operator
go mod init github.com/yourorg/backtest-operator

# 使用 kubebuilder 初始化
kubebuilder init --domain yourorg.io --repo github.com/yourorg/backtest-operator
kubebuilder create api --group batch --version v1 --kind BacktestTask
```

### 7.2 CRD 类型定义

```go
// api/v1/backtest_types.go

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type BacktestTaskSpec struct {
    ImageTag       string `json:"imageTag"`
    StartDate      string `json:"startDate"`
    EndDate        string `json:"endDate"`
    StrategyName   string `json:"strategyName"`
    InitialCapital string `json:"initialCapital,omitempty"`
    // 0 = 自动按 trading days 计算（对应原 cost_calculator.py 逻辑）
    Instances      int    `json:"instances,omitempty"`
}

type SubTaskStatus struct {
    Index     int    `json:"index"`
    JobName   string `json:"jobName"`
    Phase     string `json:"phase"` // pending/running/completed/failed
    StartTime string `json:"startTime,omitempty"`
    EndTime   string `json:"endTime,omitempty"`
}

type BacktestTaskStatus struct {
    // Pending / Running / Aggregating / Completed / Failed
    Phase      string          `json:"phase"`
    SubTasks   []SubTaskStatus `json:"subTasks,omitempty"`
    ResultURL  string          `json:"resultUrl,omitempty"`
    Message    string          `json:"message,omitempty"`
    StartTime  string          `json:"startTime,omitempty"`
    FinishTime string          `json:"finishTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=`.spec.strategyName`
type BacktestTask struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   BacktestTaskSpec   `json:"spec,omitempty"`
    Status BacktestTaskStatus `json:"status,omitempty"`
}
```

### 7.3 Reconcile 核心逻辑

```go
// controllers/backtest_controller.go

func (r *BacktestTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    task := &v1.BacktestTask{}
    if err := r.Get(ctx, req.NamespacedName, task); err```

// 续接上面的 Reconcile 函数
```go
    if err := r.Get(ctx, req.NamespacedName, task); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    switch task.Status.Phase {
    case "", "Pending":
        return r.handlePending(ctx, task)
    case "Running":
        return r.handleRunning(ctx, task)
    case "Aggregating":
        return r.handleAggregating(ctx, task)
    case "Completed", "Failed":
        return ctrl.Result{}, nil
    }
    return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *BacktestTaskReconciler) handlePending(ctx context.Context, task *v1.BacktestTask) (ctrl.Result, error) {
    tradingDays := getTradingDays(task.Spec.StartDate, task.Spec.EndDate)
    instances := task.Spec.Instances
    if instances == 0 {
        instances = calculateInstances(len(tradingDays)) // 对应原 cost_calculator.py
    }

    // 将交易日按 instances 分片
    shards := splitTradingDays(tradingDays, instances)
    subTasks := make([]v1.SubTaskStatus, 0, len(shards))

    for i, shard := range shards {
        jobName := fmt.Sprintf("backtest-%s-%d", task.Name, i)
        job := buildBacktestJob(task, jobName, i, shard.Start, shard.End)
        if err := r.Create(ctx, job); err != nil && !errors.IsAlreadyExists(err) {
            return ctrl.Result{}, err
        }
        subTasks = append(subTasks, v1.SubTaskStatus{
            Index:   i,
            JobName: jobName,
            Phase:   "pending",
        })
    }

    task.Status.Phase = "Running"
    task.Status.SubTasks = subTasks
    task.Status.StartTime = time.Now().Format(time.RFC3339)
    return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, task)
}

func (r *BacktestTaskReconciler) handleRunning(ctx context.Context, task *v1.BacktestTask) (ctrl.Result, error) {
    allDone := true
    for i, sub := range task.Status.SubTasks {
        job := &batchv1.Job{}
        if err := r.Get(ctx, types.NamespacedName{Name: sub.JobName, Namespace: task.Namespace}, job); err != nil {
            return ctrl.Result{}, err
        }
        if job.Status.Succeeded > 0 {
            task.Status.SubTasks[i].Phase = "completed"
        } else if job.Status.Failed >= *job.Spec.BackoffLimit+1 {
            task.Status.SubTasks[i].Phase = "failed"
            task.Status.Phase = "Failed"
            task.Status.Message = fmt.Sprintf("shard %d failed permanently", i)
            return ctrl.Result{}, r.Status().Update(ctx, task)
        } else {
            task.Status.SubTasks[i].Phase = "running"
            allDone = false
        }
    }

    if allDone {
        // 所有分片完成，创建聚合 Job
        task.Status.Phase = "Aggregating"
        aggJob := buildAggregateJob(task)
        if err := r.Create(ctx, aggJob); err != nil && !errors.IsAlreadyExists(err) {
            return ctrl.Result{}, err
        }
    }
    return ctrl.Result{RequeueAfter: 30 * time.Second}, r.Status().Update(ctx, task)
}
```

---

## 8. KEDA 弹性伸缩配置

KEDA 用于根据回测任务队列深度弹性伸缩 backtest worker。

```yaml
# keda-scaledjob.yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata:
  name: backtest-scaledjob
  namespace: qlib
spec:
  jobTargetRef:
    template:
      spec:
        tolerations:
        - key: dedicated
          operator: Equal
          value: backtest
          effect: NoSchedule
        nodeSelector:
          node-role: backtest
        containers:
        - name: backtest
          image: registry-vpc.cn-shanghai.aliyuncs.com/yourrepo/qlib-backtest:latest
          envFrom:
          - secretRef:
              name: qlib-secrets
          resources:
            requests:
              memory: "8Gi"
              cpu: "4"
  pollingInterval: 10
  maxReplicaCount: 50
  scalingStrategy:
    strategy: "accurate"
  triggers:
  - type: redis
    metadata:
      address: "${REDIS_HOST}:${REDIS_PORT}"
      listName: backtest-task-queue
      listLength: "1"    # 队列每有1个任务就启动1个 Pod
      passwordFromEnv: REDIS_PASSWORD
```

**注意**：Go Operator 方案下，Operator 直接创建 K8s Job（无需 KEDA ScaledJob），Cluster Autoscaler 负责节点弹性。KEDA ScaledJob 适合简单的队列消费场景，两种方案二选一。

**推荐**：使用 Go Operator 直接管理 Job，更精细地控制分片逻辑和聚合触发。

---

## 9. MemoryStore Arrow IPC 改造代码

### 9.1 新增 shm_store.py

```python
# qlib_factor_platform/data/shm_store.py
"""
基于 Apache Arrow IPC + /dev/shm 的共享内存存储
替代进程内单例 MemoryStore，支持同 Pod 多 Container 共享
"""
import os
import threading
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))


class ShmStore:
    """Arrow IPC 共享内存存储，写端（collector）和读端（live_engine）均使用此类"""

    def __init__(self):
        TICK_DIR = SHM_BASE / "tick"
        ORDER_DIR = SHM_BASE / "order"
        DEAL_DIR = SHM_BASE / "deal"
        KLINE_DIR = SHM_BASE / "kline"
        for d in [TICK_DIR, ORDER_DIR, DEAL_DIR, KLINE_DIR]:
            d.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _write_arrow(self, path: Path, df: pd.DataFrame):
        """将 DataFrame 写为 Arrow IPC 文件（原子替换）"""
        table = pa.Table.from_pandas(df, preserve_index=True)
        tmp = path.with_suffix(".tmp")
        with ipc.new_file(str(tmp), table.schema) as writer:
            writer.write_table(table)
        tmp.replace(path)  # 原子替换，读端不会读到半写状态

    def _read_arrow(self, path: Path) -> Optional[pd.DataFrame]:
        """mmap 零拷贝读取 Arrow IPC 文件"""
        if not path.exists():
            return None
        with ipc.open_file(ipc.MemoryMappedFile(str(path), mode="r")) as reader:
            return reader.read_pandas()

    # --- 写接口（collector 调用）---

    def update_tick(self, code: str, df: pd.DataFrame):
        path = SHM_BASE / "tick" / f"{code}.arrow"
        with self._lock:
            existing = self._read_arrow(path)
            merged = pd.concat([existing, df]) if existing is not None else df
            self._write_arrow(path, merged)

    def update_kline(self, period: str, df: pd.DataFrame):
        path = SHM_BASE / "kline" / f"{period}.arrow"
        with self._lock:
            self._write_arrow(path, df)

    def update_daily_basic(self, df: pd.DataFrame):
        self._write_arrow(SHM_BASE / "daily_basic.arrow", df)

    # --- 读接口（live_engine 调用）---

    def get_tick(self, code: Optional[str] = None) -> Optional[pd.DataFrame]:
        if code:
            return self._read_arrow(SHM_BASE / "tick" / f"{code}.arrow")
        # 全量读取：concat 所有 tick 文件
        frames = []
        for f in (SHM_BASE / "tick").glob("*.arrow"):
            df = self._read_arrow(f)
            if df is not None:
                frames.append(df)
        return pd.concat(frames) if frames else pd.DataFrame()

    def get_kline(self, period: str = "1min") -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "kline" / f"{period}.arrow")
        return df if df is not None else pd.DataFrame()

    def get_daily_basic(self) -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "daily_basic.arrow")
        return df if df is not None else pd.DataFrame()
```

### 9.2 修改 memory_store.py 兼容入口

在 `memory_store.py` 的 `get_instance()` 方法中加入环境变量判断：

```python
# 在 MemoryStore 类的 get_instance() 方法修改为：
import os

@classmethod
def get_instance(cls):
    if os.environ.get("USE_SHM_STORE") == "true":
        from .shm_store import ShmStore
        return ShmStore()
    return cls()
```

这样 `DataAPI`（`data/api.py`）和所有策略代码**无需任何修改**，只通过环境变量切换存储后端。

---

## 10. Spot 中断优雅退出

Spot 实例被回收前 30 秒，阿里云会向实例注入 SIGTERM（K8s 侧表现为 Pod eviction）。需要确保 backtest worker 能在 30 秒内保存进度。

### 10.1 backtest/engine.py 改造点

```python
import signal
import os

class BacktestEngine:
    def __init__(self):
        self._interrupted = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """Spot 中断信号处理：保存断点到 OSS，让 K8s Job 重试时续跑"""
        self._interrupted = True
        checkpoint = {
            "task_id": os.environ["TASK_ID"],
            "shard_index": os.environ["SHARD_INDEX"],
            "last_processed_date": self._current_date,
        }
        self._save_checkpoint_to_oss(checkpoint)
        # 让 K8s 判定为失败并触发 backoffLimit 重试
        raise SystemExit(1)

    def run(self):
        # 启动时检查是否有断点
        checkpoint = self._load_checkpoint_from_oss()
        start = checkpoint["last_processed_date"] if checkpoint else self.start_date

        for date in self.trading_days_from(start):
            if self._interrupted:
                break
            self._current_date = date
            self._run_single_day(date)
```

### 10.2 K8s Job 配置要点

```yaml
spec:
  backoffLimit: 3                   # 最多重试3次
  activeDeadlineSeconds: 7200       # 超时2小时强制终止
  template:
    spec:
      terminationGracePeriodSeconds: 30  # 给 SIGTERM 30秒处理时间
      restartPolicy: OnFailure
```

---

## 11. 镜像构建改造

### 11.1 新增 backtest.Dockerfile

```dockerfile
# docker/backtest.Dockerfile
FROM python:3.11-slim

WORKDIR /app

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 只复制回测相关代码（减小镜像体积）
COPY qlib_factor_platform/backtest/ ./qlib_factor_platform/backtest/
COPY qlib_factor_platform/data/ ./qlib_factor_platform/data/
COPY qlib_factor_platform/strategy/ ./qlib_factor_platform/strategy/
COPY qlib_factor_platform/core/ ./qlib_factor_platform/core/
COPY qlib_factor_platform/__init__.py ./qlib_factor_platform/

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "qlib_factor_platform.backtest.engine"]
```

### 11.2 requirements.txt 新增依赖

```
# 在现有 requirements.txt 末尾追加
pyarrow>=14.0.0
```

### 11.3 新增 k8s/ 目录结构

```
k8s/
├── namespace.yaml
├── secrets.yaml             # kubectl create secret 生成，不入 git
├── rbac.yaml                # backtest-operator ServiceAccount + ClusterRole
├── live-engine-statefulset.yaml
├── api-deployment.yaml
├── ingress.yaml
└── keda-scaledjob.yaml      # 可选

docker/
├── collector.Dockerfile     # 已有，无需大改
├── api.Dockerfile           # 已有，无需大改
├── backtest.Dockerfile      # 新增
└── ui.Dockerfile            # 已有，不变

backtest-operator/           # 新增 Go 项目
├── main.go
├── go.mod
├── api/v1/
├── controllers/
├── config/
└── Dockerfile
```

---

## 12. 实施步骤

### 阶段一：基础设施准备

1. 阿里云控制台创建 ACK 标准托管版集群（华东2，K8s 1.29+，VPC 与 Redis/RDS/OSS 相同）
2. 创建三个节点池：
   - system-pool：ecs.r8i.4xlarge x2，按量付费，Taint: dedicated=system:NoSchedule
   - backtest-spot-pool：ecs.c8i.8xlarge（主选）+ 5种 fallback 机型，抢占式，0~50台弹性
   - infra-pool：ecs.c8i.2xlarge x1，按量付费，运行聚合 Job
3. 开通阿里云 Redis 企业版 4GB 主备版（同 VPC，仅存 quotes 快照和任务队列）
4. 创建 ACR 企业版，配置 ACK 集群免密拉取权限
5. 安装 KEDA：`helm install keda kedacore/keda -n keda --create-namespace`
6. 安装 Cluster Autoscaler（ACK 控制台组件管理一键安装）

### 阶段二：Go Operator 开发

1. 初始化项目：

```bash
mkdir backtest-operator && cd backtest-operator
kubebuilder init --domain qlib.io --repo github.com/yourorg/backtest-operator
kubebuilder create api --group batch --version v1 --kind BacktestTask
```

2. 实现 `api/v1/backtest_task_types.go` Spec/Status（见第7节）
3. 实现 `controllers/backtest_task_controller.go` Reconcile 三态逻辑（见第7节）
4. 构建推送镜像：

```bash
docker build -t registry.cn-shanghai.aliyuncs.com/yourorg/backtest-operator:v1.0.0 .
docker push registry.cn-shanghai.aliyuncs.com/yourorg/backtest-operator:v1.0.0
```

5. 部署：

```bash
make install   # 安装 CRD
make deploy IMG=registry.cn-shanghai.aliyuncs.com/yourorg/backtest-operator:v1.0.0
```

6. 验证：手动 apply 一个 BacktestTask YAML，确认 Operator 正确创建 K8s Job

### 阶段三：MemoryStore Arrow IPC 改造

1. 新增 `qlib_factor_platform/data/shm_store.py`（见第9节代码）
2. 在 `memory_store.py` 的 `get_instance()` 中根据环境变量 `USE_SHM=true` 切换实现
3. `requirements.txt` 添加 `pyarrow>=14.0.0`
4. 本地验证：docker-compose 起两个容器挂载同一 tmpfs 卷，跑通 collector 写入 + live_engine 读取
5. 确认 `data/api.py` 所有 DataAPI 调用路径无需改动

### 阶段四：镜像构建和部署

1. 构建推送全部镜像：

```bash
docker build -f docker/collector.Dockerfile -t registry.cn-shanghai.aliyuncs.com/yourorg/collector:v1.0.0 .
docker build -f docker/api.Dockerfile -t registry.cn-shanghai.aliyuncs.com/yourorg/api:v1.0.0 .
docker build -f docker/backtest.Dockerfile -t registry.cn-shanghai.aliyuncs.com/yourorg/backtest:v1.0.0 .
```

2. 创建命名空间和 Secret：

```bash
kubectl create namespace qlib
kubectl create secret generic qlib-secrets --from-env-file=.env -n qlib
```

3. 按序 apply 资源：

```bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/live-engine-statefulset.yaml
kubectl apply -f k8s/api-deployment.yaml
kubectl apply -f k8s/ingress.yaml
```

4. 验证 live-engine Pod 两个容器均 Running，`/dev/shm/store/` 下有 Arrow 文件生成

### 阶段五：回测弹性验证

1. 提交测试回测任务（1个月区间，2个分片）
2. 确认 Operator 创建 Job，Spot 节点从 0 扩容
3. 等待 Job 完成，确认聚合结果写入 OSS
4. 模拟 Spot 中断（手动 kubectl delete pod），确认检查点恢复机制正常
5. 全量压测（1年区间，20分片），观察 Cluster Autoscaler 扩节点耗时

---

## 13. 成本分析

### 常驻节点（按量付费）

| 节点池 | 机型 | 数量 | 月费估算 |
|--------|------|------|----------|
| system-pool | ecs.r8i.4xlarge (16C 128GB) | 2 | ~3,200元 |
| infra-pool | ecs.c8i.2xlarge (8C 16GB) | 1 | ~600元 |
| ACK 管控费 | - | - | ~500元 |
| **合计** | | | **~4,300元/月** |

### 回测 Spot 节点（弹性，按使用量）

| 机型 | 按量价格 | Spot 折扣 | Spot 价格 |
|------|---------|----------|----------|
| ecs.c8i.8xlarge (32C 64GB) | ~4.5元/h | 10-20% | ~0.5-0.9元/h |
| ecs.u1-c1m2.4xlarge (4C 8GB) | ~0.6元/h | 15% | ~0.09元/h |

典型场景：20个 Spot Pod，跑 2 小时 = 20 × 0.7 × 2 = **28元/次**

对比原方案手动拉起 ECS：20台 × 4.5元/h × 2h = 180元/次，**节省约 85%**

### Redis（仅存 quotes + 任务队列）

- 阿里云 Redis 企业版 4GB 主备：~300元/月
- tick/order/deal 全在共享内存，不走 Redis，压力极小

---

## 14. 关键风险与规避

### 风险1：Spot 节点被回收，回测 Job 中断

**规避：**
- Go Operator 每 30 秒轮询 Job 状态，检测到失败自动重建
- backtest worker 每完成一个交易日写检查点到 OSS（`checkpoints/{task_id}/{shard_id}/YYYYMMDD.parquet`）
- Job 重启后从最新检查点恢复，不重跑
- 节点池配置抢占保护期 1 小时，降低中断频率

### 风险2：live-engine Pod 所在节点故障，32GB 共享内存丢失

**规避：**
- system-pool 使用**按量付费**节点，不用 Spot，保障稳定性
- collector 重启后重连通联，当日数据从 OSS 历史数据补全（OSSDataLoader 已有此逻辑）
- 实盘服务盘中故障属于固有风险，与 K8s 无关

### 风险3：Spot 节点扩容慢，回测排队积压

**规避：**
- 节点池配置 warmPool 保留 2 台常驻 Spot（非交易时段成本极低）
- Cluster Autoscaler 配置 `scale-up-delay=0`
- 机型 fallback 列表至少 6 种，提高抢到概率

### 风险4：collector 通联长连接在 K8s 网络环境断连

**规避：**
- collector 已有 WebSocket 重连逻辑（`realtime/collector.py`）
- 如通联要求固定出口 IP，给 system-pool 节点绑定 EIP

### 风险5：回测 worker 镜像拉取慢，Spot 节点冷启动时间长

**规避：**
- 使用 ACR 企业版同 VPC 内网拉取，速度远快于公网
- 开启 ACK 镜像预热（ImageCache），Spot 节点启动前预拉取
- backtest 镜像分层缓存，numpy/pandas/qlib 固化在 base 层

---

## 附录：快速参考命令

```bash
# 查看所有回测任务
kubectl get backtest -n qlib

# 查看某个任务详情
kubectl describe backtest my-task-20240101 -n qlib

# 查看 worker Pod 日志
kubectl logs -l job-name=backtest-my-task-0 -n qlib --tail=100

# 查看 live-engine 共享内存中的文件
kubectl exec -it live-engine-0 -c live-engine -n qlib -- ls -lh /dev/shm/store/

# 查看节点池扩缩容事件
kubectl get events -n kube-system | grep -i autoscaler

# 强制重启 collector
kubectl rollout restart statefulset/live-engine -n qlib

# 手动提交回测任务
kubectl apply -f - <<EOF
apiVersion: batch.qlib.io/v1
kind: BacktestTask
metadata:
  name: test-task-001
  namespace: qlib
spec:
  strategyImage: registry.cn-shanghai.aliyuncs.com/yourorg/backtest:v1.0.0
  startDate: "2023-01-01"
  endDate: "2023-12-31"
  instances: 12
  resultPath: oss://your-bucket/results/test-task-001/
EOF
```


---

## 15. 最终确定方案（综合讨论结果）

### 关键决策汇总

| 决策点 | 结论 | 原因 |
|--------|------|------|
| ACK vs ACS | ACK 标准托管版 | Spot节点、固定IP+MAC行情服务器 |
| MemoryStore | 进程内单例，不改造 | 同进程import，零延迟零改动 |
| 实际内存需求 | 8-12GB（非32GB） | LRU 2小时窗口 |
| Go vs Python采集 | 全Python | 分钟级策略性能差距可忽略，复杂度远大于收益 |
| Go使用范围 | 仅Backtest Operator | K8s API交互是Go主场 |
| 通联接入方式 | CSV文件监听替代pymdl SDK | 无网络丢包，崩溃可offset续读 |
| 镜像构建 | ConfigMap注入，废弃docker build | 5-10秒提交，无安全风险 |
| 行情服务器 | 固定ECS节点 + hostNetwork | 通联校验IP+MAC，不可漂移 |

---

## 16. 服务清单（共6个）

### 服务一：tonglian-server

- 职责：运行通联行情客户端，实时追加写CSV到hostPath
- 语言：通联提供的客户端程序（非自研）
- 部署：hostNetwork Pod，强制调度到market-node
- 副本：1，固定节点不漂移
- 关键配置：hostNetwork: true，nodeSelector: role=market-data

### 服务二：live-engine

- 职责：inotify监听CSV，解析写MemoryStore，策略执行，下单
- 语言：Python（全栈，不引入Go）
- 部署：StatefulSet，固定market-node，与tonglian-server同节点共享hostPath
- 线程模型：collector线程（CSV inotify）+ live_engine线程 + 策略线程
- 副本：1
- 内存：request 8Gi / limit 16Gi（LRU 2小时窗口）
- 镜像：live-engine:vX.Y.Z（平台框架） + ConfigMap strategy.py（动态挂载）
- collector.py改动范围：仅替换_init_client()/_connect_sh_market()/_connect_sz_market()，
  改为inotify监听hostPath CSV，其余数据转换/MemoryStore写入/K线聚合全部保留不动

### 服务三：api

- 职责：策略上传/语法检查/存ConfigMap，回测任务提交，结果查询，Web UI
- 语言：Python FastAPI
- 部署：Deployment，system-pool，2副本
- 新增：POST /api/strategy/upload 替代原docker build流程
- 废弃：api/build.py中的docker build/push逻辑

### 服务四：backtest-operator

- 职责：Watch BacktestTask CRD，按日期分片创建K8s Job，管理Spot节点，触发聚合
- 语言：Go（kubebuilder）
- 部署：Deployment，system-pool，Leader Election，2副本（只有1个active）
- 完全替代：scheduler/controller.py + scheduler/aliyun_manager.py

### 服务五：backtest-worker

- 职责：执行单个回测日期分片，结果写OSS
- 语言：Python
- 部署：K8s Job（Operator按需创建），backtest-spot-pool Spot节点
- 镜像：backtest-base:vX.Y.Z（固定） + ConfigMap strategy.py（动态挂载）
- 副本：0-50弹性，Taint容忍 dedicated=backtest:NoSchedule

### 服务六：data-converter CronJob

- 职责：每日收盘后处理通联ZIP，转Parquet写OSS
- 语言：Go（deeptrade/cmd/data-converter，零改动直接复用）
- 部署：K8s CronJob，schedule: 35 15 * * 1-5

---

## 17. 代码仓库拆解方案

### 拆分原则

- 按部署单元：独立部署的服务对应独立仓库
- 按语言边界：Go和Python不混仓库
- 按变更频率：研究员改策略高频，平台框架低频，互不阻塞CI
- 按职责边界：基础设施配置与业务代码分离

### 仓库一：quant-platform（当前qlib_factor_platform演进）

```text
定位：Python业务核心，平台开发者维护

qlib_factor_platform/
  api/                   FastAPI（新增strategy upload接口，废弃build.py docker逻辑）
  backtest/              回测引擎（不动）
  realtime/
    collector.py         改：替换pymdl SDK为inotify CSV监听，其余不动
    live_engine.py       不动
  data/                  DataAPI + MemoryStore（不动）
  strategy/              策略基类（不动）
  scheduler/
    result_aggregator.py 保留
    controller.py        废弃
    aliyun_manager.py    废弃
docker/
  api.Dockerfile
  live-engine.Dockerfile      新建
  backtest-base.Dockerfile    新建
k8s/
  namespace.yaml
  rbac.yaml
  api-deployment.yaml
  live-engine-statefulset.yaml
  tonglian-server-pod.yaml
  cronjob-data-converter.yaml

CI产物（push main）：api:vX.Y.Z  live-engine:vX.Y.Z  backtest-base:vX.Y.Z
```

### 仓库二：backtest-operator（新建）

```text
定位：Go K8s Operator，平台开发者维护

api/v1/
  backtest_task_types.go       CRD Spec/Status定义
controllers/
  backtest_task_controller.go  Reconcile三态状态机
config/crd/                    CRD YAML
config/manager/                Operator Deployment YAML
config/rbac/                   RBAC YAML
Dockerfile
Makefile

CI产物：backtest-operator:vX.Y.Z
完全替代：scheduler/controller.py + aliyun_manager.py
```

### 仓库三：strategies（新建，研究员专用）

```text
定位：策略代码仓库，研究员独立维护，无需了解平台内部

strategies/
  momentum_v1/
    strategy.py       继承BaseStrategy，实现on_bar()
    backtest.yaml     回测参数（start_date/end_date/universe/instances）
  mean_revert_v2/
    strategy.py
    backtest.yaml
.github/workflows/
  submit.yaml

CI流程（研究员push，无镜像构建）：
  1. POST /api/strategy/upload 上传 strategy.py
  2. API语法检查 → kubectl apply ConfigMap
  3. 读取backtest.yaml → POST /api/backtest/submit → 创建BacktestTask CRD
  4. 全程5-10秒
```

### deeptrade仓库处理

```text
cmd/data-converter/      保留，打镜像作为K8s CronJob
tonglian/client.go       保留，作为SDK降级备用
cmd/tonglian-ingestion/  废弃
```

### 仓库间交互

```text
strategies仓库 push
  └─ GitHub Actions
       ├─ POST /api/strategy/upload  → ConfigMap
       └─ POST /api/backtest/submit  → BacktestTask CRD
            └─ backtest-operator Watch
                 └─ 创建K8s Job（backtest-base镜像 + ConfigMap挂载）
                      └─ worker计算分片 → OSS
                           └─ aggregator合并 → BacktestTask Completed
```

---

## 18. 最终成本估算

| 资源 | 规格 | 计费方式 | 月成本估算 |
|------|------|---------|----------|
| market-node | ecs.c8i.4xlarge 16核32GB | 按量付费（通联绑定不可Spot） | ~1100元 |
| system-pool x2 | ecs.c8i.2xlarge 8核16GB | 按量付费 | ~800元 |
| backtest-spot | ecs.c8i.8xlarge 32核64GB | Spot抢占式 | ~0.8元/小时/台 |
| Redis企业版 | 4GB主备 | 按量 | ~200元 |
| ACR企业版 | - | 包年包月 | ~180元 |
| ACK管理费 | - | 按量 | ~150元 |
| 固定月成本合计 | | | ~2430元 |
| 回测弹性成本 | 按实际算力用量 | Spot | ~0.8元/小时/Pod |

对比改造前（手动ECS常开）：约8000+元/月
改造后节省：约70%固定成本，回测按需付费

---

## 19. 改造工作量评估

| 模块 | 改动类型 | 工作量 |
|------|---------|--------|
| collector.py | 改：替换pymdl SDK部分为inotify CSV监听 | 小，约100行 |
| memory_store.py | 改：tick/order/deal加LRU（OrderedDict maxsize） | 极小，约20行 |
| api/build.py | 改：废弃docker build逻辑，新增ConfigMap上传接口 | 中，约150行 |
| backtest/engine.py | 改：支持动态import strategy.py路径 | 极小，约30行 |
| scheduler/* | 废弃controller.py和aliyun_manager.py | 零工作量 |
| backtest-operator | 新建Go项目，kubebuilder框架 | 大，约500行Go |
| k8s/ YAML | 新建6个K8s资源配置文件 | 中，约400行YAML |
| docker/ | 新建2个Dockerfile | 小，约60行 |
| strategies仓库 | 新建，含CI workflow | 小，约50行YAML |
| data-converter CronJob | deeptrade直接复用，只需写CronJob YAML | 极小，约30行 |

---

## 20. 实施顺序建议

1. 搭建ACK集群和节点池（基础设施先行）
2. 部署backtest-operator（Go Operator，验证CRD和Job创建）
3. 改造镜像构建流程（ConfigMap注入，废弃docker build）
4. 部署backtest-worker，验证端到端回测流程
5. 改造collector.py（pymdl SDK改CSV inotify监听）
6. 部署tonglian-server和live-engine，验证实盘数据流
7. 建立strategies仓库，研究员接入CI流程
8. 上线data-converter CronJob，替代手动批处理


---

## 21. 策略保密方案（替代 strategies 仓库）

研究员策略需要保密，不建立公共仓库。改用 API 上传 + K8s Secret 存储。

### 21.1 仓库最终拆解（2个仓库）

| 仓库 | 维护者 | 说明 |
|------|--------|------|
| quant-platform | 平台开发者 | Python业务核心+K8s配置，内部仓库 |
| backtest-operator | 平台开发者 | Go Operator，内部仓库 |

研究员无需访问任何代码仓库，通过 API 直接上传策略文件。

### 21.2 策略上传流程

```text
研究员本地
  strategy.py
    └─ POST /api/strategy/upload（Bearer token认证）
         └─ API: 语法检查（ast.parse）
         └─ API: kubectl apply Secret（KMS加密）
         └─ 返回 strategy_id
    └─ POST /api/backtest/submit { strategy_id, start_date, end_date, universe }
         └─ 创建 BacktestTask CRD（引用Secret名）
         └─ Operator创建Job，挂载对应Secret
         └─ Job完成后Operator删除Secret
```

### 21.3 用 Secret 替代 ConfigMap 的原因

| | ConfigMap | Secret + KMS |
|--|-----------|-------------|
| 访问控制 | kubectl权限即可查看 | RBAC细粒度控制，只有指定SA能get |
| 静态加密 | etcd明文存储 | 阿里云KMS加密etcd静态数据 |
| 审计 | 无 | 阿里云操作审计记录所有访问 |
| 生命周期 | 手动清理 | Operator回测完成后自动删除 |

### 21.4 RBAC 配置

```yaml
# backtest-worker ServiceAccount 只能 get 指定 Secret，不能 list
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: strategy-reader
  namespace: qlib
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get"]   # 只能get，不能list，无法枚举其他策略
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: backtest-worker
  namespace: qlib
```

### 21.5 策略生命周期

```text
上传阶段：
  strategy.py → K8s Secret（KMS加密，etcd静态加密）
  Secret名：strategy-{strategy_id}

运行阶段：
  backtest-worker Pod 挂载 Secret → /app/strategy/strategy.py
  动态 import，仅在 Pod 内存中存在
  Pod 销毁后内存清除

结果阶段：
  OSS 只存回测指标（sharpe/drawdown/returns等），不存策略代码

清理阶段：
  BacktestTask Completed/Failed 后
  Operator 自动执行 kubectl delete secret strategy-{strategy_id}
  策略代码从集群彻底消除
```

### 21.6 API 改造范围（api/build.py）

```text
废弃：
  build_image()         docker build/push 全部删除
  _create_dockerfile()  删除
  push_to_registry()    删除

新增：
  upload_strategy()     接收strategy.py → ast语法检查 → kubectl apply Secret
  delete_strategy()     kubectl delete Secret（Operator也会调用）
  list_strategies()     只返回strategy_id列表和元数据，不返回代码内容
```
