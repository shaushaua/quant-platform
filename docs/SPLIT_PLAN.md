# 项目拆分方案：backtest-operator + quant-platform

> 基于当前代码库状态与 K8s 迁移目标，将 qlib_factor_platform 拆分为两个独立项目

生成时间：2026-03-16

---

## 目录

1. [拆分动机](#1-拆分动机)
2. [当前代码库现状](#2-当前代码库现状)
3. [目标项目划分](#3-目标项目划分)
4. [各项目模块归属](#4-各项目模块归属)
5. [当前迁移进度](#5-当前迁移进度)
6. [待完成工作](#6-待完成工作)

---

## 1. 拆分动机

当前 `qlib_factor_platform` 将以下职责混合在一个仓库：

- K8s Job 调度、ECS Spot 实例管理、回测任务编排（**控制面**）
- 实盘行情采集、MemoryStore、实盘引擎（**数据面 + 执行面**）
- 分布式因子计算 Worker（**计算面**）
- FastAPI 接口（**接入面**）

**问题：**
- 控制面依赖 K8s/Aliyun SDK，与实盘 Python 逻辑耦合
- 实盘服务需要固定 ECS 节点，不可弹性，与 Spot 调度逻辑混在一起
- 镜像构建（docker build/push）混入 API 服务，安全边界不清
- Go Operator 无法放在 Python 项目里维护

**拆分后：**

| 项目 | 定位 | 语言 | 部署 |
|------|------|------|------|
| `backtest-operator` | K8s 控制面：CRD 调度、Spot 节点编排、镜像构建 API | Go（kubebuilder）| ACK system-pool，无状态 |
| `quant-platform` | 数据面 + 执行面：实盘采集、MemoryStore、实盘引擎、因子计算 Worker | Python | ACK market-node StatefulSet，固定节点 |

---

## 2. 当前代码库现状

```
qlib_factor_platform/
├── api/
│   ├── main.py           # FastAPI 路由入口
│   ├── backtest.py       # 回测提交接口
│   ├── build.py          # 镜像构建接口（docker build/push，需废弃）
│   └── result.py         # 结果查询接口
├── backtest/
│   └── engine.py         # 回测引擎（单机，策略 dynamic import）
├── core/
│   ├── config.py
│   └── constants.py
├── data/
│   ├── memory_store.py   # 实时内存存储（LRU，进程内单例）
│   ├── api.py            # DataAPI 统一查询接口
│   ├── converter.py      # 通联数据格式转换
│   ├── mysql_loader.py   # MySQL 价格缓存
│   └── oss_loader.py     # OSS 数据加载
├── realtime/
│   ├── collector.py      # 通联行情采集（当前：pymdl SDK；目标：inotify CSV）
│   └── live_engine.py    # 实盘引擎（collector + 策略执行 + 信号输出）
├── scheduler/
│   ├── controller.py     # 分布式回测主编排（任务状态机）
│   ├── aliyun_manager.py # ECS Spot 实例管理（Aliyun SDK，将废弃）
│   ├── benchmark.py      # 单日回测基准测速
│   ├── cost_calculator.py
│   ├── task_splitter.py
│   └── result_aggregator.py
├── strategy/
├── docker/               # api / collector / ui 三个 Dockerfile
└── docker-compose.yml    # 当前单机部署
```

**K8s 基础设施现状：无**
- 无 `k8s/`、`deploy/`、`helm/` 目录
- 无 Go Operator 代码
- 仅 `docker-compose.yml` 单机部署

**openspec changes 状态：**

| change | 状态 | 说明 |
|--------|------|------|
| `refactor-realtime-backtest-split` | ✓ Complete（6/6） | 实盘/回测边界梳理完成，文档已更新 |
| `add-distributed-backtest-scheduler` | 71/79 tasks | Python 调度逻辑已完成，8 项未完成（见下） |
| `add-redis-oss-support` | 0/255 tasks | 未开始 |

**add-distributed-backtest-scheduler 未完成的 8 项：**

```
1.3  Build and push base image to ACR（未实际推送）
1.4  Test base image locally with sample backtest
2.7  Test end-to-end: upload → build → push → delete
9.4  Add authentication middleware
9.5  Add request validation
12.4 Integration test: single instance flow
12.5 Integration test: multi-instance flow
12.6 End-to-end test: upload → submit → aggregate
```

---

## 3. 目标项目划分

### 项目一：backtest-operator

**职责：K8s 控制面**

- Watch `BacktestTask` CRD，按日期分片创建 K8s Job
- 管理 backtest-worker Pod 生命周期（配合 KEDA + Cluster Autoscaler）
- 接收策略代码，加密存入 K8s Secret（KMS），回测完成后自动删除
- 触发 kaniko Job 构建回测 Worker 镜像（不依赖宿主机 Docker）
- 触发 OSS 结果聚合
- Leader Election，保证单实例控制

**技术栈：** Go（kubebuilder v4）

**部署：** ACK system-pool，Deployment，2 副本

**仓库结构（目标）：**
```
backtest-operator/
├── api/v1/
│   └── backtest_task_types.go       # CRD 类型定义
├── internal/controller/
│   └── backtest_task_controller.go  # Reconcile 核心逻辑
├── pkg/
│   ├── splitter/    # 日期分片（从 task_splitter.py 迁移逻辑）
│   ├── cost/        # Spot 成本计算（从 cost_calculator.py 迁移逻辑）
│   └── aggregator/  # OSS 结果聚合触发
├── config/
│   ├── crd/         # CRD YAML
│   ├── rbac/        # RBAC
│   └── manager/     # Deployment YAML
└── Dockerfile
```

---

### 项目二：quant-platform

**职责：数据面 + 执行面 + 因子计算 Worker**

- **数据采集**：inotify 监听 tonglian-server 写出的 hostPath CSV，写入 MemoryStore
- **MemoryStore**：进程内单例，LRU 2 小时窗口，约 8-12GB
- **实盘引擎**：collector 线程 + live_engine 线程 + 策略线程
- **因子计算 Worker**：作为 K8s Job 被 backtest-operator 创建，执行单段回测
- **DataAPI**：MemoryStore / OSS / MySQL 统一查询
- **行情查询 API**：对外提供实时行情、因子数据查询接口

**技术栈：** 全 Python

**部署：** ACK market-node StatefulSet（实盘）+ Spot backtest-worker Job（回测）

**仓库结构（目标）：**
```
quant-platform/
├── realtime/
│   ├── collector.py             # inotify CSV 监听（需改造约 100 行）
│   └── live_engine.py           # 实盘引擎（直接迁移）
├── data/
│   ├── memory_store.py          # 进程内 MemoryStore（直接迁移）
│   ├── api.py
│   ├── converter.py
│   ├── mysql_loader.py
│   └── oss_loader.py
├── backtest/
│   └── engine.py                # 回测引擎（Worker 内运行，直接迁移）
├── factor/                      # 新增：因子计算框架
│   ├── base.py
│   └── runner.py
├── strategy/
├── core/
├── api/
│   └── main.py                  # 行情查询 / 策略管理接口（剥离回测提交）
├── docker/
│   ├── live-engine.Dockerfile
│   └── backtest-worker.Dockerfile   # 新增，回测 Worker 基础镜像
└── k8s/
    ├── live-engine-statefulset.yaml
    └── backtest-worker-job-template.yaml
```

---

## 4. 各项目模块归属

### → backtest-operator

| 当前路径 | 处理方式 | 备注 |
|----------|----------|------|
| `scheduler/controller.py` | 重写为 Go Reconcile | 任务状态机逻辑迁移 |
| `scheduler/task_splitter.py` | 重写为 Go `pkg/splitter` | 逻辑简单，易迁移 |
| `scheduler/cost_calculator.py` | 重写为 Go `pkg/cost` | |
| `scheduler/result_aggregator.py` | 重写为 Go `pkg/aggregator` 或保留 Python CronJob | |
| `scheduler/benchmark.py` | 保留为 Python Job（一次性工具） | 不值得用 Go 重写 |
| `scheduler/aliyun_manager.py` | **废弃** | K8s 原生替代，不再直接管 ECS |
| `api/build.py` | 重写：kaniko Job + K8s Secret | 废弃宿主机 docker build/push |
| `api/backtest.py` | 重写为 Go HTTP handler | CRD 提交接口 |
| `api/result.py` | 重写为 Go HTTP handler | |

### → quant-platform

| 当前路径 | 处理方式 | 备注 |
|----------|----------|------|
| `realtime/collector.py` | **需改造**：pymdl → inotify CSV | 约改 100 行，其余保留 |
| `realtime/live_engine.py` | 直接迁移 | 接口不变 |
| `data/memory_store.py` | 直接迁移 | LRU 已实现 |
| `data/api.py` | 直接迁移 | |
| `data/converter.py` | 直接迁移 | |
| `data/mysql_loader.py` | 直接迁移 | |
| `data/oss_loader.py` | 直接迁移 | |
| `backtest/engine.py` | 直接迁移 | Worker 内运行 |
| `strategy/` | 直接迁移 | |
| `core/` | 直接迁移 | |
| `api/main.py`（行情部分） | 直接迁移 | 剥离回测提交逻辑 |

---

## 5. 当前迁移进度

### 总体进度

```
分类                               进度     说明
──────────────────────────────────────────────────────────────────
backtest-operator Go 核心代码      90%     kaniko builder/Secret/PhaseBuilding 已完成
backtest-operator K8s 配置         90%     CRD名修正，RBAC完整（SA+Role+Binding+secrets权限）
backtest-operator 测试             60%     单元测试5个全部通过，集成测试待 ACK 集群
quant-platform Python 代码         95%     build.py 已删除，backtest.py 已对接 operator HTTP API
quant-platform K8s 配置            95%     tonglian-server DaemonSet/quant-secrets模板/live-engine nodeSelector修正
collector inotify 改造             100%    pymdl SDK 已替换为 inotify CSV 监听，offset 续读已实现
add-redis-oss-support change       0%      255 项任务全部未开始（建议暂缓）
```

### backtest-operator：Go 代码完成度

| 文件 | 状态 | 说明 |
|------|------|------|
| `api/v1/backtest_types.go` | ✓ 完成 | BacktestTask CRD 类型定义，含完整 Spec/Status/Phase |
| `api/v1/groupversion_info.go` | ✓ 完成 | GVK 注册 |
| `api/v1/zz_generated_deepcopy.go` | ✓ 完成 | 自动生成 |
| `internal/controller/backtesttask_controller.go` | ✓ 完成 | Reconcile 状态机：Pending→Running→Aggregating→Done/Failed |
| `internal/controller/httpserver.go` | ✓ 完成 | HTTP API：POST /tasks、GET /tasks、GET /tasks/{id}、/healthz |
| `internal/builder/job.go` | ✓ 完成 | SplitDateRange + BuildWorkerJob + BuildAggregatorJob |
| `cmd/operator/main.go` | ✓ 完成 | Manager 启动 + HTTP Server goroutine + Leader Election |
| `config/crd/backtest.quant.lianghua.io.yaml` | ✓ 存在 | |
| `config/manager/deployment.yaml` | ✓ 存在 | |
| `config/rbac/role.yaml` | ✓ 存在 | |
| 镜像构建（kaniko Job）| ✗ 未实现 | 当前无策略上传/镜像构建流程 |
| 认证中间件 | ✗ 未实现 | HTTP API 无鉴权 |
| `internal/aggregator/` | ✗ 目录为空 | 聚合逻辑仅靠 BuildAggregatorJob 触发 Python |
| 单元测试 / 集成测试 | ✗ 未实现 | |
| git 初始提交 | ✗ 未提交 | main 分支无 commit |

### quant-platform：模块迁移就绪度

| 模块 | 就绪状态 | 备注 |
|------|----------|------|
| data/memory_store.py | ✓ 就绪 | LRU 已实现，直接迁移 |
| data/api.py | ✓ 就绪 | |
| data/converter.py | ✓ 就绪 | |
| data/mysql_loader.py | ✓ 就绪 | |
| data/oss_loader.py | ✓ 就绪 | |
| backtest/engine.py | ✓ 就绪 | |
| realtime/live_engine.py | ✓ 就绪 | |
| realtime/collector.py | ✗ 需改造 | pymdl → inotify，约 100 行 |
| factor/ 模块 | ✗ 待新建 | 分布式因子计算框架 |
| docker/live-engine.Dockerfile | ✗ 待新建 | 现有 collector.Dockerfile 可参考 |
| docker/backtest-worker.Dockerfile | ✗ 待新建 | |
| k8s/ StatefulSet YAML | ✗ 待新建 | |

---

## 6. 待完成工作

### 6.1 backtest-operator（优先级：高）

**已完成：**
- [x] Go 项目初始化（go.mod，kubebuilder 框架）
- [x] 定义 `BacktestTask` CRD 类型（api/v1/backtest_types.go）
- [x] Reconcile 核心逻辑（状态机：Pending → Running → Aggregating → Done/Failed）
- [x] HTTP API Server（POST/GET /tasks，/healthz）
- [x] Job Builder（SplitDateRange + BuildWorkerJob + BuildAggregatorJob）
- [x] config/crd YAML、config/manager/deployment.yaml、config/rbac/role.yaml

**待完成：**

**阶段一：镜像构建流程**
- [ ] `internal/builder/image.go`：接收策略代码 → 创建 kaniko Job → 推送 ACR
- [ ] HTTP API 新增 `POST /strategies/upload` 端点
- [ ] 策略代码用 K8s Secret 存储（而非写入镜像）
- [ ] `internal/builder/secret.go`：策略 Secret 创建/注入/回测完成后删除

**阶段二：补全 K8s 配置**
- [ ] `config/rbac/` 补全 ClusterRole（当前仅有 role.yaml，缺少 ServiceAccount / Binding）
- [ ] `config/manager/deployment.yaml` 确认 system-pool nodeSelector
- [ ] Leader Election 配置验证

**阶段三：鉴权与健壮性**
- [ ] HTTP API 认证中间件（Bearer Token 或 mTLS）
- [ ] 请求参数校验（日期格式、imageTag 合法性）
- [ ] Spot 节点中断处理：Job backoffLimit 配置

**阶段四：测试**
- [ ] `go test ./...` 单元测试（splitter 日期分片逻辑）
- [ ] Build and push base backtest-worker image to ACR
- [ ] 集成测试：单 shard 端到端流程
- [ ] 集成测试：多 shard 并发流程
- [ ] git 首次提交

---

### 6.2 quant-platform（优先级：高）

**已完成：**
- [x] 仓库已建立（/Users/zhangyang/Desktop/lianghua/quant-platform）
- [x] 全部 Python 模块已迁移：data/、realtime/、backtest/、core/、strategy/、api/
- [x] `data/shm_store.py` 新增（ShmStore 跨进程共享内存，与 MemoryStore 双写）
- [x] `backtest/worker.py` 新增（K8s Job 入口，env 注入参数）
- [x] `backtest/aggregator.py` 新增（OSS 分片结果聚合）
- [x] collector.py 集成 ShmStore 双写（USE_SHM_STORE 环境变量控制）
- [x] k8s/ 目录完整：live-engine StatefulSet、backtest Job 模板、KEDA ScaledObject、API Deployment、Ingress、Prometheus ServiceMonitor
- [x] docker/ 新增 backtest.Dockerfile

**待完成：**

**阶段一：collector inotify 改造（最关键缺口）**
- [ ] 替换 `_init_client()`：移除 pymdl SDK 初始化
- [ ] 替换 `_connect_sh_market()` / `_connect_sz_market()`：改为 inotify 监听 hostPath CSV
- [ ] 实现 CSV offset 续读（崩溃重启后从上次位置继续）
- [ ] tonglian-server 服务（运行通联客户端程序，写 CSV 到 hostPath）部署方案确认

**阶段二：api/build.py 废弃**
- [ ] 删除 `api/build.py`（docker build/push 逻辑迁移至 backtest-operator）
- [ ] `api/backtest.py`：将回测提交从本地调度改为调用 backtest-operator HTTP API

**阶段三：K8s 配置补全**
- [ ] live-engine StatefulSet：确认 nodeSelector `node-role: market-data`（当前为 `system`）
- [ ] `k8s/tonglian-server/`：新增 hostNetwork Pod 配置
- [ ] `k8s/base/namespace.yaml` 确认命名空间
- [ ] Secret 模板（quant-secrets）

**阶段四：测试**
- [ ] backtest worker 本地端到端测试
- [ ] live-engine + collector 集成测试
- [ ] git 首次提交

---

### 6.3 add-redis-oss-support（优先级：低，暂缓）

当前 255 项任务全部未开始。建议等 backtest-operator 和 quant-platform 基础骨架跑通后再推进，避免并行过多未完成 change。

---

### 优先级排序

```
1. backtest-operator Go 项目初始化 + CRD 定义
2. quant-platform 代码迁移 + collector inotify 改造
3. 两个项目的 K8s YAML 资源文件
4. 端到端集成测试（回测链路打通）
5. add-redis-oss-support（暂缓）
```