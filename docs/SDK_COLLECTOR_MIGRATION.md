# MDL SDK Collector 实盘改造方案

> 目标：直接用 `pymdl` 订阅本机通联客户端 `9012` 端口，替换当前 CSV 监听 collector。交易员 `DataAPI` 和策略接口不变，实时数据继续通过 `ShmStore` 提供给 `live-engine`，并在有限内存下做到低延迟、可监控、可校验。

## 1. 背景与结论

当前实时链路是：

```text
通联客户端 -> CSV 文件 -> watchdog/offset 监听 -> pandas read_csv -> converter -> ShmStore -> live-engine
```

这条链路的问题：

- CSV 文件监听依赖 offset、文件事件和文本解析，存在首批跳过、解析失败后 offset 前移、收盘 backlog 无事件触发等丢数据风险。
- `pandas.read_csv(dtype=str)` + 后续类型转换在逐笔行情高峰期成本高、延迟尖峰大。
- 当前深市 order/deal 转换存在类型映射风险，可能出现“CSV 读到了但转换为空/字段错”的等效丢数据。
- CSV 是通联客户端的落地副产品，不应作为实盘主实时总线。

改造后主链路：

```text
通联客户端 -> 本机 9012 端口 -> pymdl SDK callback -> 批量标准化 -> ShmStore -> live-engine/DataAPI
```

CSV 后续仅作为通联原始旁路文件，用于盘后审计、补数和 SDK 对账，不再进入实时主链路。

## 2. 设计原则

1. **交易员 API 不变**
   - 保持 `DataAPI(mode="realtime")`、`get_tick()`、`get_order()`、`get_deal()`、`get_all_quotes()` 等接口不变。
   - 保持 `ShmStore` 的 tick/order/deal Arrow chunk 布局不变。

2. **直接全量替换 CSV collector**
   - 实盘主 collector 改为 `sdk_collector`。
   - 原 CSV collector 不再作为实时输入源，只保留为工具/旁路校验代码。

3. **全市场订阅**
   - 不按股票池过滤订阅。
   - 交易员在策略或 DataAPI 层自行筛选。
   - 避免股票池动态变化导致订阅遗漏。

4. **有限内存优先**
   - SDK callback 不缓存大 DataFrame。
   - collector 只做短周期 micro-batch。
   - `ShmStore` 继续采用滚动 chunk，`live-engine` 消费后删除 chunk。
   - 内存中只保留“尚未 flush 的短队列”和“尚未被 live-engine 消费的短窗口”。

5. **进程存活期间不静默丢数据**
   - SDK 消息入队、转换、写 ShmStore 都必须有计数闭环。
   - 队列积压时报警；不做静默 drop。
   - 用 `SequenceID` 做实时连续性校验。

## 3. 目标架构

```text
┌────────────────────┐
│ 通联 MDL 客户端     │
│ - 负责接入通联行情  │
│ - 继续落 CSV 旁路   │
│ - 本机 9012 级联    │
└─────────┬──────────┘
          │ 127.0.0.1:9012
          ▼
┌────────────────────┐
│ sdk_collector       │
│ pymdl Subscriber    │
│ 订阅 4.4/4.24/      │
│      6.28/6.33/6.36 │
└─────────┬──────────┘
          │ callback 轻量入队
          ▼
┌────────────────────┐
│ micro-batch mapper  │
│ SDK msg -> 标准列    │
└─────────┬──────────┘
          │ DataFrame batch
          ▼
┌────────────────────┐
│ ShmStore            │
│ /dev/shm/quant-store│
│ tick/order/deal     │
│ rolling chunks      │
└─────────┬──────────┘
          │ consume + unlink
          ▼
┌────────────────────┐
│ live-engine         │
│ StockState 增量更新 │
│ 因子计算            │
└────────────────────┘
```

## 4. 订阅消息

连接本机通联客户端：

```python
sub.SetServerAddress("127.0.0.1:9012")
sub.SetMessageEncoding(1)
sub.EnableMergeMessage(False)
sub.SetHeartbeatInterval(10)
sub.SetHeartbeatTimeout(30)
```

本机 `9012` 不需要 token，不调用 `SetUserName()`。

全市场订阅：

```python
sub.AddSubscription(4, 101, 4)    # SH L2 tick, 对应 mdl_4_4_0.csv
sub.AddSubscription(4, 101, 24)   # SH order/deal, 对应 mdl_4_24_0.csv
sub.AddSubscription(6, 101, 28)   # SZ tick, 对应 mdl_6_28_0.csv
sub.AddSubscription(6, 101, 33)   # SZ order, 对应 mdl_6_33_0.csv
sub.AddSubscription(6, 101, 36)   # SZ deal, 对应 mdl_6_36_0.csv
```

消息映射：

| SDK 消息 | CSV 旁路文件 | 标准数据类型 |
|---|---|---|
| `4.4` SHL2MarketData | `mdl_4_4_0.csv` | tick |
| `4.24` NGTSTick | `mdl_4_24_0.csv` | order + deal |
| `6.28` Snapshot300111_v2 | `mdl_6_28_0.csv` | tick |
| `6.33` Order300192_v2 | `mdl_6_33_0.csv` | order |
| `6.36` Transaction300191_v2 | `mdl_6_36_0.csv` | deal |

## 5. 新增模块

新增以下模块：

```text
quant_platform/collector/sdk_config.py
quant_platform/collector/sdk_callback.py
quant_platform/collector/sdk_mapper.py
quant_platform/collector/sdk_collector.py
quant_platform/scripts/verify_sdk_vs_csv.py
```

### 5.1 sdk_config.py

负责读取环境变量并生成订阅配置。

建议环境变量：

```bash
COLLECTOR_SOURCE=sdk
MDL_SERVER=127.0.0.1:9012
MDL_IO_THREADS=4
MDL_CALLBACK_MULTITHREAD=true
MDL_ENCODING=1
MDL_ENABLE_MERGE=false
MDL_SUBS=4.4,4.24,6.28,6.33,6.36

MDL_FLUSH_INTERVAL_MS=10
MDL_BATCH_SIZE=2000
MDL_QUEUE_WARN_SIZE=50000
MDL_QUEUE_HARD_LIMIT=0

SHM_STORE_PATH=/dev/shm/quant-store
ROLLING_WINDOW_SECONDS=1800
```

说明：

- `MDL_QUEUE_HARD_LIMIT=0` 表示不设置硬丢弃上限。队列积压只报警，不静默丢。
- 若后续必须保护进程内存，可改为“阻塞 SDK callback + 高优先级报警”，仍不做静默 drop。

### 5.2 sdk_callback.py

继承 `pymdl.MsgCallback`。

职责：

- 处理连接、登录、断线日志。
- `OnMDLSHL2Message` 解析 `4.4/4.24`。
- `OnMDLSZL2Message` 解析 `6.28/6.33/6.36`。
- 记录 `SequenceID` 连续性。
- 将轻量消息对象放入内存队列。

callback 中禁止：

- 构造大 DataFrame。
- 写 ShmStore。
- 写 parquet/OSS。
- 高频 `INFO` 日志。

callback 入队对象：

```python
RawMdlMessage(
    service_id: int,
    message_id: int,
    sequence_id: int,
    local_time: int,
    receive_ts: float,
    msg: object,
)
```

### 5.3 sdk_mapper.py

把 SDK 消息对象映射成当前标准列。

输出仍使用：

- `ORDER_COLUMNS`
- `DEAL_COLUMNS`
- `TICK_COLUMNS`

代码格式保持：

```text
600000.XSHG
000001.XSHE
```

核心函数建议：

```python
map_sh_tick(msg, hd) -> dict
map_sh_ngts_tick(msg, hd) -> tuple[Optional[dict], Optional[dict]]
map_sz_tick(msg, hd) -> dict
map_sz_order(msg, hd) -> dict
map_sz_deal(msg, hd) -> dict
```

其中 `4.24` 需要根据消息对象字段区分 order/deal，输出到不同 batch。

字段按 SDK 手册和 `pymdl/api/*_msg.py` 生成定义执行：

#### SH 4.24 NGTSTick -> order/deal

SDK 字段：

```text
BizIndex, Channel, SecurityID, TickTime, Type,
BuyOrderNO, SellOrderNO, Price, Qty, TradeMoney, TickBSFlag
```

映射规则：

- `Type in ("A", "D")` -> order
- `Type == "T"` -> deal

order:

| 标准列 | SDK 字段/规则 |
|---|---|
| `TradingDay` | 当前交易日 |
| `Code` | `SecurityID + ".XSHG"` |
| `Time` | `TickTime` |
| `UpdateTime` | `hd.LocalTime` 或 `TickTime` |
| `OrderID` | `BuyOrderNO + SellOrderNO` |
| `Side` | `TickBSFlag`: `B -> 0`, `S -> 1`, 其他 `10` |
| `Price` | `Price` |
| `Volume` | `Qty` |
| `OrderType` | `Type`: `A -> 2`, `D -> 5` |
| `Channel` | `Channel` |
| `SeqNum` | `BizIndex` |

deal:

| 标准列 | SDK 字段/规则 |
|---|---|
| `TradingDay` | 当前交易日 |
| `Code` | `SecurityID + ".XSHG"` |
| `Time` | `TickTime` |
| `UpdateTime` | `hd.LocalTime` 或 `TickTime` |
| `SaleOrderID` | `SellOrderNO` |
| `BuyOrderID` | `BuyOrderNO` |
| `Side` | `TickBSFlag`: `B -> 0`, `S -> 1`, `N/其他 -> 10` |
| `Price` | `Price` |
| `Volume` | `Qty` |
| `Money` | `TradeMoney`，为空时 `Price * Qty` |
| `Channel` | `Channel` |
| `SeqNum` | `BizIndex` |

#### SZ 6.33 Order300192_v2 -> order

SDK 字段：

```text
ChannelNo, ApplSeqNum, MDStreamID, SecurityID, SecurityIDSource,
Price, OrderQty, Side, TransactTime, OrdType
```

映射：

| 标准列 | SDK 字段/规则 |
|---|---|
| `TradingDay` | 当前交易日 |
| `Code` | `SecurityID + ".XSHE"` |
| `Time` | `TransactTime` |
| `UpdateTime` | `hd.LocalTime` 或 `TransactTime` |
| `OrderID` | `ApplSeqNum` |
| `Side` | `Side`: `49 -> 0`, `50 -> 1`, 其他 `10` |
| `Price` | `Price` |
| `Volume` | `OrderQty` |
| `OrderType` | `OrdType`: `49 -> 1`, `50 -> 2`, `85 -> 3` |
| `Channel` | `ChannelNo` |
| `SeqNum` | `ApplSeqNum` |

#### SZ 6.36 Transaction300191_v2 -> deal

SDK 字段：

```text
ChannelNo, ApplSeqNum, MDStreamID, BidApplSeqNum, OfferApplSeqNum,
SecurityID, SecurityIDSource, LastPx, LastQty, ExecType, TransactTime
```

映射：

| 标准列 | SDK 字段/规则 |
|---|---|
| `TradingDay` | 当前交易日 |
| `Code` | `SecurityID + ".XSHE"` |
| `Time` | `TransactTime` |
| `UpdateTime` | `hd.LocalTime` 或 `TransactTime` |
| `SaleOrderID` | `OfferApplSeqNum` |
| `BuyOrderID` | `BidApplSeqNum` |
| `Side` | `BidApplSeqNum > OfferApplSeqNum -> 0`，否则 `1`; `ExecType == 52 -> 4` |
| `Price` | `LastPx` |
| `Volume` | `LastQty` |
| `Money` | `LastPx * LastQty` |
| `Channel` | `ChannelNo` |
| `SeqNum` | `ApplSeqNum` |

#### tick 映射

SH `4.4 SHL2MarketData` 和 SZ `6.28 Snapshot300111_v2` 映射到 `TICK_COLUMNS`，字段名以 SDK 生成类为准，保持与现有 CSV converter 的语义一致：

- `SecurityID -> Code`
- 行情时间 `UpdateTime -> Time`
- 最新价、昨收、开高低、成交量/金额、成交笔数、十档买卖盘、加权均价等映射到现有 `TICK_COLUMNS`
- `Channel` 无原始通道时填 `0`
- `SeqNum` 优先使用 SDK/消息内序号字段；若无稳定序号，则使用 `hd.SequenceID`

### 5.4 sdk_collector.py

主流程：

```text
1. 创建 ShmStore
2. 创建 pymdl IOManager
3. 创建 Subscriber
4. 订阅全市场消息
5. 启动后台 flush loop
6. signal pause / 主循环保活
7. 退出时 Shutdown
```

flush loop：

```text
每 MDL_FLUSH_INTERVAL_MS 或累计 MDL_BATCH_SIZE：
  从 raw queue 取消息
  按 tick/order/deal 映射成 rows
  每类 rows 构造一个 DataFrame
  ShmStore.update_tick/order/deal(df)
  记录写入计数、耗时、队列积压
```

## 6. 内存控制方案

当前 `ShmStore` 是 rolling chunk 模式：

```text
/dev/shm/quant-store/tick/chunk_*.arrow
/dev/shm/quant-store/order/chunk_*.arrow
/dev/shm/quant-store/deal/chunk_*.arrow
```

`live-engine` 消费 chunk 后会删除文件。这一点保留。

### 6.1 collector 侧内存

collector 侧只保留短队列和 micro-batch：

```text
SDK callback queue: 原始消息对象短暂排队
batch rows: 5~10ms 的 rows
DataFrame: 写入 ShmStore 后立即释放
```

建议初始参数：

```text
MDL_FLUSH_INTERVAL_MS=10
MDL_BATCH_SIZE=2000
MDL_IO_THREADS=4
```

若高峰期队列积压：

1. 先把 `MDL_FLUSH_INTERVAL_MS` 降到 `5`。
2. 把 `MDL_BATCH_SIZE` 调到 `5000`。
3. 增加 collector CPU。
4. 检查 mapper 是否有 Decimal/对象转换瓶颈。

### 6.2 ShmStore 侧内存

保留滚动窗口，但要避免 live-engine 慢时旧 chunk 被清理造成实时数据缺口。

建议：

```text
ROLLING_WINDOW_SECONDS=1800
```

同时增加监控：

```text
shm_chunk_count{data_type}
shm_oldest_chunk_age_ms{data_type}
shm_total_size_mb{data_type}
```

报警：

```text
shm_oldest_chunk_age_ms > 60_000
shm_total_size_mb > 机器可用 /dev/shm 的 70%
```

如果 live-engine 正常，chunk 会被及时删除，窗口只是兜底。

### 6.3 live-engine 侧内存

live-engine 当前维护的是 per-stock `StockState`，不是保留全量 tick/order/deal DataFrame。这个方向是正确的。

要求：

- live-engine 消费 chunk 后立即 `unlink`。
- 只更新 `StockState` 聚合状态。
- 因子计算只读 `StockState` 或轻量状态，不回扫大 DataFrame。

后续如果部分因子确实需要窗口级 tick/order/deal 明细，需要单独设计按股票/按分钟的有界 ring buffer，不能在全市场层面无限保留。

## 7. 数据不丢与校验方案

### 7.1 实时 SequenceID 校验

手册说明 `SequenceID` 是通联生成消息序列号，同一类型完整且连续。

按 `(ServiceID, MessageID)` 维护：

```python
last_seq[(4, 4)]
last_seq[(4, 24)]
last_seq[(6, 28)]
last_seq[(6, 33)]
last_seq[(6, 36)]
```

收到消息时：

```text
expected = last_seq + 1
if current_seq != expected:
  记录 gap
```

指标：

```text
sdk_received_total{sid,mid}
sdk_seq_gap_total{sid,mid}
sdk_seq_gap_size_total{sid,mid}
sdk_last_seq{sid,mid}
```

验收标准：

```text
盘中 sdk_seq_gap_total 必须为 0
出现 gap 立即报警并保留上下文日志
```

### 7.2 接收-映射-写入闭环

每个消息类型记录三段计数：

```text
sdk_received_total{sid,mid}
sdk_mapped_rows_total{data_type}
sdk_shm_written_rows_total{data_type}
```

关系：

```text
4.4  received -> tick rows
4.24 received -> order rows + deal rows
6.28 received -> tick rows
6.33 received -> order rows
6.36 received -> deal rows
```

其中 `4.24` 一条消息可能对应 order 或 deal，不能简单要求一条等于一行同类型数据，需要按 mapper 分流统计。

### 7.3 队列与延迟校验

指标：

```text
sdk_queue_size
sdk_oldest_queue_age_ms
sdk_callback_cost_ms
sdk_flush_cost_ms
sdk_mapper_cost_ms
sdk_shm_write_cost_ms
```

健康标准：

```text
sdk_queue_size 不持续增长
sdk_oldest_queue_age_ms 大部分时间 < 1000ms
sdk_flush_cost_ms 大部分时间 < MDL_FLUSH_INTERVAL_MS * 2
```

如果队列增长，说明 collector 吞吐低于 SDK 输入速度，需要调优 mapper、batch 或 CPU。

### 7.4 SDK vs CSV 旁路分钟级对账

CSV 仍由通联客户端写入，但不参与实时链路。

当前代码先实现“轻量分钟计数校验”，不做 SDK 行情双写落盘，避免影响实时处理：

```text
sdk_collector:
  stage=sdk_minute_write
  service_id, message_id, data_type, market_minute, rows

live-engine:
  stage=stream_minute_consume
  data_type, market_minute, rows
```

实时链路判断：

```text
sdk_minute_write.rows == stream_minute_consume.rows
```

如果 `sdk_minute_write` 大于 `stream_minute_consume`，说明数据已从 SDK 写入 ShmStore，但 live-engine 消费跟不上或中间 chunk 被清理。

如果 `sdk_minute_write` 自身低于 CSV 行数，说明 SDK 接收/映射/过滤链路存在差异，需要先看 `sdk-seq-gap` 和 mapper 过滤规则。

分钟级对账：

| SDK | CSV |
|---|---|
| `4.4` | `mdl_4_4_0.csv` |
| `4.24` | `mdl_4_24_0.csv` |
| `6.28` | `mdl_6_28_0.csv` |
| `6.33` | `mdl_6_33_0.csv` |
| `6.36` | `mdl_6_36_0.csv` |

输出：

```text
minute, source, sid, mid, data_type, row_count, unique_code_count, min_seq, max_seq
```

规则：

- 最近 1~2 分钟允许 CSV 落盘延迟差异。
- T-3 分钟以前 SDK 与 CSV 数量应接近一致。
- 盘后全日必须一致，或给出明确过滤差异。

查日志示例：

```bash
# SDK 写入 ShmStore 的分钟计数
grep '"stage": "sdk_minute_write"' /data/quant/pipeline_logs/pipeline_YYYYMMDD.log

# live-engine 已消费进 StockState 的分钟计数
grep '"stage": "stream_minute_consume"' /data/quant/pipeline_logs/pipeline_YYYYMMDD.log

# SDK SequenceID 跳号
grep 'sdk-seq-gap' /data/quant/*/*.log

# 判断 collector 是否跟不上
grep '"stage": "sdk_shm_write"' /data/quant/pipeline_logs/pipeline_YYYYMMDD.log

# 判断 live-engine 是否消费慢
grep '"stage": "chunk_read"' /data/quant/pipeline_logs/pipeline_YYYYMMDD.log

# 判断因子计算耗时
grep '"stage": "factor_compute"' /data/quant/pipeline_logs/pipeline_YYYYMMDD.log
```

如果主日志目录不可写，检查兜底目录或容器日志：

```bash
ls /tmp/quant_pipeline_logs/
kubectl logs -n quant deploy/collector
kubectl logs -n quant deploy/live-engine
```

丢失或不一致时的处理顺序：

1. `sdk-seq-gap` 非 0：先查通联连接/本机 9012/SDK 回调是否阻塞。
2. `sdk_shm_write.oldest_queue_age_ms` 持续上升：collector 跟不上，增大 CPU、增大 `MDL_BATCH_SIZE`、减少 callback 内工作。
3. `chunk_read.chunk_age_ms` 持续上升：live-engine 消费跟不上，增大 live-engine CPU，降低因子计算频率或把消费线程和计算线程拆开。
4. `stream_minute_consume.rows < sdk_minute_write.rows`：检查 `/dev/shm/quant-store` 是否被 `ROLLING_WINDOW_SECONDS` 清理，必要时临时增大窗口。
5. SDK 与 CSV 行数不一致但无 gap：检查字段映射和股票过滤规则，例如当前只保留沪市 `6/9`、深市 `0/3`。

### 7.5 盘后 key 级对账

盘后把 SDK 输出 audit/parquet 与 CSV 转换结果做 key 级对账。

建议 key：

```text
order: Code + Channel + SeqNum
deal:  Code + Channel + SeqNum
tick:  Code + Channel + SeqNum；若 tick SeqNum 不稳定，则 Code + Time
```

检查：

```text
missing_in_sdk
extra_in_sdk
price_diff
volume_diff
time_diff
```

## 8. 审计落盘

为了盘后校验，SDK collector 建议轻量落一份 audit，不作为实时路径阻塞条件。

两种选择：

1. **只落分钟统计**
   - 最省内存和磁盘。
   - 可证明总体数量和延迟，但不能做字段级对账。

2. **落标准化 parquet 分片**
   - 可做完整 key 级对账。
   - 需要控制异步落盘队列。

建议初期采用：

```text
实时主链路：SDK -> ShmStore
旁路审计：SDK -> async parquet chunk
CSV：通联客户端原始文件
```

注意：审计落盘失败不应影响实时 ShmStore 写入，但必须报警。

## 9. 镜像与 K8S 改造

为避免影响现有回测 worker、CSV collector 和 live-engine 镜像，新增专用 SDK collector 镜像：

```text
docker/sdk-collector.Dockerfile
```

该镜像基于现有 `backtest-base:latest`，只额外安装 `pymdl`，并在构建阶段执行 `import pymdl` 校验。

构建前需要把 SDK 离线包放到 Docker build context 根目录：

```bash
cp "/Users/zhangyang/Downloads/MDL Python SDK客户端/pymdl-2.13.232-py3.tar.gz" .
```

示例构建命令：

```bash
docker build \
  -f docker/sdk-collector.Dockerfile \
  -t 172.24.99.176:5000/quant-platform/sdk-collector:latest \
  .
```

构建成功后推送到内部仓库：

```bash
docker push 172.24.99.176:5000/quant-platform/sdk-collector:latest
```

直接改造现有 `collector` Deployment，保持名称和标签不变，避免新旧 collector 双写同一个 ShmStore。

关键点：

```yaml
hostNetwork: true
dnsPolicy: Default
nodeSelector:
  role: live-trading
```

容器命令：

```yaml
command: ["python", "-m", "quant_platform.collector.sdk_collector"]
```

镜像：

```yaml
image: 172.24.99.176:5000/quant-platform/sdk-collector:latest
```

环境变量：

```yaml
- name: MDL_SERVER
  value: "127.0.0.1:9012"
- name: MDL_IO_THREADS
  value: "4"
- name: MDL_ENCODING
  value: "1"
- name: MDL_ENABLE_MERGE
  value: "false"
- name: MDL_SUBS
  value: "4.4,4.24,6.28,6.33,6.36"
- name: SHM_STORE_PATH
  value: "/dev/shm/quant-store"
- name: ROLLING_WINDOW_SECONDS
  value: "1800"
```

资源建议：

```yaml
resources:
  requests:
    cpu: "4"
    memory: 8Gi
  limits:
    cpu: "8"
    memory: 16Gi
```

根据实盘机器资源可下调，但全市场逐笔建议先保证 CPU。

## 10. 上线步骤

1. 在实盘镜像中安装 `pymdl-2.13.232-py3.tar.gz`。
2. 在 live-trading 节点上验证 `python -c "import pymdl"` 成功。
3. 运行最小 demo，连接 `127.0.0.1:9012`，订阅 `4.24/6.33/6.36`，确认有消息。
4. 实现 `sdk_collector`，第一版只统计每秒消息数和 SequenceID gap。
5. 接入 mapper，写 `ShmStore`。
6. 停用 CSV collector 主链路，启用 SDK collector。
7. live-engine 继续读取 `/dev/shm/quant-store`，无需改策略 API。
8. 当天盘中观察：
   - `sdk_seq_gap_total`
   - `sdk_queue_size`
   - `sdk_oldest_queue_age_ms`
   - `shm_oldest_chunk_age_ms`
   - live-engine factor output 行数
9. 盘后运行 `verify_sdk_vs_csv.py` 与 CSV 对账。

## 11. 回滚方案

保留旧 CSV collector Deployment YAML。

如果 SDK collector 出现严重问题：

1. 停止 `sdk-collector`。
2. 启动旧 `collector`。
3. live-engine 仍使用同一个 `SHM_STORE_PATH=/dev/shm/quant-store`。
4. 通过 CSV 旁路补齐当日历史数据。

注意：回滚只用于应急。CSV collector 不能作为长期主实时链路。

## 12. 待确认细节

实现前还需要确认：

1. `pymdl` 在实盘镜像的 Python 版本和 Linux x86_64 环境中是否可正常 import。
2. 本机通联客户端 `9012` 是否支持全市场 `4.4/4.24/6.28/6.33/6.36` 手动订阅。
3. tick 的 `SeqNum` 在 SDK 和 CSV 中是否完全对应；若不对应，tick 盘后 key 对账使用 `Code + Time`。
4. 是否需要 SDK collector 同步落标准化 parquet，作为未来替代 CSV 的历史数据源。
