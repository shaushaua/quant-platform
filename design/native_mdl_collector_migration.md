# Native MDL Collector 改造方案

## 目标

将 MDL 高频行情热链路从 Python callback 迁移到 C++ native collector。Python 不再逐条接收 MDL 消息，只负责分钟级因子计算和后续推理。

最终链路：

```text
MDL C++ SDK
  -> C++ callback
  -> C++ parse tick/order/deal
  -> C++ write mmap
  -> Python read mmap
  -> factor + inference
```

部署形态：

```text
single Pod / single container / two processes

entrypoint
  ├─ native-mdl-collector
  └─ python native engine
```

## 目录规划

```text
native_mdl_collector/
  CMakeLists.txt
  include/
    shm_writer.h
    mdl_handler.h
    parsers.h
    schema.h
  src/
    main.cpp
    shm_writer.cpp
    mdl_handler.cpp
    parser_sh.cpp
    parser_sz.cpp
    metrics.cpp

quant_platform/data/
  native_shm_reader.py

quant_platform/live_engine/
  native_engine.py

scripts/
  compare_native_vs_pymdl.py
  replay_native_shm.py

design/
  native_mdl_collector_migration.md
```

当前已创建：

```text
native_mdl_collector/include/shm_writer.h
design/native_mdl_collector_migration.md
```

## 共享内存协议

第一版使用与现有 Rust `ShmStockBuffer` 接近的 row-major `float64` 格式，降低 Python reader 接入风险。后续稳定后再升级为 typed binary row，减少内存占用。

文件路径：

```text
/data/quant/shm/quant_tick_600000_XSHG.mmap
/data/quant/shm/quant_order_600000_XSHG.mmap
/data/quant/shm/quant_deal_600000_XSHG.mmap
```

Header：

```text
magic        uint64
version      uint64
kind         uint64  # 1=tick, 2=order, 3=deal
capacity     uint64
n_cols       uint64
row_count    uint64
write_pos    uint64
generation   uint64
```

Rows：

```text
float64[capacity][n_cols]
```

读写规则：

```text
writer:
  generation += 1  # odd, writing
  write row
  row_count/write_pos update
  generation += 1  # even, committed

reader:
  read generation
  if odd: retry
  read header + rows
  read generation again
  if changed: retry
```

## 字段 Schema

第一版列顺序沿用现有 Python 常量：

```text
quant_platform/core/constants.py
  TICK_COLUMNS
  ORDER_COLUMNS
  DEAL_COLUMNS
```

C++ 写入时跳过 `TradingDay` 和 `Code`，只写数值列，与现有 Rust shm buffer 一致：

```text
tick:  79 columns
order: 9 columns
deal:  10 columns
```

Python reader 负责根据文件名补回 `TradingDay` 和 `Code`。

## Native Collector

职责：

```text
1. 创建 MDL IOManager
2. 创建 Subscriber
3. 订阅 4.4, 4.24, 6.28, 6.33, 6.36
4. 在 C++ callback 内解析官方 SDK struct
5. 写入 mmap
6. 记录 seq gap、msg/s、latency、reconnect
```

SDK 回调：

```text
OnMDLSHL2Message
  4.4   SHL2MarketData      -> tick
  4.24  NGTSTick            -> order/deal

OnMDLSZL2Message
  6.28  Snapshot300111_v2   -> tick
  6.33  Order300192_v2      -> order
  6.36  Transaction300191_v2 -> deal
```

## Python Native Engine

职责：

```text
1. 不 import pymdl
2. 扫描 /data/quant/shm
3. 读取 mmap header/rows
4. 构造 StockData
5. 调用现有 factor_calculation
6. 后续接推理
```

保留现有 `combined_engine.py` 作为旧链路和对照工具。新增 `native_engine.py` 作为新生产链路。

## 容器集成

Dockerfile 增加 C++ build stage：

```text
cpp-builder:
  install build tools
  copy MDL C++ SDK
  cmake build native-mdl-collector

final:
  copy native-mdl-collector
  copy libmdl_api.so
  copy Python code
```

entrypoint：

```bash
native-mdl-collector &
collector_pid=$!

python -m quant_platform.live_engine.native_engine &
python_pid=$!

wait -n "$collector_pid" "$python_pid"
exit 1
```

任一进程退出，容器退出，由 Kubernetes 重启。

## K8s 配置

新增环境变量：

```text
ENGINE_MODE=native
NATIVE_MDL_SUBS=4.4,4.24,6.28,6.33,6.36
NATIVE_SHM_DIR=/data/quant/shm
NATIVE_ROWS_PER_STOCK=200000
NATIVE_MDL_IO_THREADS=8
NATIVE_MDL_CALLBACK_THREADS=true
```

资源仍使用 48C/96G 节点，prod limit 可保持：

```text
cpu: 46
memory: 88Gi
```

## 验证计划

字段校验：

```text
1. C++ SDK header layout 校验
2. Python SDK layout 校验
3. native mmap row 与旧 Rust parser 输出对比
```

运行时校验：

```text
1. msg/s
2. seq gap
3. mmap row_count
4. per-kind latency
5. factor output diff
```

对比脚本：

```text
scripts/compare_native_vs_pymdl.py
scripts/replay_native_shm.py
```

## 实施顺序

```text
1. 完成 shm_writer.cpp 和 Python native_shm_reader.py
2. 完成 C++ SDK subscriber 骨架
3. 接入 SH tick
4. 接入 SZ tick
5. 接入 SH NGTS order/deal
6. 接入 SZ order/deal
7. 完成 native_engine.py
8. Dockerfile/entrypoint/k8s 集成
9. 跑字段一致性
10. 跑因子一致性
11. 切 ENGINE_MODE=native
```

## 风险点

```text
1. C++ SDK struct 字段和 Python 常量列顺序必须严格一致。
2. mmap 写入必须避免 Python 读到半行。
3. ring buffer 覆盖后 Python reader 要处理 row_count/write_pos。
4. 多线程 callback 写同一股票 mmap 需要锁或分片锁。
5. C++ collector 和 Python engine 在同一容器内共享内存总量。
```

## 预期收益

```text
1. 去掉 Python per-message callback。
2. 去掉 Python tuple/dict/set/lock 热路径。
3. feeder 输出缓冲不再因为 Python 消费慢持续堆积。
4. order/deal 峰值吞吐更稳定。
5. Python 推理可以独立扩展，不影响行情接收。
```
