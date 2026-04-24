# 因子计算框架 - 使用手册

> 面向交易员：使用因子计算框架开发、测试、运行策略。

---

## 一、整体架构

```
策略文件 (你写的)
    │
    ├── factor_info        → 告诉引擎需要什么数据
    ├── securities         → 告诉引擎处理哪些股票
    ├── end_times          → 告诉引擎按什么时间切分
    ├── factor_calculation → 核心计算逻辑（每只股票调用一次）
    └── outfun             → 结果处理（可选，如写文件）
          │
          ▼
    calc_factors_by_date_range()  ← 引擎入口
          │
          ├── 遍历交易日 → 遍历时间切片 → 遍历股票
          ├── 自动从 OSS 加载数据（deal/tick/order/daily_basic）
          ├── 自动还原精度（Code、价格、成交量、时间）
          └── 调用你的 factor_calculation 函数
```

---

## 二、策略文件结构

一个完整的策略文件长这样：

```python
# === 1. 数据需求声明（必须） ===
factor_info = {
    "market_count": 1,       # 需要几日 daily_basic（1=仅当天）
    "need_l1_tick": True,    # 需要 L1 tick 快照
    "need_l2_deal": True,    # 需要 L2 逐笔成交
    "need_l2_order": False,  # 不需要 L2 逐笔委托
}

# === 2. 股票列表（可选，空列表=全市场） ===
securities = ["000001.SZ", "000002.SZ", "600000.SH"]
# securities = []  # 全市场模式

# === 3. 时间切片（可选，空字符串=全天一次） ===
end_times = [""]
# end_times = ["093000", "100000", "150000"]  # 多个时间点

# === 4. 因子计算函数（必须） ===
def factor_calculation(data, code, date, end_time):
    """
    每只股票、每个时间切片调用一次。

    Args:
        data: StockData 对象，包含：
              - data.l1_tick    → L1 tick 快照 (DataFrame)
              - data.l2_deal    → L2 逐笔成交 (DataFrame)
              - data.l2_order   → L2 逐笔委托 (DataFrame)
              - data.market     → 日频基础数据 (DataFrame)
              - data.daily_basic→ 同 market
        code:     股票代码 "000001.SZ"
        date:     交易日 "20250106"
        end_time: 时间切片 "" 或 "093000"

    Returns:
        dict: 计算结果，必须包含 "code" 和 "date"
        None: 跳过该股票
    """
    deal = data.l2_deal
    tick = data.l1_tick

    result = {"code": code, "date": date}

    if deal.empty:
        return result

    result["deal_count"] = len(deal)
    result["total_volume"] = float(deal["Volume"].sum())
    result["avg_price"] = float(deal["Price"].mean())

    return result

# === 5. 结果处理函数（可选） ===
def outfun(date, end_time, test_df):
    """
    每个日期×时间切片的批次完成后调用。

    Args:
        date:     "20250106"
        end_time: "" 或 "093000"
        test_df:  该批次所有股票结果的 DataFrame
    """
    output_path = f"result_{date}_{end_time}.csv"
    test_df.to_csv(output_path, index=False)
    print(f"已保存 {output_path}: {len(test_df)} 行")
```

---

## 三、StockData 数据对象

引擎传给 `factor_calculation` 的 `data` 对象包含以下属性：

| 属性 | 类型 | 说明 | 数据量 |
|------|------|------|--------|
| `data.l1_tick` | DataFrame | L1 tick 快照（3秒一条） | ~5000行/天/股 |
| `data.l2_deal` | DataFrame | L2 逐笔成交 | ~10000-100000行/天/股 |
| `data.l2_order` | DataFrame | L2 逐笔委托 | 更大 |
| `data.market` | DataFrame | 日频基础数据（open/close/volume等） | 1行 |
| `data.daily_basic` | DataFrame | 同 market | 1行 |

### 数据已经自动还原

引擎会自动处理 OSS 历史数据的压缩格式，你拿到的数据已经是原始精度：

| 字段 | OSS 存储 | 还原后 |
|------|---------|--------|
| Code | int32 (SECURITY_ID) | 字符串 "000001.SZ" |
| Price / AskPrice1-10 / BidPrice1-10 | int32 (×100) | float64 原始价格 |
| Volume / AskVolume1-10 / BidVolume1-10 | int32 (÷100) | float64 原始成交量 |
| Time | int64 (UnixMicro) | datetime64[ns] |
| UpdateTime | int32 (微秒偏移) | datetime64[ns] |

### deal 常用列

```
Code, Time, UpdateTime, Price, Volume, Side, SaleOrderID, BuyOrderID, SeqNum
```

### tick 常用列

```
Code, Time, UpdateTime, CurrentPrice, PreClosePrice, OpenPrice,
HighestPrice, LowestPrice, HighLimitPrice, LowLimitPrice,
AskPrice1-10, BidPrice1-10, AskVolume1-10, BidVolume1-10,
TotalVolume, TotalBidVolume, TotalAskVolume, IOPV,
AvgBidPrice, AvgAskPrice, TradeNum
```

### 注意事项

```python
# ❌ 错误写法
tick_data = data.tick       # 属性名不对
deal_data = data.deal       # 属性名不对

# ✅ 正确写法
tick_data = data.l1_tick    # L1 tick
deal_data = data.l2_deal    # L2 成交
order_data = data.l2_order  # L2 委托
```

---

## 四、执行方式

### 方式一：本地测试（推荐先用这个）

```bash
# 设置环境变量（首次）
export OSS_ACCESS_KEY_ID='your_key'
export OSS_ACCESS_KEY_SECRET='your_secret'

# 单只股票、单天
python local_test.py my_strategy.py --date 20250106 --codes "000001.SZ" -v

# 多只股票
python local_test.py my_strategy.py --date 20250106 --codes "000001.SZ,000002.SZ,600000.SH" -v

# 多天
python local_test.py my_strategy.py --date 20250106-20250108 --codes "000001.SZ" -v

# 指定输出格式（默认 csv）
python local_test.py my_strategy.py --date 20250106 --codes "000001.SZ" --format parquet -v
python local_test.py my_strategy.py --date 20250106 --codes "000001.SZ" --format json -v
```

输出文件：
- `--format csv`（默认）→ `./local_test_result.csv`
- `--format parquet` → `./local_test_result.parquet`
- `--format json` → `./local_test_result.json`

### 方式二：Python 脚本直接调用引擎

```python
"""run_my_strategy.py"""
from quant_platform.factor.engine import calc_factors_by_date_range

factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_deal": True,
    "need_l2_order": False,
}

def factor_calculation(data, code, date, end_time):
    deal = data.l2_deal
    result = {"code": code, "date": date}
    if not deal.empty:
        result["vwap"] = float((deal["Price"] * deal["Volume"]).sum() / deal["Volume"].sum())
    return result

def outfun(date, end_time, test_df):
    test_df.to_parquet(f"result_{date}.parquet", index=False)
    print(f"{date}: {len(test_df)} 只股票")

calc_factors_by_date_range(
    factor_info=factor_info,
    start_date="20250106",
    end_date="20250106",
    end_times=[""],
    securities=["000001.SZ", "000002.SZ"],
    factor_data_handler=factor_calculation,
    outfun=outfun,
)
```

```bash
python run_my_strategy.py
```

### 方式三：Jupyter Notebook 交互式分析

```python
from quant_platform.data.api import DataAPI

api = DataAPI(mode="backtest")

# 加载单只股票的单日数据（自动还原精度）
deal = api.load_stock_data("000001.SZ", "20250106", "deal")
tick = api.load_stock_data("000001.SZ", "20250106", "tick")

print(deal[["Code", "Time", "Price", "Volume"]].head(10))
print(tick[["Code", "Time", "CurrentPrice", "AskPrice1", "BidPrice1"]].head(10))

# 自己写分析逻辑
vwap = (deal["Price"] * deal["Volume"]).sum() / deal["Volume"].sum()
print(f"VWAP: {vwap:.4f}")
```

---

## 五、完整示例：TAQ 因子策略

以下是一个实际的 TAQ（交易与报价）因子策略，计算 1 分钟和 5 分钟频率的交易/报价统计指标：

```python
"""example_taq_strategy.py"""
import numpy as np

# === 数据需求 ===
factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}
securities = ["000001.SZ"]
end_times = [""]

# === 时间处理工具 ===
def _detect_time_fmt(sample_time):
    """自动检测时间格式"""
    if sample_time > 1e12: return "unix_ms"
    if sample_time > 1e9:  return "unix_s"
    if sample_time > 10000000: return "hhmmssmmm"
    return "hhmmss"

def _normalize_time(times, fmt):
    """将各种时间格式统一转换为毫秒时间戳"""
    if fmt == "unix_ms": return times
    if fmt == "unix_s":  return times * 1000
    if fmt == "hhmmssmmm":
        h = times // 10000000
        m = (times // 100000) % 100
        s = (times // 1000) % 100
        ms = times % 1000
        return (h * 3600 + m * 60 + s) * 1000 + ms
    if fmt == "hhmmss":
        h = times // 10000
        m = (times // 100) % 100
        s = times % 100
        return (h * 3600 + m * 60 + s) * 1000
    return times

def _get_time_bins(date_str, freq_min, fmt):
    """生成 A 股交易时段的时间区间"""
    import pandas as pd
    date_str = str(date_str).replace("-", "")
    m_start = pd.Timestamp(f"{date_str} 09:30:00")
    m_end   = pd.Timestamp(f"{date_str} 11:30:00")
    a_start = pd.Timestamp(f"{date_str} 13:00:00")
    a_end   = pd.Timestamp(f"{date_str} 15:00:00")
    bins = []
    for p_start, p_end in [(m_start, m_end), (a_start, a_end)]:
        dt_range = pd.date_range(start=p_start, end=p_end, freq=f"{freq_min}min")
        for i in range(len(dt_range) - 1):
            dt_curr = dt_range[i]
            dt_nxt  = dt_range[i+1]
            label = f"_{freq_min}m_{dt_nxt.strftime('%H%M')}"
            if fmt == "hhmmssmmm":
                start_val = int(dt_curr.strftime('%H%M%S')) * 1000
                end_val   = int(dt_nxt.strftime('%H%M%S')) * 1000
            elif fmt == "unix_ms":
                start_val = int(dt_curr.tz_localize("Asia/Shanghai").timestamp() * 1000)
                end_val   = int(dt_nxt.tz_localize("Asia/Shanghai").timestamp() * 1000)
            else:
                start_val = int(dt_curr.strftime('%H%M%S'))
                end_val   = int(dt_nxt.strftime('%H%M%S'))
            bins.append((start_val, end_val, label))
    return bins

# === 核心 TAQ 计算 ===
def _calc_taq_metrics(df_deal, df_tick, suffix, prev_state, norm_start, norm_end, norm_times):
    """计算单时间窗口内的 TAQ 指标"""
    res = {}

    # --- 交易指标 ---
    if not df_deal.empty and "Price" in df_deal.columns and "Volume" in df_deal.columns:
        res[f"trd_cnt{suffix}"] = len(df_deal)
        trd_qty = float(df_deal["Volume"].sum())
        res[f"trd_qty{suffix}"] = trd_qty
        if trd_qty > 0:
            res[f"trd_vwap{suffix}"] = float((df_deal["Price"] * df_deal["Volume"]).sum() / trd_qty)
        else:
            res[f"trd_vwap{suffix}"] = float("nan")
        res[f"trd_high{suffix}"] = float(df_deal["Price"].max())
        res[f"trd_low{suffix}"]  = float(df_deal["Price"].min())
        res[f"trd_last{suffix}"] = float(df_deal.iloc[-1]["Price"])
    else:
        res[f"trd_cnt{suffix}"]  = 0
        res[f"trd_qty{suffix}"]  = 0.0
        res[f"trd_vwap{suffix}"] = res[f"trd_high{suffix}"] = res[f"trd_low{suffix}"] = res[f"trd_last{suffix}"] = float("nan")

    # --- 报价指标（TWAP/TWAD） ---
    if not df_tick.empty and len(norm_times) > 0 and "AskPrice1" in df_tick.columns:
        sort_idx = np.argsort(norm_times)
        times = norm_times[sort_idx]
        ask_p = df_tick["AskPrice1"].values[sort_idx]
        ask_v = df_tick["AskVolume1"].values[sort_idx]
        bid_p = df_tick["BidPrice1"].values[sort_idx]
        bid_v = df_tick["BidVolume1"].values[sort_idx]
        dt = np.diff(times)
        last_dt = max(norm_end - times[-1], 0)
        dt = np.append(dt, last_dt)
        dt = np.maximum(dt, 0)
        total_time = dt.sum()
        if total_time > 0:
            res[f"ask_twap{suffix}"] = float((ask_p * dt).sum() / total_time)
            res[f"ask_twad{suffix}"] = float((ask_v * dt).sum() / total_time)
            res[f"bid_twap{suffix}"] = float((bid_p * dt).sum() / total_time)
            res[f"bid_twad{suffix}"] = float((bid_v * dt).sum() / total_time)
        else:
            res[f"ask_twap{suffix}"] = float(ask_p.mean())
            res[f"ask_twad{suffix}"] = float(ask_v.mean())
            res[f"bid_twap{suffix}"] = float(bid_p.mean())
            res[f"bid_twad{suffix}"] = float(bid_v.mean())
        res[f"ask_last{suffix}"] = float(ask_p[-1])
        res[f"bid_last{suffix}"] = float(bid_p[-1])
        # 中间价
        valid_mid = (ask_p > 0) & (bid_p > 0)
        mid_prices = np.where(valid_mid, (ask_p + bid_p) / 2, np.nan)
        valid_mids_only = mid_prices[~np.isnan(mid_prices)]
        if len(valid_mids_only) > 0:
            res[f"mid_high{suffix}"] = float(valid_mids_only.max())
            res[f"mid_low{suffix}"]  = float(valid_mids_only.min())
        else:
            res[f"mid_high{suffix}"] = res[f"mid_low{suffix}"] = float("nan")
        # 更新 prev_state
        for k in ["ask_last", "bid_last", "ask_twap", "bid_twap", "ask_twad", "bid_twad"]:
            prev_state[k] = res[f"{k}{suffix}"]
    else:
        # 无 tick 数据时沿用上一期
        for k in ["ask_last", "bid_last", "ask_twap", "bid_twap", "ask_twad", "bid_twad"]:
            res[f"{k}{suffix}"] = prev_state[k]
        res[f"mid_high{suffix}"] = float("nan")
        res[f"mid_low{suffix}"]  = float("nan")

    return res

# === 因子计算入口 ===
def factor_calculation(data, code, date, end_time):
    import pandas as pd
    result = {"code": code, "date": date}
    df_deal = data.l2_deal
    df_tick = data.l1_tick

    # 检测时间格式
    sample_time = 0
    if not df_deal.empty:
        sample_time = int(df_deal["Time"].iloc[0])
    elif not df_tick.empty:
        sample_time = int(df_tick["Time"].iloc[0])
    else:
        return result
    fmt = _detect_time_fmt(sample_time)

    deal_times = df_deal["Time"].values if not df_deal.empty else np.array([])
    tick_times = df_tick["Time"].values if not df_tick.empty else np.array([])
    norm_tick_times = _normalize_time(tick_times, fmt) if len(tick_times) > 0 else np.array([])

    # 计算 1 分钟和 5 分钟因子
    for freq_min in [1, 5]:
        bins = _get_time_bins(date, freq_min, fmt)
        prev_state = {k: float("nan") for k in
                      ["ask_last", "bid_last", "ask_twap", "bid_twap", "ask_twad", "bid_twad"]}
        for start_val, end_val, label in bins:
            sub_deal = df_deal[(deal_times >= start_val) & (deal_times < end_val)] if len(deal_times) > 0 else df_deal
            if len(tick_times) > 0:
                mask_tick = (tick_times >= start_val) & (tick_times < end_val)
                sub_tick = df_tick[mask_tick]
                sub_norm_times = norm_tick_times[mask_tick]
            else:
                sub_tick = df_tick
                sub_norm_times = np.array([])
            norm_start = _normalize_time(np.array([start_val]), fmt)[0]
            norm_end   = _normalize_time(np.array([end_val]), fmt)[0]
            res = _calc_taq_metrics(sub_deal, sub_tick, label, prev_state, norm_start, norm_end, sub_norm_times)
            result.update(res)

    return result

# === 结果输出 ===
def outfun(date, end_time, test_df):
    fname = f"taq_result_{date}.csv"
    test_df.to_csv(fname, index=False)
    print(f"已保存 {fname}: {len(test_df)} 行 × {len(test_df.columns)} 列")
```

### 运行这个示例

```bash
# 本地测试
python local_test.py example_taq_strategy.py --date 20250106 --codes "000001.SZ" -v

# 或直接调用引擎
python example_taq_strategy.py
```

---

## 六、输出指标说明（以 TAQ 策略为例）

| 指标 | 含义 | 示例 |
|------|------|------|
| `trd_cnt_1m_0931` | 09:31 的成交笔数 | 234 |
| `trd_qty_1m_0931` | 09:31 的总成交量（股） | 523400 |
| `trd_vwap_1m_0931` | 09:31 的成交量加权均价 | 11.34 |
| `trd_high/low/last` | 最高/最低/最后一笔价格 | 11.36/11.30/11.32 |
| `ask_twap_1m_0931` | 09:31 的卖一价时间加权均价 | 11.33 |
| `ask_twad_1m_0931` | 09:31 的卖一量时间加权均值 | 45200 |
| `bid_twap/bid_twad` | 买一价/买一量 TWAP/TWAD | - |
| `mid_high/mid_low` | 中间价最高/最低 | 11.35/11.31 |
| `ask_last/bid_last` | 最后一笔卖一/买一价 | 11.33/11.31 |

1 分钟指标共 16 个 × 240 个时间窗口 = 3840 个
5 分钟指标共 16 个 × 48 个时间窗口 = 768 个

---

## 七、常用命令速查

```bash
# ========== 环境配置（首次） ==========
pip install -r requirements.txt
export OSS_ACCESS_KEY_ID='your_key'
export OSS_ACCESS_KEY_SECRET='your_secret'

# ========== 本地测试 ==========
# 单只股票
python local_test.py strategy.py --date 20250106 --codes "000001.SZ" -v

# 多只股票
python local_test.py strategy.py --date 20250106 --codes "000001.SZ,000002.SZ" -v

# 多天
python local_test.py strategy.py --date 20250106-20250110 --codes "000001.SZ" -v

# 输出 parquet 格式
python local_test.py strategy.py --date 20250106 --codes "000001.SZ" --format parquet -v

# ========== Jupyter 交互 ==========
from quant_platform.data.api import DataAPI
api = DataAPI(mode="backtest")
deal = api.load_stock_data("000001.SZ", "20250106", "deal")

# ========== 缓存管理 ==========
du -sh ~/.oss_cache/           # 查看缓存大小
ls -lh ~/.oss_cache/           # 查看缓存文件
rm -rf ~/.oss_cache/           # 清空缓存
```

---

## 八、推荐工作流

```
1. 写策略 → 2. 本地单股测试 → 3. 本地多股测试 → 4. 集群小规模 → 5. 集群全量
```

- 步骤 2-3 可以快速发现问题，不浪费集群资源
- 本地首次运行需下载 OSS 数据（单天约 4GB），后续有缓存秒级完成
- 缓存目录: `~/.oss_cache/`

---

## 九、常见问题

### Q: 结果全是 NaN？
检查属性名：用 `data.l1_tick` 和 `data.l2_deal`，不是 `data.tick` 和 `data.deal`。

### Q: tick=0 或 deal=0？
- 该股票当天可能停牌
- 检查 `factor_info` 中是否开启了对应数据类型

### Q: 时间格式是什么？
引擎已自动还原为 datetime64[ns]。如果你的策略需要整数时间格式（如 hhmmssmmm），需要自行转换。

### Q: 全市场模式怎么用？
策略文件中 `securities = []` 即可，引擎自动从 daily_basic 获取全部股票列表并分批处理。

### Q: 如何指定输出路径？
```bash
python local_test.py strategy.py --date 20250106 --codes "000001.SZ" --output /tmp/my_result --format csv -v
```
