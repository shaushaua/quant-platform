# DataAPI 数据接口文档

策略函数签名：`run_strategy(data_api, ctx)`

- `data_api`：DataAPI 实例，由引擎注入，已感知当前 `(date, code)` 上下文
- `ctx`：回测上下文，由引擎注入

---

## 一、获取当前股票数据（per-code 模式，推荐）

引擎按 `(日期 × 股票)` 双层迭代，每次调用策略时自动设置上下文，直接调用即可，无需传入 date/code。

```python
df = data_api.get_current_data(data_type: str) -> pd.DataFrame
```

| `data_type` | 说明 | 常用字段 |
|---|---|---|
| `"daily_basic"` | 日频基础行情 | `close`, `turnover_rate`, `volume`, `amount` |
| `"deal"` | 逐笔成交 | `Price`, `Volume`, `Side`（1=买/0=卖/4=竞价）|
| `"order"` | 逐笔委托 | `Price`, `Volume`, `Side`（1=买/0=卖），`OrderType`（1=撤单）|
| `"tick"` | Tick 快照 | `AskPrice1`, `BidPrice1`, `AskVol1`, `BidVol1` |

> `order` 和 `tick` 默认不预加载，需在策略文件顶层声明：
> ```python
> PRELOAD_DATA_TYPES = ["daily_basic", "deal", "order", "tick"]
> ```

**示例：**

```python
def run_strategy(data_api, ctx):
    daily = data_api.get_current_data("daily_basic")
    deal  = data_api.get_current_data("deal")
    order = data_api.get_current_data("order")
    tick  = data_api.get_current_data("tick")
```

---

## 二、获取指定日期数据

需要手动指定日期和股票时使用。

```python
df = data_api.get_daily_data(
    date: str,               # 日期，YYYY-MM-DD 或 YYYYMMDD
    data_type: str,
    codes: List[str] = None  # 指定股票列表，None 表示全市场
) -> pd.DataFrame
```

> `tick`/`order`/`deal` 数据量大，建议在每日循环中用此方法逐天加载，避免 OOM。

**示例：**

```python
deal = data_api.get_daily_data("2025-01-02", "deal", codes=["000001.XSHE"])
daily = data_api.get_daily_data("2025-01-02", "daily_basic")
```

---

## 三、获取多日历史数据

```python
df = data_api.get_history(
    start_date: str,
    end_date: str,
    data_type: str,
    codes: List[str] = None
) -> pd.DataFrame
```

> **注意**：`tick`/`order`/`deal` 默认限制最多加载 **1 天**。如需加载多天，设置环境变量 `HEAVY_DATA_MAX_DAYS=<N>`（需确认内存充足），或改用 `get_daily_data()` 逐天加载。

**示例：**

```python
# daily_basic 可安全加载多天
daily = data_api.get_history("2025-01-02", "2025-01-10", "daily_basic", codes=["000001.XSHE"])
```

---

## 四、ctx 上下文属性

| 属性 | 说明 |
|---|---|
| `ctx.current_date` | 当前迭代日期，格式 `YYYY-MM-DD` |
| `ctx.current_code` | 当前迭代股票代码，如 `"000001.XSHE"` |
| `ctx.start_date` | 本次回测起始日期 |
| `ctx.end_date` | 本次回测结束日期 |
| `ctx.initial_capital` | 初始资金 |

---

## 五、策略文件顶层声明

```python
RUN_MODE = "per_code"  # 固定写法

# 按需声明，不声明默认只预加载 daily_basic
# tick/order 数据量大，不用时不声明
PRELOAD_DATA_TYPES = ["daily_basic", "deal"]
```

---

## 六、策略返回值

```python
# 返回 dict（单行信号，推荐）
return {"signal": 1.0, "close": 10.5}

# 返回 DataFrame（多行）
return pd.DataFrame([...])

# 返回 None 表示无信号，跳过
return None
```

引擎自动附加 `_date`、`_code` 列，最终通过 `result["results"]` 获取汇总 DataFrame。

---

## 七、完整策略模板

```python
# -*- coding: utf-8 -*-
RUN_MODE = "per_code"
PRELOAD_DATA_TYPES = ["daily_basic", "deal"]

def run_strategy(data_api, ctx):
    # 1. 获取日线数据
    daily = data_api.get_current_data("daily_basic")
    if daily is None or len(daily) == 0:
        return None

    # 2. 获取成交数据
    deal = data_api.get_current_data("deal")
    if deal is None or len(deal) == 0:
        return None

    # 3. 计算因子
    close = daily["close"].iloc[0]
    vwap = (deal["Price"] * deal["Volume"]).sum() / deal["Volume"].sum()
    big = deal[deal["Volume"] >= 10000]
    net_big = big[big["Side"] == 1]["Volume"].sum() - big[big["Side"] == 0]["Volume"].sum()

    return {
        "close": close,
        "vwap": vwap,
        "net_big": net_big,
        "signal": 1 if net_big > 0 else -1,
    }
```
