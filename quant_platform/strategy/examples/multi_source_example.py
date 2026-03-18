# -*- coding: utf-8 -*-
"""
示例策略：多数据源因子合成
同时使用 daily_basic / deal / order / tick 四类数据计算因子。

执行方式:
    from quant_platform.backtest.engine import BacktestEngine
    from quant_platform.strategy.examples.multi_source_example import run_strategy

    engine = BacktestEngine()
    result = engine.run_per_code(
        strategy_func=run_strategy,
        start_date="2025-01-02",
        end_date="2025-01-13",
    )
    print(result["results"])
"""

import numpy as np

# 声明使用 per-code 模式：引擎按 (日期, 股票) 双层迭代，
# get_current_data() 自动返回当前股票当日数据。
RUN_MODE = "per_code"

# 只预加载内存可控的数据类型；order(1.6亿条)/tick 按需回退到 query_code_fast
PRELOAD_DATA_TYPES = ["daily_basic", "deal"]


def run_strategy(data_api, ctx):
    """
    多数据源策略 - 处理单只股票单天的数据

    因子说明:
        close        : 收盘价（来自 daily_basic）
        turnover_rate: 换手率（来自 daily_basic）
        vwap         : 成交量加权均价（来自 deal）
        price_dev    : 收盘价偏离 VWAP 的比例（deal + daily_basic）
        net_big      : 大单净买量，Volume>=10000（来自 deal）
        order_imb    : 委买委卖失衡度 = (buy_vol - sell_vol) / (buy_vol + sell_vol)（来自 order）
        tick_spread  : 平均买一卖一价差（来自 tick）
        signal       : 综合信号，四因子加权
    """
    # ── 1. 日线基础数据 ──────────────────────────────────────────
    daily = data_api.get_current_data("daily_basic")
    if daily is None or len(daily) == 0:
        return None

    close = daily["close"].iloc[0] if "close" in daily.columns else np.nan
    turnover_rate = daily["turnover_rate"].iloc[0] if "turnover_rate" in daily.columns else np.nan

    # ── 2. 逐笔成交数据（deal）────────────────────────────────────
    deal = data_api.get_current_data("deal")
    if deal is None or len(deal) == 0:
        return None

    total_vol = deal["Volume"].sum()
    if total_vol == 0:
        return None

    vwap = (deal["Volume"] * deal["Price"]).sum() / total_vol
    price_dev = (close - vwap) / vwap if vwap != 0 else 0.0

    # 大单净买（单笔 >= 10000 股视为大单），Side: 1=买, 0=卖, 4=集合竞价
    big = deal[deal["Volume"] >= 10000]
    if len(big) > 0:
        big_buy  = big.loc[big["Side"] == 1, "Volume"].sum()
        big_sell = big.loc[big["Side"] == 0, "Volume"].sum()
        net_big  = (big_buy - big_sell) / total_vol  # 归一化
    else:
        net_big = 0.0

    # ── 3. 逐笔委托数据（order）──────────────────────────────────
    # Side: 1=买, 0=卖；OrderType: 1=撤单（排除）
    order = data_api.get_current_data("order")
    if order is not None and len(order) > 0 and "Side" in order.columns:
        valid_order = order[order["OrderType"] != 1] if "OrderType" in order.columns else order
        buy_vol  = valid_order.loc[valid_order["Side"] == 1, "Volume"].sum()
        sell_vol = valid_order.loc[valid_order["Side"] == 0, "Volume"].sum()
        denom = buy_vol + sell_vol
        order_imb = (buy_vol - sell_vol) / denom if denom > 0 else 0.0
    else:
        order_imb = 0.0

    # ── 4. Tick 数据（买一卖一价差）──────────────────────────────
    tick = data_api.get_current_data("tick")
    if tick is not None and len(tick) > 0 and {"AskPrice1", "BidPrice1"}.issubset(tick.columns):
        spread = (tick["AskPrice1"] - tick["BidPrice1"]).mean()
        tick_spread = spread / close if close and close != 0 else 0.0
    else:
        tick_spread = 0.0

    # ── 5. 综合信号（四因子等权加权）────────────────────────────
    # net_big, order_imb 正向；price_dev 负向（偏离越大越谨慎）；tick_spread 负向
    signal = (net_big + order_imb - price_dev - tick_spread) / 4.0

    return {
        "close":         close,
        "turnover_rate": turnover_rate,
        "vwap":          vwap,
        "price_dev":     price_dev,
        "net_big":       net_big,
        "order_imb":     order_imb,
        "tick_spread":   tick_spread,
        "signal":        signal,
    }
