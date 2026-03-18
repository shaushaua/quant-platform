# -*- coding: utf-8 -*-
"""
示例策略：成交量选股策略
交易员上传的策略

签名: run_strategy(data_api, ctx)
- data_api: DataAPI 实例，平台注入
- ctx: 回测上下文，平台注入

数据接口（per-code 模式，引擎自动注入当前股票上下文）:
- data_api.get_current_data("daily_basic") # 当前股票当日日线
- data_api.get_current_data("deal")        # 当前股票当日成交数据（DuckDB，仅读单股 ~2MB）
- data_api.get_current_data("order")       # 当前股票当日订单数据
- data_api.get_current_data("tick")        # 当前股票当日Tick数据

执行方式:
    engine = BacktestEngine()
    result = engine.run_per_code(
        strategy_func=run_strategy,
        start_date="2025-01-02",
        end_date="2025-01-13",
    )
    # result["results"] 是所有 (date, code) 信号合并的 DataFrame

分布式:
- ctx.start_date:   本机器负责的起始日期
- ctx.end_date:     本机器负责的结束日期
- ctx.current_date: 当前遍历到的日期（引擎注入）
- ctx.current_code: 当前处理的股票代码（引擎注入）
"""

import pandas as pd
import numpy as np


def run_strategy(data_api, ctx):
    """
    策略主函数 - 处理单只股票单天的数据

    引擎自动按 (日期 × 股票) 双层迭代调用此函数，
    每次调用时 ctx.current_date 和 ctx.current_code 已由引擎注入。

    Args:
        data_api: DataAPI 实例，由平台注入，已感知当前 (date, code) 上下文
        ctx: 回测上下文，由平台注入

    Returns:
        dict 或 DataFrame，引擎自动附加 _date/_code 列后汇总
        返回 None 表示无信号，跳过
    """
    code = ctx.current_code
    date = ctx.current_date

    # ========== 1. 获取当前股票日线数据 ==========
    daily_data = data_api.get_current_data("daily_basic")
    if daily_data is None or len(daily_data) == 0:
        return None

    # ========== 2. 获取当前股票成交数据（DuckDB 只读单股，内存安全）==========
    deal_data = data_api.get_current_data("deal")
    if deal_data is None or len(deal_data) == 0:
        return None

    # ========== 3. 计算成交量因子 ==========
    total_volume = deal_data["Volume"].sum()
    vwap = (
        (deal_data["Volume"] * deal_data["Price"]).sum() / total_volume
        if total_volume > 0 else 0.0
    )

    # 大单净买（Volume >= 10000 视为大单）
    big_buy = deal_data.loc[
        (deal_data["Volume"] >= 10000) & (deal_data["BsFlag"] == "B"), "Volume"
    ].sum()
    big_sell = deal_data.loc[
        (deal_data["Volume"] >= 10000) & (deal_data["BsFlag"] == "S"), "Volume"
    ].sum()
    net_big = big_buy - big_sell

    # 收盘价（从日线取）
    close = daily_data["close"].iloc[0] if "close" in daily_data.columns else np.nan

    # ========== 4. 返回因子信号（引擎自动附加 _date/_code）==========
    return {
        "close":        close,
        "total_volume": total_volume,
        "vwap":         vwap,
        "big_buy":      big_buy,
        "big_sell":     big_sell,
        "net_big":      net_big,
        "signal":       1.0 if net_big > 0 else -1.0,
    }
