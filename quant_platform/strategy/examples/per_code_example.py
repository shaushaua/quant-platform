# -*- coding: utf-8 -*-
"""
示例策略：Per-Code 执行模式

引擎自动按 (日期, 股票) 双层迭代，交易员只需关注单只股票的逻辑。
每次调用时 ctx.current_code 和 data_api 上下文已由引擎自动注入。

数据接口（per-code 模式推荐）:
- data_api.get_current_data("deal")        # 当前股票当天成交数据 (~2MB, DuckDB)
- data_api.get_current_data("tick")        # 当前股票当天Tick数据
- data_api.get_current_data("order")       # 当前股票当天订单数据
- data_api.get_current_data("daily_basic") # 当前股票当天日线数据

执行方式:
    from quant_platform.backtest.engine import BacktestEngine, CodeRouter

    engine = BacktestEngine()

    # 方式一：所有股票用同一策略
    result = engine.run_per_code(
        strategy_func=run_strategy,
        start_date="2025-01-02",
        end_date="2025-01-13",
    )

    # 方式二：不同股票用不同策略（CodeRouter）
    router = CodeRouter()
    router.register("000001.XSHE", run_strategy_bank)  # 精确匹配
    router.register("60*.XSHG", run_strategy_sh)       # 上交所主板通配符
    result = engine.run_per_code(
        strategy_func=run_strategy,   # 未匹配时的默认策略
        start_date="2025-01-02",
        end_date="2025-01-13",
        router=router,
    )

    # result["results"] 是所有 (date, code) 信号合并的 DataFrame
    # result["results"] 包含 _date, _code 列以及策略返回的所有列
    print(result["results"])
"""

import pandas as pd
import numpy as np


def run_strategy(data_api, ctx):
    """
    策略主函数 - 每次处理一只股票的一天数据

    Args:
        data_api: DataAPI 实例，已注入当前 (date, code) 上下文
        ctx: BacktestContext，ctx.current_date / ctx.current_code 已由引擎设置

    Returns:
        dict 或 DataFrame，引擎会自动附加 _date / _code 列后汇总
        返回 None 表示当前 (date, code) 无信号，跳过
    """
    code = ctx.current_code
    date = ctx.current_date

    # ===== 获取当前股票当天成交数据（内存安全：DuckDB 只读单股 ~2MB）=====
    deal_df = data_api.get_current_data("deal")
    if deal_df.empty:
        return None

    # ===== 计算因子：大单净买入 =====
    # 假设字段：Volume, Price, BsFlag ('B'=买, 'S'=卖)
    if "BsFlag" not in deal_df.columns or "Volume" not in deal_df.columns:
        return None

    large_threshold = 10_000  # 大单阈值：1万股
    large = deal_df[deal_df["Volume"] >= large_threshold]
    big_buy = large[large["BsFlag"] == "B"]["Volume"].sum()
    big_sell = large[large["BsFlag"] == "S"]["Volume"].sum()
    net_big = big_buy - big_sell

    total_vol = deal_df["Volume"].sum()
    big_ratio = net_big / total_vol if total_vol > 0 else 0.0

    # ===== 获取日线数据（当前股票当天）=====
    daily_df = data_api.get_current_data("daily_basic")
    close = daily_df["Close"].iloc[0] if not daily_df.empty and "Close" in daily_df.columns else None

    # ===== 返回信号（引擎自动加 _date / _code 列）=====
    return {
        "big_buy": big_buy,
        "big_sell": big_sell,
        "net_big": net_big,
        "big_ratio": big_ratio,
        "total_vol": total_vol,
        "close": close,
    }


def run_strategy_bank(data_api, ctx):
    """
    针对银行股的差异化策略示例（通过 CodeRouter 路由）
    逻辑相同，可在此定制银行股特有的因子计算。
    """
    return run_strategy(data_api, ctx)


def run_strategy_sh(data_api, ctx):
    """
    针对上交所主板（60*）的差异化策略示例（通过 CodeRouter 路由）
    """
    return run_strategy(data_api, ctx)
