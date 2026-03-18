# -*- coding: utf-8 -*-
"""
示例因子：20 日动量因子

使用新接口：calc_factors_by_date_range
交易员只需提供 factor_calculation 和 outfun，引擎负责调度。
"""

import pandas as pd
from quant_platform.factor.engine import calc_factors_by_date_range


# ------------------------------------------------------------------
# 1. 单票因子计算函数
# ------------------------------------------------------------------

def factor_calculation(data, code, date, end_time):
    """
    20 日动量因子：当日收盘价 / 20 日前收盘价 - 1

    Args:
        data:     StockData，由引擎注入，包含 market（多日 daily_basic）
        code:     股票代码
        date:     当前交易日 YYYYMMDD
        end_time: 时间切片（本因子不使用）

    Returns:
        dict: {"code": code, "momentum_20d": float}
    """
    df = data.market  # 多日 daily_basic，含 _date 列

    if df.empty or "close" not in df.columns:
        return {"code": code, "momentum_20d": float("nan")}

    closes = df.sort_values("_date")["close"].dropna().values
    if len(closes) < 21:
        return {"code": code, "momentum_20d": float("nan")}

    momentum = float(closes[-1] / closes[-21] - 1)
    return {"code": code, "momentum_20d": momentum}


# ------------------------------------------------------------------
# 2. 批量结果处理函数
# ------------------------------------------------------------------

def outfun(date, end_time, test):
    """
    Args:
        date:     交易日 YYYYMMDD
        end_time: 时间切片
        test:     pd.DataFrame，每行一只股票，列为因子名
    """
    if test.empty:
        return
    print(f"[{date} {end_time}] 计算完成，共 {len(test)} 只股票")
    print(test.head())
    # 生产环境在这里写 OSS parquet 或推送数据库


# ------------------------------------------------------------------
# 3. 调用示例
# ------------------------------------------------------------------

if __name__ == "__main__":
    factor_info = {
        "market_count": 25,       # 需要 25 日 daily_basic 历史（20+缓冲）
        "need_l2_order": False,
        "need_l2_deal": False,
        "need_l1_tick": False,
    }

    securities = ["000001.SZ", "600000.SH", "000002.SZ"]

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date="20240101",
        end_date="20241231",
        end_times=[""],           # 每日收盘后计算一次
        securities=securities,
        processes=1,
        factor_data_handler=factor_calculation,
        outfun=outfun,
    )
