# -*- coding: utf-8 -*-
"""
实盘因子示例：实时 VWAP + 成交量统计

使用 ShmStore 中的实时 tick + deal 数据计算：
  - vwap:        加权平均成交价
  - total_vol:   累计成交量
  - deal_count:  成交笔数
  - spread:      买卖一档价差

用于 live-engine 实盘因子计算，不涉及历史数据。
"""

import pandas as pd


# 因子信息声明：告诉 live-engine 需要哪些数据
FACTOR_INFO = {
    "need_l1_tick": True,
    "need_l2_deal": True,
    "need_l2_order": False,
}


def factor_calculation(data, code, date, end_time):
    """
    实盘因子计算函数。

    Args:
        data:     StockData，由 live-engine 从 ShmStore 注入
        code:     股票代码
        date:     交易日 YYYYMMDD
        end_time: 截面时刻 HHMMSS

    Returns:
        dict: 因子结果
    """
    result = {
        "code": code,
        "date": date,
        "end_time": end_time,
        "vwap": float("nan"),
        "total_vol": 0,
        "deal_count": 0,
        "spread": float("nan"),
    }

    # --- VWAP + 成交量：从 L2 deal 数据计算 ---
    deal = data.l2_deal
    if not deal.empty and "Price" in deal.columns and "Volume" in deal.columns:
        total_amount = (deal["Price"] * deal["Volume"]).sum()
        total_vol = deal["Volume"].sum()
        if total_vol > 0:
            result["vwap"] = round(float(total_amount / total_vol), 4)
        result["total_vol"] = int(total_vol)
        result["deal_count"] = len(deal)

    # --- 买卖价差：从 L1 tick 最新快照计算 ---
    tick = data.l1_tick
    if not tick.empty and "AskPrice1" in tick.columns and "BidPrice1" in tick.columns:
        latest = tick.iloc[-1]
        ask1 = latest.get("AskPrice1", 0)
        bid1 = latest.get("BidPrice1", 0)
        if ask1 > 0 and bid1 > 0:
            result["spread"] = round(float(ask1 - bid1), 4)

    return result


def outfun(date, end_time, result_df):
    """
    输出回调：结果会自动上传 OSS，这里只打印摘要。
    """
    if result_df.empty:
        return
    valid = result_df.dropna(subset=["vwap"])
    print(f"[{date} {end_time}] 计算完成: {len(valid)}/{len(result_df)} 只股票")
    if not valid.empty:
        print(f"  VWAP 均值: {valid['vwap'].mean():.4f}")
        print(f"  总成交量: {valid['total_vol'].sum():,.0f}")
