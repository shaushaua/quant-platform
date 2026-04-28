#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史窗口示例策略

演示如何使用 lookback_days 参数获取历史数据列表。

lookback_days 支持两种格式：
  1. 整数：如 5，自动转换为 [0, 1, 2, 3, 4]
  2. 列表：如 [0, 1, 5]，精确指定哪些天

索引对应关系：
  - [0] = 当天
  - [1] = 1天前
  - [2] = 2天前
  - [5] = 5天前
"""

import pandas as pd

# 配置：指定需要哪几天的数据
factor_info = {
    "need_l1_tick": True,
    "need_l2_deal": True,
    "need_l2_order": False,
    "market_count": 1,
    # 方式1：列表，精确指定哪些天
    "lookback_days": [0, 1, 2],  # 当天 + 1天前 + 2天前

    # 方式2：整数，自动转换为 [0, 1, 2, 3, 4]（当天 + 前4天）
    # "lookback_days": 5,

    # 方式3：不传或只传 [0]，只获取当天
    # "lookback_days": [0],
}

securities = ["000001.SZ", "000002.SZ"]

end_times = [""]


def factor_calculation(stock_data, code, date, end_time):
    """
    计算因子：使用历史数据列表

    stock_data.l2_deal_hist: List[pd.DataFrame]
    stock_data.l1_tick_hist: List[pd.DataFrame]

    索引对应关系（lookback_days=[0, 1, 2]）：
        [0]: 当天
        [1]: 1天前
        [2]: 2天前
    """
    result = {
        "code": code,
        "date": date,
    }

    # 使用历史列表计算
    if stock_data.l2_deal_hist:
        print(f"历史数据列表长度: {len(stock_data.l2_deal_hist)}")

        # [0] = 当天
        if len(stock_data.l2_deal_hist) >= 1:
            today_deal = stock_data.l2_deal_hist[0]
            if not today_deal.empty and "Volume" in today_deal.columns:
                result["today_volume"] = int(today_deal["Volume"].sum())
                print(f"  当天成交量: {result['today_volume']}")

        # [1] = 1天前
        if len(stock_data.l2_deal_hist) >= 2:
            prev1_deal = stock_data.l2_deal_hist[1]
            if not prev1_deal.empty and "Volume" in prev1_deal.columns:
                result["prev1_volume"] = int(prev1_deal["Volume"].sum())
                print(f"  1天前成交量: {result['prev1_volume']}")

        # [2] = 2天前
        if len(stock_data.l2_deal_hist) >= 3:
            prev2_deal = stock_data.l2_deal_hist[2]
            if not prev2_deal.empty and "Volume" in prev2_deal.columns:
                result["prev2_volume"] = int(prev2_deal["Volume"].sum())
                print(f"  2天前成交量: {result['prev2_volume']}")

        # 计算成交量变化（相对1天前）
        if result.get("today_volume", 0) > 0 and result.get("prev1_volume", 0) > 0:
            result["volume_change_vs_prev1"] = (
                (result["today_volume"] - result["prev1_volume"]) /
                result["prev1_volume"]
            )

    # 当天数据也可以通过 stock_data.l2_deal 访问（向后兼容）
    if not stock_data.l2_deal.empty:
        result["today_deal_count"] = len(stock_data.l2_deal)
        if "Price" in stock_data.l2_deal.columns:
            result["vwap"] = float(
                (stock_data.l2_deal["Price"] * stock_data.l2_deal["Volume"]).sum() /
                stock_data.l2_deal["Volume"].sum()
            )

    print(f"[{code} {date}] 计算完成: {result}")
    return result
