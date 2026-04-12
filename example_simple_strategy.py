#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试示例 - 最小化的策略用于验证本地测试工具
"""

import pandas as pd

# 最简单的配置
factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}

# 测试 2 只股票
securities = ["000001.SZ", "000002.SZ"]

end_times = [""]


def initialize(context):
    pass


def factor_calculation(stock_data, code, date, end_time):
    """
    最简单的因子计算 - 只统计数据量
    """
    result = {
        "code": code,
        "date": date,
    }

    # 获取数据（使用正确的属性名）
    tick = stock_data.l1_tick
    deal = stock_data.l2_deal

    # 简单统计
    result["tick_count"] = len(tick) if not tick.empty else 0
    result["deal_count"] = len(deal) if not deal.empty else 0

    # 打印调试信息
    print(f"[INFO] {code} {date}: tick={result['tick_count']} deal={result['deal_count']}")

    # 如果有数据，计算一些基本指标
    if not deal.empty and "Volume" in deal.columns:
        result["total_volume"] = int(deal["Volume"].sum())
        result["avg_price"] = float(deal["Price"].mean()) if "Price" in deal.columns else 0.0
    else:
        result["total_volume"] = 0
        result["avg_price"] = 0.0

    return result
