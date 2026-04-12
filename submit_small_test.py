#!/usr/bin/env python3
"""提交小规模测试任务查看详细日志"""
import requests

API_URL = "http://101.37.205.208/strategies/upload"

# 使用正确的函数名 factor_calculation
test_strategy = '''# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}

# 测试 3 只股票
securities = ["000001.SZ", "000002.SZ", "600000.SH"]

end_times = [""]

def initialize(context):
    pass

def factor_calculation(stock_data, code, date, end_time):
    """正确的函数名"""
    tick = stock_data.tick
    deal = stock_data.deal

    print(f"[FACTOR] code={code} date={date}")
    print(f"[FACTOR] tick: {tick.shape if tick is not None and not tick.empty else 'EMPTY'}")
    print(f"[FACTOR] deal: {deal.shape if deal is not None and not deal.empty else 'EMPTY'}")

    result = {"code": code, "date": date}

    # 简单统计
    if deal is not None and not deal.empty:
        result["deal_count"] = len(deal)
        result["deal_volume"] = deal["Volume"].sum() if "Volume" in deal.columns else 0
    else:
        result["deal_count"] = 0
        result["deal_volume"] = 0

    if tick is not None and not tick.empty:
        result["tick_count"] = len(tick)
    else:
        result["tick_count"] = 0

    print(f"[FACTOR] result: {result}")
    return result
'''

print("=== 提交小规模测试任务 ===")
print("股票: 000001.SZ, 000002.SZ, 600000.SH")
print("日期: 2025-01-06 到 2025-01-07 (2个交易日)")
print()

files = {
    'file': ('strategy.py', test_strategy.encode('utf-8'), 'text/x-python')
}

data = {
    'strategy_name': 'taq_small_test',
    'start_date': '2025-01-06',
    'end_date': '2025-01-07',
    'instances': '2',
}

try:
    response = requests.post(API_URL, files=files, data=data, timeout=30)
    print(f"状态码: {response.status_code}")
    print(f"响应: {response.text}")

    if response.status_code in [200, 201]:
        print("\n✅ 任务提交成功")
        print("\n监控命令:")
        print("  kubectl get backtesttasks -n quant | grep taq-small")
        print("  kubectl get pods -n quant | grep taq-small")
    else:
        print(f"\n❌ 任务提交失败")

except Exception as e:
    print(f"\n❌ 请求失败: {e}")
