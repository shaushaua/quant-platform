#!/usr/bin/env python3
"""使用正确属性名的测试"""
import requests

API_URL = "http://101.37.205.208/strategies/upload"

test_strategy = '''# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}

securities = ["000001.SZ", "000002.SZ"]
end_times = [""]

def initialize(context):
    pass

def factor_calculation(stock_data, code, date, end_time):
    """使用正确的属性名"""
    # 正确的属性名
    tick = stock_data.l1_tick
    deal = stock_data.l2_deal

    print(f"[DEBUG] code={code} date={date}")
    print(f"[DEBUG] l1_tick shape: {tick.shape if not tick.empty else 'EMPTY'}")
    print(f"[DEBUG] l2_deal shape: {deal.shape if not deal.empty else 'EMPTY'}")

    if not tick.empty:
        print(f"[DEBUG] tick columns: {list(tick.columns)[:10]}")
        print(f"[DEBUG] tick first row Code: {tick.iloc[0]['Code'] if 'Code' in tick.columns else 'NO CODE'}")

    if not deal.empty:
        print(f"[DEBUG] deal columns: {list(deal.columns)[:10]}")
        print(f"[DEBUG] deal first row Code: {deal.iloc[0]['Code'] if 'Code' in deal.columns else 'NO CODE'}")

    result = {"code": code, "date": date}
    result["tick_count"] = len(tick) if not tick.empty else 0
    result["deal_count"] = len(deal) if not deal.empty else 0

    print(f"[DEBUG] result: {result}")
    return result
'''

print("=== 提交正确属性名测试 ===\n")

resp = requests.post(API_URL,
    files={'file': ('strategy.py', test_strategy.encode('utf-8'), 'text/x-python')},
    data={'strategy_name': 'taq_correct_attr', 'start_date': '2025-01-06', 'end_date': '2025-01-06', 'instances': '1'},
    timeout=30)

print(f"{resp.status_code}: {resp.text}")
