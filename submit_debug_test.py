#!/usr/bin/env python3
"""提交单个测试任务用于调试"""
import requests
import time

API_URL = "http://101.37.205.208/strategies/upload"

# 只测试 3 只股票，1 个交易日
test_strategy = '''# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}

securities = ["000001.SZ", "000002.SZ", "600000.SH"]

end_times = [""]

def _detect_time_fmt(sample_time):
    if sample_time > 1e12: return "unix_ms"
    if sample_time > 1e9:  return "unix_s"
    if sample_time > 10000000: return "hhmmssmmm"
    return "hhmmss"

def _normalize_time(times, fmt):
    if fmt == "unix_ms": return times
    if fmt == "unix_s": return times * 1000
    if fmt == "hhmmssmmm": return times / 1000
    return times

def initialize(context):
    pass

def compute_factor(stock_data):
    """
    计算 TAQ 因子
    """
    result = {"date": stock_data.date, "code": stock_data.code}

    tick = stock_data.tick
    deal = stock_data.deal

    print(f"[DEBUG] code={stock_data.code} date={stock_data.date}")
    print(f"[DEBUG] tick shape: {tick.shape if tick is not None and not tick.empty else 'EMPTY'}")
    print(f"[DEBUG] deal shape: {deal.shape if deal is not None and not deal.empty else 'EMPTY'}")

    if tick is not None and not tick.empty:
        print(f"[DEBUG] tick columns: {list(tick.columns)}")
        print(f"[DEBUG] tick first 3 rows:")
        print(tick.head(3))

    if deal is not None and not deal.empty:
        print(f"[DEBUG] deal columns: {list(deal.columns)}")
        print(f"[DEBUG] deal first 3 rows:")
        print(deal.head(3))

    # 简单计算一些因子
    if deal is not None and not deal.empty:
        result["trd_cnt"] = len(deal)
        result["trd_qty"] = deal["Volume"].sum() if "Volume" in deal.columns else 0
    else:
        result["trd_cnt"] = 0
        result["trd_qty"] = 0

    if tick is not None and not tick.empty:
        result["tick_cnt"] = len(tick)
    else:
        result["tick_cnt"] = 0

    return result
'''

print("=== 提交调试任务 ===")
print("策略: 3只股票, 1个交易日")
print("日期: 2025-01-06 (单日)")

files = {
    'file': ('strategy.py', test_strategy.encode('utf-8'), 'text/x-python')
}

data = {
    'strategy_name': 'taq_debug_test',
    'start_date': '2025-01-06',
    'end_date': '2025-01-06',
    'instances': '1',
}

try:
    response = requests.post(API_URL, files=files, data=data, timeout=30)
    print(f"\n状态码: {response.status_code}")
    print(f"响应: {response.text}")

    if response.status_code in [200, 201]:
        print("\n✅ 任务提交成功")
        print("\n监控命令:")
        print("  kubectl get backtesttasks -n quant | grep taq-debug")
        print("  kubectl get pods -n quant | grep taq-debug")
        print("  kubectl logs -n quant <pod-name> --tail=100")
    else:
        print(f"\n❌ 任务提交失败")

except Exception as e:
    print(f"\n❌ 请求失败: {e}")
