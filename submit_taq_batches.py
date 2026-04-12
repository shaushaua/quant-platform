#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分批提交 TAQ 生成任务，避免 OOM

将全市场股票分成多批，每批独立提交任务
"""

import subprocess
import requests
from datetime import datetime

# API 端点
API_URL = "http://101.37.205.208/strategies/upload"

# 任务配置
STRATEGY_FILE = "/Users/zhangyang/Desktop/lianghua/quant-platform/taq_generator_strategy.py"
STRATEGY_NAME = "taq_generator"
START_DATE = "2025-01-06"
END_DATE = "2025-01-31"
INSTANCES = 26  # 每天一个节点

# 股票列表分批（每批数量）
BATCH_SIZE = 50

# 示例股票列表（实际使用时从文件或数据库获取）
ALL_STOCKS = [
    # 沪深300成分股（示例）
    "000001.SZ", "000002.SZ", "000063.SZ", "000069.SZ", "000100.SZ",
    "000157.SZ", "000166.SZ", "000333.SZ", "000338.SZ", "000585.SZ",
    "000651.SZ", "000708.SZ", "000725.SZ", "000768.SZ", "000776.SZ",
    "000783.SZ", "000792.SZ", "000858.SZ", "000876.SZ", "000888.SZ",
    "000895.SZ", "000901.SZ", "000938.SZ", "000961.SZ", "000983.SZ",
    "001979.SZ", "002027.SZ", "002044.SZ", "002050.SZ", "002142.SZ",
    "002415.SZ", "002456.SZ", "002475.SZ", "002594.SZ", "002714.SZ",
    "600000.SH", "600009.SH", "600010.SH", "600016.SH", "600025.SH",
    "600028.SH", "600030.SH", "600031.SH", "600036.SH", "600048.SH",
    "600050.SH", "600104.SH", "600109.SH", "600111.SH", "600196.SH",
    "600219.SH", "600276.SH", "600309.SH", "600332.SH", "600346.SH",
    "600519.SH", "600547.SH", "600570.SH", "600585.SH", "600588.SH",
    "600690.SH", "600703.SH", "600745.SH", "600761.SH", "600783.SH",
    "600837.SH", "600887.SH", "600893.SH", "600900.SH", "600905.SH",
    "600926.SH", "600958.SH", "600963.SH", "600999.SH", "601012.SH",
    "601066.SH", "601077.SH", "601088.SH", "601100.SH", "601138.SH",
    "601166.SH", "601168.SH", "601169.SH", "601179.SH", "601187.SH",
    "601211.SH", "601225.SH", "601238.SH", "601288.SH", "601318.SH",
    "601336.SH", "601360.SH", "601377.SH", "601398.SH", "601555.SH",
    "601601.SH", "601618.SH", "601628.SH", "601633.SH", "601666.SH",
    "601668.SH", "601688.SH", "601766.SH", "601788.SH", "601818.SH",
    "601857.SH", "601888.SH", "601901.SH", "601919.SH", "601939.SH",
    "601985.SH", "601988.SH", "601998.SH",
    # 添加更多股票...
]

def generate_strategy_file(stock_list):
    """生成包含指定股票列表的策略文件"""
    securities_str = str(stock_list).replace("'", '"')

    strategy_content = f'''# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

factor_info = {{
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}}

securities = {securities_str}

end_times = [""]

def _detect_time_fmt(sample_time):
    if sample_time > 1e12: return "unix_ms"
    if sample_time > 1e9:  return "unix_s"
    if sample_time > 10000000: return "hhmmssmmm"
    return "hhmmss"

def _normalize_time(times, fmt):
    if fmt == "unix_ms": return times
    if fmt == "unix_s":  return times * 1000
    if fmt == "hhmmssmmm":
        h = times // 10000000
        m = (times // 100000) % 100
        s = (times // 1000) % 100
        ms = times % 1000
        return (h * 3600 + m * 60 + s) * 1000 + ms
    if fmt == "hhmmss":
        h = times // 10000
        m = (times // 100) % 100
        s = times % 100
        return (h * 3600 + m * 60 + s) * 1000
    return times

def _get_time_bins(date_str, freq_min, fmt):
    date_str = str(date_str).replace("-", "")
    m_start = pd.Timestamp(f"{{date_str}} 09:30:00")
    m_end   = pd.Timestamp(f"{{date_str}} 11:30:00")
    a_start = pd.Timestamp(f"{{date_str}} 13:00:00")
    a_end   = pd.Timestamp(f"{{date_str}} 15:00:00")

    bins = []
    for p_start, p_end in [(m_start, m_end), (a_start, a_end)]:
        dt_range = pd.date_range(start=p_start, end=p_end, freq=f"{{freq_min}}min")
        for i in range(len(dt_range) - 1):
            dt_curr = dt_range[i]
            dt_nxt  = dt_range[i+1]
            label = f"_{{freq_min}}m_{{dt_nxt.strftime('%H%M')}}"

            if fmt == "unix_ms":
                start_val = int(dt_curr.tz_localize("Asia/Shanghai").timestamp() * 1000)
                end_val   = int(dt_nxt.tz_localize("Asia/Shanghai").timestamp() * 1000)
            elif fmt == "unix_s":
                start_val = int(dt_curr.tz_localize("Asia/Shanghai").timestamp())
                end_val   = int(dt_nxt.tz_localize("Asia/Shanghai").timestamp())
            elif fmt == "hhmmssmmm":
                start_val = int(dt_curr.strftime('%H%M%S')) * 1000
                end_val   = int(dt_nxt.strftime('%H%M%S')) * 1000
            else:
                start_val = int(dt_curr.strftime('%H%M%S'))
                end_val   = int(dt_nxt.strftime('%H%M%S'))

            bins.append((start_val, end_val, label))
    return bins

def _calc_taq_metrics(df_deal, df_tick, suffix, prev_state, norm_start, norm_end, norm_times):
    res = {{}}

    if not df_deal.empty and "Price" in df_deal.columns and "Volume" in df_deal.columns:
        res[f"trd_cnt{{suffix}}"] = len(df_deal)
        trd_qty = float(df_deal["Volume"].sum())
        res[f"trd_qty{{suffix}}"] = trd_qty

        if trd_qty > 0:
            res[f"trd_vwap{{suffix}}"] = float((df_deal["Price"] * df_deal["Volume"]).sum() / trd_qty)
        else:
            res[f"trd_vwap{{suffix}}"] = float("nan")

        res[f"trd_high{{suffix}}"] = float(df_deal["Price"].max())
        res[f"trd_low{{suffix}}"]  = float(df_deal["Price"].min())
        res[f"trd_last{{suffix}}"] = float(df_deal.iloc[-1]["Price"])
    else:
        res[f"trd_cnt{{suffix}}"]  = 0
        res[f"trd_qty{{suffix}}"]  = 0.0
        res[f"trd_vwap{{suffix}}"] = res[f"trd_high{{suffix}}"] = res[f"trd_low{{suffix}}"] = res[f"trd_last{{suffix}}"] = float("nan")

    if not df_tick.empty and len(norm_times) > 0 and "AskPrice1" in df_tick.columns:
        sort_idx = np.argsort(norm_times)
        times = norm_times[sort_idx]

        ask_p = df_tick["AskPrice1"].values[sort_idx]
        ask_v = df_tick["AskVolume1"].values[sort_idx]
        bid_p = df_tick["BidPrice1"].values[sort_idx]
        bid_v = df_tick["BidVolume1"].values[sort_idx]

        dt = np.diff(times)
        last_dt = max(norm_end - times[-1], 0)
        dt = np.append(dt, last_dt)
        dt = np.maximum(dt, 0)
        total_time = dt.sum()

        if total_time > 0:
            res[f"ask_twap{{suffix}}"] = float((ask_p * dt).sum() / total_time)
            res[f"ask_twad{{suffix}}"] = float((ask_v * dt).sum() / total_time)
            res[f"bid_twap{{suffix}}"] = float((bid_p * dt).sum() / total_time)
            res[f"bid_twad{{suffix}}"] = float((bid_v * dt).sum() / total_time)
        else:
            res[f"ask_twap{{suffix}}"] = float(ask_p.mean())
            res[f"ask_twad{{suffix}}"] = float(ask_v.mean())
            res[f"bid_twap{{suffix}}"] = float(bid_p.mean())
            res[f"bid_twad{{suffix}}"] = float(bid_v.mean())

        res[f"ask_last{{suffix}}"] = float(ask_p[-1])
        res[f"bid_last{{suffix}}"] = float(bid_p[-1])

        valid_mid = (ask_p > 0) & (bid_p > 0)
        mid_prices = np.where(valid_mid, (ask_p + bid_p) / 2, np.nan)
        valid_mids_only = mid_prices[~np.isnan(mid_prices)]

        if len(valid_mids_only) > 0:
            res[f"mid_high{{suffix}}"] = float(valid_mids_only.max())
            res[f"mid_low{{suffix}}"]  = float(valid_mids_only.min())
        else:
            res[f"mid_high{{suffix}}"] = res[f"mid_low{{suffix}}"] = float("nan")

        prev_state["ask_last"] = res[f"ask_last{{suffix}}"]
        prev_state["bid_last"] = res[f"bid_last{{suffix}}"]
        prev_state["ask_twap"] = res[f"ask_twap{{suffix}}"]
        prev_state["bid_twap"] = res[f"bid_twap{{suffix}}"]
        prev_state["ask_twad"] = res[f"ask_twad{{suffix}}"]
        prev_state["bid_twad"] = res[f"bid_twad{{suffix}}"]

    else:
        res[f"ask_last{{suffix}}"] = prev_state["ask_last"]
        res[f"bid_last{{suffix}}"] = prev_state["bid_last"]
        res[f"ask_twap{{suffix}}"] = prev_state["ask_twap"]
        res[f"bid_twap{{suffix}}"] = prev_state["bid_twap"]
        res[f"ask_twad{{suffix}}"] = prev_state["ask_twad"]
        res[f"bid_twad{{suffix}}"] = prev_state["bid_twad"]

        res[f"mid_high{{suffix}}"] = float("nan")
        res[f"mid_low{{suffix}}"]  = float("nan")

    return res

def factor_calculation(data, code, date, end_time):
    result = {{"code": code, "date": date}}

    df_deal = data.l2_deal
    df_tick = data.l1_tick

    sample_time = 0
    if not df_deal.empty:
        sample_time = int(df_deal["Time"].iloc[0])
    elif not df_tick.empty:
        sample_time = int(df_tick["Time"].iloc[0])
    else:
        return result

    fmt = _detect_time_fmt(sample_time)

    deal_times = df_deal["Time"].values if not df_deal.empty else np.array([])
    tick_times = df_tick["Time"].values if not df_tick.empty else np.array([])
    norm_tick_times = _normalize_time(tick_times, fmt) if len(tick_times) > 0 else np.array([])

    bins_1m = _get_time_bins(date, 1, fmt)
    prev_state_1m = {{
        "ask_last": float("nan"), "bid_last": float("nan"),
        "ask_twap": float("nan"), "bid_twap": float("nan"),
        "ask_twad": float("nan"), "bid_twad": float("nan"),
    }}

    for start_val, end_val, label in bins_1m:
        if len(deal_times) > 0:
            sub_deal = df_deal[(deal_times >= start_val) & (deal_times < end_val)]
        else:
            sub_deal = df_deal

        if len(tick_times) > 0:
            mask_tick = (tick_times >= start_val) & (tick_times < end_val)
            sub_tick = df_tick[mask_tick]
            sub_norm_times = norm_tick_times[mask_tick]
        else:
            sub_tick = df_tick
            sub_norm_times = np.array([])

        norm_start = _normalize_time(np.array([start_val]), fmt)[0]
        norm_end   = _normalize_time(np.array([end_val]), fmt)[0]

        res_1m = _calc_taq_metrics(sub_deal, sub_tick, label, prev_state_1m, norm_start, norm_end, sub_norm_times)
        result.update(res_1m)

    bins_5m = _get_time_bins(date, 5, fmt)
    prev_state_5m = {{
        "ask_last": float("nan"), "bid_last": float("nan"),
        "ask_twap": float("nan"), "bid_twap": float("nan"),
        "ask_twad": float("nan"), "bid_twad": float("nan"),
    }}

    for start_val, end_val, label in bins_5m:
        if len(deal_times) > 0:
            sub_deal = df_deal[(deal_times >= start_val) & (deal_times < end_val)]
        else:
            sub_deal = df_deal

        if len(tick_times) > 0:
            mask_tick = (tick_times >= start_val) & (tick_times < end_val)
            sub_tick = df_tick[mask_tick]
            sub_norm_times = norm_tick_times[mask_tick]
        else:
            sub_tick = df_tick
            sub_norm_times = np.array([])

        norm_start = _normalize_time(np.array([start_val]), fmt)[0]
        norm_end   = _normalize_time(np.array([end_val]), fmt)[0]

        res_5m = _calc_taq_metrics(sub_deal, sub_tick, label, prev_state_5m, norm_start, norm_end, sub_norm_times)
        result.update(res_5m)

    return result
'''

    # 写入临时策略文件
    temp_file = "/tmp/taq_strategy_batch.py"
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write(strategy_content)

    return temp_file


def submit_batch(stock_batch, batch_num):
    """提交一批股票的任务"""
    print(f"\n=== 提交批次 {batch_num} ({len(stock_batch)} 只股票) ===")

    # 生成策略文件
    strategy_file = generate_strategy_file(stock_batch)

    # 提交任务
    try:
        with open(strategy_file, 'rb') as f:
            files = {'file': f}
            data = {
                'strategy_name': f'{STRATEGY_NAME}_batch{batch_num}',
                'start_date': START_DATE,
                'end_date': END_DATE,
                'instances': str(INSTANCES),
            }
            response = requests.post(API_URL, files=files, data=data, timeout=30)

        if response.status_code == 200:
            result = response.json()
            task_id = result.get('taskId')
            print(f"✅ 批次 {batch_num} 提交成功")
            print(f"   任务ID: {task_id}")
            print(f"   状态: {result.get('status')}")
            print(f"   消息: {result.get('message')}")
            return task_id
        else:
            print(f"❌ 批次 {batch_num} 提交失败")
            print(f"   状态码: {response.status_code}")
            print(f"   响应: {response.text}")
            return None
    except Exception as e:
        print(f"❌ 批次 {batch_num} 提交异常: {e}")
        return None


def main():
    """主函数：分批提交所有股票"""
    total_stocks = len(ALL_STOCKS)
    num_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"=== TAQ 生成任务分批提交 ===")
    print(f"总股票数: {total_stocks}")
    print(f"批次大小: {BATCH_SIZE}")
    print(f"批次数: {num_batches}")
    print(f"每批并发: {INSTANCES} 个节点 (每天一个)")

    task_ids = []

    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, total_stocks)
        stock_batch = ALL_STOCKS[start_idx:end_idx]

        task_id = submit_batch(stock_batch, i + 1)
        if task_id:
            task_ids.append(task_id)

        # 避免提交过快
        if i < num_batches - 1:
            print("\n等待 5 秒后提交下一批...")
            import time
            time.sleep(5)

    print(f"\n=== 提交完成 ===")
    print(f"成功提交: {len(task_ids)}/{num_batches} 个批次")
    print(f"\n任务ID列表:")
    for i, task_id in enumerate(task_ids, 1):
        print(f"  批次 {i}: {task_id}")

    print(f"\n监控命令:")
    print(f"  kubectl get backtesttasks -n quant")
    print(f"  kubectl get pods -n quant -l backtest-task")


if __name__ == "__main__":
    main()
