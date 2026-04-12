# -*- coding: utf-8 -*-
"""
TAQ 数据生成策略 —— 交易员参考示例

【使用数据】
- L1 tick     : 计算 BBO (Top of book) 的 Ask/Bid TWAP, TWAD, Mid High/Low 等。
- L2 deal     : 计算交易的 VWAP, OHLC, 成交数量等。

【输出因子】
- 交易相关 (Trade):
  trd_qty, trd_vwap, trd_high, trd_low, trd_last, trd_cnt
- 报价相关 (Quote):
  ask_twap, ask_last, ask_twad, bid_twap, bid_last, bid_twad, mid_high, mid_low

【提交方式】
  curl -X POST http://101.37.205.208/strategies/upload \
    -F 'strategy_name=taq_generator' \
    -F 'start_date=2025-01-06' \
    -F 'end_date=2025-01-31' \
    -F 'instances=4' \
    -F 'file=@strategy.py'
"""

import numpy as np
import pandas as pd

# 声明：仅需要 L1 Tick 和 L2 Deal 来生成 TAQ
factor_info = {
    "market_count": 1,
    "need_l1_tick": True,
    "need_l2_order": False,
    "need_l2_deal": True,
}

# 全市场模式（引擎会自动启用流式加载模式避免 OOM）
securities = []  # 空列表表示全市场

# 收盘后计算一次
end_times = [""]

def _detect_time_fmt(sample_time):
    """探测时间字段格式"""
    if sample_time > 1e12: return "unix_ms"      # 例: 1704504600000
    if sample_time > 1e9:  return "unix_s"       # 例: 1704504600
    if sample_time > 10000000: return "hhmmssmmm" # 例: 93000000 (09:30:00.000)
    return "hhmmss"                              # 例: 93000 (09:30:00)

def _normalize_time(times, fmt):
    """将各类时间转为累计毫秒，用于安全计算 dt 和 TWAP"""
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
    """利用 Pandas 严格按照时钟切分交易日时间段，防止 60进制与 10进制计算越界"""
    date_str = str(date_str).replace("-", "")
    m_start = pd.Timestamp(f"{date_str} 09:30:00")
    m_end   = pd.Timestamp(f"{date_str} 11:30:00")
    a_start = pd.Timestamp(f"{date_str} 13:00:00")
    a_end   = pd.Timestamp(f"{date_str} 15:00:00")

    bins = []
    for p_start, p_end in [(m_start, m_end), (a_start, a_end)]:
        dt_range = pd.date_range(start=p_start, end=p_end, freq=f"{freq_min}min")
        for i in range(len(dt_range) - 1):
            dt_curr = dt_range[i]
            dt_nxt  = dt_range[i+1]

            # 使用区间结束时刻作为 label
            label = f"_{freq_min}m_{dt_nxt.strftime('%H%M')}"

            # 将时钟时间严格映射回原始的格式对应数值，保证切片匹配度100%
            if fmt == "unix_ms":
                start_val = int(dt_curr.tz_localize("Asia/Shanghai").timestamp() * 1000)
                end_val   = int(dt_nxt.tz_localize("Asia/Shanghai").timestamp() * 1000)
            elif fmt == "unix_s":
                start_val = int(dt_curr.tz_localize("Asia/Shanghai").timestamp())
                end_val   = int(dt_nxt.tz_localize("Asia/Shanghai").timestamp())
            elif fmt == "hhmmssmmm":
                start_val = int(dt_curr.strftime('%H%M%S')) * 1000
                end_val   = int(dt_nxt.strftime('%H%M%S')) * 1000
            else: # hhmmss
                start_val = int(dt_curr.strftime('%H%M%S'))
                end_val   = int(dt_nxt.strftime('%H%M%S'))

            bins.append((start_val, end_val, label))
    return bins

def _calc_taq_metrics(df_deal, df_tick, suffix, prev_state, norm_start, norm_end, norm_times):
    res = {}

    # ========================================================
    # 1. Trade (基于 l2_deal)
    # ========================================================
    if not df_deal.empty and "Price" in df_deal.columns and "Volume" in df_deal.columns:
        res[f"trd_cnt{suffix}"] = len(df_deal)
        trd_qty = float(df_deal["Volume"].sum())
        res[f"trd_qty{suffix}"] = trd_qty

        if trd_qty > 0:
            res[f"trd_vwap{suffix}"] = float((df_deal["Price"] * df_deal["Volume"]).sum() / trd_qty)
        else:
            res[f"trd_vwap{suffix}"] = float("nan")

        res[f"trd_high{suffix}"] = float(df_deal["Price"].max())
        res[f"trd_low{suffix}"]  = float(df_deal["Price"].min())
        res[f"trd_last{suffix}"] = float(df_deal.iloc[-1]["Price"])
    else:
        res[f"trd_cnt{suffix}"]  = 0
        res[f"trd_qty{suffix}"]  = 0.0
        res[f"trd_vwap{suffix}"] = res[f"trd_high{suffix}"] = res[f"trd_low{suffix}"] = res[f"trd_last{suffix}"] = float("nan")

    # ========================================================
    # 2. Quote (基于 l1_tick)
    # ========================================================
    if not df_tick.empty and len(norm_times) > 0 and "AskPrice1" in df_tick.columns:
        # 按归一化时间排序确保安全
        sort_idx = np.argsort(norm_times)
        times = norm_times[sort_idx]

        ask_p = df_tick["AskPrice1"].values[sort_idx]
        ask_v = df_tick["AskVolume1"].values[sort_idx]
        bid_p = df_tick["BidPrice1"].values[sort_idx]
        bid_v = df_tick["BidVolume1"].values[sort_idx]

        # 计算毫秒级的时间差 dt 作为权重 (严格规避了跨分钟 HHMMSS 的相减污染)
        dt = np.diff(times)
        last_dt = max(norm_end - times[-1], 0)
        dt = np.append(dt, last_dt)
        dt = np.maximum(dt, 0)
        total_time = dt.sum()

        if total_time > 0:
            res[f"ask_twap{suffix}"] = float((ask_p * dt).sum() / total_time)
            res[f"ask_twad{suffix}"] = float((ask_v * dt).sum() / total_time)
            res[f"bid_twap{suffix}"] = float((bid_p * dt).sum() / total_time)
            res[f"bid_twad{suffix}"] = float((bid_v * dt).sum() / total_time)
        else:
            res[f"ask_twap{suffix}"] = float(ask_p.mean())
            res[f"ask_twad{suffix}"] = float(ask_v.mean())
            res[f"bid_twap{suffix}"] = float(bid_p.mean())
            res[f"bid_twad{suffix}"] = float(bid_v.mean())

        res[f"ask_last{suffix}"] = float(ask_p[-1])
        res[f"bid_last{suffix}"] = float(bid_p[-1])

        valid_mid = (ask_p > 0) & (bid_p > 0)
        mid_prices = np.where(valid_mid, (ask_p + bid_p) / 2, np.nan)
        valid_mids_only = mid_prices[~np.isnan(mid_prices)]

        if len(valid_mids_only) > 0:
            res[f"mid_high{suffix}"] = float(valid_mids_only.max())
            res[f"mid_low{suffix}"]  = float(valid_mids_only.min())
        else:
            res[f"mid_high{suffix}"] = res[f"mid_low{suffix}"] = float("nan")

        # 更新给下个周期的状态继承
        prev_state["ask_last"] = res[f"ask_last{suffix}"]
        prev_state["bid_last"] = res[f"bid_last{suffix}"]
        prev_state["ask_twap"] = res[f"ask_twap{suffix}"]
        prev_state["bid_twap"] = res[f"bid_twap{suffix}"]
        prev_state["ask_twad"] = res[f"ask_twad{suffix}"]
        prev_state["bid_twad"] = res[f"bid_twad{suffix}"]

    else:
        # [状态继承]
        res[f"ask_last{suffix}"] = prev_state["ask_last"]
        res[f"bid_last{suffix}"] = prev_state["bid_last"]
        res[f"ask_twap{suffix}"] = prev_state["ask_twap"]
        res[f"bid_twap{suffix}"] = prev_state["bid_twap"]
        res[f"ask_twad{suffix}"] = prev_state["ask_twad"]
        res[f"bid_twad{suffix}"] = prev_state["bid_twad"]

        res[f"mid_high{suffix}"] = float("nan")
        res[f"mid_low{suffix}"]  = float("nan")

    return res

def factor_calculation(data, code, date, end_time):
    result = {"code": code, "date": date}

    df_deal = data.l2_deal
    df_tick = data.l1_tick

    # 1. 验证本股今日是否拥有基础行情数据
    sample_time = 0
    if not df_deal.empty:
        sample_time = int(df_deal["Time"].iloc[0])
    elif not df_tick.empty:
        sample_time = int(df_tick["Time"].iloc[0])
    else:
        return result

    # 2. 探测真实的时间字段格式类型
    fmt = _detect_time_fmt(sample_time)

    deal_times = df_deal["Time"].values if not df_deal.empty else np.array([])
    tick_times = df_tick["Time"].values if not df_tick.empty else np.array([])
    # 仅针对 tick 时间做归一化，用于 dt 安全加权
    norm_tick_times = _normalize_time(tick_times, fmt) if len(tick_times) > 0 else np.array([])

    # === 执行全天 1 分钟区间步进推演 ===
    bins_1m = _get_time_bins(date, 1, fmt)
    prev_state_1m = {
        "ask_last": float("nan"), "bid_last": float("nan"),
        "ask_twap": float("nan"), "bid_twap": float("nan"),
        "ask_twad": float("nan"), "bid_twad": float("nan"),
    }

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

    # === 执行全天 5 分钟区间步进推演 ===
    bins_5m = _get_time_bins(date, 5, fmt)
    prev_state_5m = {
        "ask_last": float("nan"), "bid_last": float("nan"),
        "ask_twap": float("nan"), "bid_twap": float("nan"),
        "ask_twad": float("nan"), "bid_twad": float("nan"),
    }

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
