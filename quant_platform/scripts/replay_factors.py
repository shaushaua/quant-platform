#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子计算全市场对比验证

从 OSS 拉取实盘因子结果，从通联 CSV 回放因子计算，全市场对比。
只处理前几分钟，不过滤具体股票。

优化：只读因子计算需要的列（usecols），节省内存。

用法：
    python3 -m quant_platform.scripts.replay_factors --date 20260521
    python3 -m quant_platform.scripts.replay_factors --date 20260521 --minutes 5
"""

import argparse
import gc
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from quant_platform.factor.base import StockState
from quant_platform.factor.examples.live_momentum import factor_calculation

TL_FILES = {
    "sh_tick":       "mdl_4_4_0.csv",
    "sz_tick":       "mdl_6_28_0.csv",
    "sh_order_deal": "mdl_4_24_0.csv",
    "sz_order":      "mdl_6_33_0.csv",
    "sz_deal":       "mdl_6_36_0.csv",
}

COMPARE_FIELDS = [
    "latest_price", "high", "low", "change_pct", "spread",
    "vwap", "total_vol", "deal_count",
    "order_imbalance", "order_buy_vol_ratio", "cancel_ratio", "order_count",
]

# 只读因子计算需要的列（而不是全部 104 列）
SH_TICK_COLS = [
    "UpdateTime", "SecurityID",
    "PreCloPrice", "LastPrice", "HighPrice", "LowPrice",
    "AskPrice1", "AskVolume1", "BidPrice1", "BidVolume1",
    "LocalTime", "SeqNo",
]
SZ_TICK_COLS = [
    "UpdateTime", "SecurityID",
    "PreCloPrice", "LastPrice", "HighPrice", "LowPrice",
    "AskPrice1", "AskVolume1", "BidPrice1", "BidVolume1",
    "LocalTime", "SeqNo",
]
SH_ORDER_DEAL_COLS = [
    "TickTime", "SecurityID", "LocalTime",
    "Price", "Qty", "TickBSFlag", "Type",
    "BuyOrderNO", "SellOrderNO", "Channel", "BizIndex",
]
SZ_ORDER_COLS = [
    "TransactTime", "SecurityID", "LocalTime",
    "Side", "OrderQty", "OrdType",
    "ApplSeqNum", "ChannelNo",
]
SZ_DEAL_COLS = [
    "TransactTime", "SecurityID", "LocalTime",
    "LastPx", "LastQty",
    "OfferApplSeqNum", "BidApplSeqNum", "ChannelNo", "ApplSeqNum",
    "ExecType",
]

# 股票 SecurityID 范围
SH_STOCK_PREFIX = ("6", "9")
SZ_STOCK_PREFIX = ("0", "3")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup")
    p.add_argument("--minutes", type=int, default=1,
                   help="只处理开盘后前几分钟（默认1）")
    p.add_argument("--nrows", type=int, default=5_000_000,
                   help="每个 CSV 读前多少行（默认5M）")
    return p.parse_args()


def _filter_stock_security_id(raw, id_col="SecurityID"):
    """过滤只保留股票 SecurityID（6开头/9开头=SH，0开头/3开头=SZ）。"""
    ids = raw[id_col].astype(str).str.strip().str.zfill(6)
    mask = ids.str[0].isin(list(SH_STOCK_PREFIX) + list(SZ_STOCK_PREFIX))
    return raw[mask].copy()


def _safe_int(val):
    """安全转 int，处理 NaN。"""
    try:
        v = float(val)
        return 0 if np.isnan(v) else int(v)
    except (ValueError, TypeError):
        return 0


def _safe_float(val):
    """安全转 float，处理 NaN。"""
    try:
        v = float(val)
        return 0.0 if np.isnan(v) else v
    except (ValueError, TypeError):
        return 0.0


# ============================================================
# 1. 从 OSS 拉实盘因子
# ============================================================

def load_live_factors_from_oss(date_str, max_minutes):
    import oss2

    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    if not ak or not sk:
        print("错误: OSS 凭据未设置")
        sys.exit(1)

    ep = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
    ep_host = ep.replace("https://", "").replace("http://", "")
    bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
    prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")

    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, ep_host, bucket_name)

    year = date_str[:4]
    month = date_str[4:6]
    search_prefix = f"{prefix}/{year}/{year}{month}/{date_str}/"

    all_times = []
    for obj in oss2.ObjectIterator(bucket, prefix=search_prefix):
        if not obj.key.endswith(".json"):
            continue
        fname = obj.key.split("/")[-1].replace(".json", "")
        if fname.isdigit() and len(fname) == 6:
            all_times.append(fname)

    all_times.sort()
    print(f"  OSS 上共 {len(all_times)} 个时间点")

    # 只取开盘后 max_minutes 分钟内
    cutoff_h = 9
    cutoff_m = 30 + max_minutes
    if cutoff_m >= 60:
        cutoff_h += cutoff_m // 60
        cutoff_m = cutoff_m % 60
    cutoff_str = f"{cutoff_h:02d}{cutoff_m:02d}00"
    target_times = [t for t in all_times if t <= cutoff_str]
    if not target_times:
        target_times = all_times[:max_minutes]

    print(f"  对比时间点 ({len(target_times)} 个): {target_times}")

    all_records = []
    for ts in target_times:
        key = f"{search_prefix}{ts}.json"
        try:
            data = bucket.get_object(key).read()
            records = json.loads(data)
            all_records.extend(records)
        except oss2.exceptions.NoSuchKey:
            print(f"    {ts}.json 不存在")
        except Exception as e:
            print(f"    {ts}.json 读取失败: {e}")

    if not all_records:
        return pd.DataFrame(), []

    df = pd.DataFrame(all_records)
    print(f"  实盘因子: {len(df)} 条 ({df['code'].nunique()} 只 × {len(target_times)} 个时间点)")
    return df, target_times


# ============================================================
# 2. 读通联 CSV（只读需要的列，省内存）
# ============================================================

def load_tonglance(msg_dir, date_str, nrows):
    """读通联 CSV 前 nrows 行，只读因子计算需要的列，返回按 Code 分组的数据。"""
    day_dir = Path(msg_dir) / date_str
    trading_day = datetime.strptime(date_str, "%Y%m%d")

    tick_records = []  # list of (code, time_str, price_dict)
    deal_records = []  # list of (code, time_str, price, volume)
    order_records = []  # list of (code, time_str, side, volume, order_type)

    # --- SH tick ---
    p = day_dir / TL_FILES["sh_tick"]
    if p.exists():
        mb = p.stat().st_size / 1024 / 1024
        print(f"    SH tick ({mb:.0f}MB) 读前{nrows:,}行...", end="", flush=True)
        raw = pd.read_csv(p, nrows=nrows, usecols=SH_TICK_COLS)
        raw = _filter_stock_security_id(raw)
        print(f" {len(raw):,}行股票")
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHG"
            t = str(r["UpdateTime"]).strip()
            tick_records.append((code, t, {
                "CurrentPrice": _safe_float(r.get("LastPrice", 0)),
                "PreClosePrice": _safe_float(r.get("PreCloPrice", 0)),
                "HighPrice": _safe_float(r.get("HighPrice", 0)),
                "LowPrice": _safe_float(r.get("LowPrice", 0)),
                "AskPrice1": _safe_float(r.get("AskPrice1", 0)),
                "BidPrice1": _safe_float(r.get("BidPrice1", 0)),
                "AskVolume1": _safe_int(r.get("AskVolume1", 0)),
                "BidVolume1": _safe_int(r.get("BidVolume1", 0)),
            }))
        del raw
        gc.collect()

    # --- SZ tick ---
    p = day_dir / TL_FILES["sz_tick"]
    if p.exists():
        mb = p.stat().st_size / 1024 / 1024
        print(f"    SZ tick ({mb:.0f}MB) 读前{nrows:,}行...", end="", flush=True)
        raw = pd.read_csv(p, nrows=nrows, usecols=SZ_TICK_COLS)
        raw = _filter_stock_security_id(raw)
        print(f" {len(raw):,}行股票")
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHE"
            t = str(r["UpdateTime"]).strip()
            tick_records.append((code, t, {
                "CurrentPrice": _safe_float(r.get("LastPrice", 0)),
                "PreClosePrice": _safe_float(r.get("PreCloPrice", 0)),
                "HighPrice": _safe_float(r.get("HighPrice", 0)),
                "LowPrice": _safe_float(r.get("LowPrice", 0)),
                "AskPrice1": _safe_float(r.get("AskPrice1", 0)),
                "BidPrice1": _safe_float(r.get("BidPrice1", 0)),
                "AskVolume1": _safe_int(r.get("AskVolume1", 0)),
                "BidVolume1": _safe_int(r.get("BidVolume1", 0)),
            }))
        del raw
        gc.collect()

    # --- SH order+deal ---
    p = day_dir / TL_FILES["sh_order_deal"]
    if p.exists():
        mb = p.stat().st_size / 1024 / 1024
        print(f"    SH order+deal ({mb:.0f}MB) 读前{nrows:,}行...", end="", flush=True)
        raw = pd.read_csv(p, nrows=nrows, usecols=SH_ORDER_DEAL_COLS)
        raw = _filter_stock_security_id(raw)
        print(f" {len(raw):,}行股票")
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHG"
            t = str(r["TickTime"]).strip()
            ptype = str(r.get("Type", "")).strip()
            side = 0 if str(r.get("TickBSFlag", "")).strip() == "B" else 1
            price = _safe_float(r.get("Price", 0))
            vol = _safe_int(r.get("Qty", 0))
            otype = 5 if ptype == "D" else 2

            if ptype == "T":
                deal_records.append((code, t, price, vol))
            else:
                order_records.append((code, t, side, vol, otype))
        del raw
        gc.collect()

    # --- SZ order ---
    p = day_dir / TL_FILES["sz_order"]
    if p.exists():
        mb = p.stat().st_size / 1024 / 1024
        print(f"    SZ order ({mb:.0f}MB) 读前{nrows:,}行...", end="", flush=True)
        raw = pd.read_csv(p, nrows=nrows, usecols=SZ_ORDER_COLS)
        raw = _filter_stock_security_id(raw)
        print(f" {len(raw):,}行股票")
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHE"
            t = str(r["TransactTime"]).strip()
            side_val = r.get("Side")
            side = 0 if (side_val == 49 or str(side_val) == "49") else 1
            vol = _safe_int(r.get("OrderQty", 0))
            order_records.append((code, t, side, vol, 1))
        del raw
        gc.collect()

    # --- SZ deal ---
    p = day_dir / TL_FILES["sz_deal"]
    if p.exists():
        mb = p.stat().st_size / 1024 / 1024
        print(f"    SZ deal ({mb:.0f}MB) 读前{nrows:,}行...", end="", flush=True)
        raw = pd.read_csv(p, nrows=nrows, usecols=SZ_DEAL_COLS)
        raw = _filter_stock_security_id(raw)
        print(f" {len(raw):,}行股票")
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHE"
            t = str(r["TransactTime"]).strip()
            price = _safe_float(r.get("LastPx", 0))
            vol = _safe_int(r.get("LastQty", 0))
            deal_records.append((code, t, price, vol))
        del raw
        gc.collect()

    print(f"  合计: tick={len(tick_records):,} deal={len(deal_records):,} order={len(order_records):,}")

    return tick_records, deal_records, order_records


# ============================================================
# 3. 回放因子
# ============================================================

def _time_to_str(t):
    """时间字符串统一为 HHMMSS 格式。"""
    s = str(t).strip()
    if ":" in s:
        # "09:30:00.000" -> "093000"
        parts = s.split(":")
        return parts[0] + parts[1] + parts[2][:2]
    # 纯数字
    s = s.replace(".", "").ljust(9, "0")
    return s[:6]


def _time_le(t1, cutoff_str):
    """判断时间 t1 是否 <= cutoff (HHMMSS 格式)。"""
    t1s = _time_to_str(t1)
    return t1s <= cutoff_str


def replay_all_stocks(tick_records, deal_records, order_records, date_str, target_times):
    """模拟实盘引擎，全市场回放。"""
    # 按 code 分组
    from collections import defaultdict
    tick_by_code = defaultdict(list)
    deal_by_code = defaultdict(list)
    order_by_code = defaultdict(list)

    for code, t, data in tick_records:
        tick_by_code[code].append((t, data))
    for code, t, price, vol in deal_records:
        deal_by_code[code].append((t, price, vol))
    for code, t, side, vol, otype in order_records:
        order_by_code[code].append((t, side, vol, otype))

    all_codes = set(tick_by_code.keys()) | set(deal_by_code.keys()) | set(order_by_code.keys())
    print(f"  共 {len(all_codes)} 只股票")

    del tick_records, deal_records, order_records
    gc.collect()

    results = []
    for ts in target_times:
        count = 0
        for code in all_codes:
            state = StockState(code=code)

            # tick
            for t, data in tick_by_code.get(code, []):
                if not _time_le(t, ts):
                    continue
                # 手动更新 StockState（不走完整 converter）
                price = data["CurrentPrice"]
                if price > 0:
                    if state.open == 0.0:
                        state.open = price
                    state.latest_price = price
                    state.high = max(state.high, price)
                    if price < state.low:
                        state.low = price
                if data["PreClosePrice"] > 0:
                    state.pre_close = data["PreClosePrice"]
                if data["AskPrice1"] > 0:
                    state.ask1 = data["AskPrice1"]
                if data["BidPrice1"] > 0:
                    state.bid1 = data["BidPrice1"]
                state.ask_volume1 = data["AskVolume1"]
                state.bid_volume1 = data["BidVolume1"]
                state.tick_count += 1

            # deal
            for t, price, vol in deal_by_code.get(code, []):
                if not _time_le(t, ts):
                    continue
                if price > 0 and vol > 0:
                    state.cum_amount += price * vol
                    state.cum_volume += vol
                state.deal_count += 1

            # order
            for t, side, vol, otype in order_by_code.get(code, []):
                if not _time_le(t, ts):
                    continue
                if side == 0:
                    state.buy_order_count += 1
                    state.buy_order_volume += vol
                else:
                    state.sell_order_count += 1
                    state.sell_order_volume += vol
                if otype == 5:
                    state.cancel_count += 1
                state.order_count += 1

            if state.tick_count > 0 or state.deal_count > 0 or state.order_count > 0:
                try:
                    result = factor_calculation(state, code, date_str, ts)
                    if result:
                        results.append(result)
                        count += 1
                except Exception:
                    pass

        print(f"    {ts}: {count} 只股票")

    return pd.DataFrame(results)


# ============================================================
# 4. 对比
# ============================================================

def compare(live_df, replay_df):
    if live_df.empty:
        print("\n实盘因子为空，无法对比")
        return
    if replay_df.empty:
        print("\n回放因子为空，无法对比")
        return

    live_df["end_time"] = live_df["end_time"].astype(str).str.zfill(6)

    merged = live_df.merge(replay_df, on=["code", "end_time"], how="inner",
                           suffixes=("_live", "_replay"))

    print(f"\n{'=' * 70}")
    print(f"对比: 实盘 {len(live_df)} 条, 回放 {len(replay_df)} 条, 匹配 {len(merged)} 条")
    print(f"股票数: 实盘 {live_df['code'].nunique()}, 回放 {replay_df['code'].nunique()}, "
          f"匹配 {merged['code'].nunique() if not merged.empty else 0}")
    print(f"{'=' * 70}")

    if merged.empty:
        print(f"\n  实盘 code 样本: {live_df['code'].head(5).tolist()}")
        print(f"  回放 code 样本: {replay_df['code'].head(5).tolist()}")
        print(f"  实盘 end_time: {sorted(live_df['end_time'].unique())[:5]}")
        print(f"  回放 end_time: {sorted(replay_df['end_time'].unique())[:5]}")
        return

    # 字段级对比
    print(f"\n{'字段':<25s} {'匹配率':>8s} {'avg_diff':>10s} {'max_diff':>10s}")
    print("-" * 60)

    field_stats = []
    for field in COMPARE_FIELDS:
        lc, rc = f"{field}_live", f"{field}_replay"
        if lc not in merged.columns or rc not in merged.columns:
            continue
        lv = pd.to_numeric(merged[lc], errors="coerce")
        rv = pd.to_numeric(merged[rc], errors="coerce")
        valid = ~(lv.isna() & rv.isna())
        n = valid.sum()
        if n == 0:
            continue
        lv, rv = lv[valid], rv[valid]
        abs_diff = (lv - rv).abs()
        denom = lv.abs().replace(0, np.nan)
        rel = (abs_diff / denom * 100).fillna(0)
        rate = (rel < 1.0).sum() / n * 100
        tag = "OK" if rate > 95 else ("WARN" if rate > 80 else "BAD")
        print(f"  {field:<25s} {rate:>6.1f}% {rel.mean():>9.2f}% {rel.max():>9.2f}%  [{tag}]")
        field_stats.append((field, rate))

    if field_stats:
        print(f"\n  总体匹配率: {np.mean([s[1] for s in field_stats]):.1f}%")

    # 逐时间点
    print(f"\n逐时间点统计:")
    for ts in sorted(merged["end_time"].unique()):
        sub = merged[merged["end_time"] == ts]
        lc, rc = "latest_price_live", "latest_price_replay"
        prices_ok = 0
        total = 0
        if lc in sub.columns and rc in sub.columns:
            lv = pd.to_numeric(sub[lc], errors="coerce")
            rv = pd.to_numeric(sub[rc], errors="coerce")
            valid = ~(lv.isna() & rv.isna())
            total = valid.sum()
            if total > 0:
                diff = (lv[valid] - rv[valid]).abs()
                denom = lv[valid].abs().replace(0, np.nan)
                rel = (diff / denom * 100).fillna(0)
                prices_ok = (rel < 0.1).sum()
        print(f"  {ts}: {len(sub)} 只, 价格匹配={prices_ok}/{total}")

    # 抽样
    print(f"\n抽样对比（前5只）:")
    for code in merged["code"].unique()[:5]:
        sub = merged[merged["code"] == code].sort_values("end_time")
        for _, row in sub.head(1).iterrows():
            parts = []
            for field in ["latest_price", "change_pct", "vwap", "total_vol"]:
                lv = row.get(f"{field}_live")
                rv = row.get(f"{field}_replay")
                if pd.notna(lv) and pd.notna(rv):
                    diff = abs(float(lv) - float(rv))
                    ok = "OK" if diff < 0.01 else "!!"
                    parts.append(f"{field}={float(lv):.4f}/{float(rv):.4f}({ok})")
            print(f"  {code} {row.get('end_time','?')}  {' | '.join(parts)}")


def main():
    args = parse_args()
    date_str = args.date
    minutes = args.minutes

    print("=" * 70)
    print(f"因子全市场对比验证 — {date_str}")
    print(f"对比范围: 开盘后前 {minutes} 分钟, 每文件读前 {args.nrows:,} 行")
    print("=" * 70)

    print(f"\n[1/3] 从 OSS 拉实盘因子:")
    live_df, target_times = load_live_factors_from_oss(date_str, minutes)
    if live_df.empty:
        print("实盘因子为空，无法继续")
        sys.exit(1)

    print(f"\n[2/3] 读通联数据:")
    tick_records, deal_records, order_records = load_tonglance(
        args.msg_dir, date_str, args.nrows
    )

    print(f"\n[3/3] 回放因子:")
    replay_df = replay_all_stocks(tick_records, deal_records, order_records, date_str, target_times)
    print(f"  回放: {len(replay_df)} 条, 实盘: {len(live_df)} 条")

    compare(live_df, replay_df)
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
