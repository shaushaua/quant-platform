#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子计算全市场对比验证（严谨版）

先用 awk 预过滤通联 CSV（只保留股票 SecurityID），
再读入 Python 做因子计算和对比。

用法：
    python3 -m quant_platform.scripts.replay_factors --date 20260521
    python3 -m quant_platform.scripts.replay_factors --date 20260521 --minutes 5
"""

import argparse
import gc
import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict
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


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup")
    p.add_argument("--minutes", type=int, default=1,
                   help="对比开盘后前几分钟（默认1）")
    p.add_argument("--tmp-dir", default="/data/quant/tmp_replay",
                   help="awk 过滤临时文件目录")
    return p.parse_args()


def _safe_int(val):
    try:
        v = float(val)
        return 0 if np.isnan(v) else int(v)
    except (ValueError, TypeError):
        return 0


def _safe_float(val):
    try:
        v = float(val)
        return 0.0 if np.isnan(v) else v
    except (ValueError, TypeError):
        return 0.0


def _time_to_str(t):
    s = str(t).strip()
    if ":" in s:
        parts = s.split(":")
        return parts[0] + parts[1] + parts[2][:2]
    s = s.replace(".", "").ljust(9, "0")
    return s[:6]


def _time_le(t, cutoff_str):
    return _time_to_str(t) <= cutoff_str


# ============================================================
# awk 预过滤：只保留股票 SecurityID 的行
# ============================================================

def _awk_filter_csv(src_path, dst_path, sid_col_idx, stock_prefixes):
    """用 awk 过滤 CSV，只保留 SecurityID 以指定前缀开头的行。

    sid_col_idx: SecurityID 列的索引（0-based）
    stock_prefixes: 如 ["6", "9", "0", "3"]
    """
    # 构建 awk 条件: substr($col,1,1)=="6" || substr($col,1,1)=="9" || ...
    conditions = " || ".join(
        f'substr(${sid_col_idx},1,1)=="{p}"' for p in stock_prefixes
    )
    awk_script = f'BEGIN{{OFS=","}} NR==1 || {conditions} {{print}}'

    print(f"      awk 过滤...", end="", flush=True)
    try:
        result = subprocess.run(
            ["awk", "-F", ",", awk_script, str(src_path)],
            stdout=open(dst_path, "w"),
            stderr=subprocess.PIPE,
            timeout=600,
        )
        if result.returncode != 0:
            print(f" 失败: {result.stderr.decode()[:200]}")
            return False

        out_size = Path(dst_path).stat().st_size / 1024 / 1024
        # 统计行数
        wc = subprocess.run(["wc", "-l", dst_path], capture_output=True, text=True)
        n_lines = int(wc.stdout.strip().split()[0]) if wc.returncode == 0 else 0
        print(f" {n_lines:,}行 ({out_size:.0f}MB)")
        return True
    except Exception as e:
        print(f" 失败: {e}")
        return False


# ============================================================
# OSS 因子加载
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

    cutoff_h = 9
    cutoff_m = 30 + max_minutes
    if cutoff_m >= 60:
        cutoff_h += cutoff_m // 60
        cutoff_m = cutoff_m % 60
    cutoff_str = f"{cutoff_h:02d}{cutoff_m:02d}00"
    target_times = [t for t in all_times if "093000" <= t <= cutoff_str]
    if not target_times:
        target_times = all_times[:max_minutes]

    print(f"  对比时间点 ({len(target_times)} 个): {target_times}")

    all_records = []
    for ts in target_times:
        key = f"{search_prefix}{ts}.json"
        try:
            data = bucket.get_object(key).read()
            all_records.extend(json.loads(data))
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
# 读通联 CSV（awk 预过滤后）
# ============================================================

def load_tonglance(msg_dir, date_str, tmp_dir, cutoff_time_str):
    """用 awk 预过滤 CSV 只保留股票行，然后 Python 读取并转换。"""
    day_dir = Path(msg_dir) / date_str
    tmp = Path(tmp_dir)
    tmp.mkdir(parents=True, exist_ok=True)

    tick_records = []
    deal_records = []
    order_records = []

    # SH tick: SecurityID 在第2列 (idx=2), 股票前缀 6,9
    # 加上时间过滤：只保留 <= cutoff 的行
    def _process(label, src_file, filter_prefixes, sid_col_idx,
                 usecols, process_fn, is_order_deal=False,
                 time_col_idx=None, time_cutoff=None):
        src = day_dir / src_file
        if not src.exists():
            print(f"    {label}: 不存在")
            return

        mb = src.stat().st_size / 1024 / 1024
        print(f"    {label} ({mb:.0f}MB):")

        # awk 预过滤：只保留股票行
        filtered = tmp / f"{label.replace(' ', '_').replace('+', '_')}.csv"
        ok = _awk_filter_csv(src, filtered, sid_col_idx, filter_prefixes)
        if not ok or not filtered.exists() or filtered.stat().st_size == 0:
            print(f"      过滤结果为空")
            return

        # Python 读取过滤后的文件
        print(f"      Python 读取...", end="", flush=True)
        try:
            raw = pd.read_csv(filtered, usecols=usecols)
            print(f" {len(raw):,}行")
            if raw.empty:
                return

            process_fn(raw, tick_records, deal_records, order_records)
        except Exception as e:
            print(f" 失败: {e}")
        finally:
            # 清理临时文件
            try:
                filtered.unlink()
            except OSError:
                pass

    # SH tick 处理函数
    def process_sh_tick(raw, ticks, deals, orders):
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHG"
            t = str(r["UpdateTime"]).strip()
            ticks.append((code, t, {
                "CurrentPrice": _safe_float(r.get("LastPrice", 0)),
                "PreClosePrice": _safe_float(r.get("PreCloPrice", 0)),
                "HighPrice": _safe_float(r.get("HighPrice", 0)),
                "LowPrice": _safe_float(r.get("LowPrice", 0)),
                "AskPrice1": _safe_float(r.get("AskPrice1", 0)),
                "BidPrice1": _safe_float(r.get("BidPrice1", 0)),
                "AskVolume1": _safe_int(r.get("AskVolume1", 0)),
                "BidVolume1": _safe_int(r.get("BidVolume1", 0)),
            }))

    # SZ tick 处理函数
    def process_sz_tick(raw, ticks, deals, orders):
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHE"
            t = str(r["UpdateTime"]).strip()
            ticks.append((code, t, {
                "CurrentPrice": _safe_float(r.get("LastPrice", 0)),
                "PreClosePrice": _safe_float(r.get("PreCloPrice", 0)),
                "HighPrice": _safe_float(r.get("HighPrice", 0)),
                "LowPrice": _safe_float(r.get("LowPrice", 0)),
                "AskPrice1": _safe_float(r.get("AskPrice1", 0)),
                "BidPrice1": _safe_float(r.get("BidPrice1", 0)),
                "AskVolume1": _safe_int(r.get("AskVolume1", 0)),
                "BidVolume1": _safe_int(r.get("BidVolume1", 0)),
            }))

    # SH order+deal 处理函数
    def process_sh_order_deal(raw, ticks, deals, orders):
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
                deals.append((code, t, price, vol))
            else:
                orders.append((code, t, side, vol, otype))

    # SZ order 处理函数
    def process_sz_order(raw, ticks, deals, orders):
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHE"
            t = str(r["TransactTime"]).strip()
            side_val = r.get("Side")
            side = 0 if (side_val == 49 or str(side_val) == "49") else 1
            vol = _safe_int(r.get("OrderQty", 0))
            orders.append((code, t, side, vol, 1))

    # SZ deal 处理函数
    def process_sz_deal(raw, ticks, deals, orders):
        for _, r in raw.iterrows():
            sid = str(r["SecurityID"]).zfill(6)
            code = f"{sid}.XSHE"
            t = str(r["TransactTime"]).strip()
            price = _safe_float(r.get("LastPx", 0))
            vol = _safe_int(r.get("LastQty", 0))
            deals.append((code, t, price, vol))

    # 执行：SH tick (SecurityID 在第2列, 股票前缀 6,9)
    print(f"  读 SH tick:")
    _process("SH tick", TL_FILES["sh_tick"], ["6", "9"], 2,
             SH_TICK_COLS, process_sh_tick)

    print(f"  读 SZ tick:")
    _process("SZ tick", TL_FILES["sz_tick"], ["0", "3"], 3,
             SZ_TICK_COLS, process_sz_tick)

    print(f"  读 SH order+deal:")
    _process("SH order+deal", TL_FILES["sh_order_deal"], ["6", "9"], 2,
             SH_ORDER_DEAL_COLS, process_sh_order_deal)

    print(f"  读 SZ order:")
    _process("SZ order", TL_FILES["sz_order"], ["0", "3"], 2,
             SZ_ORDER_COLS, process_sz_order)

    print(f"  读 SZ deal:")
    _process("SZ deal", TL_FILES["sz_deal"], ["0", "3"], 2,
             SZ_DEAL_COLS, process_sz_deal)

    print(f"  合计: tick={len(tick_records):,} deal={len(deal_records):,} order={len(order_records):,}")
    return tick_records, deal_records, order_records


# ============================================================
# 回放因子
# ============================================================

def replay_all_stocks(tick_records, deal_records, order_records, date_str, target_times):
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

            for t, data in tick_by_code.get(code, []):
                if not _time_le(t, ts):
                    continue
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

            for t, price, vol in deal_by_code.get(code, []):
                if not _time_le(t, ts):
                    continue
                if price > 0 and vol > 0:
                    state.cum_amount += price * vol
                    state.cum_volume += vol
                state.deal_count += 1

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
# 对比（修复 NaN 处理）
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
        return

    # 字段级对比（修复：任一方为 NaN 都排除）
    print(f"\n{'字段':<25s} {'匹配率':>8s} {'avg_diff':>10s} {'max_diff':>10s}")
    print("-" * 60)

    field_stats = []
    for field in COMPARE_FIELDS:
        lc, rc = f"{field}_live", f"{field}_replay"
        if lc not in merged.columns or rc not in merged.columns:
            continue
        lv = pd.to_numeric(merged[lc], errors="coerce")
        rv = pd.to_numeric(merged[rc], errors="coerce")
        # 修复：排除任一方为 NaN 的行
        valid = lv.notna() & rv.notna()
        n = valid.sum()
        if n == 0:
            print(f"  {field:<25s} 无有效对比数据")
            continue

        lv, rv = lv[valid], rv[valid]
        # 排除双方都为 0 的行（不算匹配率）
        both_zero = (lv == 0) & (rv == 0)
        nonzero = ~both_zero
        n_nonzero = nonzero.sum()

        abs_diff = (lv - rv).abs()
        denom = lv.abs().replace(0, np.nan)
        rel = (abs_diff / denom * 100)
        rel = rel.fillna(0)

        if n_nonzero > 0:
            rate = (rel[nonzero] < 1.0).sum() / n_nonzero * 100
            avg_diff = rel[nonzero].mean()
            max_diff = rel[nonzero].max()
        else:
            rate = 100.0
            avg_diff = 0.0
            max_diff = 0.0

        tag = "OK" if rate > 95 else ("WARN" if rate > 80 else "BAD")
        print(f"  {field:<25s} {rate:>6.1f}% {avg_diff:>9.2f}% {max_diff:>9.2f}%  [{tag}] ({n}条, 非零{n_nonzero}条)")
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
            valid = lv.notna() & rv.notna()
            nonzero = valid & ~((lv == 0) & (rv == 0))
            total = nonzero.sum()
            if total > 0:
                diff = (lv[nonzero] - rv[nonzero]).abs()
                denom = lv[nonzero].abs().replace(0, np.nan)
                rel = (diff / denom * 100).fillna(0)
                prices_ok = (rel < 0.1).sum()
        print(f"  {ts}: {len(sub)} 只, 价格匹配={prices_ok}/{total}")

    # 抽样（排除零价）
    print(f"\n抽样对比（有价格的股票前10只）:")
    sample = merged[
        (pd.to_numeric(merged.get("latest_price_live", 0), errors="coerce") > 0) &
        (pd.to_numeric(merged.get("latest_price_replay", 0), errors="coerce") > 0)
    ]
    for code in sample["code"].unique()[:10]:
        row = sample[sample["code"] == code].iloc[0]
        parts = []
        for field in ["latest_price", "change_pct", "vwap", "total_vol"]:
            lv = row.get(f"{field}_live")
            rv = row.get(f"{field}_replay")
            if pd.notna(lv) and pd.notna(rv):
                lv_f, rv_f = float(lv), float(rv)
                diff = abs(lv_f - rv_f)
                ok = "OK" if diff < 0.01 else "!!"
                parts.append(f"{field}={lv_f:.4f}/{rv_f:.4f}({ok})")
        ts = row.get("end_time", "?")
        print(f"  {code} {ts}  {' | '.join(parts)}")


def main():
    args = parse_args()
    date_str = args.date
    minutes = args.minutes

    cutoff_time_str = f"{9 + (30 + minutes) // 60:02d}{(30 + minutes) % 60:02d}00"

    print("=" * 70)
    print(f"因子全市场对比验证 — {date_str}")
    print(f"对比范围: 09:30 ~ {cutoff_time_str} (前{minutes}分钟)")
    print("方法: awk 预过滤 CSV → Python 读取 → 因子计算 → 对比")
    print("=" * 70)

    print(f"\n[1/3] 从 OSS 拉实盘因子:")
    live_df, target_times = load_live_factors_from_oss(date_str, minutes)
    if live_df.empty:
        print("实盘因子为空")
        sys.exit(1)

    print(f"\n[2/3] 读通联数据 (awk 预过滤):")
    tick_records, deal_records, order_records = load_tonglance(
        args.msg_dir, date_str, args.tmp_dir, cutoff_time_str
    )

    print(f"\n[3/3] 回放因子:")
    replay_df = replay_all_stocks(tick_records, deal_records, order_records, date_str, target_times)
    print(f"  回放: {len(replay_df)} 条, 实盘: {len(live_df)} 条")

    compare(live_df, replay_df)
    print("\n" + "=" * 70)

    # 清理临时目录
    tmp = Path(args.tmp_dir)
    if tmp.exists():
        for f in tmp.glob("*.csv"):
            f.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
