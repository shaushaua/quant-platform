#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子计算回放对比工具

在 ECS 服务器上运行：
  1. 读通联原始 CSV（本地磁盘）
  2. 从 OSS 拉实盘因子结果
  3. 用同样的 converter + StockState + factor_calculation 重算
  4. 逐字段对比

用法：
    python3 scripts/replay_factors.py --date 20260520
    python3 scripts/replay_factors.py --date 20260520 --stocks 5 --timepoints 24
    python3 scripts/replay_factors.py --date 20260520 --codes 000001.XSHE 600000.XSHG
"""

import argparse
import io
import json
import os
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from quant_platform.data.converter import TonglanceDataConverter
from quant_platform.factor.base import StockState
from quant_platform.factor.examples.live_momentum import factor_calculation

COMPARE_FIELDS = [
    "latest_price", "high", "low", "change_pct", "spread",
    "price_pos", "buy_pressure",
    "vwap", "total_vol", "deal_count", "vol_ratio",
    "order_imbalance", "order_buy_vol_ratio", "cancel_ratio", "order_count",
]

TL_FILES = {
    "sh_tick":       "mdl_4_4_0.csv",
    "sz_tick":       "mdl_6_28_0.csv",
    "sh_order_deal": "mdl_4_24_0.csv",
    "sz_order":      "mdl_6_33_0.csv",
    "sz_deal":       "mdl_6_36_0.csv",
}


def parse_args():
    p = argparse.ArgumentParser(description="因子计算回放对比")
    p.add_argument("--date", required=True, help="日期 YYYYMMDD")
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup",
                   help="通联数据目录")
    p.add_argument("--stocks", type=int, default=10, help="对比股票数量")
    p.add_argument("--codes", nargs="*", help="指定股票代码")
    p.add_argument("--timepoints", type=int, default=0,
                   help="对比几个时间点（0=全部，推荐 24 快速验证）")
    return p.parse_args()


# ──────────────────────────────────────
# 从 OSS 读实盘因子结果
# ──────────────────────────────────────

def load_live_factors(date_str: str) -> pd.DataFrame:
    """从 OSS 读实盘因子 JSON。"""
    import oss2

    ak = os.environ["OSS_ACCESS_KEY_ID"]
    sk = os.environ["OSS_ACCESS_KEY_SECRET"]
    ep = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
    ep_host = ep.replace("https://", "").replace("http://", "")
    bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
    prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")

    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, ep_host, bucket_name)

    year = date_str[:4]
    month = date_str[4:6]

    # 尝试新路径: live-factors/2026/202605/20260520/*.json
    search_prefix = f"{prefix}/{year}/{year}{month}/{date_str}/"
    keys = [obj.key for obj in oss2.ObjectIterator(bucket, prefix=search_prefix)
            if obj.key.endswith(".json")]

    if not keys:
        # 回退旧路径: live-factors/2026/202605/20260520_*.json
        search_prefix = f"{prefix}/{year}/{year}{month}/{date_str}_"
        keys = [obj.key for obj in oss2.ObjectIterator(bucket, prefix=search_prefix)
                if obj.key.endswith(".json")]

    print(f"  OSS 因子文件: {len(keys)} 个 (prefix={search_prefix})")

    all_records = []
    for key in keys:
        try:
            data = bucket.get_object(key).read()
            records = json.loads(data)
            all_records.extend(records)
        except Exception as e:
            print(f"    读取失败 {key}: {e}")

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    print(f"  实盘因子: {len(df):,} 条, {df['code'].nunique()} 只股票")
    return df


# ──────────────────────────────────────
# 读通联原始 CSV 并转换
# ──────────────────────────────────────

def load_tonglance(msg_dir: str, date_str: str, target_codes: list):
    """读通联 CSV，转换为标准格式，只保留目标股票。"""
    day_dir = Path(msg_dir) / date_str
    trading_day = datetime.strptime(date_str, "%Y%m%d")
    converter = TonglanceDataConverter()

    all_ticks, all_deals, all_orders = [], [], []

    def _read(label, filename, fn):
        p = day_dir / f"{date_str}_{filename}"
        if not p.exists():
            print(f"    {label}: 不存在")
            return
        mb = p.stat().st_size / 1024 / 1024
        print(f"    {label}: {p.name} ({mb:.0f}MB)", end="")
        try:
            raw = pd.read_csv(p)
            result = fn(raw)
        except Exception as e:
            print(f" 失败: {e}")
            return

        if isinstance(result, tuple):
            orders, deals = result
            if not orders.empty:
                all_orders.append(orders)
            if not deals.empty:
                all_deals.append(deals)
            print(f" -> orders={len(orders):,} deals={len(deals):,}")
        else:
            if not result.empty:
                print(f" -> {len(result):,} 行")
            else:
                print(f" -> 空")

    print(f"  读 SH tick:")
    _read("SH tick", TL_FILES["sh_tick"],
          lambda r: converter.convert_sh_tick(r, trading_day))

    print(f"  读 SZ tick:")
    _read("SZ tick", TL_FILES["sz_tick"],
          lambda r: converter.convert_sz_tick(r, trading_day))

    print(f"  读 SH order+deal:")
    _read("SH order+deal", TL_FILES["sh_order_deal"],
          lambda r: converter.convert_sh_order_deal(r, trading_day))

    print(f"  读 SZ order:")
    _read("SZ order", TL_FILES["sz_order"],
          lambda r: converter.convert_sz_order(r, trading_day))

    print(f"  读 SZ deal:")
    _read("SZ deal", TL_FILES["sz_deal"],
          lambda r: converter.convert_sz_deal(r, trading_day))

    tick_df  = pd.concat(all_ticks,  ignore_index=True) if all_ticks  else pd.DataFrame()
    deal_df  = pd.concat(all_deals,  ignore_index=True) if all_deals  else pd.DataFrame()
    order_df = pd.concat(all_orders, ignore_index=True) if all_orders else pd.DataFrame()

    # 只保留目标股票
    for attr in ("tick_df", "deal_df", "order_df"):
        df = locals()[attr]
        if not df.empty and "Code" in df.columns:
            df = df[df["Code"].isin(target_codes)]
            locals()[attr] = df
            # hack: reassign in outer scope
            if attr == "tick_df":   tick_df  = df
            elif attr == "deal_df": deal_df  = df
            else:                   order_df = df

    print(f"  过滤后: tick={len(tick_df):,} deal={len(deal_df):,} order={len(order_df):,}")
    return tick_df, deal_df, order_df


# ──────────────────────────────────────
# 回放因子计算
# ──────────────────────────────────────

def build_state(tick_df, deal_df, order_df, code: str, cutoff) -> StockState:
    state = StockState(code=code)

    if not tick_df.empty and "Code" in tick_df.columns:
        mask = tick_df["Code"] == code
        if "Time" in tick_df.columns:
            mask &= tick_df["Time"] <= cutoff
        sub = tick_df[mask]
        if not sub.empty:
            state.update_tick(sub)

    if not deal_df.empty and "Code" in deal_df.columns:
        mask = deal_df["Code"] == code
        if "Time" in deal_df.columns:
            mask &= deal_df["Time"] <= cutoff
        sub = deal_df[mask]
        if not sub.empty:
            state.update_deal(sub)

    if not order_df.empty and "Code" in order_df.columns:
        mask = order_df["Code"] == code
        if "Time" in order_df.columns:
            mask &= order_df["Time"] <= cutoff
        sub = order_df[mask]
        if not sub.empty:
            state.update_order(sub)

    return state


def replay(tick_df, deal_df, order_df, codes, timestamps, date_str):
    results = []
    total = len(timestamps) * len(codes)

    for i, ts_str in enumerate(timestamps):
        h, m, s = int(ts_str[:2]), int(ts_str[2:4]), int(ts_str[4:6])
        cutoff = datetime.strptime(date_str, "%Y%m%d").replace(hour=h, minute=m, second=s)

        for code in codes:
            state = build_state(tick_df, deal_df, order_df, code, cutoff)
            result = factor_calculation(state, code, date_str, ts_str)
            if result is not None:
                results.append(result)

        if (i + 1) % 30 == 0 or i == len(timestamps) - 1:
            done = (i + 1) * len(codes)
            print(f"    {ts_str} ({done}/{total})")

    return pd.DataFrame(results)


# ──────────────────────────────────────
# 对比报告
# ──────────────────────────────────────

def compare_and_report(live_df: pd.DataFrame, replay_df: pd.DataFrame):
    merged = live_df.merge(
        replay_df, on=["code", "end_time"], how="inner",
        suffixes=("_live", "_replay"),
    )

    print(f"\n{'=' * 70}")
    print(f"对比: {len(merged):,} 条记录 (code × end_time)")
    print(f"{'=' * 70}")

    if merged.empty:
        print("无匹配记录")
        return

    # 逐字段
    print(f"\n{'字段':<25s} {'匹配率':>8s} {'avg_diff':>10s} {'max_diff':>10s} {'≥1%':>6s}")
    print("-" * 63)

    for field in COMPARE_FIELDS:
        lc, rc = f"{field}_live", f"{field}_replay"
        if lc not in merged.columns or rc not in merged.columns:
            print(f"  {field:<25s}  字段缺失")
            continue

        lv = merged[lc].astype(float)
        rv = merged[rc].astype(float)
        valid = ~(lv.isna() & rv.isna())
        n = valid.sum()
        if n == 0:
            print(f"  {field:<25s}  全部NaN")
            continue

        lv, rv = lv[valid], rv[valid]
        abs_diff = (lv - rv).abs()
        denom = lv.abs().replace(0, np.nan)
        rel = (abs_diff / denom * 100).fillna(0)

        rate = (rel < 1.0).sum() / n * 100
        tag = "OK" if rate > 95 else ("WARN" if rate > 80 else "BAD")
        print(f"  {field:<25s} {rate:>6.1f}% {rel.mean():>9.2f}% {rel.max():>9.2f}% {(rel >= 1).sum():>6d}  [{tag}]")

    # 整体
    total_m, total_n = 0, 0
    for field in COMPARE_FIELDS:
        lc, rc = f"{field}_live", f"{field}_replay"
        if lc not in merged.columns or rc not in merged.columns:
            continue
        lv = merged[lc].astype(float)
        rv = merged[rc].astype(float)
        valid = ~(lv.isna() & rv.isna())
        if valid.sum() == 0:
            continue
        lv, rv = lv[valid], rv[valid]
        denom = lv.abs().replace(0, np.nan)
        rel = ((lv - rv).abs() / denom * 100).fillna(0)
        total_m += (rel < 1.0).sum()
        total_n += len(rel)
    print(f"\n  整体: {total_m}/{total_n} = {total_m / total_n * 100 if total_n else 0:.1f}%")

    # 样本
    sample_codes = merged["code"].unique()[:3]
    unique_ts = sorted(merged["end_time"].unique())
    picks = []
    for t in ["0930", "1030", "1130", "1400", "1450"]:
        if unique_ts:
            picks.append(min(unique_ts, key=lambda x: abs(int(x[:4]) - int(t))))
    picks = list(dict.fromkeys(picks))[:5]

    print(f"\n样本对比（{len(sample_codes)} 只 × {len(picks)} 个时间点）:")
    show = ["latest_price", "change_pct", "vwap", "total_vol", "order_imbalance"]
    for ts in picks:
        print(f"\n  ── {ts} ──")
        sub = merged[(merged["end_time"] == ts) & (merged["code"].isin(sample_codes))]
        for _, row in sub.head(3).iterrows():
            parts = []
            for f in show:
                lv = row.get(f"{f}_live")
                rv = row.get(f"{f}_replay")
                if pd.notna(lv) and pd.notna(rv):
                    d = abs(float(lv) - float(rv))
                    tag = "OK" if d < 0.01 else "!!"
                    parts.append(f"{f}={float(lv):.2f}/{float(rv):.2f}")
            print(f"    {row['code']:12s}  {' | '.join(parts)}")


# ──────────────────────────────────────
# main
# ──────────────────────────────────────

def main():
    args = parse_args()
    date_str = args.date

    print("=" * 70)
    print(f"因子计算回放对比 — {date_str}")
    print("=" * 70)

    # 检查 OSS 凭据
    for key in ("OSS_ACCESS_KEY_ID", "OSS_ACCESS_KEY_SECRET"):
        if not os.environ.get(key):
            print(f"错误: 环境变量 {key} 未设置")
            print("export OSS_ACCESS_KEY_ID='...'")
            print("export OSS_ACCESS_KEY_SECRET='...'")
            sys.exit(1)

    # 1. 从 OSS 读实盘因子
    print(f"\n[1/3] 从 OSS 读实盘因子结果:")
    live_df = load_live_factors(date_str)
    if live_df.empty:
        print("实盘因子为空，退出")
        sys.exit(1)

    # 确定股票
    if args.codes:
        target_codes = args.codes
    else:
        valid = live_df[live_df["latest_price"] > 0]
        counts = valid.groupby("code").size().sort_values(ascending=False)
        target_codes = counts.head(args.stocks).index.tolist()
    print(f"  对比股票 ({len(target_codes)}): {target_codes[:5]}...")

    # 确定时间点
    timestamps = sorted(live_df["end_time"].unique().tolist())
    if args.timepoints > 0 and args.timepoints < len(timestamps):
        step = max(1, len(timestamps) // args.timepoints)
        timestamps = timestamps[::step][:args.timepoints]
    print(f"  时间点: {len(timestamps)} ({timestamps[0]} ~ {timestamps[-1]})")

    live_df = live_df[live_df["code"].isin(target_codes) & live_df["end_time"].isin(timestamps)]

    # 2. 从 ECS 磁盘读通联原始数据
    print(f"\n[2/3] 从 ECS 磁盘读通联原始数据:")
    tick_df, deal_df, order_df = load_tonglance(args.msg_dir, date_str, target_codes)

    # 3. 回放 + 对比
    print(f"\n[3/3] 回放因子计算:")
    replay_df = replay(tick_df, deal_df, order_df, target_codes, timestamps, date_str)
    print(f"  完成: {len(replay_df):,} 条")

    compare_and_report(live_df, replay_df)
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
