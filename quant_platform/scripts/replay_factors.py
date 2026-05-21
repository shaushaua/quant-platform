#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子计算抽检验证

只读通联 CSV 前几分钟的数据，对比开盘后几个时间点的因子。
快速、省内存。

用法：
    python3 -m quant_platform.scripts.replay_factors --date 20260521
    python3 -m quant_platform.scripts.replay_factors --date 20260521 --codes 000001.XSHE 600000.XSHG
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from quant_platform.data.converter import TonglanceDataConverter
from quant_platform.factor.base import StockState
from quant_platform.factor.examples.live_momentum import factor_calculation

TL_FILES = {
    "sh_tick":       "mdl_4_4_0.csv",
    "sz_tick":       "mdl_6_28_0.csv",
    "sh_order_deal": "mdl_4_24_0.csv",
    "sz_order":      "mdl_6_33_0.csv",
    "sz_deal":       "mdl_6_36_0.csv",
}

# 只对比这几个时间点
CHECK_TIMES = ["093000", "093100", "093500", "094000"]
COMPARE_FIELDS = [
    "latest_price", "high", "low", "change_pct", "spread",
    "vwap", "total_vol", "deal_count",
    "order_imbalance", "order_buy_vol_ratio", "cancel_ratio", "order_count",
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup")
    p.add_argument("--codes", nargs="*", default=["000001.XSHE", "600000.XSHG", "000012.XSHE"])
    p.add_argument("--nrows", type=int, default=200_000,
                   help="每个 CSV 读前多少行（覆盖前几分钟）")
    return p.parse_args()


def load_live_factors_from_oss(date_str, codes, times):
    """从 OSS 拉实盘因子，只取目标股票和时间点。"""
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

    # 找目标时间点的 JSON 文件
    search_prefix = f"{prefix}/{year}/{year}{month}/{date_str}/"
    all_records = []

    for ts in times:
        # 文件名就是时间戳: 093000.json
        key = f"{search_prefix}{ts}.json"
        try:
            data = bucket.get_object(key).read()
            records = json.loads(data)
            all_records.extend(records)
        except Exception:
            pass  # 该时间点可能没有文件

    if not all_records:
        # 回退旧路径
        for obj in oss2.ObjectIterator(bucket, prefix=search_prefix):
            if not obj.key.endswith(".json"):
                continue
            # 从文件名提取时间
            fname = obj.key.split("/")[-1].replace(".json", "")
            if fname in times:
                try:
                    data = bucket.get_object(obj.key).read()
                    all_records.extend(json.loads(data))
                except Exception:
                    pass

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df = df[df["code"].isin(codes)]
    print(f"  实盘因子: {len(df)} 条 ({len(codes)} 只 × {len(times)} 个时间点)")
    return df


def load_tonglance_head(msg_dir, date_str, target_codes, nrows):
    """只读通联 CSV 前 nrows 行，过滤目标股票后转换。"""
    day_dir = Path(msg_dir) / date_str
    trading_day = datetime.strptime(date_str, "%Y%m%d")
    converter = TonglanceDataConverter()

    # 目标股票 ID（通联 CSV 里 SecurityID 就是股票代码）
    target_ids = {c.split(".")[0].zfill(6) for c in target_codes}

    all_ticks, all_deals, all_orders = [], [], []

    def _read_head(label, filename, convert_fn, id_col="SecurityID"):
        p = day_dir / filename
        if not p.exists():
            print(f"    {label}: 不存在")
            return

        mb = p.stat().st_size / 1024 / 1024
        print(f"    {label}: {p.name} ({mb:.0f}MB) 读前{nrows:,}行", end="")

        try:
            raw = pd.read_csv(p, nrows=nrows)
            # 过滤目标股票
            if id_col in raw.columns:
                raw[id_col] = raw[id_col].astype(str).str.zfill(6)
                raw = raw[raw[id_col].isin(target_ids)]
            print(f" 匹配{len(raw):,}行", end="")

            if raw.empty:
                print()
                return

            result = convert_fn(raw)
            del raw

            if isinstance(result, tuple):
                orders, deals = result
                if not orders.empty:
                    all_orders.append(orders)
                if not deals.empty:
                    all_deals.append(deals)
                print(f" -> orders={len(orders):,} deals={len(deals):,}")
            else:
                if not result.empty:
                    if "tick" in label.lower():
                        all_ticks.append(result)
                    elif "deal" in label.lower():
                        all_deals.append(result)
                    else:
                        all_orders.append(result)
                    print(f" -> {len(result):,} 行")
                else:
                    print(" -> 空")
        except Exception as e:
            print(f" 失败: {e}")

    print(f"  读 SH tick:")
    _read_head("SH tick", TL_FILES["sh_tick"],
               lambda r: converter.convert_sh_tick(r, trading_day))

    print(f"  读 SZ tick:")
    _read_head("SZ tick", TL_FILES["sz_tick"],
               lambda r: converter.convert_sz_tick(r, trading_day))

    print(f"  读 SH order+deal:")
    _read_head("SH order+deal", TL_FILES["sh_order_deal"],
               lambda r: converter.convert_sh_order_deal(r, trading_day))

    print(f"  读 SZ order:")
    _read_head("SZ order", TL_FILES["sz_order"],
               lambda r: converter.convert_sz_order(r, trading_day))

    print(f"  读 SZ deal:")
    _read_head("SZ deal", TL_FILES["sz_deal"],
               lambda r: converter.convert_sz_deal(r, trading_day))

    tick_df  = pd.concat(all_ticks,  ignore_index=True) if all_ticks  else pd.DataFrame()
    deal_df  = pd.concat(all_deals,  ignore_index=True) if all_deals  else pd.DataFrame()
    order_df = pd.concat(all_orders, ignore_index=True) if all_orders else pd.DataFrame()

    print(f"  合计: tick={len(tick_df):,} deal={len(deal_df):,} order={len(order_df):,}")
    return tick_df, deal_df, order_df


def replay_at_times(tick_df, deal_df, order_df, codes, times, date_str):
    """在指定时间点回放因子。"""
    results = []

    for ts in times:
        h, m, s = int(ts[:2]), int(ts[2:4]), int(ts[4:6])
        cutoff = datetime.strptime(date_str, "%Y%m%d").replace(hour=h, minute=m, second=s)

        for code in codes:
            state = StockState(code=code)

            if not tick_df.empty and "Code" in tick_df.columns:
                sub = tick_df[(tick_df["Code"] == code) & (tick_df["Time"] <= cutoff)]
                if not sub.empty:
                    state.update_tick(sub)

            if not deal_df.empty and "Code" in deal_df.columns:
                sub = deal_df[(deal_df["Code"] == code) & (deal_df["Time"] <= cutoff)]
                if not sub.empty:
                    state.update_deal(sub)

            if not order_df.empty and "Code" in order_df.columns:
                sub = order_df[(order_df["Code"] == code) & (order_df["Time"] <= cutoff)]
                if not sub.empty:
                    state.update_order(sub)

            result = factor_calculation(state, code, date_str, ts)
            if result:
                results.append(result)

    return pd.DataFrame(results)


def compare(live_df, replay_df):
    if live_df.empty or replay_df.empty:
        print("\n数据不足，无法对比")
        return

    merged = live_df.merge(replay_df, on=["code", "end_time"], how="inner",
                           suffixes=("_live", "_replay"))

    print(f"\n{'=' * 70}")
    print(f"对比: {len(merged)} 条记录")
    print(f"{'=' * 70}")

    if merged.empty:
        print("无匹配")
        return

    print(f"\n{'字段':<25s} {'匹配':>6s} {'avg_diff':>10s} {'max_diff':>10s}")
    print("-" * 55)

    for field in COMPARE_FIELDS:
        lc, rc = f"{field}_live", f"{field}_replay"
        if lc not in merged.columns or rc not in merged.columns:
            continue

        lv = merged[lc].astype(float)
        rv = merged[rc].astype(float)
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
        print(f"  {field:<25s} {rate:>5.0f}% {rel.mean():>9.2f}% {rel.max():>9.2f}%  [{tag}]")

    # 逐条对比
    print(f"\n逐条对比:")
    for _, row in merged.iterrows():
        code = row["code"]
        ts = row["end_time"]
        parts = []
        for field in ["latest_price", "change_pct", "vwap", "total_vol", "order_imbalance"]:
            lv = row.get(f"{field}_live")
            rv = row.get(f"{field}_replay")
            if pd.notna(lv) and pd.notna(rv):
                diff = abs(float(lv) - float(rv))
                ok = "OK" if diff < 0.01 else "!!"
                parts.append(f"{field}={float(lv):.4f}/{float(rv):.4f}({ok})")
        print(f"  {code} {ts}  {' | '.join(parts)}")


def main():
    args = parse_args()
    date_str = args.date
    codes = args.codes

    print("=" * 70)
    print(f"因子抽检验证 — {date_str}")
    print(f"股票: {codes}")
    print(f"时间点: {CHECK_TIMES}")
    print("=" * 70)

    for key in ("OSS_ACCESS_KEY_ID", "OSS_ACCESS_KEY_SECRET"):
        if not os.environ.get(key):
            print(f"错误: {key} 未设置")
            sys.exit(1)

    # 1. 从 OSS 拉实盘因子
    print(f"\n[1/3] 从 OSS 拉实盘因子:")
    live_df = load_live_factors_from_oss(date_str, codes, CHECK_TIMES)

    # 2. 读通联前几行
    print(f"\n[2/3] 读通联数据（前{args.nrows:,}行）:")
    tick_df, deal_df, order_df = load_tonglance_head(args.msg_dir, date_str, codes, args.nrows)

    # 3. 回放 + 对比
    print(f"\n[3/3] 回放因子:")
    replay_df = replay_at_times(tick_df, deal_df, order_df, codes, CHECK_TIMES, date_str)
    print(f"  回放: {len(replay_df)} 条, 实盘: {len(live_df)} 条")

    compare(live_df, replay_df)
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
