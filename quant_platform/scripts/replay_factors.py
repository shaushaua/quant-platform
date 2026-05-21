#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
因子计算全市场对比验证

从 OSS 拉取实盘因子结果，从通联 CSV 回放因子计算，全市场对比。
只处理前几分钟（默认 5 分钟），不过滤具体股票。

用法：
    python3 -m quant_platform.scripts.replay_factors --date 20260521
    python3 -m quant_platform.scripts.replay_factors --date 20260521 --minutes 10
"""

import argparse
import gc
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from quant_platform.data.converter import TonglanceDataConverter
from quant_platform.factor.base import StockState
from quant_platform.factor.examples.live_momentum import factor_calculation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

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


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup")
    p.add_argument("--minutes", type=int, default=5,
                   help="只处理开盘后前几分钟（默认5）")
    p.add_argument("--chunk-size", type=int, default=500_000,
                   help="通联 CSV 每次读取行数")
    return p.parse_args()


# ============================================================
# 1. 从 OSS 拉实盘因子
# ============================================================

def load_live_factors_from_oss(date_str, max_minutes):
    """从 OSS 拉实盘因子，只取开盘前 max_minutes 分钟的结果。"""
    import oss2

    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    if not ak or not sk:
        print("错误: OSS_ACCESS_KEY_ID 或 OSS_ACCESS_KEY_SECRET 未设置")
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

    # 列出该日期所有因子文件
    all_times = []
    for obj in oss2.ObjectIterator(bucket, prefix=search_prefix):
        if not obj.key.endswith(".json"):
            continue
        fname = obj.key.split("/")[-1].replace(".json", "")
        if fname.isdigit() and len(fname) == 6:
            all_times.append(fname)

    all_times.sort()
    print(f"  OSS 上共 {len(all_times)} 个时间点: {all_times[:5]}...{all_times[-3:] if len(all_times)>5 else ''}")

    # 只取开盘前 max_minutes 分钟
    # 开盘 09:30, 只取到 09:30 + max_minutes
    cutoff_h = 9
    cutoff_m = 30 + max_minutes
    if cutoff_m >= 60:
        cutoff_h += cutoff_m // 60
        cutoff_m = cutoff_m % 60
    cutoff_str = f"{cutoff_h:02d}{cutoff_m:02d}00"

    target_times = [t for t in all_times if t <= cutoff_str]
    if not target_times:
        # 回退：取前 max_minutes 个
        target_times = all_times[:max_minutes]

    print(f"  对比时间点 ({len(target_times)} 个): {target_times}")

    # 加载每个时间点的 JSON
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
# 2. 流式读通联 CSV + 转换
# ============================================================

def load_tonglance_streaming(msg_dir, date_str, cutoff_time, chunk_size):
    """流式读取通联 CSV，只读到 cutoff_time 为止，返回转换后的 tick/deal/order DataFrames。

    cutoff_time: datetime，如 2026-05-21 09:35:00
    """
    day_dir = Path(msg_dir) / date_str
    trading_day = datetime.strptime(date_str, "%Y%m%d")
    converter = TonglanceDataConverter()

    all_ticks, all_deals, all_orders = [], [], []

    def _read_streaming(label, filename, convert_fn, is_order_deal=False):
        p = day_dir / filename
        if not p.exists():
            print(f"    {label}: 不存在")
            return

        mb = p.stat().st_size / 1024 / 1024
        print(f"    {label}: {p.name} ({mb:.0f}MB) 流式读取...", end="", flush=True)

        total_rows = 0
        matched_rows = 0
        done = False

        for chunk in pd.read_csv(p, chunksize=chunk_size, dtype=str, low_memory=False):
            if done:
                break
            total_rows += len(chunk)

            # 用 UpdateTime 判断是否超过 cutoff
            if "UpdateTime" in chunk.columns:
                # 只取 <= cutoff_time 的行
                # UpdateTime 格式: "09:30:00.000" 或 "HHMMSSmmm"
                times = chunk["UpdateTime"].astype(str).str.strip()
                # 简单按字符串前5字符 (HH:MM 或 HHMM) 比较
                time_prefix = cutoff_time.strftime("%H:%M")
                # 兼容两种格式: "09:30:xx" 或 "0930xxxxx"
                mask = times.str[:5] <= time_prefix
                if not mask.any():
                    # 当前 chunk 全部超过 cutoff，检查是否有刚好在边界的数据
                    # 如果 chunk 第一行就超过，可以停了
                    if (times.str[:5] > time_prefix).all():
                        done = True
                        break
                chunk = chunk[mask]
                if chunk.empty:
                    continue

            matched_rows += len(chunk)

            try:
                if is_order_deal:
                    orders, deals = convert_fn(chunk, trading_day)
                    if not orders.empty:
                        all_orders.append(orders)
                    if not deals.empty:
                        all_deals.append(deals)
                else:
                    result = convert_fn(chunk, trading_day)
                    if result is not None and not result.empty:
                        if isinstance(result, tuple):
                            # 不应该到这里，但防御性处理
                            pass
                        elif "CurrentPrice" in result.columns or "TradeNum" in result.columns:
                            all_ticks.append(result)
                        elif "SaleOrderID" in result.columns:
                            all_deals.append(result)
                        else:
                            all_orders.append(result)
            except Exception as e:
                print(f"\n      转换失败: {e}")

            # 进度
            print(f"\r    {label}: {p.name} ({mb:.0f}MB) 已读{total_rows:,}行 匹配{matched_rows:,}行", end="", flush=True)

            # 如果当前 chunk 全部在 cutoff 之前，继续读下一个
            # 如果最后一个 UpdateTime 已经远超 cutoff，可以停了
            if "UpdateTime" in chunk.columns:
                last_time = chunk["UpdateTime"].astype(str).str.strip().iloc[-1][:5]
                cutoff_prefix = cutoff_time.strftime("%H:%M")
                # 给一点余量：如果最后时间比 cutoff 超过 2 分钟就停
                try:
                    last_m = int(last_time[:2]) * 60 + int(last_time[3:5])
                    cut_m = cutoff_time.hour * 60 + cutoff_time.minute
                    if last_m > cut_m + 2:
                        done = True
                except (ValueError, IndexError):
                    pass

        print(f"\r    {label}: {p.name} ({mb:.0f}MB) 读完 {total_rows:,} 行, 匹配 {matched_rows:,} 行")

    print(f"  读 SH tick:")
    _read_streaming("SH tick", TL_FILES["sh_tick"],
                    lambda r, d: converter.convert_sh_tick(r, d))

    print(f"  读 SZ tick:")
    _read_streaming("SZ tick", TL_FILES["sz_tick"],
                    lambda r, d: converter.convert_sz_tick(r, d))

    print(f"  读 SH order+deal:")
    _read_streaming("SH order+deal", TL_FILES["sh_order_deal"],
                    lambda r, d: converter.convert_sh_order_deal(r, d),
                    is_order_deal=True)

    print(f"  读 SZ order:")
    _read_streaming("SZ order", TL_FILES["sz_order"],
                    lambda r, d: converter.convert_sz_order(r, d))

    print(f"  读 SZ deal:")
    _read_streaming("SZ deal", TL_FILES["sz_deal"],
                    lambda r, d: converter.convert_sz_deal(r, d))

    tick_df  = pd.concat(all_ticks,  ignore_index=True) if all_ticks  else pd.DataFrame()
    deal_df  = pd.concat(all_deals,  ignore_index=True) if all_deals  else pd.DataFrame()
    order_df = pd.concat(all_orders, ignore_index=True) if all_orders else pd.DataFrame()

    print(f"  合计: tick={len(tick_df):,} deal={len(deal_df):,} order={len(order_df):,}")

    # 释放中间列表
    del all_ticks, all_deals, all_orders
    gc.collect()

    return tick_df, deal_df, order_df


# ============================================================
# 3. 回放因子：模拟实盘引擎的行为
# ============================================================

def replay_all_stocks(tick_df, deal_df, order_df, date_str, target_times):
    """模拟实盘引擎：维护所有股票的 StockState，在每个时间点算因子。

    和 StreamingEngine 逻辑一致：
    1. 创建 per-stock StockState
    2. 按 Code 分组更新 tick/deal/order
    3. 在每个 end_time 调用 factor_calculation
    """
    trading_day = datetime.strptime(date_str, "%Y%m%d")
    all_codes = set()

    # 收集所有股票代码
    if not tick_df.empty and "Code" in tick_df.columns:
        all_codes.update(tick_df["Code"].unique())
    if not deal_df.empty and "Code" in deal_df.columns:
        all_codes.update(deal_df["Code"].unique())
    if not order_df.empty and "Code" in order_df.columns:
        all_codes.update(order_df["Code"].unique())

    print(f"  共 {len(all_codes)} 只股票")

    # 先按 Code 分组，避免重复 groupby
    tick_groups = {}
    deal_groups = {}
    order_groups = {}

    if not tick_df.empty and "Code" in tick_df.columns:
        for code, grp in tick_df.groupby("Code"):
            tick_groups[code] = grp.sort_values("Time")

    if not deal_df.empty and "Code" in deal_df.columns:
        for code, grp in deal_df.groupby("Code"):
            deal_groups[code] = grp.sort_values("Time")

    if not order_df.empty and "Code" in order_df.columns:
        for code, grp in order_df.groupby("Code"):
            order_groups[code] = grp.sort_values("Time")

    # 释放原始 df
    del tick_df, deal_df, order_df
    gc.collect()

    # 对每个时间点，初始化 StockState 并计算因子
    results = []

    for ts in target_times:
        h, m, s = int(ts[:2]), int(ts[2:4]), int(ts[4:6])
        cutoff = trading_day.replace(hour=h, minute=m, second=s)
        end_time_str = ts

        count = 0
        for code in all_codes:
            state = StockState(code=code)

            # tick 数据：累积到 cutoff
            if code in tick_groups:
                sub = tick_groups[code]
                mask = sub["Time"] <= cutoff
                if mask.any():
                    state.update_tick(sub[mask])

            # deal 数据：累积到 cutoff
            if code in deal_groups:
                sub = deal_groups[code]
                mask = sub["Time"] <= cutoff
                if mask.any():
                    state.update_deal(sub[mask])

            # order 数据：累积到 cutoff
            if code in order_groups:
                sub = order_groups[code]
                mask = sub["Time"] <= cutoff
                if mask.any():
                    state.update_order(sub[mask])

            # 和实盘引擎一样：只对有数据的股票计算因子
            if state.tick_count > 0 or state.deal_count > 0 or state.order_count > 0:
                try:
                    result = factor_calculation(state, code, date_str, end_time_str)
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

def compare(live_df, replay_df, compare_fields):
    if live_df.empty:
        print("\n实盘因子为空，无法对比")
        return
    if replay_df.empty:
        print("\n回放因子为空，无法对比")
        return

    # 统一 code 格式（回放结果已经带了 .XSHE/.XSHG 后缀）
    # end_time 格式统一
    live_df["end_time"] = live_df["end_time"].astype(str).str.zfill(6)

    merged = live_df.merge(replay_df, on=["code", "end_time"], how="inner",
                           suffixes=("_live", "_replay"))

    print(f"\n{'=' * 70}")
    print(f"对比: 实盘 {len(live_df)} 条, 回放 {len(replay_df)} 条, 匹配 {len(merged)} 条")
    live_codes = live_df["code"].nunique()
    replay_codes = replay_df["code"].nunique()
    merged_codes = merged["code"].nunique() if not merged.empty else 0
    print(f"股票数: 实盘 {live_codes}, 回放 {replay_codes}, 匹配 {merged_codes}")
    print(f"{'=' * 70}")

    if merged.empty:
        # 看看为什么匹配不上
        live_sample = live_df["code"].head(5).tolist()
        replay_sample = replay_df["code"].head(5).tolist()
        print(f"\n  实盘 code 样本: {live_sample}")
        print(f"  回放 code 样本: {replay_sample}")

        live_ts = sorted(live_df["end_time"].unique())[:5]
        replay_ts = sorted(replay_df["end_time"].unique())[:5]
        print(f"  实盘 end_time 样本: {live_ts}")
        print(f"  回放 end_time 样本: {replay_ts}")
        return

    # 字段级对比
    print(f"\n{'字段':<25s} {'匹配率':>8s} {'avg_diff':>10s} {'max_diff':>10s}")
    print("-" * 60)

    field_stats = []
    for field in compare_fields:
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
        field_stats.append((field, rate, rel.mean()))

    # 总体通过率
    if field_stats:
        avg_rate = np.mean([s[1] for s in field_stats])
        print(f"\n  总体匹配率: {avg_rate:.1f}%")

    # 逐时间点统计
    print(f"\n逐时间点统计:")
    for ts in sorted(merged["end_time"].unique()):
        sub = merged[merged["end_time"] == ts]
        n_match = len(sub)
        prices_ok = 0
        price_field = "latest_price"
        lc, rc = f"{price_field}_live", f"{price_field}_replay"
        if lc in sub.columns and rc in sub.columns:
            lv = pd.to_numeric(sub[lc], errors="coerce")
            rv = pd.to_numeric(sub[rc], errors="coerce")
            valid = ~(lv.isna() & rv.isna())
            if valid.sum() > 0:
                diff = (lv[valid] - rv[valid]).abs()
                denom = lv[valid].abs().replace(0, np.nan)
                rel = (diff / denom * 100).fillna(0)
                prices_ok = (rel < 0.1).sum()

        print(f"  {ts}: {n_match} 只, 价格匹配={prices_ok}/{valid.sum() if valid.sum()>0 else 0}")

    # 抽样打印几只股票
    print(f"\n抽样对比（前5只）:")
    sample_codes = merged["code"].unique()[:5]
    for code in sample_codes:
        sub = merged[merged["code"] == code].sort_values("end_time")
        for _, row in sub.iterrows():
            parts = []
            for field in ["latest_price", "change_pct", "vwap", "total_vol", "order_imbalance"]:
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

    print("=" * 70)
    print(f"因子全市场对比验证 — {date_str}")
    print(f"对比范围: 开盘后前 {minutes} 分钟")
    print("=" * 70)

    # 1. 从 OSS 拉实盘因子
    print(f"\n[1/3] 从 OSS 拉实盘因子:")
    cutoff_time = datetime.strptime(date_str, "%Y%m%d").replace(
        hour=9, minute=30 + minutes, second=0
    )
    live_df, target_times = load_live_factors_from_oss(date_str, minutes)

    if live_df.empty:
        print("实盘因子为空，无法继续")
        sys.exit(1)

    # 2. 读通联数据（流式，只读前几分钟）
    print(f"\n[2/3] 读通联数据（流式，到 {cutoff_time.strftime('%H:%M')}）:")
    tick_df, deal_df, order_df = load_tonglance_streaming(
        args.msg_dir, date_str, cutoff_time, args.chunk_size
    )

    # 3. 回放 + 对比
    print(f"\n[3/3] 回放因子（全市场）:")
    replay_df = replay_all_stocks(tick_df, deal_df, order_df, date_str, target_times)
    print(f"  回放: {len(replay_df)} 条, 实盘: {len(live_df)} 条")

    compare(live_df, replay_df, COMPARE_FIELDS)
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
