#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实盘因子数据完整性验证

直接从 OSS 读取因子结果，检查：
1. 每个时间点股票数量是否稳定
2. 是否有股票在中间时间点消失/出现
3. 关键字段（价格、成交量）是否有异常
4. 因子值是否连续变化（非跳变）

不需要读通联数据，零内存压力。

用法：
    python3 -m quant_platform.scripts.verify_live_factors --date 20260521
    python3 -m quant_platform.scripts.verify_live_factors --date 20260521 --full
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict

import numpy as np
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--full", action="store_true",
                   help="输出详细报告（默认只输出摘要）")
    return p.parse_args()


def load_all_factors(date_str):
    """从 OSS 加载该日期所有因子 JSON。"""
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

    # 列出所有 JSON 文件
    all_keys = []
    for obj in oss2.ObjectIterator(bucket, prefix=search_prefix):
        if obj.key.endswith(".json"):
            all_keys.append(obj.key)

    all_keys.sort()
    print(f"  OSS 上共 {len(all_keys)} 个因子文件")

    # 加载所有 JSON
    all_records = []
    time_stock_count = {}  # end_time -> stock_count
    for i, key in enumerate(all_keys):
        fname = key.split("/")[-1].replace(".json", "")
        try:
            data = bucket.get_object(key).read()
            records = json.loads(data)
            all_records.extend(records)
            time_stock_count[fname] = len(records)
        except Exception as e:
            print(f"    读取 {fname} 失败: {e}")

        if (i + 1) % 50 == 0:
            print(f"    已加载 {i+1}/{len(all_keys)} 个文件...", flush=True)

    print(f"  加载完成: {len(all_records)} 条记录")
    return pd.DataFrame(all_records), time_stock_count


def analyze(df, time_stock_count, full):
    """分析数据完整性。"""
    print(f"\n{'=' * 70}")
    print(f"数据完整性分析")
    print(f"{'=' * 70}")

    # 1. 每个时间点股票数量
    print(f"\n[1] 每个时间点股票数量:")
    times = sorted(time_stock_count.keys())
    counts = [time_stock_count[t] for t in times]

    if not counts:
        print("  无数据")
        return

    print(f"  总时间点: {len(times)}")
    print(f"  股票数量: min={min(counts)}, max={max(counts)}, "
          f"mean={np.mean(counts):.0f}, median={np.median(counts):.0f}")

    # 找异常时间点（股票数量偏差 > 20%）
    median_count = np.median(counts)
    anomalies = []
    for t, c in zip(times, counts):
        if abs(c - median_count) / median_count > 0.2:
            anomalies.append((t, c))

    if anomalies:
        print(f"  异常时间点（偏差>20%）: {len(anomalies)} 个")
        for t, c in anomalies[:10]:
            print(f"    {t}: {c} 只 (中位数={median_count:.0f})")
    else:
        print(f"  无异常时间点")

    # 按时段统计
    print(f"\n  按时段统计:")
    periods = {
        "盘前 (09:15-09:25)": ("091500", "092559"),
        "集合竞价 (09:25-09:30)": ("092500", "093059"),
        "早盘 (09:30-10:30)": ("093000", "103059"),
        "盘中 (10:30-11:30)": ("103000", "113059"),
        "午盘 (13:00-14:00)": ("130000", "140059"),
        "尾盘 (14:00-15:00)": ("140000", "150059"),
    }
    for label, (start, end) in periods.items():
        period_counts = [time_stock_count[t] for t in times if start <= t <= end]
        if period_counts:
            print(f"    {label}: {len(period_counts)} 个时间点, "
                  f"股票数 min={min(period_counts)} max={max(period_counts)} "
                  f"avg={np.mean(period_counts):.0f}")

    # 2. 股票连续性
    print(f"\n[2] 股票连续性:")

    # 只看交易时段 (09:30-15:00)
    trading_times = [t for t in times if "093000" <= t <= "150100"]
    if len(trading_times) < 2:
        print("  交易时段时间点不足，跳过")
    else:
        # 每只股票出现的时间点数量
        df["end_time"] = df["end_time"].astype(str).str.zfill(6)
        trading_df = df[df["end_time"].isin(trading_times)]
        code_time_count = trading_df.groupby("code")["end_time"].nunique()
        total_trading_times = len(trading_times)

        always_present = (code_time_count == total_trading_times).sum()
        never_absent_pct = always_present / len(code_time_count) * 100

        print(f"  交易时段: {total_trading_times} 个时间点")
        print(f"  股票总数: {len(code_time_count)}")
        print(f"  全程在场的股票: {always_present} ({never_absent_pct:.1f}%)")

        # 偶尔缺席的股票
        sometimes_absent = (code_time_count < total_trading_times * 0.9).sum()
        print(f"  缺席>10%时间点的股票: {sometimes_absent}")

        # 按缺席次数分布
        absent_counts = total_trading_times - code_time_count
        absent_dist = absent_counts.value_counts().sort_index()
        if full:
            print(f"\n  缺席次数分布:")
            for n_absent, n_stocks in absent_dist.head(10).items():
                print(f"    缺席{n_absent}个时间点: {n_stocks} 只股票")

        # 找缺席最多的股票
        most_absent = absent_counts.nlargest(5)
        if most_absent.iloc[0] > 0:
            print(f"\n  缺席最多的股票:")
            for code, n in most_absent.items():
                pct = n / total_trading_times * 100
                print(f"    {code}: 缺席{n}个时间点 ({pct:.1f}%)")

    # 3. 相邻时间点股票变化
    print(f"\n[3] 相邻时间点股票变化:")
    if len(trading_times) >= 2:
        big_drops = []
        for i in range(1, len(trading_times)):
            t_prev = trading_times[i - 1]
            t_curr = trading_times[i]
            prev_codes = set(trading_df[trading_df["end_time"] == t_prev]["code"])
            curr_codes = set(trading_df[trading_df["end_time"] == t_curr]["code"])

            disappeared = prev_codes - curr_codes
            appeared = curr_codes - prev_codes

            if disappeared and len(disappeared) > len(prev_codes) * 0.05:
                big_drops.append((t_prev, t_curr, len(disappeared), len(appeared)))

        if big_drops:
            print(f"  大量股票消失的时间点（>5%）: {len(big_drops)} 个")
            for t_prev, t_curr, n_dis, n_app in big_drops[:10]:
                print(f"    {t_prev} → {t_curr}: 消失{n_dis}, 新增{n_app}")
        else:
            print(f"  无大量股票消失的时间点")

    # 4. 关键字段异常检测
    print(f"\n[4] 关键字段异常:")
    if not df.empty and "latest_price" in df.columns:
        prices = pd.to_numeric(df["latest_price"], errors="coerce")
        total = len(prices)
        zero_price = (prices == 0).sum()
        neg_price = (prices < 0).sum()
        na_price = prices.isna().sum()

        print(f"  latest_price: {total} 条")
        print(f"    零价: {zero_price} ({zero_price/total*100:.1f}%)")
        print(f"    负价: {neg_price}")
        print(f"    NaN: {na_price}")

        # 交易时段的零价
        if trading_times:
            trading_prices = pd.to_numeric(
                trading_df["latest_price"], errors="coerce"
            )
            trading_zero = (trading_prices == 0).sum()
            trading_total = len(trading_prices)
            print(f"    交易时段零价: {trading_zero}/{trading_total} "
                  f"({trading_zero/trading_total*100:.1f}%)")

    if not df.empty and "total_vol" in df.columns:
        vols = pd.to_numeric(df["total_vol"], errors="coerce")
        # 成交量应该单调递增（对同一只股票）
        print(f"\n  total_vol (成交量):")
        print(f"    负值: {(vols < 0).sum()}")
        print(f"    NaN: {vols.isna().sum()}")

    if not df.empty and "vwap" in df.columns:
        vwaps = pd.to_numeric(df["vwap"], errors="coerce")
        print(f"\n  vwap:")
        print(f"    NaN: {vwaps.isna().sum()}")
        print(f"    零: {(vwaps == 0).sum()}")
        valid_vwap = vwaps[(vwaps > 0) & vwaps.notna()]
        if not valid_vwap.empty:
            print(f"    范围: {valid_vwap.min():.2f} ~ {valid_vwap.max():.2f}")

    # 5. 单只股票时间序列连续性（抽样检查）
    print(f"\n[5] 单只股票时间序列连续性（抽样5只）:")
    if trading_times and not trading_df.empty:
        sample_codes = trading_df["code"].value_counts().head(5).index
        for code in sample_codes:
            sub = trading_df[trading_df["code"] == code].sort_values("end_time")
            prices = pd.to_numeric(sub["latest_price"], errors="coerce")
            vols = pd.to_numeric(sub["total_vol"], errors="coerce")

            # 价格跳变检测
            if prices.notna().sum() > 1 and prices.max() > 0:
                price_changes = prices.diff().abs()
                # 相对跳变 > 5%
                rel_change = price_changes / prices * 100
                big_jumps = (rel_change > 5).sum()
            else:
                big_jumps = 0

            # 成交量应单调递增
            vol_decreases = (vols.diff() < 0).sum()

            # VWAP 合理性
            vwaps = pd.to_numeric(sub["vwap"], errors="coerce")
            valid_vwap = vwaps[vwaps > 0]
            vwap_ok = "OK" if len(valid_vwap) == 0 or valid_vwap.std() / valid_vwap.mean() < 0.1 else "WARN"

            n_points = len(sub)
            print(f"    {code}: {n_points}个时间点, "
                  f"价格跳变>5%={big_jumps}, "
                  f"成交量递减={vol_decreases}, "
                  f"VWAP={vwap_ok}")

    # 6. 总结
    print(f"\n{'=' * 70}")
    print(f"总结:")
    issues = []
    if anomalies:
        issues.append(f"有 {len(anomalies)} 个时间点股票数量异常")
    if sometimes_absent > 0:
        issues.append(f"有 {sometimes_absent} 只股票缺席>10%时间点")
    if big_drops:
        issues.append(f"有 {len(big_drops)} 个时间点大量股票消失")
    if zero_price / total > 0.3:
        issues.append(f"零价占比 {zero_price/total*100:.1f}%（含盘前正常）")

    if issues:
        print(f"  发现问题:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(f"  数据完整性良好，未发现明显问题")
    print(f"{'=' * 70}")


def main():
    args = parse_args()
    print("=" * 70)
    print(f"实盘因子数据完整性验证 — {args.date}")
    print("=" * 70)

    print(f"\n[1/2] 从 OSS 加载因子数据:")
    df, time_stock_count = load_all_factors(args.date)

    if df.empty:
        print("无数据")
        sys.exit(1)

    print(f"\n[2/2] 分析:")
    analyze(df, time_stock_count, args.full)


if __name__ == "__main__":
    main()
