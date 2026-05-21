#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实盘数据准确性验证：CSV 统计 vs OSS 因子结果

用 awk 从通联 CSV 统计每只股票的成交量/笔数，
与实盘因子最后一个时间点的 total_vol/deal_count 对比。
纯 awk 统计，不需要读整个文件到 Python 内存。

用法：
    python3 -m quant_platform.scripts.verify_data_accuracy --date 20260521
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup")
    return p.parse_args()


TL_FILES = {
    "sz_deal":       "mdl_6_36_0.csv",
    "sh_order_deal": "mdl_4_24_0.csv",
}


def load_last_factors(date_str):
    """从 OSS 加载最后一个时间点的因子结果。"""
    import oss2

    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    ep = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
    bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
    prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")
    ep_host = ep.replace("https://", "").replace("http://", "")

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

    if not all_times:
        return pd.DataFrame()

    all_times.sort()
    last_ts = all_times[-1]
    print(f"  最后时间点: {last_ts}")

    key = f"{search_prefix}{last_ts}.json"
    data = bucket.get_object(key).read()
    records = json.loads(data)
    df = pd.DataFrame(records)
    print(f"  因子结果: {len(df)} 条")

    # 统一 code 格式，去掉 .XSHG/.XSHE 后缀方便跟 CSV SecurityID 对比
    df["sid"] = df["code"].str.split(".").str[0]
    return df


def awk_count_deal_sz(csv_path):
    """用 awk 从 SZ deal CSV 统计每只股票的成交量+笔数。

    mdl_6_36_0.csv 列顺序: ChannelNo,ApplSeqNum,MDStreamID,BidApplSeqNum,
        OfferApplSeqNum,SecurityID,SecurityIDSource,LastPx,LastQty,ExecType,...
    SecurityID=第6列, LastQty=第9列
    """
    awk_script = 'NR>1 && substr($6,1,1)~/^[03]/{vol[$6]+=$9; cnt[$6]++} END{for(s in vol) print s,cnt[s],vol[s]}'
    print(f"    SZ deal ({csv_path.stat().st_size/1024/1024/1024:.1f}GB): awk 统计...", end="", flush=True)
    try:
        result = subprocess.run(
            ["awk", "-F,", awk_script, str(csv_path)],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            print(f" 失败: {result.stderr[:200]}")
            return {}
        if result.stderr:
            print(f" stderr: {result.stderr[:200]}")

        lines = result.stdout.strip().split("\n")
        stats = {}
        for line in lines:
            parts = line.strip().split()
            if len(parts) >= 3:
                sid = parts[0].zfill(6)
                stats[sid] = {"deal_count": int(parts[1]), "total_vol": float(parts[2])}
        print(f" {len(stats)} 只股票")
        return stats
    except Exception as e:
        print(f" 失败: {e}")
        return {}


def awk_count_deal_sh(csv_path):
    """用 awk 从 SH order+deal CSV 统计成交数据。

    mdl_4_24_0.csv: Type=="T" 的是成交，SecurityID=第2列, Qty=第6列, Type=第8列
    """
    awk_script = 'NR==1{next} $5=="T" && substr($3,1,1)~/^[69]/{vol[$3]+=$9; cnt[$3]++} END{for(s in vol) print s,cnt[s],vol[s]}'
    print(f"    SH deal ({csv_path.stat().st_size/1024/1024/1024:.1f}GB): awk 统计...", end="", flush=True)
    try:
        result = subprocess.run(
            ["awk", "-F,", awk_script, str(csv_path)],
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            print(f" 失败: {result.stderr[:200]}")
            return {}
        if result.stderr:
            print(f" stderr: {result.stderr[:200]}")

        lines = result.stdout.strip().split("\n")
        stats = {}
        for line in lines:
            parts = line.strip().split()
            if len(parts) >= 3:
                sid = parts[0].zfill(6)
                stats[sid] = {"deal_count": int(parts[1]), "total_vol": float(parts[2])}
        print(f" {len(stats)} 只股票")
        return stats
    except Exception as e:
        print(f" 失败: {e}")
        return {}


def compare(factor_df, csv_stats, label):
    """对比实盘因子 vs CSV 统计。"""
    if factor_df.empty or not csv_stats:
        print(f"\n  {label}: 数据不足")
        return

    csv_df = pd.DataFrame([
        {"sid": sid, f"csv_deal_count": v["deal_count"], f"csv_total_vol": v["total_vol"]}
        for sid, v in csv_stats.items()
    ])

    merged = factor_df.merge(csv_df, on="sid", how="inner")
    n = len(merged)
    print(f"\n  {label}: 匹配 {n} 只股票")

    if n == 0:
        return

    # 成交量对比
    vol_live = pd.to_numeric(merged["total_vol"], errors="coerce")
    vol_csv = merged["csv_total_vol"]

    valid = vol_live.notna() & vol_csv.notna() & (vol_csv > 0)
    nv = valid.sum()
    if nv > 0:
        ratio = vol_live[valid] / vol_csv[valid] * 100
        within_1pct = ((ratio > 99) & (ratio < 101)).sum()
        within_5pct = ((ratio > 95) & (ratio < 105)).sum()

        print(f"  成交量: 实盘 vs CSV")
        print(f"    差异<1%:  {within_1pct}/{nv} ({within_1pct/nv*100:.1f}%)")
        print(f"    差异<5%:  {within_5pct}/{nv} ({within_5pct/nv*100:.1f}%)")
        print(f"    中位数比率: {ratio.median():.2f}%")
        print(f"    平均比率: {ratio.mean():.2f}%")

        bad = ratio[(ratio < 90) | (ratio > 110)]
        if len(bad) > 0:
            print(f"    差异>10%: {len(bad)} 只")
            worst = ratio.abs().sub(100).abs().nlargest(5)
            for idx in worst.index:
                code = merged.loc[idx, "code"]
                lv = vol_live.loc[idx]
                cv = vol_csv.loc[idx]
                r = ratio.loc[idx]
                print(f"      {code}: 实盘={lv:,.0f} CSV={cv:,.0f} 比率={r:.1f}%")

    # 成交笔数对比
    cnt_live = pd.to_numeric(merged["deal_count"], errors="coerce")
    cnt_csv = merged["csv_deal_count"]

    valid = cnt_live.notna() & cnt_csv.notna() & (cnt_csv > 0)
    nv = valid.sum()
    if nv > 0:
        ratio = cnt_live[valid] / cnt_csv[valid] * 100
        within_5pct = ((ratio > 95) & (ratio < 105)).sum()
        print(f"  成交笔数: 实盘 vs CSV")
        print(f"    差异<5%: {within_5pct}/{nv} ({within_5pct/nv*100:.1f}%)")
        print(f"    中位数比率: {ratio.median():.2f}%")

    return {
        "vol_within_5pct": within_5pct if nv > 0 else 0,
        "total": nv if nv > 0 else 0,
    }


def main():
    args = parse_args()
    date_str = args.date
    day_dir = Path(args.msg_dir) / date_str

    print("=" * 70)
    print(f"实盘数据准确性验证 — {date_str}")
    print(f"方法: awk 从 CSV 统计成交量/笔数 vs OSS 实盘因子结果")
    print("=" * 70)

    # 1. OSS 因子结果
    print(f"\n[1/3] 从 OSS 加载最后一个时间点的因子结果:")
    factor_df = load_last_factors(date_str)
    if factor_df.empty:
        print("无因子结果")
        sys.exit(1)

    # 2. CSV 统计
    print(f"\n[2/3] 从 CSV 统计成交量（awk）:")

    all_csv_stats = {}

    # SZ deal
    p = day_dir / TL_FILES["sz_deal"]
    if p.exists():
        sz_stats = awk_count_deal_sz(p)
        all_csv_stats.update(sz_stats)

    # SH order+deal (只取 Type=T 的成交)
    p = day_dir / TL_FILES["sh_order_deal"]
    if p.exists():
        sh_stats = awk_count_deal_sh(p)
        # 合并（SH 和 SZ 的 SecurityID 不会重叠）
        all_csv_stats.update(sh_stats)

    print(f"  CSV 统计合计: {len(all_csv_stats)} 只股票")

    # 3. 对比
    print(f"\n[3/3] 对比:")
    print(f"{'=' * 70}")

    # 分别对比 SH 和 SZ
    sh_factor = factor_df[factor_df["code"].str.endswith(".XSHG")]
    sz_factor = factor_df[factor_df["code"].str.endswith(".XSHE")]

    sh_csv = {sid: v for sid, v in all_csv_stats.items() if sid.startswith(("6", "9"))}
    sz_csv = {sid: v for sid, v in all_csv_stats.items() if sid.startswith(("0", "3"))}

    r1 = compare(sh_factor, sh_csv, "SH 股票")
    r2 = compare(sz_factor, sz_csv, "SZ 股票")

    # 总结
    print(f"\n{'=' * 70}")
    print(f"总结:")

    total_ok = 0
    total = 0
    for r in [r1, r2]:
        if r:
            total_ok += r["vol_within_5pct"]
            total += r["total"]

    if total > 0:
        pct = total_ok / total * 100
        if pct > 95:
            print(f"  成交量差异<5%的股票占 {pct:.1f}% ({total_ok}/{total})")
            print(f"  实盘 deal 数据与 CSV 原始数据高度一致，未发现数据丢失")
        else:
            print(f"  成交量差异<5%的股票占 {pct:.1f}% ({total_ok}/{total})")
            print(f"  可能存在 deal 数据丢失")
    else:
        print(f"  无有效对比数据")

    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
