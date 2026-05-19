#!/usr/bin/env python3
"""
验证实盘因子计算结果

对比 live-engine 输出的 change_pct 与通联原始数据。
在服务器上运行：
    python scripts/verify_live_factors.py
"""
import pandas as pd
from pathlib import Path
import datetime

FACTOR_DIR = Path("/data/quant/factors")
MSG_DIR = Path("/www/wwwroot/mdl/msg_backup")

# ---------- 1. 读最新因子结果 ----------
csv_files = sorted(FACTOR_DIR.glob("*.csv"))
if not csv_files:
    print("没有因子结果文件")
    exit(1)

latest = csv_files[-1]
factors = pd.read_csv(latest)
print(f"因子结果: {latest.name}, {len(factors)} 只股票\n")

valid = factors[factors["latest_price"] > 0].copy()
print(f"有价格: {len(valid)} 只, 无价格: {len(factors) - len(valid)} 只")

# ---------- 2. 读通联原始 tick 数据 ----------
today = datetime.date.today().strftime("%Y%m%d")

sz_file = MSG_DIR / today / f"{today}_mdl_6_28_0.csv"
sh_file = MSG_DIR / today / f"{today}_mdl_4_4_0.csv"

results = []

for label, path, market in [("SZ", sz_file, "XSHE"), ("SH", sh_file, "XSHG")]:
    if not path.exists():
        print(f"{label} tick 文件不存在: {path}")
        continue

    print(f"\n读 {label} tick: {path.name} ({path.stat().st_size/1024/1024:.0f}MB)")
    try:
        raw = pd.read_csv(path, usecols=["SecurityID", "PreCloPrice", "LastPrice"])
    except ValueError as e:
        print(f"  列不存在: {e}")
        continue

    raw = raw[(raw["LastPrice"] > 0) & (raw["PreCloPrice"] > 0)].copy()
    last = raw.groupby("SecurityID").last().reset_index()
    last["code"] = last["SecurityID"].astype(str).str.zfill(6) + "." + market
    last["source_change_pct"] = (last["LastPrice"] - last["PreCloPrice"]) / last["PreCloPrice"] * 100
    last = last[["code", "LastPrice", "PreCloPrice", "source_change_pct"]]
    results.append(last)
    print(f"  {len(last)} 只股票有价格")

if not results:
    print("没有原始数据")
    exit(1)

source = pd.concat(results, ignore_index=True)

# ---------- 3. 对比 ----------
merged = valid.merge(source[["code", "source_change_pct", "PreCloPrice"]], on="code", how="inner")
print(f"\n{'='*70}")
print(f"对比: {len(merged)} 只股票")
print(f"{'='*70}")

if merged.empty:
    print("没有可对比的数据")
    exit(0)

merged["pct_diff"] = abs(merged["change_pct"] - merged["source_change_pct"]).round(4)

exact = (merged["pct_diff"] < 0.01).sum()
close = (merged["pct_diff"] < 0.1).sum()
large = (merged["pct_diff"] >= 1.0).sum()

print(f"  完全匹配 (< 0.01%): {exact} 只")
print(f"  接近匹配 (< 0.1%):  {close} 只")
print(f"  偏差较大 (>= 1%):   {large} 只")

print(f"\n样本对比（前10只）:")
sample = merged.head(10)
for _, row in sample.iterrows():
    status = "OK" if row["pct_diff"] < 0.1 else "XX"
    print(f"  {status} {row['code']:12s}  "
          f"factor={row['change_pct']:+7.3f}%  "
          f"source={row['source_change_pct']:+7.3f}%  "
          f"diff={row['pct_diff']:.3f}%")

if large > 0:
    print(f"\n偏差最大的 5 只:")
    worst = merged.nlargest(5, "pct_diff")
    for _, row in worst.iterrows():
        print(f"  XX {row['code']:12s}  "
              f"factor={row['change_pct']:+7.3f}%  "
              f"source={row['source_change_pct']:+7.3f}%  "
              f"diff={row['pct_diff']:.3f}%")
