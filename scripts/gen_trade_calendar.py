#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成交易日历 ConfigMap（A 股）。
用法: python gen_trade_calendar.py [--year 2026] > trade-calendar.yaml
依赖: pip install akshare
"""
import argparse
import datetime
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.date.today().year)
    args = parser.parse_args()

    try:
        import akshare as ak
    except ImportError:
        print("pip install akshare", file=sys.stderr)
        sys.exit(1)

    df = ak.tool_trade_date_hist_sina()
    dates = df["trade_date"].dt.strftime("%Y%m%d").tolist()
    year_dates = [d for d in dates if d.startswith(str(args.year))]

    # Output as ConfigMap
    print("apiVersion: v1")
    print("kind: ConfigMap")
    print("metadata:")
    print(f"  name: trade-calendar")
    print(f"  namespace: quant")
    print("data:")
    # Store as comma-separated for easy lookup
    print(f"  trading_days: \"{','.join(year_dates)}\"")
    print(f"  year: \"{args.year}\"")
    # Store count for sanity check
    print(f"  count: \"{len(year_dates)}\"")

    print(f"\n# Generated {len(year_dates)} trading days for {args.year}", file=sys.stderr)


if __name__ == "__main__":
    main()
