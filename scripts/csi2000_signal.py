#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""中证2000信号手动入口(核心逻辑在 quant_platform/inference/csi2000_product.py)。

日常无需手动跑 — 引擎内 csi2000_signal schedule 每交易日 15:12 自动产出。
本脚本用于: 首次验证 / 回补历史日期 / CronJob 已废弃不用。

用法(pod/镜像环境):
    python3 scripts/csi2000_signal.py                     # 跑今天
    python3 scripts/csi2000_signal.py --date 20261102     # 跑指定日
    python3 scripts/csi2000_signal.py --wait-daily        # 等 daily 因子就绪再跑
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quant_platform.inference.csi2000_product import (  # noqa: E402
    run_csi2000_signal, daily_factors_ready,
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="交易日 YYYYMMDD (默认=今天)")
    ap.add_argument("--wait-daily", action="store_true",
                    help="等待当天 daily 因子上传 OSS 后再运行")
    args = ap.parse_args()

    from datetime import datetime
    trading_day = args.date or datetime.now().strftime("%Y%m%d")

    if args.wait_daily:
        t0 = time.time()
        while time.time() - t0 < 3600:
            if daily_factors_ready(trading_day):
                break
            print(f"[csi2000] 等待 daily 因子上传 ({int(time.time()-t0)}s)...", flush=True)
            time.sleep(60)
        else:
            print(f"等待 daily 因子超时: {trading_day}")
            return 1

    targets = run_csi2000_signal(trading_day)
    print(f"[csi2000] 完成: {len(targets)} 只 targets ({trading_day})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
