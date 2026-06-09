#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
内存探测脚本：模拟回测引擎一个 data_batch 的内存占用。

用法:
    # 基本测试（默认 200 只股票）
    python bench_memory.py --date 20250106

    # 测试不同 batch size
    python bench_memory.py --date 20250106 --batch-size 100 200 500

    # 带 factor_info（控制加载哪些数据）
    python bench_memory.py --date 20250106 --batch-size 200 --need-tick --need-deal

    # 只测 filter + build，不测策略调用
    python bench_memory.py --date 20250106 --batch-size 200 --no-strategy

    # 指定具体股票
    python bench_memory.py --date 20250106 --codes "000001.SZ,600000.SH"

环境变量:
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT
    DATA_PATH (default: /data)
"""

import argparse
import os
import resource
import sys
import gc
import time

import pandas as pd


def _rss_mb() -> float:
    """当前进程 RSS (MB)。macOS ru_maxrss 是 bytes, Linux 是 KB。"""
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return raw / 1024 / 1024  # bytes → MB
    return raw / 1024  # KB → MB


def _mem(label: str, baseline_mb: float = 0) -> float:
    """打印并返回当前 RSS 增量。"""
    gc.collect()
    rss = _rss_mb()
    delta = rss - baseline_mb
    print(f"  [MEM] {label:40s}  RSS={rss:8.1f}MB  Δ={delta:+8.1f}MB")
    return rss


def run_benchmark(args):
    from quant_platform.data.api import DataAPI
    from quant_platform.factor.engine import (
        _load_day_bundle,
        _build_security_id_map,
        _filter_codes_from_bundle,
        _stock_data_from_filtered,
        _code_to_id_qi,
        _FilteredCode,
        _normalize_multi_code_batch_results,
    )
    from quant_platform.factor.stock_data_builder import build_stock_data

    factor_info = {
        "need_l1_tick": args.need_tick,
        "need_l2_deal": args.need_deal,
        "need_l2_order": args.need_order,
        "market_count": 1,
    }

    api = DataAPI(mode="backtest", oss_base_path=args.data_path)

    # 获取股票列表
    if args.codes:
        all_codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        daily_basic = api.get_daily_data(args.date, "daily_basic")
        if daily_basic.empty:
            print(f"ERROR: daily_basic 为空，date={args.date}")
            sys.exit(1)
        col = next((c for c in ("ID_QI", "ts_code", "Code") if c in daily_basic.columns), None)
        if col is None:
            print(f"ERROR: 找不到股票代码列, columns={list(daily_basic.columns)}")
            sys.exit(1)
        all_codes = daily_basic[col].tolist()
        print(f"从 daily_basic 获取到 {len(all_codes)} 只股票")

    results = []

    for batch_size in args.batch_size:
        codes = all_codes[:batch_size]
        print(f"\n{'='*60}")
        print(f"Batch size: {batch_size} 只股票 (取前 {len(codes)} 只)")
        print(f"{'='*60}")

        baseline = _mem("baseline")

        # 1. 加载 bundle
        t0 = time.time()
        bundle = _load_day_bundle(args.date, factor_info, api, codes)
        load_s = time.time() - t0
        after_load = _mem(f"_load_day_bundle ({load_s:.2f}s)", baseline)

        # 打印 bundle 大小
        for attr in ("l1_tick", "l2_deal", "l2_order", "market"):
            df = getattr(bundle, attr)
            mb = df.memory_usage(deep=True).sum() / 1024 / 1024 if not df.empty else 0
            rows = len(df)
            print(f"         bundle.{attr:12s}  {rows:8d} rows  {mb:8.1f}MB")

        # 2. Filter + build StockData (模拟 worker 内部操作)
        _id_map = _build_security_id_map(bundle.market)
        security_id_map = {code: _id_map.get(_code_to_id_qi(code)) for code in codes}

        t0 = time.time()
        filtered_codes = _filter_codes_from_bundle(
            bundle, codes, security_id_map, factor_info,
        )
        filter_s = time.time() - t0
        after_filter = _mem(f"_filter_codes_from_bundle ({filter_s:.2f}s)", baseline)

        # 3. Build StockData map (模拟 data_map 构建)
        t0 = time.time()
        data_map = {}
        end_time = args.end_time or "093500"
        for fc in filtered_codes:
            data_map[fc.code] = _stock_data_from_filtered(
                fc, args.date, end_time,
                factor_info=factor_info, zero_copy=True,
            )
        build_s = time.time() - t0
        after_build = _mem(f"build data_map ({build_s:.2f}s, {len(data_map)} codes)", baseline)

        # 打印 per-code 内存估算
        if data_map:
            sample_code = list(data_map.keys())[0]
            sd = data_map[sample_code]
            sd_mem = 0
            for attr in ("l1_tick", "l2_deal", "l2_order", "market", "daily_basic"):
                df = getattr(sd, attr, pd.DataFrame())
                if isinstance(df, pd.DataFrame) and not df.empty:
                    sd_mem += df.memory_usage(deep=True).sum() / 1024 / 1024
            estimated_total = sd_mem * len(data_map)
            print(f"         per-code StockData ≈ {sd_mem:.2f}MB × {len(data_map)} = {estimated_total:.0f}MB (理论)")

        # 4. 可选：测试策略调用的内存峰值
        strategy_delta = 0
        if args.strategy_module and not args.no_strategy:
            try:
                import importlib
                mod = importlib.import_module(args.strategy_module)
                calc_fn = mod.factor_calculation
                t0 = time.time()
                raw = calc_fn(data_map, list(data_map.keys()), args.date, [end_time])
                strat_s = time.time() - t0
                after_strategy = _mem(f"factor_calculation ({strat_s:.2f}s)", baseline)
                strategy_delta = after_strategy - after_build

                # 检查返回格式
                if isinstance(raw, dict):
                    n_results = sum(1 for v in raw.values() if v is not None)
                    print(f"         策略返回 {n_results}/{len(data_map)} 只股票结果")
                else:
                    print(f"         策略返回类型: {type(raw).__name__}")
            except Exception as e:
                print(f"         策略调用失败: {e}")

        # 5. 清理
        del data_map
        del filtered_codes
        del bundle
        gc.collect()
        after_cleanup = _mem("cleanup", baseline)

        result = {
            "batch_size": batch_size,
            "codes": len(codes),
            "load_s": load_s,
            "filter_s": filter_s,
            "build_s": build_s,
            "bundle_mb": after_load - baseline,
            "filter_mb": after_filter - after_load,
            "build_mb": after_build - after_filter,
            "strategy_mb": strategy_delta,
            "peak_mb": max(after_load, after_filter, after_build, after_cleanup),
            "final_mb": after_cleanup - baseline,
        }
        results.append(result)

        print(f"\n  汇总:")
        print(f"    加载耗时: {load_s:.2f}s")
        print(f"    过滤耗时: {filter_s:.2f}s")
        print(f"    构建耗时: {build_s:.2f}s")
        print(f"    峰值 RSS: {result['peak_mb']:.0f}MB")
        print(f"    bundle 内存: {result['bundle_mb']:.0f}MB")
        print(f"    filter 内存: {result['filter_mb']:.0f}MB")
        print(f"    data_map 内存: {result['build_mb']:.0f}MB")

    # 打印汇总表
    if len(results) > 1:
        print(f"\n{'='*80}")
        print(f"{'BatchSize':>10} {'Load(s)':>10} {'Filter(s)':>10} {'Build(s)':>10} "
              f"{'BundleMB':>10} {'FilterMB':>10} {'DataMapMB':>10} {'PeakMB':>10}")
        print(f"{'-'*80}")
        for r in results:
            print(f"{r['batch_size']:>10d} {r['load_s']:>10.2f} {r['filter_s']:>10.2f} {r['build_s']:>10.2f} "
                  f"{r['bundle_mb']:>10.0f} {r['filter_mb']:>10.0f} {r['build_mb']:>10.0f} {r['peak_mb']:>10.0f}")

    # 内存建议
    print(f"\n内存建议:")
    for r in results:
        per_code = r['peak_mb'] / max(r['codes'], 1)
        full_market_5k = per_code * 5200
        print(f"  batch_size={r['batch_size']}: "
              f"per-code={per_code:.1f}MB, "
              f"全市场5200股预估峰值={full_market_5k:.0f}MB "
              f"({'⚠️ 可能 OOM' if full_market_5k > 6400 else '✅ OK on 8GB'})")


def main():
    parser = argparse.ArgumentParser(description="回测引擎内存探测")
    parser.add_argument("--date", required=True, help="交易日 YYYYMMDD")
    parser.add_argument("--batch-size", type=int, nargs="+", default=[200],
                        help="测试的 batch size 列表，如 100 200 500")
    parser.add_argument("--codes", default=None, help="指定股票列表，逗号分隔")
    parser.add_argument("--data-path", default=os.environ.get("DATA_PATH", "/data"),
                        help="OSS 数据路径")
    parser.add_argument("--end-time", default="093500", help="end_time (默认 093500)")
    parser.add_argument("--need-tick", action="store_true", default=True, help="加载 tick")
    parser.add_argument("--no-tick", dest="need_tick", action="store_false")
    parser.add_argument("--need-deal", action="store_true", default=True, help="加载 deal")
    parser.add_argument("--no-deal", dest="need_deal", action="store_false")
    parser.add_argument("--need-order", action="store_true", default=False, help="加载 order")
    parser.add_argument("--no-strategy", action="store_true", help="跳过策略调用测试")
    parser.add_argument("--strategy-module", default=None, help="策略模块路径（可选）")
    args = parser.parse_args()
    run_benchmark(args)


if __name__ == "__main__":
    main()
