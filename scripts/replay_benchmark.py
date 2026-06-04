#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Full-pipeline replay benchmark using captured PML data.

Replays recorded market data through the complete pipeline:
  PML frames → Rust parse → MemoryStore (ShmStockBuffer mmap) → warm
  → persistent pool workers → DataFrame + factor compute → timing

Usage:
    # Basic replay (parse only)
    python scripts/replay_benchmark.py /path/to/pml_capture.bin

    # Full pipeline with factor module
    python scripts/replay_benchmark.py /path/to/pml_capture.bin \
        --factor quant_platform.factor.examples.protected_eillen_strategy \
        --workers 12 --compute

    # Multi-round benchmark
    python scripts/replay_benchmark.py /path/to/pml_capture.bin \
        --factor quant_platform.factor.examples.protected_eillen_strategy \
        --workers 12 --rounds 3
"""

import argparse
import importlib
import os
import struct
import sys
import time
import multiprocessing
import tempfile
import shutil

# Add project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd

from quant_platform.core.constants import TICK_COLUMNS, DEAL_COLUMNS, ORDER_COLUMNS
from quant_platform.data.memory_store import MemoryStore
from quant_platform.factor.base import StockData, StockState


# ---- PML frame reader ----

def read_frames(path):
    """Read all frames from a capture file."""
    frames = []
    with open(path, "rb") as f:
        while True:
            header = f.read(20)  # 8 + 4 + 4 + 4
            if len(header) < 20:
                break
            ts, mid, seq_id, buf_len = struct.unpack("<dIII", header)
            buf = f.read(buf_len)
            if len(buf) < buf_len:
                break
            frames.append((ts, mid, seq_id, buf))
    return frames


# ---- Replay into MemoryStore ----

MID_SH_TICK = 4
MID_SH_NGTS = 24
MID_SZ_TICK = 28
MID_SZ_ORDER = 33
MID_SZ_DEAL = 36


def replay_into_store(frames, store, trading_day):
    """Replay all frames through Rust parser into MemoryStore.
    Returns (tick_count, order_count, deal_count, error_count, parse_ms)."""
    import mdl_parser

    tick_count = 0
    order_count = 0
    deal_count = 0
    error_count = 0
    t0 = time.perf_counter()

    for ts, mid, seq_id, buf in frames:
        try:
            if mid == MID_SH_TICK:
                result = mdl_parser.parse_sh_tick(buf, trading_day, seq_id)
                if result:
                    code, tup = result
                    store.append_tick(code, tup)
                    tick_count += 1
            elif mid == MID_SH_NGTS:
                result = mdl_parser.parse_sh_ngts(buf, trading_day)
                if result:
                    code, order_tup, deal_tup = result
                    if order_tup:
                        store.append_order(code, order_tup)
                        order_count += 1
                    if deal_tup:
                        store.append_deal(code, deal_tup)
                        deal_count += 1
            elif mid == MID_SZ_TICK:
                result = mdl_parser.parse_sz_tick(buf, trading_day, seq_id)
                if result:
                    code, tup = result
                    store.append_tick(code, tup)
                    tick_count += 1
            elif mid == MID_SZ_ORDER:
                result = mdl_parser.parse_sz_order(buf, trading_day, seq_id)
                if result:
                    code, tup = result
                    store.append_order(code, tup)
                    order_count += 1
            elif mid == MID_SZ_DEAL:
                result = mdl_parser.parse_sz_deal(buf, trading_day, seq_id)
                if result:
                    code, tup = result
                    store.append_deal(code, tup)
                    deal_count += 1
        except Exception as exc:
            error_count += 1
            if error_count <= 5:
                print(f"  ERROR mid={mid} seq={seq_id}: {exc}")

    parse_ms = (time.perf_counter() - t0) * 1000
    return tick_count, order_count, deal_count, error_count, parse_ms


# ---- Warm benchmark ----

def bench_warm(store):
    """Warm all Rust buffers and measure timing."""
    tick_codes = list(store._tick_buf.keys())
    deal_codes = list(store._deal_buf.keys())
    order_codes = list(store._order_buf.keys())
    if not tick_codes and not deal_codes and not order_codes:
        return 0, 0

    t0 = time.perf_counter()
    tick_upd = store.warm_tick_batch(tick_codes)
    deal_upd = store.warm_deal_batch(deal_codes)
    order_upd = store.warm_order_batch(order_codes)
    warm_ms = (time.perf_counter() - t0) * 1000
    return max(len(tick_codes), len(deal_codes), len(order_codes)), warm_ms


# ---- Compute benchmark ----

def _compute_one_stock(args):
    """Worker function: read mmap → build DataFrame → compute factor."""
    code, date_str, end_time, factor_fn, tick_path, deal_path, order_path, trading_day = args
    try:
        import mdl_parser as _mdl

        tick_df = pd.DataFrame()
        deal_df = pd.DataFrame()
        order_df = pd.DataFrame()

        if tick_path and os.path.exists(tick_path):
            try:
                reader = _mdl.ShmBufferReader(tick_path)
                if reader.len > 0:
                    arr = reader.to_numpy()
                    buf_cols = TICK_COLUMNS[2:]
                    base = pd.Timestamp(trading_day)
                    data = {'TradingDay': trading_day, 'Code': code}
                    for i, col in enumerate(buf_cols):
                        if col in ('Time', 'UpdateTime'):
                            data[col] = base + pd.to_timedelta(arr[:, i], unit='s')
                        else:
                            data[col] = arr[:, i]
                    tick_df = pd.DataFrame(data, columns=TICK_COLUMNS)
            except Exception:
                pass

        if deal_path and os.path.exists(deal_path):
            try:
                reader = _mdl.ShmBufferReader(deal_path)
                if reader.len > 0:
                    arr = reader.to_numpy()
                    buf_cols = DEAL_COLUMNS[2:]
                    base = pd.Timestamp(trading_day)
                    data = {'TradingDay': trading_day, 'Code': code}
                    for i, col in enumerate(buf_cols):
                        if col in ('Time', 'UpdateTime'):
                            data[col] = base + pd.to_timedelta(arr[:, i], unit='s')
                        else:
                            data[col] = arr[:, i]
                    deal_df = pd.DataFrame(data, columns=DEAL_COLUMNS)
            except Exception:
                pass

        if order_path and os.path.exists(order_path):
            try:
                reader = _mdl.ShmBufferReader(order_path)
                if reader.len > 0:
                    arr = reader.to_numpy()
                    buf_cols = ORDER_COLUMNS[2:]
                    base = pd.Timestamp(trading_day)
                    data = {'TradingDay': trading_day, 'Code': code}
                    for i, col in enumerate(buf_cols):
                        if col in ('Time', 'UpdateTime'):
                            data[col] = base + pd.to_timedelta(arr[:, i], unit='s')
                        else:
                            data[col] = arr[:, i]
                    order_df = pd.DataFrame(data, columns=ORDER_COLUMNS)
            except Exception:
                pass

        if factor_fn is None:
            # No factor: just measure data prep
            return code, {"code": code, "tick_rows": len(tick_df),
                          "deal_rows": len(deal_df), "order_rows": len(order_df)}, None

        stock_data = StockData(
            code=code, date=date_str, end_time=end_time,
            l1_tick=tick_df, l2_deal=deal_df, l2_order=order_df,
        )
        result = factor_fn(stock_data, code, date_str, end_time)
        return code, result, None
    except Exception as exc:
        return code, None, str(exc)


def bench_compute(store, factor_fn, n_workers, date_str):
    """Benchmark factor computation with persistent pool + mmap."""
    codes = list(store._tick_buf.keys())
    if not codes:
        return 0, 0, 0

    end_time = "093000"
    trading_day = store.get_trading_day()

    # Get mmap paths
    tick_paths = store.get_buffer_paths("tick", codes)
    deal_paths = store.get_buffer_paths("deal", codes)
    order_paths = store.get_buffer_paths("order", codes)

    args = [
        (code, date_str, end_time, factor_fn,
         tick_paths.get(code, ""), deal_paths.get(code, ""), order_paths.get(code, ""),
         trading_day)
        for code in codes
    ]

    results = []
    errors = 0
    t0 = time.perf_counter()

    if n_workers > 1:
        ctx = multiprocessing.get_context('fork')
        pool = ctx.Pool(processes=n_workers)
        try:
            for code, result, err in pool.imap_unordered(_compute_one_stock, args, chunksize=32):
                if err:
                    errors += 1
                elif result is not None:
                    results.append(result)
        finally:
            pool.close()
            pool.join()
    else:
        for arg in args:
            code, result, err = _compute_one_stock(arg)
            if err:
                errors += 1
            elif result is not None:
                results.append(result)

    compute_ms = (time.perf_counter() - t0) * 1000
    return len(results), errors, compute_ms


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description="Full-pipeline replay benchmark")
    parser.add_argument("capture", help="Path to PML capture file")
    parser.add_argument("--factor", default="", help="Factor module path")
    parser.add_argument("--workers", type=int, default=1, help="Number of compute workers")
    parser.add_argument("--compute", action="store_true", help="Run factor compute benchmark")
    parser.add_argument("--rounds", type=int, default=1, help="Number of compute rounds")
    args = parser.parse_args()

    # Setup
    frames = read_frames(args.capture)
    print(f"=== PML Replay Benchmark ===")
    print(f"Capture: {args.capture}")
    print(f"Frames:  {len(frames)}")

    # Classify frames
    by_type = {}
    for ts, mid, seq_id, buf in frames:
        by_type.setdefault(mid, []).append((ts, seq_id, buf))
    for mid, items in sorted(by_type.items()):
        name = {MID_SH_TICK: "SH_tick", MID_SH_NGTS: "SH_ngts",
                MID_SZ_TICK: "SZ_tick", MID_SZ_ORDER: "SZ_order",
                MID_SZ_DEAL: "SZ_deal"}.get(mid, f"unknown_{mid}")
        print(f"  {name} (mid={mid}): {len(items)} frames")

    # Load factor module
    factor_fn = None
    if args.factor:
        mod = importlib.import_module(args.factor)
        factor_fn = mod.factor_calculation
        print(f"Factor:  {args.factor}")

    print(f"Workers: {args.workers}")
    print()

    # Create temp shm dir for mmap buffers
    shm_dir = tempfile.mkdtemp(prefix="quant_bench_")
    os.environ["SHM_DIR"] = shm_dir

    try:
        trading_day = time.strftime("%Y%m%d")

        # Phase 1: Parse + ingest
        print("--- Phase 1: Parse + Ingest ---")
        store = MemoryStore.get_instance()
        store.set_trading_day(trading_day)

        tick_c, order_c, deal_c, err_c, parse_ms = replay_into_store(frames, store, trading_day)
        print(f"  Parse: {parse_ms:.0f}ms | tick={tick_c} order={order_c} deal={deal_c} err={err_c}")

        # Phase 2: Warm (ShmStockBuffer)
        print("--- Phase 2: Warm (ShmStockBuffer mmap) ---")
        n_stocks, warm_ms = bench_warm(store)
        print(f"  Warm:  {warm_ms:.0f}ms | {n_stocks} stocks")

        # Phase 3: Compute
        if args.compute or factor_fn:
            print(f"--- Phase 3: Compute ({args.rounds} round{'s' if args.rounds > 1 else ''}) ---")
            for r in range(args.rounds):
                n_res, n_err, compute_ms = bench_compute(
                    store, factor_fn, args.workers, trading_day
                )
                print(f"  Round {r+1}: {compute_ms:.0f}ms | {n_res} results, {n_err} errors, "
                      f"{len(store._tick_buf)} stocks, {args.workers} workers")

                # Per-stock timing estimate
                n_stocks = len(store._tick_buf)
                if n_stocks > 0 and compute_ms > 0:
                    per_stock = compute_ms / n_stocks * args.workers
                    print(f"           ~{per_stock:.1f}ms/stock (wall), "
                          f"{n_stocks/args.workers * per_stock:.0f}ms estimated per worker")
        else:
            print("--- Phase 3: Compute (skipped, use --compute or --factor) ---")

        # Summary
        print(f"\n=== Summary ===")
        print(f"  Stocks: {n_stocks}")
        print(f"  Parse:  {parse_ms:.0f}ms")
        print(f"  Warm:   {warm_ms:.0f}ms")
        if args.compute or factor_fn:
            print(f"  Compute: {compute_ms:.0f}ms ({args.workers} workers)")
            print(f"  TOTAL:  {parse_ms + warm_ms + compute_ms:.0f}ms")

    finally:
        # Cleanup
        shutil.rmtree(shm_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
