#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OSS → combined-engine 全流程回灌基准测试。

完整复用 combined_engine 的代码路径：
  OSS parquet → DataFrame → tuple → MemoryStore.append_tick/deal/order
  → ShmStockBuffer(mmap) → warm → mmap → 零拷贝 DataFrame → 因子计算

用法:
    python scripts/pipeline_benchmark.py --date 20250530 \
        --factor quant_platform.factor.examples.protected_eillen_strategy \
        --codes "000001.SZ,600000.SH,000002.SZ,600036.SH" \
        --workers 8 --rounds 3

    # 全市场（需要大内存节点）
    python scripts/pipeline_benchmark.py --date 20250530 \
        --factor quant_platform.factor.examples.protected_eillen_strategy \
        --workers 40 --rounds 1

    # 不指定 --codes 也不指定 --limit：加载全部股票
    python scripts/pipeline_benchmark.py --date 20250530 \
        --factor quant_platform.factor.examples.protected_eillen_strategy \
        --limit 100 --workers 8
"""

import argparse
import copy
import hashlib
import importlib
import logging
import multiprocessing
import os
import sys
import tempfile
import shutil
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from quant_platform.core.constants import TICK_COLUMNS, DEAL_COLUMNS, ORDER_COLUMNS
from quant_platform.data.memory_store import MemoryStore
from quant_platform.factor.base import StockData, StockState

# Import combined_engine's compute functions (same code path as live)
from quant_platform.live_engine.combined_engine import (
    _compute_stock_shm,
    _zerocopy_arr,
    _build_df_from_path,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Module-level globals for COW workers (same as combined_engine)
_factor_fn = None
_market_df = pd.DataFrame()
_daily_basic_df = pd.DataFrame()

# Monkey-patch combined_engine module globals so _compute_stock_shm sees them
import quant_platform.live_engine.combined_engine as _ce_mod


# ---- OSS helpers (same as oss_backtest.py) ----

def _oss_key(date_str, kind):
    year = date_str[:4]
    month = date_str[4:6]
    suffix = {"tick": "tick", "deal": "deal", "order": "order",
              "daily_basic": "daily_basic_data"}[kind]
    return f"{year}/{year}{month}/{date_str}/{date_str}_{suffix}.parquet"


def _download_cached(bucket, key, cache_dir):
    cache_name = hashlib.md5(key.encode()).hexdigest() + ".parquet"
    cached = os.path.join(cache_dir, cache_name)
    if os.path.exists(cached):
        try:
            with open(cached, "rb") as f:
                f.seek(-4, 2)
                if f.read(4) == b"PAR1":
                    return cached
        except Exception:
            pass
        os.remove(cached)
    os.makedirs(cache_dir, exist_ok=True)
    tmp = cached + f".{os.getpid()}.tmp"
    t0 = time.time()
    bucket.get_object_to_file(key, tmp)
    size = os.path.getsize(tmp)
    os.rename(tmp, cached)
    logger.info("Downloaded %s (%.1fMB, %.1fs)", key, size / 1e6, time.time() - t0)
    return cached


def _normalize_code(code):
    if code.endswith(".SZ"):
        return code[:-3] + ".XSHE"
    elif code.endswith(".SH"):
        return code[:-3] + ".XSHG"
    return code


def _denormalize_code(code):
    if code.endswith(".XSHE"):
        return code[:-5] + ".SZ"
    elif code.endswith(".XSHG"):
        return code[:-5] + ".SH"
    return code


def _resolve_security_ids(code_str, id_to_code):
    if not code_str:
        return None
    codes = [c.strip() for c in code_str.split(",")]
    code_to_id = {v: k for k, v in id_to_code.items()}
    sec_ids = [code_to_id[_normalize_code(c)] for c in codes if _normalize_code(c) in code_to_id]
    return sec_ids, codes


def _read_parquet(path, date_str, sec_ids=None):
    try:
        import duckdb
        if sec_ids:
            ids_str = ",".join(str(i) for i in sec_ids)
            df = duckdb.query(
                f"SELECT * FROM read_parquet('{path}') WHERE Code IN ({ids_str})"
            ).df()
        else:
            df = duckdb.query(f"SELECT * FROM read_parquet('{path}')").df()
    except Exception:
        df = pd.read_parquet(path)
    if "TradingDay" not in df.columns:
        df["TradingDay"] = date_str
    return df


def _map_code_column(df, id_to_code):
    if "Code" in df.columns and df["Code"].dtype in (np.int32, np.int64, np.float64):
        df["Code"] = df["Code"].astype(int).map(id_to_code)
        before = len(df)
        df = df.dropna(subset=["Code"])
        dropped = before - len(df)
        if dropped:
            logger.warning("Dropped %d rows with unmapped SECURITY_ID", dropped)
    return df


def _align_columns(df, target_columns):
    for col in target_columns:
        if col not in df.columns:
            df[col] = float("nan")
    base = pd.Timestamp(df["TradingDay"].iloc[0]) if "TradingDay" in df.columns else None
    for col in ("Time", "UpdateTime"):
        if col in df.columns:
            if not np.issubdtype(df[col].dtype, np.datetime64):
                if base is not None:
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except Exception:
                        pass
    return df[target_columns]


def _group_by_code(df):
    if df.empty or "Code" not in df.columns:
        return {}
    return {code: group for code, group in df.groupby("Code", sort=False)}


# ---- Phase 1: Load OSS data into per-stock groups ----

def load_oss_data(date_str, codes_str, limit, cache_dir):
    """Download and parse OSS parquet files. Returns (groups, daily_basic, id_to_code)."""
    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    endpoint = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
    bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")

    if not ak or not sk:
        print("ERROR: Set OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET")
        sys.exit(1)

    import oss2
    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)

    # daily_basic
    db_key = _oss_key(date_str, "daily_basic")
    try:
        db_path = _download_cached(bucket, db_key, cache_dir)
        daily_basic = pd.read_parquet(db_path)
    except Exception as e:
        logger.warning("daily_basic not found: %s", e)
        daily_basic = pd.DataFrame()

    id_to_code = {}
    if "ID_QI" in daily_basic.columns and "SECURITY_ID" in daily_basic.columns:
        for _, row in daily_basic.iterrows():
            id_qi = str(row["ID_QI"]).zfill(6)
            sec_id = int(row["SECURITY_ID"])
            market = "XSHG" if id_qi.startswith(("6", "9", "68")) else "XSHE"
            id_to_code[sec_id] = f"{id_qi}.{market}"
        print(f"  ID mapping: {len(id_to_code)} stocks")

    # Resolve SECURITY_IDs for predicate pushdown
    sec_filter = None
    if codes_str and id_to_code:
        sec_filter, _ = _resolve_security_ids(codes_str, id_to_code)
        if sec_filter:
            print(f"  DuckDB predicate pushdown: {len(sec_filter)} SECURITY_IDs")
    elif limit > 0 and id_to_code:
        # --limit: pick N random SECURITY_IDs from daily_basic
        import random
        all_ids = list(id_to_code.keys())
        random.shuffle(all_ids)
        sec_filter = all_ids[:limit]
        print(f"  DuckDB predicate pushdown (--limit {limit}): {len(sec_filter)} SECURITY_IDs")

    # Download tick/deal/order
    t_download = time.perf_counter()
    paths = {}
    for kind in ("tick", "deal", "order"):
        key = _oss_key(date_str, kind)
        try:
            paths[kind] = _download_cached(bucket, key, cache_dir)
        except Exception as e:
            logger.warning("Download %s failed: %s", kind, e)
    download_ms = (time.perf_counter() - t_download) * 1000
    print(f"  Download: {download_ms:.0f}ms")

    # Read & convert
    t_load = time.perf_counter()
    raw_groups = {}
    for kind, target_cols in [("tick", TICK_COLUMNS), ("deal", DEAL_COLUMNS), ("order", ORDER_COLUMNS)]:
        if kind not in paths:
            raw_groups[kind] = {}
            continue
        df = _read_parquet(paths[kind], date_str, sec_ids=sec_filter)
        df = _map_code_column(df, id_to_code)
        df = _align_columns(df, target_cols)

        # Filter by requested codes
        code_filter = None
        if codes_str:
            code_filter = set(_normalize_code(c.strip()) for c in codes_str.split(","))
        if code_filter and "Code" in df.columns:
            df = df[df["Code"].isin(code_filter)]

        groups = _group_by_code(df)
        raw_groups[kind] = groups
        total_rows = sum(len(g) for g in groups.values())
        print(f"  {kind}: {len(groups)} stocks, {total_rows} rows")
        del df

    # Determine stock list
    all_codes = sorted(
        set(raw_groups["tick"].keys())
        | set(raw_groups["deal"].keys())
        | set(raw_groups["order"].keys())
    )
    if limit > 0 and limit < len(all_codes):
        all_codes = all_codes[:limit]
        code_set = set(all_codes)
        for kind in raw_groups:
            raw_groups[kind] = {c: v for c, v in raw_groups[kind].items() if c in code_set}

    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"  Load: {load_ms:.0f}ms")
    print(f"  Total stocks: {len(all_codes)}")

    return raw_groups, daily_basic, all_codes


# ---- Phase 2: Feed into MemoryStore (ShmStockBuffer) ----

def _time_str(val):
    """Convert datetime/Timestamp to 'YYYYMMDD HH:MM:SS.mmm' string for Rust parser."""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y%m%d %H:%M:%S.") + str(val.microsecond // 1000).zfill(3)
    return str(val)


def feed_into_store(raw_groups, trading_day):
    """Feed DataFrames into MemoryStore via ShmStockBuffer batch writes.
    Uses buf.append_tuples() for bulk writes instead of per-row append."""
    store = MemoryStore.get_instance()
    store.set_trading_day(trading_day)

    t0 = time.perf_counter()
    counts = {"tick": 0, "deal": 0, "order": 0}
    time_cols = {"Time", "UpdateTime"}

    for kind, columns in [("tick", TICK_COLUMNS), ("deal", DEAL_COLUMNS), ("order", ORDER_COLUMNS)]:
        groups = raw_groups.get(kind, {})
        for code, df in groups.items():
            if df.empty:
                continue
            # Convert DataFrame rows to tuples, datetime→string for Rust parser
            tuples = []
            for row in df.itertuples(index=False):
                tup = tuple(_time_str(getattr(row, c, 0)) if c in time_cols else getattr(row, c, 0)
                            for c in columns)
                tuples.append(tup)
            buf = store._get_or_create_buf(kind, code)
            buf.append_tuples(tuples, 2)  # start_col=2, skip TradingDay/Code
            store._dirty_codes.add(code)
            getattr(store, f"_dirty_{kind}").add(code)
            counts[kind] += len(tuples)

    store._last_append_ts = time.time()
    feed_ms = (time.perf_counter() - t0) * 1000
    print(f"  Feed: {feed_ms:.0f}ms | tick={counts['tick']} deal={counts['deal']} order={counts['order']}")
    return store, counts


# ---- Phase 3: Warm (sync cache_len from ShmStockBuffer) ----

def warm_store(store):
    """Warm all buffers — syncs cache state."""
    tick_codes = list(store._tick_buf.keys())
    deal_codes = list(store._deal_buf.keys())
    order_codes = list(store._order_buf.keys())

    t0 = time.perf_counter()
    store.warm_tick_batch(tick_codes)
    store.warm_deal_batch(deal_codes)
    store.warm_order_batch(order_codes)
    warm_ms = (time.perf_counter() - t0) * 1000
    print(f"  Warm: {warm_ms:.1f}ms | {len(tick_codes)} tick, {len(deal_codes)} deal, {len(order_codes)} order")
    return warm_ms


# ---- Phase 4: Compute (persistent pool + mmap zero-copy) ----

def compute_factor(store, factor_fn, n_workers, date_str, trading_day, all_codes):
    """Run factor computation using the same persistent pool + mmap path as combined_engine."""
    # Set module-level globals for workers
    global _factor_fn, _market_df, _daily_basic_df
    _factor_fn = factor_fn
    _market_df = pd.DataFrame()
    _daily_basic_df = pd.DataFrame()
    _ce_mod._factor_fn = factor_fn
    _ce_mod._market_df = _market_df
    _ce_mod._daily_basic_df = _daily_basic_df

    end_time = "150000"
    wall_secs = 34200  # arbitrary

    # Get mmap paths
    tick_paths = store.get_buffer_paths("tick", all_codes)
    deal_paths = store.get_buffer_paths("deal", all_codes)
    order_paths = store.get_buffer_paths("order", all_codes)

    args = [
        (code, date_str, end_time, wall_secs, None,
         tick_paths.get(code, ""), deal_paths.get(code, ""), order_paths.get(code, ""),
         trading_day)
        for code in all_codes
    ]

    results = []
    errors = 0
    error_msgs = []
    t0 = time.perf_counter()

    if n_workers > 1:
        ctx = multiprocessing.get_context("fork")
        pool = ctx.Pool(processes=n_workers)
        try:
            for code, result, err in pool.imap_unordered(_compute_stock_shm, args, chunksize=32):
                if err:
                    errors += 1
                    if len(error_msgs) < 3:
                        error_msgs.append(f"{code}: {err[:200]}")
                elif result is not None:
                    results.append(result)
        finally:
            pool.close()
            pool.join()
    else:
        for arg in args:
            code, result, err = _compute_stock_shm(arg)
            if err:
                errors += 1
                if len(error_msgs) < 3:
                    error_msgs.append(f"{code}: {err[:200]}")
            elif result is not None:
                results.append(result)

    compute_ms = (time.perf_counter() - t0) * 1000

    for msg in error_msgs:
        logger.warning("Error: %s", msg)

    return len(results), errors, compute_ms


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description="Full pipeline benchmark (OSS → combined-engine)")
    parser.add_argument("--date", required=True, help="Trading day YYYYMMDD")
    parser.add_argument("--factor", default="", help="Factor module path")
    parser.add_argument("--workers", type=int, default=1, help="Compute workers")
    parser.add_argument("--rounds", type=int, default=1, help="Benchmark rounds")
    parser.add_argument("--codes", default="", help="Comma-separated stock codes")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of stocks (0=all)")
    parser.add_argument("--end-time", default="150000", help="end_time for factor")
    parser.add_argument("--output", default="", help="Save results to CSV/parquet")
    parser.add_argument("--cache-dir",
                        default=os.environ.get("OSS_LOCAL_CACHE_DIR", "/data/oss_cache"),
                        help="Local cache dir for downloaded parquets")
    args = parser.parse_args()

    # Load factor module
    factor_fn = None
    if args.factor:
        mod = importlib.import_module(args.factor)
        factor_fn = mod.factor_calculation
        print(f"Factor: {args.factor}")

    print("=" * 72)
    print("Full Pipeline Benchmark (OSS → ShmStockBuffer → mmap → Factor)")
    print("=" * 72)
    print(f"Date:    {args.date}")
    print(f"Workers: {args.workers}")
    print(f"Rounds:  {args.rounds}")
    print()

    # Create temp SHM dir for mmap buffers
    shm_dir = tempfile.mkdtemp(prefix="quant_bench_")
    os.environ["SHM_DIR"] = shm_dir
    print(f"SHM dir: {shm_dir}")
    print()

    # Reset MemoryStore singleton for fresh state
    MemoryStore._instance = None

    try:
        # ==== Phase 1: Load OSS data ====
        print("--- Phase 1: Download & Parse (OSS → DataFrame groups) ---")
        raw_groups, daily_basic, all_codes = load_oss_data(
            args.date, args.codes, args.limit, args.cache_dir
        )
        if not all_codes:
            print("No stocks found. Check --codes or --date.")
            return

        # ==== Phase 2: Feed into MemoryStore ====
        print("\n--- Phase 2: Feed (tuple → ShmStockBuffer mmap write) ---")
        store, counts = feed_into_store(raw_groups, args.date)

        # ==== Phase 3: Warm ====
        print("\n--- Phase 3: Warm (sync cache_len from ShmStockBuffer) ---")
        warm_ms = warm_store(store)

        # ==== Phase 4: Compute ====
        if not factor_fn:
            print("\n--- Phase 4: Compute (skipped, use --factor) ---")
            return

        print(f"\n--- Phase 4: Compute ({args.rounds} round{'s' if args.rounds > 1 else ''}, "
              f"{args.workers} workers, mmap zero-copy) ---")

        best_ms = float("inf")
        for r in range(args.rounds):
            n_res, n_err, compute_ms = compute_factor(
                store, factor_fn, args.workers, args.date, args.date, all_codes
            )
            best_ms = min(best_ms, compute_ms)
            per_stock = compute_ms / max(len(all_codes), 1) * args.workers
            print(f"  Round {r+1}: {compute_ms:.0f}ms | {n_res} ok, {n_err} err, "
                  f"{len(all_codes)} stocks, {args.workers}w | ~{per_stock:.1f}ms/stock")

        # ==== Summary ====
        print(f"\n{'=' * 72}")
        print("Summary")
        print(f"{'=' * 72}")
        print(f"  Stocks:    {len(all_codes)}")
        print(f"  Tick rows: {counts['tick']}")
        print(f"  Deal rows: {counts['deal']}")
        print(f"  Order rows:{counts['order']}")
        print(f"  Warm:      {warm_ms:.1f}ms")
        print(f"  Compute:   {best_ms:.0f}ms best ({args.workers} workers)")
        print(f"  Per stock: {best_ms / max(len(all_codes), 1) * args.workers:.1f}ms (wall)")

        # Extrapolate to full market
        if len(all_codes) < 5000:
            est_full = best_ms / len(all_codes) * 5000 / args.workers
            print(f"  Est. full market (5000/{args.workers}w): {est_full:.0f}ms")

        # Save results
        if args.output:
            pass  # TODO: collect results from compute_factor if needed

    finally:
        # Cleanup
        print(f"\n  Cleaning up {shm_dir}")
        shutil.rmtree(shm_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
