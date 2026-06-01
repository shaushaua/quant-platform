#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OSS historical data backtest & benchmark.

Downloads tick/deal/order from OSS, builds per-stock DataFrames,
runs factor computation with multiprocessing — same code path as live engine.

Usage:
    # Full market, 40 workers, 3 rounds
    python scripts/oss_backtest.py --date 20250530 \\
        --factor quant_platform.factor.examples.protected_eillen_strategy \\
        --workers 40 --rounds 3

    # Subset of stocks
    python scripts/oss_backtest.py --date 20250530 \\
        --factor quant_platform.factor.examples.protected_eillen_strategy \\
        --codes "000001.SZ,600000.SH" --workers 4

    # First 100 stocks (quick test)
    python scripts/oss_backtest.py --date 20250530 \\
        --factor quant_platform.factor.examples.protected_eillen_strategy \\
        --limit 100 --workers 12
"""

import argparse
import hashlib
import importlib
import logging
import multiprocessing
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from quant_platform.core.constants import TICK_COLUMNS, DEAL_COLUMNS, ORDER_COLUMNS
from quant_platform.factor.base import StockData

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---- Module-level data (COW-shared with forked workers) ----

_tick_groups: dict = {}
_deal_groups: dict = {}
_order_groups: dict = {}
_market_df: pd.DataFrame = pd.DataFrame()
_daily_basic_df: pd.DataFrame = pd.DataFrame()
_factor_fn = None


def _compute_one(args):
    """Worker: read COW-shared DataFrames, compute factor."""
    code, date_str, end_time = args
    try:
        stock_data = StockData(
            code=code,
            date=date_str,
            end_time=end_time,
            l1_tick=_tick_groups.get(code, pd.DataFrame()),
            l2_deal=_deal_groups.get(code, pd.DataFrame()),
            l2_order=_order_groups.get(code, pd.DataFrame()),
            market=_market_df,
            daily_basic=_daily_basic_df,
        )
        result = _factor_fn(stock_data, code, date_str, end_time)
        return code, result, None
    except Exception as exc:
        import traceback
        return code, None, traceback.format_exc()


# ---- OSS helpers ----

def _oss_key(date_str, kind):
    """Build OSS object key for a given date and data type."""
    year = date_str[:4]
    month = date_str[4:6]
    suffix = {"tick": "tick", "deal": "deal", "order": "order",
              "daily_basic": "daily_basic_data"}[kind]
    return f"{year}/{year}{month}/{date_str}/{date_str}_{suffix}.parquet"


def _download_cached(bucket, key, cache_dir):
    """Download OSS object to local cache, return path."""
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


def _resolve_security_ids(code_str, id_to_code):
    """Convert string codes like '000001.SZ' to SECURITY_ID list for DuckDB WHERE."""
    if not code_str:
        return None
    codes = [c.strip() for c in code_str.split(",")]
    # Reverse map: string code -> SECURITY_ID
    code_to_id = {v: k for k, v in id_to_code.items()}
    sec_ids = [code_to_id[c] for c in codes if c in code_to_id]
    return sec_ids, codes


def _read_parquet(path, date_str, sec_ids=None):
    """Read parquet with optional DuckDB predicate pushdown for SECURITY_ID filter.
    If sec_ids is provided, only reads rows where Code IN (sec_ids) — avoids OOM."""
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

    # Add TradingDay if missing
    if "TradingDay" not in df.columns:
        df["TradingDay"] = date_str

    return df


def _map_code_column(df, id_to_code):
    """If Code is int (SECURITY_ID), map to string code."""
    if "Code" in df.columns and df["Code"].dtype in (np.int32, np.int64, np.float64):
        df["Code"] = df["Code"].astype(int).map(id_to_code)
        before = len(df)
        df = df.dropna(subset=["Code"])
        dropped = before - len(df)
        if dropped:
            logger.warning("Dropped %d rows with unmapped SECURITY_ID", dropped)
    return df


def _align_columns(df, target_columns):
    """Ensure DataFrame has all target columns (add NaN for missing)."""
    for col in target_columns:
        if col not in df.columns:
            df[col] = float("nan")
    # Ensure Time/UpdateTime are datetime64[ns]
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
    """Group DataFrame by Code, return dict."""
    if df.empty or "Code" not in df.columns:
        return {}
    return {code: group for code, group in df.groupby("Code", sort=False)}


# ---- Main ----

def main():
    parser = argparse.ArgumentParser(description="OSS backtest & benchmark")
    parser.add_argument("--date", required=True, help="Trading day YYYYMMDD")
    parser.add_argument("--factor", default="", help="Factor module path")
    parser.add_argument("--workers", type=int, default=1, help="Compute workers")
    parser.add_argument("--rounds", type=int, default=1, help="Benchmark rounds")
    parser.add_argument("--codes", default="", help="Comma-separated stock codes (empty=all)")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of stocks (0=all)")
    parser.add_argument("--end-time", default="150000", help="end_time for factor (default 150000)")
    parser.add_argument("--output", default="", help="Save results to CSV/parquet")
    parser.add_argument(
        "--cache-dir",
        default=os.environ.get("OSS_LOCAL_CACHE_DIR", os.path.expanduser("~/.oss_cache")),
        help="Local cache dir for downloaded parquets",
    )
    args = parser.parse_args()

    # ---- OSS setup ----
    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    endpoint = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com")
    bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")

    if not ak or not sk:
        print("ERROR: Set OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET")
        print("  export OSS_ACCESS_KEY_ID='...'")
        print("  export OSS_ACCESS_KEY_SECRET='...'")
        sys.exit(1)

    import oss2

    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)

    # ---- Factor module ----
    factor_fn = None
    if args.factor:
        mod = importlib.import_module(args.factor)
        factor_fn = mod.factor_calculation
        print(f"Factor: {args.factor}")

    date_str = args.date

    print("=" * 60)
    print("OSS Backtest Benchmark")
    print("=" * 60)
    print(f"Date:    {date_str}")
    print(f"Workers: {args.workers}")
    print(f"Rounds:  {args.rounds}")
    print()

    # ==== Phase 1: Download & Load ====
    print("--- Phase 1: Download & Load ---")

    # 1a. daily_basic (small, ~1MB) — for ID mapping if Code is int32
    db_key = _oss_key(date_str, "daily_basic")
    try:
        db_path = _download_cached(bucket, db_key, args.cache_dir)
        daily_basic = pd.read_parquet(db_path)
    except Exception as e:
        logger.warning("daily_basic not found: %s (will try without ID mapping)", e)
        daily_basic = pd.DataFrame()

    # Build SECURITY_ID -> code mapping (in case Code is int32 in parquets)
    id_to_code = {}
    if "ID_QI" in daily_basic.columns and "SECURITY_ID" in daily_basic.columns:
        for _, row in daily_basic.iterrows():
            id_qi = str(row["ID_QI"]).zfill(6)
            sec_id = int(row["SECURITY_ID"])
            market = "XSHG" if id_qi.startswith(("6", "9", "68")) else "XSHE"
            id_to_code[sec_id] = f"{id_qi}.{market}"
        print(f"  ID mapping: {len(id_to_code)} stocks")

    # 1b. Download tick/deal/order
    t_download = time.perf_counter()
    paths = {}
    for kind in ("tick", "deal", "order"):
        key = _oss_key(date_str, kind)
        try:
            paths[kind] = _download_cached(bucket, key, args.cache_dir)
        except Exception as e:
            logger.warning("Download %s failed: %s", kind, e)
    download_ms = (time.perf_counter() - t_download) * 1000
    print(f"  Download: {download_ms:.0f}ms")

    # 1c. Read & convert
    t_load = time.perf_counter()

    # Resolve SECURITY_IDs for predicate pushdown (avoids loading 7GB into 8GB RAM)
    sec_filter = None
    if args.codes and id_to_code:
        sec_filter, code_filter_list = _resolve_security_ids(args.codes, id_to_code)
        if sec_filter:
            print(f"  DuckDB predicate pushdown: {len(sec_filter)} SECURITY_IDs")

    raw_groups = {}
    for kind, target_cols in [("tick", TICK_COLUMNS), ("deal", DEAL_COLUMNS), ("order", ORDER_COLUMNS)]:
        if kind not in paths:
            raw_groups[kind] = {}
            continue
        df = _read_parquet(paths[kind], date_str, sec_ids=sec_filter)
        df = _map_code_column(df, id_to_code)
        df = _align_columns(df, target_cols)

        # Filter by requested codes (in case predicate pushdown was skipped)
        code_filter = None
        if args.codes:
            code_filter = set(c.strip() for c in args.codes.split(","))
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
    if args.limit > 0 and args.limit < len(all_codes):
        all_codes = all_codes[: args.limit]
        # Re-filter groups
        code_set = set(all_codes)
        for kind in raw_groups:
            raw_groups[kind] = {c: v for c, v in raw_groups[kind].items() if c in code_set}

    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"  Load: {load_ms:.0f}ms")
    print(f"  Total stocks: {len(all_codes)}")

    # Per-stock row stats
    tick_sizes = [len(raw_groups["tick"].get(c, [])) for c in all_codes]
    deal_sizes = [len(raw_groups["deal"].get(c, [])) for c in all_codes]
    order_sizes = [len(raw_groups["order"].get(c, [])) for c in all_codes]
    if tick_sizes:
        print(
            f"  Rows/stock: tick avg={np.mean(tick_sizes):.0f} max={max(tick_sizes)}, "
            f"deal avg={np.mean(deal_sizes):.0f}, order avg={np.mean(order_sizes):.0f}"
        )

    # ==== Phase 2: Compute ====
    if not factor_fn:
        print("\n--- Phase 2: Compute (skipped, use --factor) ---")
        return

    # Set module-level globals for COW sharing
    global _tick_groups, _deal_groups, _order_groups, _factor_fn
    global _market_df, _daily_basic_df
    _tick_groups = raw_groups["tick"]
    _deal_groups = raw_groups["deal"]
    _order_groups = raw_groups["order"]
    _factor_fn = factor_fn
    _market_df = pd.DataFrame()
    _daily_basic_df = daily_basic

    end_time = args.end_time
    best_ms = float("inf")

    print(f"\n--- Phase 2: Compute ({args.rounds} round{'s' if args.rounds > 1 else ''}) ---")

    for r in range(args.rounds):
        args_list = [(code, date_str, end_time) for code in all_codes]
        results = []
        errors = 0
        t0 = time.perf_counter()

        if args.workers > 1:
            ctx = multiprocessing.get_context("fork")
            pool = ctx.Pool(processes=args.workers)
            try:
                for code, result, err in pool.imap_unordered(
                    _compute_one, args_list, chunksize=max(1, len(all_codes) // (args.workers * 4))
                ):
                    if err:
                        errors += 1
                        if errors <= 3:
                            logger.warning("Error %s: %s", code, err[:200])
                    elif result is not None:
                        results.append(result)
            finally:
                pool.close()
                pool.join()
        else:
            for a in args_list:
                code, result, err = _compute_one(a)
                if err:
                    errors += 1
                    if errors <= 3:
                        logger.warning("Error %s: %s", code, err[:200])
                elif result is not None:
                    results.append(result)

        compute_ms = (time.perf_counter() - t0) * 1000
        best_ms = min(best_ms, compute_ms)

        per_stock = compute_ms / max(len(all_codes), 1) * args.workers
        print(
            f"  Round {r + 1}: {compute_ms:.0f}ms | "
            f"{len(results)} ok, {errors} err, "
            f"{len(all_codes)} stocks, {args.workers}w | "
            f"~{per_stock:.1f}ms/stock"
        )

    # ==== Summary ====
    print(f"\n{'=' * 60}")
    print("Summary")
    print(f"{'=' * 60}")
    print(f"  Stocks:   {len(all_codes)}")
    print(f"  Download: {download_ms:.0f}ms")
    print(f"  Load:     {load_ms:.0f}ms")
    if factor_fn:
        print(f"  Compute:  {best_ms:.0f}ms best ({args.workers} workers)")
        print(f"  Per stock: {best_ms / max(len(all_codes), 1) * args.workers:.1f}ms (wall)")

    # ==== Save results ====
    if results and args.output:
        result_df = pd.DataFrame(results)
        if args.output.endswith(".parquet"):
            result_df.to_parquet(args.output, index=False)
        else:
            result_df.to_csv(args.output, index=False)
        print(f"\n  Results saved: {args.output} ({len(results)} rows)")

    # Show sample results
    if results:
        print(f"\n  Sample results ({min(5, len(results))}):")
        for r in results[:5]:
            if isinstance(r, dict):
                items = list(r.items())[:6]
                print(f"    {dict(items)}")
            else:
                print(f"    {r}")

    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    main()
