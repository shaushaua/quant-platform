#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compare historical-engine path vs live-worker path on the SAME OSS data.

Path A: calc_factors_by_date_range() — the historical engine end-to-end.
Path B: native_engine._compute_code_batch_shm() — the live worker function,
        invoked directly with the worker task tuple. Data loading is
        monkey-patched to return OSS parquet bytes (not SHM), so the live
        compute code runs unchanged on identical input data.

Both paths ingest identical OSS bytes for tick/deal/order/daily_basic.
Any result divergence points to engine-plumbing differences (batching,
StockData construction, sort order, call signature), NOT data source.

Usage:
  python scripts/compare_paths.py --date 20260703
  python scripts/compare_paths.py --date 20260703 --codes 000001.SZ,600000.SH
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from quant_platform.data.api import DataAPI
from quant_platform.factor.engine import (
    calc_factors_by_date_range,
    _restore_oss_precision,
)
from quant_platform.live_engine import native_engine as ne


STRATEGY_MODULE = "quant_platform.strategies.protected_eillen_strategy_v2"
END_TIME = "150000"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normal_code(s: "pd.Series") -> "pd.Series":
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.SZ$", ".XSHE", regex=True)
    out = out.str.replace(r"\.SH$", ".XSHG", regex=True)
    no_suffix = ~out.str.contains(r"\.", regex=True)
    out.loc[no_suffix] = out.loc[no_suffix].str.zfill(6)
    return out


def _factor_cols(df: pd.DataFrame) -> list[str]:
    return sorted(c for c in df.columns if c.startswith("F_"))


def _to_code6_series(s: "pd.Series") -> "pd.Series":
    return _normal_code(s).str.split(".").str[0].str.zfill(6)


def _compare(a: pd.DataFrame, b: pd.DataFrame, label_a: str, label_b: str,
             atol: float, rtol: float) -> int:
    a = a.copy()
    b = b.copy()
    for df in (a, b):
        if "code" in df.columns:
            df["_code"] = _to_code6_series(df["code"])
        elif "ID_QI" in df.columns:
            df["_code"] = _to_code6_series(df["ID_QI"])
        else:
            raise RuntimeError(f"no code/ID_QI column: {list(df.columns)[:20]}")

    a_cols = _factor_cols(a)
    b_cols = _factor_cols(b)
    common = sorted(set(a_cols) & set(b_cols))
    only_a = sorted(set(a_cols) - set(b_cols))
    only_b = sorted(set(b_cols) - set(a_cols))
    print(f"{label_a}_shape={a.shape} {label_b}_shape={b.shape}")
    print(f"{label_a}_factor_cols={len(a_cols)} {label_b}_factor_cols={len(b_cols)} common={len(common)}")
    if only_a:
        print(f"only_{label_a}_cols sample={only_a[:10]}")
    if only_b:
        print(f"only_{label_b}_cols sample={only_b[:10]}")

    aa = a[["_code", *common]].drop_duplicates("_code").set_index("_code").sort_index()
    bb = b[["_code", *common]].drop_duplicates("_code").set_index("_code").sort_index()
    common_codes = aa.index.intersection(bb.index)
    print(f"{label_a}_codes={len(aa)} {label_b}_codes={len(bb)} common={len(common_codes)}")
    print(f"only_{label_a}_codes={len(aa.index.difference(bb.index))} "
          f"only_{label_b}_codes={len(bb.index.difference(aa.index))}")

    aa = aa.loc[common_codes, common]
    bb = bb.loc[common_codes, common]

    bad_cells = 0
    max_abs = 0.0
    bad_cols: list[tuple[str, int, float]] = []
    samples = []
    for col in common:
        av = pd.to_numeric(aa[col], errors="coerce").to_numpy(dtype="float64")
        bv = pd.to_numeric(bb[col], errors="coerce").to_numpy(dtype="float64")
        both_nan = np.isnan(av) & np.isnan(bv)
        close = np.isclose(av, bv, rtol=rtol, atol=atol, equal_nan=True)
        bad = ~(both_nan | close)
        n_bad = int(bad.sum())
        if n_bad:
            abs_diff = np.abs(av - bv)
            finite_mask = np.isfinite(abs_diff)
            col_max = float(np.nanmax(abs_diff[bad & finite_mask])) if (bad & finite_mask).any() else float("nan")
            bad_cols.append((col, n_bad, col_max))
            bad_cells += n_bad
            if np.isfinite(col_max):
                max_abs = max(max_abs, col_max)
            if len(samples) < 20:
                idxs = np.flatnonzero(bad)[: 20 - len(samples)]
                for i in idxs:
                    samples.append((str(common_codes[i]), col, av[i], bv[i], abs_diff[i]))

    bad_cols.sort(key=lambda x: x[1], reverse=True)
    print(f"bad_cells={bad_cells} bad_cols={len(bad_cols)} max_abs={max_abs:.12g}")
    for row in bad_cols[:20]:
        print(row)
    if samples:
        print("samples (code, col, A, B, |A-B|):")
        for row in samples:
            print(row)

    return 0 if not only_a and not only_b and bad_cells == 0 else 1


# ── Path A: historical engine ─────────────────────────────────────────────────

def run_path_a(date: str, end_time: str, securities: list[str],
               factor_info: dict, factor_fn, workers: int) -> pd.DataFrame:
    print(f"\n=== Path A (historical engine) date={date} end_time={end_time} codes={len(securities)}")
    t0 = time.time()
    results: list[pd.DataFrame] = []

    def outfun(d: str, et: str, df: pd.DataFrame) -> None:
        print(f"  [A] outfun date={d} end_time={et} shape={df.shape}")
        results.append(df.copy())

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date=date,
        end_date=date,
        end_times=[end_time],
        securities=securities,
        processes=workers,
        factor_data_handler=factor_fn,
        outfun=outfun,
        oss_base_path=os.environ.get("OSS_DATA_PATH", "2026"),
    )
    if not results:
        raise RuntimeError("Path A produced no result")
    out = pd.concat(results, ignore_index=True)
    fcols = out.select_dtypes(include=["float64", "float32"]).columns
    if len(fcols) > 0:
        out[fcols] = out[fcols].round(6).astype("float32")
    print(f"  [A] done in {time.time()-t0:.1f}s shape={out.shape}")
    return out


# ── Path B: live worker _compute_code_batch_shm with OSS-patched loader ──────

def run_path_b(date: str, end_time: str, codes: list[str],
               factor_module: str, factor_info: dict,
               api: DataAPI, daily_basic_df: pd.DataFrame) -> pd.DataFrame:
    print(f"\n=== Path B (live worker _compute_code_batch_shm) date={date} end_time={end_time} codes={len(codes)}")
    t0 = time.time()
    need_tick = factor_info.get("need_l1_tick", True)
    need_deal = factor_info.get("need_l2_deal", True)
    need_order = factor_info.get("need_l2_order", False)
    print(f"  [B] need_tick={need_tick} need_deal={need_deal} need_order={need_order}")

    # 1) Pre-load all OSS data per code into a cache keyed by (code6, kind)
    print(f"  [B] preloading OSS data for {len(codes)} codes...")
    load_t0 = time.time()
    oss_cache: dict[tuple[str, str], pd.DataFrame] = {}

    all_codes_arg = codes
    if need_tick:
        tick_all = api.get_daily_data(date, "tick", codes=all_codes_arg)
        print(f"  [B] tick loaded shape={tick_all.shape}")
    if need_deal:
        deal_all = api.get_daily_data(date, "deal", codes=all_codes_arg)
        print(f"  [B] deal loaded shape={deal_all.shape}")
    if need_order:
        order_all = api.get_daily_data(date, "order", codes=all_codes_arg)
        print(f"  [B] order loaded shape={order_all.shape}")

    # Resolve code → SECURITY_ID for filtering via Code (int) column
    id_map: dict[str, int] = {}
    if "ID_QI" in daily_basic_df.columns and "SECURITY_ID" in daily_basic_df.columns:
        for _, row in daily_basic_df.iterrows():
            id_qi = str(row["ID_QI"]).strip()
            if id_qi:
                id_map[id_qi.zfill(6)] = int(row["SECURITY_ID"])

    def _per_code(df: pd.DataFrame, code: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        code6 = code.split(".")[0].zfill(6)
        sec_id = id_map.get(code6)
        for col in ("Code", "stock_code"):
            if col in df.columns and sec_id is not None:
                return df[df[col] == sec_id].reset_index(drop=True)
        for col in ("code", "ID_QI"):
            if col in df.columns:
                normalized = df[col].astype(str).str.split(".").str[0].str.zfill(6)
                return df[normalized == code6].reset_index(drop=True)
        return df

    for code in codes:
        code6 = code.split(".")[0].zfill(6)
        if need_tick:
            df = _per_code(tick_all, code)
            df = _restore_oss_precision(df, code)
            if "SeqNum" in df.columns:
                df = df.sort_values("SeqNum", kind="mergesort").reset_index(drop=True)
            oss_cache[(code6, "tick")] = df
        if need_deal:
            df = _per_code(deal_all, code)
            df = _restore_oss_precision(df, code)
            if "SeqNum" in df.columns:
                df = df.sort_values("SeqNum", kind="mergesort").reset_index(drop=True)
            oss_cache[(code6, "deal")] = df
        if need_order:
            df = _per_code(order_all, code)
            df = _restore_oss_precision(df, code)
            if "SeqNum" in df.columns:
                df = df.sort_values("SeqNum", kind="mergesort").reset_index(drop=True)
            oss_cache[(code6, "order")] = df
    print(f"  [B] preload done in {time.time()-load_t0:.1f}s cache_entries={len(oss_cache)}")

    # 2) Monkey-patch _build_df_from_native to serve OSS data from cache
    #    This makes the live worker code run on OSS bytes instead of SHM.
    _orig_build = ne._build_df_from_native

    def _patched_build_df(reader, columns, buf_cols, time_idx, updtime_idx,
                          trading_day, code, volume_idx=None,
                          historical_compat=False, kind_name=""):
        code6 = (str(code).split(".")[0].zfill(6)) if code else ""
        df = oss_cache.get((code6, kind_name))
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        # Match _build_df_from_native's post-processing to keep format identical:
        # 1) TradingDay column (OSS parquet lacks it)
        if "TradingDay" not in df.columns:
            df.insert(0, "TradingDay", trading_day)
        # 2) Cast integer columns to int64 (historical_compat branch)
        for col in ("OrderID", "SaleOrderID", "BuyOrderID", "Side", "OrderType",
                    "TradeNum", "Channel", "SeqNum"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
        # 3) Ensure Code column is the string code (in case OSS Code wasn't restored)
        if "Code" in df.columns:
            df["Code"] = code
        return df

    ne._build_df_from_native = _patched_build_df
    ne._cached_reader = lambda path: None  # reader unused by patch

    # 3) Set up live worker module-level globals it expects
    ne._market_df = daily_basic_df
    ne._daily_basic_df = daily_basic_df
    ne._base_ns = pd.Timestamp(date).value

    try:
        # 4) Build worker task tuple and invoke _compute_code_batch_shm directly.
        #    Signature:
        #    (codes, date_str, end_time, wall_secs, states_snapshot,
        #     tick_paths, deal_paths, order_paths, trading_day,
        #     factor_module, factor_info, is_daily)
        tick_paths = {c: "oss://" for c in codes}
        deal_paths = {c: "oss://" for c in codes}
        order_paths = {c: "oss://" for c in codes}
        states_snapshot = {c: None for c in codes}
        wall_secs = 0  # informational only for live (data_latency)

        batch_size = int(os.environ.get("LIVE_FACTOR_BATCH_SIZE", "128"))
        all_results: list = []
        n_batches = (len(codes) + batch_size - 1) // batch_size
        for bi in range(n_batches):
            batch_codes = codes[bi * batch_size : (bi + 1) * batch_size]
            task = (
                batch_codes,                       # codes
                date,                              # date_str (used by strategy)
                end_time,                          # end_time
                wall_secs,                         # wall_secs
                states_snapshot,                   # states_snapshot (None per code)
                {c: tick_paths.get(c, "") for c in batch_codes},
                {c: deal_paths.get(c, "") for c in batch_codes},
                {c: order_paths.get(c, "") for c in batch_codes},
                date,                              # trading_day
                factor_module,                     # factor_module path
                factor_info,                       # factor_info dict
                True,                              # is_daily (V2 daily schedule)
            )
            batch_results, batch_errors, build_ms, factor_ms, err = ne._compute_code_batch_shm(task)
            if err:
                print(f"  [B] batch {bi} returned error: {err}")
            all_results.extend(batch_results)
            if (bi + 1) % 5 == 0 or bi == n_batches - 1:
                elapsed = time.time() - t0
                print(f"  [B] batch {bi+1}/{n_batches} accumulated_rows={len(all_results)} elapsed={elapsed:.1f}s")
    finally:
        ne._build_df_from_native = _orig_build

    if not all_results:
        raise RuntimeError("Path B produced no result")
    out = pd.DataFrame(all_results)
    fcols = out.select_dtypes(include=["float64", "float32"]).columns
    if len(fcols) > 0:
        out[fcols] = out[fcols].round(6).astype("float32")
    print(f"  [B] done in {time.time()-t0:.1f}s shape={out.shape}")
    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--strategy-module", default=STRATEGY_MODULE)
    ap.add_argument("--end-time", default=END_TIME)
    ap.add_argument("--codes", default=None,
                    help="Comma-separated code subset; default = strategy securities")
    ap.add_argument("--workers", type=int, default=int(os.environ.get("WORKERS", "4")))
    ap.add_argument("--atol", type=float, default=1e-6)
    ap.add_argument("--rtol", type=float, default=1e-6)
    ap.add_argument("--out-a", default=None)
    ap.add_argument("--out-b", default=None)
    args = ap.parse_args()

    os.environ.setdefault("FACTOR_FORCE_STREAMING", "1")
    os.environ.setdefault("DATA_BATCH_SIZE", "200")
    os.environ.setdefault("FACTOR_BATCH_SIZE", "100")
    os.environ.setdefault("FACTOR_TASK_CODE_BATCH_SIZE", "25")
    # Match prod live engine setting: V2 strategy uses multi-code batch protocol
    os.environ.setdefault("DAILY_USE_MULTI_CODE", "true")

    mod = importlib.import_module(args.strategy_module)
    factor_info = getattr(mod, "factor_info", getattr(mod, "FACTOR_INFO", None))
    strategy_securities = getattr(mod, "securities", None) or []
    print(f"strategy={args.strategy_module} factor_info={factor_info} securities={len(strategy_securities)}")

    if args.codes:
        codes_arg = [c.strip() for c in args.codes.split(",")]
    else:
        codes_arg = strategy_securities
    print(f"codes_arg={len(codes_arg)} sample={codes_arg[:5]}")

    # ── Path A ──
    a = run_path_a(args.date, args.end_time, codes_arg, factor_info, mod.factor_calculation, args.workers)
    out_a = args.out_a or f"/tmp/path_a_{args.date}.parquet"
    a.to_parquet(out_a, index=False)
    print(f"saved Path A → {out_a}")

    # ── Path B: use same code list as Path A actually returned ──
    if "code" in a.columns:
        path_b_codes = _normal_code(a["code"]).tolist()
    elif "ID_QI" in a.columns:
        path_b_codes = _normal_code(a["ID_QI"]).tolist()
    else:
        path_b_codes = codes_arg
    seen = set()
    path_b_codes = [c for c in path_b_codes if not (c in seen or seen.add(c))]
    print(f"path_b_codes_from_A={len(path_b_codes)} sample={path_b_codes[:5]}")

    api = DataAPI(mode="backtest")
    daily_basic_df = api.get_daily_data(args.date, "daily_basic", codes=None)
    print(f"daily_basic loaded shape={daily_basic_df.shape}")

    b = run_path_b(args.date, args.end_time, path_b_codes,
                   args.strategy_module, factor_info, api, daily_basic_df)
    out_b = args.out_b or f"/tmp/path_b_{args.date}.parquet"
    b.to_parquet(out_b, index=False)
    print(f"saved Path B → {out_b}")

    # ── Compare ──
    print("\n=== compare A vs B")
    return _compare(a, b, "A", "B", args.atol, args.rtol)


if __name__ == "__main__":
    raise SystemExit(main())
