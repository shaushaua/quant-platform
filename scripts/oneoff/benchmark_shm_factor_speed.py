#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Benchmark today's archive data against live minute-factor hot paths.

This is intended for one-off Kubernetes pods. It reads archived tick/deal/order
parquets from OSS, then measures:
  1) StockData construction with daily_basic filtering/cache.
  2) Strategy batch calculation on archived data.
  3) _build_df_from_native using a mock native reader built from archive rows.

Run the same script in two pods:
  - baseline image code
  - candidate code mounted over /app/quant_platform/...
"""

from __future__ import annotations

import argparse
import gc
import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from quant_platform.data.api import DataAPI
from quant_platform.factor.stock_data_builder import build_stock_data
from quant_platform.live_engine import native_engine as ne


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _code6(code: str) -> str:
    return str(code).split(".")[0].zfill(6)


def _normal_code(code: str) -> str:
    c = _code6(code)
    return f"{c}.XSHG" if c.startswith(("5", "6", "9")) else f"{c}.XSHE"


def _add_code_key(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "_ID_QI_PAD" in df.columns:
        return df
    if "ID_QI" in df.columns:
        out = df.copy()
        out["_ID_QI_PAD"] = out["ID_QI"].astype(str).str.split(".").str[0].str.zfill(6)
        return out
    if "TICKER_SYMBOL" in df.columns:
        out = df.copy()
        out["_ID_QI_PAD"] = out["TICKER_SYMBOL"].astype(str).str.split(".").str[0].str.zfill(6)
        return out
    return df


def _security_maps(daily_basic: pd.DataFrame) -> tuple[dict[str, int], dict[int, str]]:
    if daily_basic.empty or "SECURITY_ID" not in daily_basic.columns:
        return {}, {}
    code_col = "ID_QI" if "ID_QI" in daily_basic.columns else (
        "TICKER_SYMBOL" if "TICKER_SYMBOL" in daily_basic.columns else None
    )
    if not code_col:
        return {}, {}
    codes = daily_basic[code_col].astype(str).str.split(".").str[0].str.zfill(6)
    sec = pd.to_numeric(daily_basic["SECURITY_ID"], errors="coerce")
    c2s: dict[str, int] = {}
    s2c: dict[int, str] = {}
    for c, sid in zip(codes, sec):
        if pd.isna(sid):
            continue
        sid_i = int(sid)
        c2s.setdefault(c, sid_i)
        s2c.setdefault(sid_i, c)
    return c2s, s2c


def _pick_codes(strategy_module: str, daily_basic: pd.DataFrame, limit: int) -> list[str]:
    mod = importlib.import_module(strategy_module)
    securities = list(getattr(mod, "securities", None) or [])
    if securities:
        return [_normal_code(c) for c in securities[:limit]]
    if "ID_QI" in daily_basic.columns:
        return [_normal_code(c) for c in daily_basic["ID_QI"].astype(str).head(limit)]
    if "TICKER_SYMBOL" in daily_basic.columns:
        return [_normal_code(c) for c in daily_basic["TICKER_SYMBOL"].astype(str).head(limit)]
    raise RuntimeError("cannot pick codes: no strategy securities and no daily_basic code column")


def _per_code(df: pd.DataFrame, code: str, code_to_sec: dict[str, int]) -> pd.DataFrame:
    if df.empty:
        return df
    c6 = _code6(code)
    sec = code_to_sec.get(c6)
    if sec is not None and "Code" in df.columns:
        return df[df["Code"] == sec].reset_index(drop=True)
    for col in ("ID_QI", "code", "stock_code"):
        if col in df.columns:
            norm = df[col].astype(str).str.split(".").str[0].str.zfill(6)
            return df[norm == c6].reset_index(drop=True)
    return df


def _preload_archive(api: DataAPI, date: str, codes: list[str], need_tick: bool,
                     need_deal: bool, need_order: bool) -> dict[str, pd.DataFrame]:
    data: dict[str, pd.DataFrame] = {}
    if need_tick:
        t0 = _now_ms()
        data["tick"] = api.get_daily_data(date, "tick", codes=codes)
        print(f"PRELOAD kind=tick rows={len(data['tick'])} ms={_now_ms() - t0:.1f}", flush=True)
    else:
        data["tick"] = pd.DataFrame()
    if need_deal:
        t0 = _now_ms()
        data["deal"] = api.get_daily_data(date, "deal", codes=codes)
        print(f"PRELOAD kind=deal rows={len(data['deal'])} ms={_now_ms() - t0:.1f}", flush=True)
    else:
        data["deal"] = pd.DataFrame()
    if need_order:
        t0 = _now_ms()
        data["order"] = api.get_daily_data(date, "order", codes=codes)
        print(f"PRELOAD kind=order rows={len(data['order'])} ms={_now_ms() - t0:.1f}", flush=True)
    else:
        data["order"] = pd.DataFrame()
    return data


def _bench_stockdata(label: str, date: str, end_time: str, codes: list[str],
                     archive: dict[str, pd.DataFrame], daily_basic: pd.DataFrame,
                     factor_info: dict[str, Any], code_to_sec: dict[str, int],
                     rounds: int) -> dict[str, Any]:
    rows = 0
    t0 = _now_ms()
    for _ in range(rounds):
        for code in codes:
            tick = _per_code(archive["tick"], code, code_to_sec)
            deal = _per_code(archive["deal"], code, code_to_sec)
            order = _per_code(archive["order"], code, code_to_sec)
            sd = build_stock_data(
                code=code,
                date=date,
                end_time=end_time,
                tick_df=tick,
                deal_df=deal,
                order_df=order,
                market_df=daily_basic,
                daily_basic_df=daily_basic,
                factor_info=factor_info,
                validate=False,
            )
            rows += len(sd.l1_tick) + len(sd.l2_deal) + len(sd.l2_order)
    elapsed = _now_ms() - t0
    return {
        "case": "stockdata",
        "label": label,
        "stocks": len(codes),
        "rounds": rounds,
        "rows_seen": int(rows),
        "elapsed_ms": round(elapsed, 3),
        "ms_per_stock": round(elapsed / max(len(codes) * rounds, 1), 6),
    }


def _bench_strategy(label: str, date: str, end_time: str, codes: list[str],
                    archive: dict[str, pd.DataFrame], daily_basic: pd.DataFrame,
                    factor_info: dict[str, Any], factor_fn, code_to_sec: dict[str, int],
                    rounds: int) -> dict[str, Any]:
    out_rows = 0
    t0 = _now_ms()
    for _ in range(rounds):
        data_map = {}
        for code in codes:
            data_map[code] = build_stock_data(
                code=code,
                date=date,
                end_time=end_time,
                tick_df=_per_code(archive["tick"], code, code_to_sec),
                deal_df=_per_code(archive["deal"], code, code_to_sec),
                order_df=_per_code(archive["order"], code, code_to_sec),
                market_df=daily_basic,
                daily_basic_df=daily_basic,
                factor_info=factor_info,
                validate=False,
            )
        raw = factor_fn(data_map, codes, date, [end_time])
        if isinstance(raw, dict):
            out_rows += len(raw)
        elif raw is not None:
            try:
                out_rows += len(raw)
            except TypeError:
                out_rows += 1
        data_map.clear()
    elapsed = _now_ms() - t0
    return {
        "case": "strategy_batch",
        "label": label,
        "stocks": len(codes),
        "rounds": rounds,
        "result_items": int(out_rows),
        "elapsed_ms": round(elapsed, 3),
        "ms_per_stock": round(elapsed / max(len(codes) * rounds, 1), 6),
    }


class _MockReader:
    def __init__(self, arr: np.ndarray):
        self._arr = arr

    def view_rows(self) -> np.ndarray:
        return self._arr


def _seconds_from_time_col(s: pd.Series) -> np.ndarray:
    if np.issubdtype(s.dtype, np.datetime64):
        dt = pd.to_datetime(s, errors="coerce")
        return (
            dt.dt.hour.astype("float64") * 3600
            + dt.dt.minute.astype("float64") * 60
            + dt.dt.second.astype("float64")
            + dt.dt.microsecond.astype("float64") / 1_000_000
            + dt.dt.nanosecond.astype("float64") / 1_000_000_000
        ).to_numpy(dtype="float64")
    num = pd.to_numeric(s, errors="coerce")
    if num.notna().any() and float(num.dropna().max()) > 100000:
        dt = pd.to_datetime(s, errors="coerce")
        return (
            dt.dt.hour.astype("float64") * 3600
            + dt.dt.minute.astype("float64") * 60
            + dt.dt.second.astype("float64")
        ).to_numpy(dtype="float64")
    return num.to_numpy(dtype="float64")


def _archive_to_native_arr(df: pd.DataFrame, buf_cols: list[str], max_rows: int) -> np.ndarray:
    n = min(len(df), max_rows)
    arr = np.zeros((n, len(buf_cols)), dtype="float64")
    sample = df.head(n).reset_index(drop=True)
    for i, col in enumerate(buf_cols):
        if col in ("Time", "UpdateTime") and col in sample.columns:
            arr[:, i] = _seconds_from_time_col(sample[col])
        elif col in sample.columns:
            arr[:, i] = pd.to_numeric(sample[col], errors="coerce").fillna(0).to_numpy(dtype="float64")
    return arr


def _bench_native_df(label: str, archive: dict[str, pd.DataFrame], trading_day: str,
                     rows: int, rounds: int) -> list[dict[str, Any]]:
    out = []
    cases = [
        ("tick", archive["tick"], ne._tick_columns, ne._tick_buf_cols,
         ne._tick_time_idx, ne._tick_updtime_idx, ne._tick_volume_idx),
        ("deal", archive["deal"], ne._deal_columns, ne._deal_buf_cols,
         ne._deal_time_idx, ne._deal_updtime_idx, ne._deal_volume_idx),
    ]
    for kind, df, cols, buf_cols, time_idx, upd_idx, vol_idx in cases:
        if df.empty:
            continue
        arr = _archive_to_native_arr(df, buf_cols, rows)
        reader = _MockReader(arr)
        result_rows = 0
        t0 = _now_ms()
        for _ in range(rounds):
            built = ne._build_df_from_native(
                reader, cols, buf_cols, time_idx, upd_idx,
                trading_day, "000001.XSHE", vol_idx, True, kind,
            )
            result_rows += len(built)
        elapsed = _now_ms() - t0
        out.append({
            "case": "native_df_mock",
            "label": label,
            "kind": kind,
            "input_rows": int(arr.shape[0]),
            "rounds": rounds,
            "result_rows": int(result_rows),
            "elapsed_ms": round(elapsed, 3),
            "ms_per_10k_rows": round(elapsed / max(arr.shape[0] * rounds, 1) * 10000, 6),
        })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="20260709")
    ap.add_argument("--label", default=os.environ.get("BENCH_LABEL", "unknown"))
    ap.add_argument("--strategy-module", default=os.environ.get(
        "FACTOR_MODULE", "quant_platform.strategies.protected_eillen_intraday_strategy"))
    ap.add_argument("--end-time", default=os.environ.get("END_TIME", "150000"))
    ap.add_argument("--sizes", default="100,1000")
    ap.add_argument("--rounds-stockdata", type=int, default=2)
    ap.add_argument("--rounds-strategy", type=int, default=1)
    ap.add_argument("--rounds-native", type=int, default=5)
    ap.add_argument("--native-rows", type=int, default=200000)
    ap.add_argument("--skip-strategy", action="store_true")
    args = ap.parse_args()

    os.environ.setdefault("OSS_LOCAL_CACHE_DIR", "/data/bench_oss_cache")
    os.environ.setdefault("DAILY_USE_MULTI_CODE", "true")

    mod = importlib.import_module(args.strategy_module)
    factor_info = dict(getattr(mod, "factor_info", getattr(mod, "FACTOR_INFO", {}) or {}))
    factor_fn = getattr(mod, "factor_calculation")
    print(f"BENCH_START label={args.label} date={args.date} strategy={args.strategy_module} factor_info={factor_info}", flush=True)

    api = DataAPI(mode="backtest", oss_base_path=os.environ.get("OSS_DATA_PATH", "2026"))
    daily_basic = api._oss.read_daily_basic(args.date, int(factor_info.get("market_count", 60) or 60))
    daily_basic = _add_code_key(daily_basic)
    try:
        from quant_platform.factor.stock_data_builder import _ensure_daily_group_cache
        _ensure_daily_group_cache(daily_basic)
    except Exception as exc:
        print(f"daily cache prewarm failed: {exc}", flush=True)
    code_to_sec, _ = _security_maps(daily_basic)
    print(f"DAILY_BASIC rows={len(daily_basic)} code_map={len(code_to_sec)}", flush=True)

    max_size = max(int(x) for x in args.sizes.split(",") if x.strip())
    all_codes = _pick_codes(args.strategy_module, daily_basic, max_size)
    print(f"CODES picked={len(all_codes)} sample={all_codes[:5]}", flush=True)

    need_tick = bool(factor_info.get("need_l1_tick", True))
    need_deal = bool(factor_info.get("need_l2_deal", True))
    need_order = bool(factor_info.get("need_l2_order", False))
    archive = _preload_archive(api, args.date, all_codes, need_tick, need_deal, need_order)

    for result in _bench_native_df(args.label, archive, args.date, args.native_rows, args.rounds_native):
        print("BENCH_RESULT " + json.dumps(result, ensure_ascii=False), flush=True)
    gc.collect()

    for raw_size in args.sizes.split(","):
        size = int(raw_size.strip())
        codes = all_codes[:size]
        res = _bench_stockdata(
            args.label, args.date, args.end_time, codes, archive, daily_basic,
            factor_info, code_to_sec, args.rounds_stockdata,
        )
        print("BENCH_RESULT " + json.dumps(res, ensure_ascii=False), flush=True)
        gc.collect()
        if not args.skip_strategy:
            res = _bench_strategy(
                args.label, args.date, args.end_time, codes, archive, daily_basic,
                factor_info, factor_fn, code_to_sec, args.rounds_strategy,
            )
            print("BENCH_RESULT " + json.dumps(res, ensure_ascii=False), flush=True)
            gc.collect()

    print(f"BENCH_DONE label={args.label}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
