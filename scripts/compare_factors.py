#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run daily V2 factor calc for one date, optionally compare with live.

Two modes:
  1. Live exists for date → run historical + compare cell-by-cell
  2. Live missing → run historical only, upload to OSS daily-feature path

Usage:
  python scripts/compare_factors.py --date 20260702   # live exists → compare
  python scripts/compare_factors.py --date 20260703   # live missing → run + upload

Env:
  OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET (required)
  OSS_ENDPOINT (default internal)
  OSS_RESULT_BUCKET (default stock-mdl-data-result)
  WORKERS (default 4)
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import importlib
import numpy as np
import oss2
import pandas as pd

from quant_platform.factor.engine import calc_factors_by_date_range


STRATEGY_MODULE = "quant_platform.strategies.protected_eillen_strategy_v2"
END_TIME = "150000"
CATEGORY = "daily-feature"
LIVE_OBJECT = "daily.parquet"


# ── OSS helpers ───────────────────────────────────────────────────────────────

def _bucket(bucket_name: str) -> oss2.Bucket:
    return oss2.Bucket(
        oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"]),
        os.environ["OSS_ENDPOINT"],
        bucket_name,
    )


def _oss_exists(bucket_name: str, key: str) -> bool:
    try:
        _bucket(bucket_name).head_object(key)
        return True
    except oss2.exceptions.NoSuchKey:
        return False


def _read_oss_parquet(bucket_name: str, key: str) -> pd.DataFrame:
    obj = _bucket(bucket_name).get_object(key)
    return pd.read_parquet(io.BytesIO(obj.read()))


def _upload_oss_parquet(bucket_name: str, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    _bucket(bucket_name).put_object(key, buf)
    print(f"uploaded oss://{bucket_name}/{key} ({len(df)} rows, {len(df.columns)} cols)")


# ── Strategy loader ───────────────────────────────────────────────────────────

def _load_strategy(module_path: str):
    mod = importlib.import_module(module_path)
    factor_info = getattr(mod, "factor_info", getattr(mod, "FACTOR_INFO", None))
    factor_fn = getattr(mod, "factor_calculation")
    securities = getattr(mod, "securities", None)
    return mod, factor_info, factor_fn, securities


def _normal_code(s: "pd.Series") -> "pd.Series":
    out = s.astype(str).str.strip()
    out = out.str.replace(r"\.SZ$", ".XSHE", regex=True)
    out = out.str.replace(r"\.SH$", ".XSHG", regex=True)
    no_suffix = ~out.str.contains(r"\.", regex=True)
    out.loc[no_suffix] = out.loc[no_suffix].str.zfill(6)
    return out


def _factor_cols(df: pd.DataFrame) -> list[str]:
    return sorted(c for c in df.columns if c.startswith("F_"))


# ── Compare ───────────────────────────────────────────────────────────────────

def _compare(hist: pd.DataFrame, live: pd.DataFrame, atol: float, rtol: float) -> int:
    hist = hist.copy()
    live = live.copy()
    if "code" in hist.columns:
        hist["_code"] = _normal_code(hist["code"]).str.split(".").str[0].str.zfill(6)
    elif "ID_QI" in hist.columns:
        hist["_code"] = _normal_code(hist["ID_QI"]).str.split(".").str[0].str.zfill(6)
    else:
        raise RuntimeError(f"historical result has no code/ID_QI column: {list(hist.columns)[:20]}")

    if "code" in live.columns:
        live["_code"] = _normal_code(live["code"]).str.split(".").str[0].str.zfill(6)
    elif "ID_QI" in live.columns:
        live["_code"] = _normal_code(live["ID_QI"]).str.split(".").str[0].str.zfill(6)
    else:
        raise RuntimeError(f"live result has no code/ID_QI column: {list(live.columns)[:20]}")

    hist_cols = _factor_cols(hist)
    live_cols = _factor_cols(live)
    common_cols = sorted(set(hist_cols) & set(live_cols))
    only_hist = sorted(set(hist_cols) - set(live_cols))
    only_live = sorted(set(live_cols) - set(hist_cols))

    print(f"hist_shape={hist.shape} live_shape={live.shape}")
    print(f"hist_factor_cols={len(hist_cols)} live_factor_cols={len(live_cols)} common={len(common_cols)}")
    print(f"only_hist_cols={len(only_hist)} sample={only_hist[:10]}")
    print(f"only_live_cols={len(only_live)} sample={only_live[:10]}")

    h = hist[["_code", *common_cols]].drop_duplicates("_code").set_index("_code").sort_index()
    l = live[["_code", *common_cols]].drop_duplicates("_code").set_index("_code").sort_index()
    common_codes = h.index.intersection(l.index)
    only_hist_codes = h.index.difference(l.index)
    only_live_codes = l.index.difference(h.index)
    print(f"hist_codes={len(h)} live_codes={len(l)} common_codes={len(common_codes)}")
    print(f"only_hist_codes={len(only_hist_codes)} sample={only_hist_codes[:10].tolist()}")
    print(f"only_live_codes={len(only_live_codes)} sample={only_live_codes[:10].tolist()}")

    h = h.loc[common_codes, common_cols]
    l = l.loc[common_codes, common_cols]

    bad_cells = 0
    max_abs = 0.0
    max_rel = 0.0
    bad_cols: list[tuple[str, int, float, float]] = []
    samples = []
    for col in common_cols:
        hv = pd.to_numeric(h[col], errors="coerce").to_numpy(dtype="float64")
        lv = pd.to_numeric(l[col], errors="coerce").to_numpy(dtype="float64")
        both_nan = np.isnan(hv) & np.isnan(lv)
        close = np.isclose(hv, lv, rtol=rtol, atol=atol, equal_nan=True)
        bad = ~(both_nan | close)
        n_bad = int(bad.sum())
        if n_bad:
            abs_diff = np.abs(hv - lv)
            rel_diff = abs_diff / np.maximum(np.abs(hv), np.abs(lv))
            col_max_abs = float(np.nanmax(abs_diff[bad]))
            col_max_rel = float(np.nanmax(rel_diff[bad]))
            bad_cols.append((col, n_bad, col_max_abs, col_max_rel))
            bad_cells += n_bad
            max_abs = max(max_abs, col_max_abs)
            max_rel = max(max_rel, col_max_rel)
            if len(samples) < 20:
                idxs = np.flatnonzero(bad)[: 20 - len(samples)]
                for i in idxs:
                    samples.append((str(common_codes[i]), col, hv[i], lv[i], abs_diff[i]))

    bad_cols.sort(key=lambda x: x[1], reverse=True)
    print(f"bad_cells={bad_cells} bad_cols={len(bad_cols)} max_abs={max_abs:.12g} max_rel={max_rel:.12g}")
    print("bad_cols_top20=")
    for row in bad_cols[:20]:
        print(row)
    print("bad_samples=")
    for row in samples:
        print(row)

    return 0 if not only_hist and not only_live and bad_cells == 0 else 1


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--strategy-module", default=STRATEGY_MODULE)
    ap.add_argument("--end-time", default=END_TIME)
    ap.add_argument("--data-path", default=os.environ.get("OSS_DATA_PATH", "2026"))
    ap.add_argument("--result-bucket", default=os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result"))
    ap.add_argument("--live-prefix", default=os.environ.get("LIVE_FACTOR_PREFIX", "live-factors"))
    ap.add_argument("--out", default=None, help="Local parquet path (default /tmp/daily_factor_{date}.parquet)")
    ap.add_argument("--no-upload", action="store_true", help="Skip uploading to OSS")
    ap.add_argument("--no-compare", action="store_true", help="Skip comparison even if live exists")
    ap.add_argument("--codes", default=None, help="Comma-separated code subset")
    ap.add_argument("--securities-from-live", action="store_true",
                    help="Use live's code list as universe (when live exists)")
    ap.add_argument("--atol", type=float, default=0.0)
    ap.add_argument("--rtol", type=float, default=0.0)
    args = ap.parse_args()

    os.environ.setdefault("FACTOR_FORCE_STREAMING", "1")
    os.environ.setdefault("DATA_BATCH_SIZE", "200")
    os.environ.setdefault("FACTOR_BATCH_SIZE", "100")
    os.environ.setdefault("FACTOR_TASK_CODE_BATCH_SIZE", "25")

    y = args.date[:4]
    ym = args.date[:6]
    live_key = f"{args.live_prefix}/{y}/{ym}/{args.date}/{CATEGORY}/{LIVE_OBJECT}"
    out_path = args.out or f"/tmp/daily_factor_{args.date}.parquet"

    print(f"=== run_daily_factor date={args.date} strategy={args.strategy_module} end_time={args.end_time}")
    print(f"target_oss=oss://{args.result_bucket}/{live_key}")

    live = None
    if not args.no_compare and _oss_exists(args.result_bucket, live_key):
        live = _read_oss_parquet(args.result_bucket, live_key)
        print(f"live_loaded shape={live.shape}")
    else:
        print(f"live_missing_or_skipped: oss://{args.result_bucket}/{live_key}")

    _mod, factor_info, factor_fn, securities = _load_strategy(args.strategy_module)
    if factor_info is None:
        raise RuntimeError(f"{args.strategy_module} has no factor_info")
    print(f"strategy_loaded factor_info={factor_info} strategy_securities={len(securities or [])}")

    if args.codes:
        securities = [c.strip() for c in args.codes.split(",")]
        print(f"codes_override={len(securities)} sample={securities[:5]}")
    elif args.securities_from_live and live is not None:
        live_code_col = "code" if "code" in live.columns else "ID_QI"
        securities = _normal_code(live[live_code_col]).tolist()
        print(f"securities_from_live={len(securities)} sample={securities[:5]}")
    else:
        securities = securities or []
        print(f"using_strategy_securities (empty=full_market): {len(securities)}")

    results: list[pd.DataFrame] = []

    def outfun(date: str, et: str, df: pd.DataFrame) -> None:
        print(f"historical_out date={date} end_time={et} shape={df.shape}")
        results.append(df.copy())

    workers = int(os.environ.get("WORKERS", os.environ.get("FACTOR_WORKERS", "4")))
    print(f"workers={workers}")

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date=args.date,
        end_date=args.date,
        end_times=[args.end_time],
        securities=securities,
        processes=workers,
        factor_data_handler=factor_fn,
        outfun=outfun,
        oss_base_path=args.data_path,
    )

    if not results:
        raise RuntimeError("historical calculation produced no result")

    hist = pd.concat(results, ignore_index=True)
    fcols = hist.select_dtypes(include=["float64", "float32"]).columns
    if len(fcols) > 0:
        hist[fcols] = hist[fcols].round(6).astype("float32")
    hist.to_parquet(out_path, index=False)
    print(f"historical_saved_local={out_path} shape={hist.shape}")

    if not args.no_upload:
        _upload_oss_parquet(args.result_bucket, live_key, hist)

    if live is not None:
        print("\n=== compare_with_live")
        return _compare(hist, live, args.atol, args.rtol)

    print("\n=== done (no live comparison — live was missing or skipped)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
