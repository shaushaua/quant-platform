#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Recompute daily factors from current SHM and compare with live daily parquet."""

import argparse
import io
import os
import time

import numpy as np
import oss2
import pandas as pd

from quant_platform.live_engine import native_engine as ne
from quant_platform.live_engine.schedule import ComputationSchedule


def _bucket(bucket_name: str) -> oss2.Bucket:
    return oss2.Bucket(
        oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"]),
        os.environ["OSS_ENDPOINT"],
        bucket_name,
    )


def _read_live_daily(date: str, bucket_name: str, prefix: str) -> pd.DataFrame:
    key = f"{prefix}/{date[:4]}/{date[:6]}/{date}/daily-feature/daily.parquet"
    obj = _bucket(bucket_name).get_object(key)
    return pd.read_parquet(io.BytesIO(obj.read()))


def _code6(df: pd.DataFrame) -> pd.Series:
    col = "code" if "code" in df.columns else "ID_QI"
    return df[col].astype(str).str.strip().str.split(".").str[0].str.zfill(6)


def _product_precision(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cols = out.select_dtypes(include=["float64", "float32"]).columns
    if len(cols) > 0:
        out[cols] = out[cols].round(6).astype("float32")
    return out


def _compare(a: pd.DataFrame, b: pd.DataFrame) -> int:
    a = a.copy()
    b = b.copy()
    a["_code"] = _code6(a)
    b["_code"] = _code6(b)
    cols = sorted(set(c for c in a.columns if c.startswith("F_")) & set(c for c in b.columns if c.startswith("F_")))
    ah = a[["_code", *cols]].drop_duplicates("_code").set_index("_code").sort_index()
    bh = b[["_code", *cols]].drop_duplicates("_code").set_index("_code").sort_index()
    cc = ah.index.intersection(bh.index)
    print(f"shm_shape={a.shape} live_shape={b.shape}")
    print(f"codes shm={len(ah)} live={len(bh)} common={len(cc)} only_shm={len(ah.index.difference(bh.index))} only_live={len(bh.index.difference(ah.index))}")
    print(f"factor_cols common={len(cols)}")
    if len(ah.index.difference(bh.index)):
        print("only_shm_sample", ah.index.difference(bh.index)[:20].tolist())
    if len(bh.index.difference(ah.index)):
        print("only_live_sample", bh.index.difference(ah.index)[:20].tolist())

    bad_cells = 0
    bad_cols = []
    samples = []
    for col in cols:
        av = pd.to_numeric(ah.loc[cc, col], errors="coerce").to_numpy("float64")
        bv = pd.to_numeric(bh.loc[cc, col], errors="coerce").to_numpy("float64")
        bad = ~np.isclose(av, bv, rtol=0, atol=0, equal_nan=True)
        nbad = int(bad.sum())
        if not nbad:
            continue
        ad = np.abs(av - bv)
        max_abs = float(np.nanmax(ad[bad])) if np.any(~np.isnan(ad[bad])) else float("nan")
        bad_cols.append((col, nbad, max_abs))
        bad_cells += nbad
        if len(samples) < 20:
            for i in np.flatnonzero(bad)[: 20 - len(samples)]:
                samples.append((str(cc[i]), col, av[i], bv[i], ad[i]))
    bad_cols.sort(key=lambda x: x[1], reverse=True)
    print(f"bad_cells={bad_cells} bad_cols={len(bad_cols)}")
    print("bad_cols_top20=")
    for row in bad_cols[:20]:
        print(row)
    print("bad_samples=")
    for row in samples:
        print(row)
    return 0 if bad_cells == 0 and len(ah) == len(bh) == len(cc) else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--end-time", default=os.environ.get("END_TIMES", "150000").split(",")[0])
    ap.add_argument("--batch-size", type=int, default=int(os.environ.get("LIVE_FACTOR_BATCH_SIZE", "128")))
    ap.add_argument("--out", default="/tmp/shm_daily_recompute.parquet")
    ap.add_argument("--live-prefix", default=os.environ.get("OSS_LIVE_PREFIX", "live-factors"))
    ap.add_argument("--result-bucket", default=os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result"))
    args = ap.parse_args()

    os.environ.setdefault("DAILY_USE_MULTI_CODE", "1")
    eng = ne.NativeEngine()
    eng.trading_day = args.date
    eng.factor_module = os.environ.get("FACTOR_MODULE", eng.factor_module)
    eng.daily_factor_module = os.environ.get("DAILY_FACTOR_MODULE", eng.daily_factor_module)
    eng._load_daily_basic()

    files_by_code = eng._scan_shm_files()
    all_codes = sorted(files_by_code.keys())
    print(f"shm_files codes={len(all_codes)} sample={all_codes[:5]}")

    factor_module_path = eng.daily_factor_module or eng.factor_module
    active_factor_info = eng._daily_factor_info or eng.factor_info
    if not active_factor_info:
        mod = ne.import_strategy_module(factor_module_path)
        active_factor_info = getattr(mod, "FACTOR_INFO", getattr(mod, "factor_info", {})) or {}
    ne._market_df = eng._market_df
    ne._daily_basic_df = eng._daily_basic_df
    ne._base_ns = pd.Timestamp(args.date).value

    tick_paths = {code: files_by_code[code].get(ne.KIND_TICK, "") for code in all_codes}
    deal_paths = {code: files_by_code[code].get(ne.KIND_DEAL, "") for code in all_codes}
    order_paths = {code: files_by_code[code].get(ne.KIND_ORDER, "") for code in all_codes}
    batches = [all_codes[i:i + args.batch_size] for i in range(0, len(all_codes), args.batch_size)]
    wall_secs = 15 * 3600 + 10 * 60

    rows = []
    errors = 0
    t0 = time.time()
    for i, batch in enumerate(batches, start=1):
        task = (
            batch, args.date, args.end_time, wall_secs, {},
            {code: tick_paths.get(code, "") for code in batch},
            {code: deal_paths.get(code, "") for code in batch},
            {code: order_paths.get(code, "") for code in batch},
            args.date, factor_module_path, active_factor_info, True,
        )
        batch_rows, batch_errors, _build_ms, _factor_ms, err = ne._compute_code_batch_shm(task)
        if err:
            print(f"batch {i}/{len(batches)} err={err}")
        errors += batch_errors
        rows.extend(batch_rows)
        if i == 1 or i == len(batches) or i % 5 == 0:
            print(f"batch {i}/{len(batches)} rows={len(batch_rows)} total_rows={len(rows)} errors={errors}")

    shm_df = _product_precision(pd.DataFrame(rows))
    shm_df.to_parquet(args.out, index=False)
    print(f"shm_recomputed_saved={args.out} shape={shm_df.shape} elapsed={time.time() - t0:.1f}s errors={errors}")

    live_df = _read_live_daily(args.date, args.result_bucket, args.live_prefix)
    return _compare(shm_df, live_df)


if __name__ == "__main__":
    raise SystemExit(main())
