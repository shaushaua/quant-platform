#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Low-memory historical daily factor check filtered by SHM SeqNum.

Unlike compare_hist_filtered_by_shm.py, this script does not keep all SHM
SeqNum sets in memory.  It reads only the current engine data_batch's codes
inside DataAPI.get_daily_data(), filters historical rows, then releases them.
"""

from __future__ import annotations

import argparse
import os
from functools import lru_cache

import numpy as np
import pandas as pd

from quant_platform.data.api import DataAPI
from quant_platform.data.native_shm_reader import NativeShmReader
from quant_platform.live_engine import native_engine as ne
from quant_platform.factor.engine import calc_factors_by_date_range

from scripts.oneoff.compare_hist_daily_vs_live import (
    _compare,
    _load_strategy,
    _normal_code,
    _read_oss_parquet,
)


_KIND_ID = {"tick": ne.KIND_TICK, "order": ne.KIND_ORDER, "deal": ne.KIND_DEAL}
_SEQ_IDX = {
    "tick": ne._tick_buf_cols.index("SeqNum"),
    "order": ne._order_buf_cols.index("SeqNum"),
    "deal": ne._deal_buf_cols.index("SeqNum"),
}


def _code6(code: str) -> str:
    return str(code).strip().split(".")[0].zfill(6)


def _normalize_code(code: str) -> str:
    raw = str(code).strip()
    if "." not in raw:
        raw = raw.zfill(6)
    raw = raw.replace(".SZ", ".XSHE").replace(".SH", ".XSHG")
    if "." not in raw:
        return raw
    left, right = raw.split(".", 1)
    return f"{left.zfill(6)}.{right}"


def _build_security_maps(daily_basic: pd.DataFrame) -> tuple[dict[str, int], dict[int, str]]:
    if daily_basic.empty or "SECURITY_ID" not in daily_basic.columns:
        return {}, {}
    code_col = "ID_QI" if "ID_QI" in daily_basic.columns else ("code" if "code" in daily_basic.columns else None)
    if code_col is None:
        return {}, {}
    code6 = daily_basic[code_col].astype(str).str.split(".").str[0].str.zfill(6)
    sec = pd.to_numeric(daily_basic["SECURITY_ID"], errors="coerce")
    ok = sec.notna()
    code_to_sec = dict(zip(code6[ok], sec[ok].astype("int64")))
    sec_to_code = {int(v): k for k, v in code_to_sec.items()}
    return code_to_sec, sec_to_code


def _patch_data_api(shm_dir: str, needed_kinds: set[str]) -> None:
    original = DataAPI.get_daily_data
    shm_files = ne.scan_shm_dir(shm_dir)
    print(f"[hist-filter-lazy] shm_files={len(shm_files)} dir={shm_dir}")
    map_cache: dict[tuple[int, str], tuple[dict[str, int], dict[int, str]]] = {}

    def _maps(self: DataAPI, date: str) -> tuple[dict[str, int], dict[int, str]]:
        key = (id(self), date)
        if key not in map_cache:
            daily_basic = original(self, date, "daily_basic", codes=None)
            map_cache[key] = _build_security_maps(daily_basic)
            print(f"[hist-filter-lazy] daily_basic map date={date} codes={len(map_cache[key][0])}")
        return map_cache[key]

    @lru_cache(maxsize=2048)
    def _read_seq(kind: str, code: str) -> np.ndarray:
        path = shm_files.get((_normalize_code(code), _KIND_ID[kind]))
        if not path:
            return np.array([], dtype=np.int64)
        try:
            rows = NativeShmReader(path).read_rows(max_retries=50)
        except Exception as exc:
            print(f"[hist-filter-lazy] skip {kind} {code}: {exc}")
            return np.array([], dtype=np.int64)
        if rows.size == 0:
            return np.array([], dtype=np.int64)
        raw = rows[:, _SEQ_IDX[kind]]
        raw = raw[np.isfinite(raw)]
        return np.unique(raw.astype(np.int64, copy=False))

    def filtered_get_daily_data(self: DataAPI, date: str, data_type: str = "tick", codes=None) -> pd.DataFrame:
        df = original(self, date, data_type, codes=codes)
        if data_type not in needed_kinds or df.empty or "SeqNum" not in df.columns:
            return df

        requested = [_normalize_code(c) for c in (codes or [])]
        if not requested:
            print(f"[hist-filter-lazy] {data_type} no requested codes; unchanged rows={len(df)}")
            return df

        seq_by_code6 = {_code6(code): _read_seq(data_type, code) for code in requested}
        seq_by_code6 = {k: v for k, v in seq_by_code6.items() if len(v)}
        if not seq_by_code6:
            print(f"[hist-filter-lazy] {data_type} no SHM seq for batch; rows {len(df)}->0")
            return df.iloc[0:0].copy()

        code_col = next((c for c in ("Code", "code", "stock_code", "SECURITY_ID") if c in df.columns), None)
        if code_col is None:
            print(f"[hist-filter-lazy] {data_type} no code column; unchanged rows={len(df)}")
            return df

        _code_to_sec, sec_to_code = _maps(self, date)
        before = len(df)
        seq_num = pd.to_numeric(df["SeqNum"], errors="coerce").fillna(-1).astype("int64")
        parts = []

        if pd.api.types.is_integer_dtype(df[code_col]) or code_col == "SECURITY_ID":
            code_values = pd.to_numeric(df[code_col], errors="coerce").fillna(-1).astype("int64")
            for sec_id, idx in code_values.groupby(code_values, sort=False).groups.items():
                code6 = sec_to_code.get(int(sec_id))
                if code6 is None or code6 not in seq_by_code6:
                    continue
                local_seq = seq_num.loc[idx]
                keep_idx = local_seq.index[local_seq.isin(seq_by_code6[code6])]
                if len(keep_idx):
                    parts.append(df.loc[keep_idx])
        else:
            norm = df[code_col].astype(str).str.split(".").str[0].str.zfill(6)
            for code6, idx in norm.groupby(norm, sort=False).groups.items():
                if code6 not in seq_by_code6:
                    continue
                local_seq = seq_num.loc[idx]
                keep_idx = local_seq.index[local_seq.isin(seq_by_code6[code6])]
                if len(keep_idx):
                    parts.append(df.loc[keep_idx])

        out = pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()
        removed = before - len(out)
        print(
            f"[hist-filter-lazy] {data_type} batch_codes={len(requested)} "
            f"seq_codes={len(seq_by_code6)} rows {before}->{len(out)} removed={removed}"
        )
        return out

    DataAPI.get_daily_data = filtered_get_daily_data


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--strategy-module", default=os.environ.get("DAILY_FACTOR_MODULE", "quant_platform.strategies.protected_eillen_strategy_v2"))
    ap.add_argument("--end-time", default=os.environ.get("END_TIMES", "150000").split(",")[0])
    ap.add_argument("--data-path", default=os.environ.get("OSS_DATA_PATH", "2026"))
    ap.add_argument("--live-bucket", default=os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result"))
    ap.add_argument("--live-prefix", default=os.environ.get("LIVE_FACTOR_PREFIX", "live-factors"))
    ap.add_argument("--shm-dir", default=os.environ.get("NATIVE_SHM_DIR", "/data/quant/shm"))
    ap.add_argument("--out", default="/tmp/hist_filtered_by_shm_daily.parquet")
    ap.add_argument("--atol", type=float, default=0.0)
    ap.add_argument("--rtol", type=float, default=0.0)
    args = ap.parse_args()

    os.environ.setdefault("FACTOR_FORCE_STREAMING", "1")
    os.environ.setdefault("DATA_BATCH_SIZE", "100")
    os.environ.setdefault("FACTOR_BATCH_SIZE", "50")
    os.environ.setdefault("FACTOR_TASK_CODE_BATCH_SIZE", "25")

    _mod, factor_info, factor_fn, securities = _load_strategy(args.strategy_module)
    if factor_info is None:
        raise RuntimeError(f"{args.strategy_module} has no factor_info")

    y = args.date[:4]
    ym = args.date[:6]
    live_key = f"{args.live_prefix}/{y}/{ym}/{args.date}/daily-feature/daily.parquet"
    live = _read_oss_parquet(args.live_bucket, live_key)
    print(f"live_loaded=oss://{args.live_bucket}/{live_key} shape={live.shape}")
    live_code_col = "code" if "code" in live.columns else "ID_QI"
    securities = _normal_code(live[live_code_col]).tolist()
    print(f"securities_from_live={len(securities)} sample={securities[:5]}")

    needed_kinds = set()
    if factor_info.get("need_l1_tick"):
        needed_kinds.add("tick")
    if factor_info.get("need_l2_order"):
        needed_kinds.add("order")
    if factor_info.get("need_l2_deal"):
        needed_kinds.add("deal")
    _patch_data_api(args.shm_dir, needed_kinds)

    results: list[pd.DataFrame] = []

    def outfun(date: str, end_time: str, df: pd.DataFrame) -> None:
        print(f"historical_filtered_out date={date} end_time={end_time} shape={df.shape}")
        results.append(df.copy())

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date=args.date,
        end_date=args.date,
        end_times=[args.end_time],
        securities=securities or [],
        processes=int(os.environ.get("WORKERS", os.environ.get("FACTOR_WORKERS", "1"))),
        factor_data_handler=factor_fn,
        outfun=outfun,
        oss_base_path=args.data_path,
    )
    if not results:
        raise RuntimeError("historical filtered calculation produced no result")

    hist = pd.concat(results, ignore_index=True)
    fcols = hist.select_dtypes(include=["float64", "float32"]).columns
    if len(fcols) > 0:
        hist[fcols] = hist[fcols].round(6).astype("float32")
    hist.to_parquet(args.out, index=False)
    print(f"historical_filtered_saved={args.out} shape={hist.shape}")
    return _compare(hist, live, args.atol, args.rtol)


if __name__ == "__main__":
    raise SystemExit(main())
