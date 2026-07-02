#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run historical daily factors after filtering archive rows to SHM SeqNum.

This is a diagnostic control-variable check:

1. Build per-code SeqNum sets from restored/live SHM mmap files.
2. Monkey-patch DataAPI.get_daily_data() for tick/order/deal so historical
   parquet rows not present in SHM are removed before the normal factor engine
   sees them.
3. Run the same historical factor path and compare with live daily output.

It does not change production code or upload any result.
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from typing import Dict

import numpy as np
import pandas as pd

from quant_platform.data.api import DataAPI
from quant_platform.data.native_shm_reader import NativeShmReader
from quant_platform.live_engine import native_engine as ne

from scripts.oneoff.compare_hist_daily_vs_live import (
    _compare,
    _load_strategy,
    _normal_code,
    _read_oss_parquet,
)
from quant_platform.factor.engine import calc_factors_by_date_range


_KIND_NAME_BY_ID = {
    ne.KIND_TICK: "tick",
    ne.KIND_ORDER: "order",
    ne.KIND_DEAL: "deal",
}
_SEQ_IDX_BY_KIND = {
    "tick": ne._tick_buf_cols.index("SeqNum"),
    "order": ne._order_buf_cols.index("SeqNum"),
    "deal": ne._deal_buf_cols.index("SeqNum"),
}


def _code6(code: str) -> str:
    return str(code).strip().split(".")[0].zfill(6)


def _build_shm_seq_sets(shm_dir: str, needed_kinds: set[str], codes: list[str]) -> Dict[str, Dict[str, np.ndarray]]:
    wanted_codes = {_normal_code(pd.Series(codes)).iloc[i] for i in range(len(codes))} if codes else set()
    wanted_code6 = {_code6(c) for c in wanted_codes}
    files = ne.scan_shm_dir(shm_dir)
    seqs: Dict[str, Dict[str, np.ndarray]] = {kind: {} for kind in needed_kinds}
    scanned = defaultdict(int)
    skipped = 0
    for (code, kind_id), path in sorted(files.items()):
        kind = _KIND_NAME_BY_ID.get(kind_id)
        if kind not in needed_kinds:
            continue
        if wanted_code6 and _code6(code) not in wanted_code6:
            skipped += 1
            continue
        try:
            rows = NativeShmReader(path).read_rows(max_retries=50)
        except Exception as exc:
            print(f"[shm-seq] skip {kind} {code}: {exc}")
            continue
        if rows.size == 0:
            seqs[kind][code] = np.array([], dtype=np.int64)
            continue
        raw = rows[:, _SEQ_IDX_BY_KIND[kind]]
        raw = raw[np.isfinite(raw)]
        seqs[kind][code] = np.unique(raw.astype(np.int64, copy=False))
        scanned[kind] += 1
    for kind in sorted(needed_kinds):
        total_rows = sum(len(v) for v in seqs[kind].values())
        print(f"[shm-seq] kind={kind} codes={len(seqs[kind])} seqs={total_rows}")
    if skipped:
        print(f"[shm-seq] skipped_non_requested_files={skipped}")
    return seqs


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


def _patch_data_api(seq_sets: Dict[str, Dict[str, np.ndarray]]) -> None:
    original = DataAPI.get_daily_data
    map_cache: dict[tuple[int, str], tuple[dict[str, int], dict[int, str]]] = {}

    def _maps(self: DataAPI, date: str) -> tuple[dict[str, int], dict[int, str]]:
        key = (id(self), date)
        if key not in map_cache:
            daily_basic = original(self, date, "daily_basic", codes=None)
            map_cache[key] = _build_security_maps(daily_basic)
            print(
                f"[hist-filter] daily_basic map date={date} "
                f"codes={len(map_cache[key][0])}"
            )
        return map_cache[key]

    def filtered_get_daily_data(self: DataAPI, date: str, data_type: str = "tick", codes=None) -> pd.DataFrame:
        df = original(self, date, data_type, codes=codes)
        if data_type not in seq_sets or df.empty or "SeqNum" not in df.columns:
            return df

        seq_by_code = {_code6(k): v for k, v in seq_sets[data_type].items()}
        requested = {_code6(c) for c in codes} if codes else None
        if requested is not None:
            seq_by_code = {k: v for k, v in seq_by_code.items() if k in requested}
        if not seq_by_code:
            print(f"[hist-filter] {data_type} no seq set for requested codes; rows {len(df)}->0")
            return df.iloc[0:0].copy()

        code_col = next((c for c in ("Code", "code", "stock_code", "SECURITY_ID") if c in df.columns), None)
        if code_col is None:
            print(f"[hist-filter] {data_type} has no code column; unchanged rows={len(df)}")
            return df

        _code_to_sec, sec_to_code = _maps(self, date)
        parts = []
        before = len(df)
        seen = 0
        seq_num = pd.to_numeric(df["SeqNum"], errors="coerce").fillna(-1).astype("int64")

        if pd.api.types.is_integer_dtype(df[code_col]) or code_col == "SECURITY_ID":
            code_values = pd.to_numeric(df[code_col], errors="coerce").fillna(-1).astype("int64")
            for sec_id, idx in code_values.groupby(code_values, sort=False).groups.items():
                code6 = sec_to_code.get(int(sec_id))
                if code6 is None or code6 not in seq_by_code:
                    continue
                local_seq = seq_num.loc[idx]
                keep_idx = local_seq.index[local_seq.isin(seq_by_code[code6])]
                if len(keep_idx):
                    parts.append(df.loc[keep_idx])
                seen += len(idx)
        else:
            norm = df[code_col].astype(str).str.split(".").str[0].str.zfill(6)
            for code6, idx in norm.groupby(norm, sort=False).groups.items():
                if code6 not in seq_by_code:
                    continue
                local_seq = seq_num.loc[idx]
                keep_idx = local_seq.index[local_seq.isin(seq_by_code[code6])]
                if len(keep_idx):
                    parts.append(df.loc[keep_idx])
                seen += len(idx)

        out = pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()
        print(
            f"[hist-filter] {data_type} rows {before}->{len(out)} "
            f"removed={before - len(out)} matched_input_rows={seen} codes={len(seq_by_code)}"
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
    ap.add_argument("--securities-from-live", action="store_true", default=True)
    ap.add_argument("--atol", type=float, default=0.0)
    ap.add_argument("--rtol", type=float, default=0.0)
    args = ap.parse_args()

    os.environ.setdefault("FACTOR_FORCE_STREAMING", "1")
    os.environ.setdefault("DATA_BATCH_SIZE", "200")
    os.environ.setdefault("FACTOR_BATCH_SIZE", "100")
    os.environ.setdefault("FACTOR_TASK_CODE_BATCH_SIZE", "25")

    _mod, factor_info, factor_fn, securities = _load_strategy(args.strategy_module)
    if factor_info is None:
        raise RuntimeError(f"{args.strategy_module} has no factor_info")

    y = args.date[:4]
    ym = args.date[:6]
    live_key = f"{args.live_prefix}/{y}/{ym}/{args.date}/daily-feature/daily.parquet"
    live = _read_oss_parquet(args.live_bucket, live_key)
    print(f"live_loaded=oss://{args.live_bucket}/{live_key} shape={live.shape}")

    if args.securities_from_live:
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
    seq_sets = _build_shm_seq_sets(args.shm_dir, needed_kinds, securities or [])
    _patch_data_api(seq_sets)

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
