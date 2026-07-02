#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Diagnose SHM vs historical parquet differences by joining on SeqNum."""

from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd

from quant_platform.data.api import DataAPI
from quant_platform.live_engine import native_engine as ne
from scripts.oneoff.compare_shm_vs_archive_code import _build_shm_df


def _cut(df: pd.DataFrame, end_time: str) -> pd.DataFrame:
    if df.empty or not end_time or "Time" not in df.columns:
        return df
    ts = pd.to_datetime(df["Time"], errors="coerce")
    day = ts.dropna().iloc[0].normalize() if ts.notna().any() else None
    if day is None:
        return df
    end = day + pd.Timedelta(
        hours=int(end_time[:2]), minutes=int(end_time[2:4]), seconds=int(end_time[4:6])
    )
    return df[ts <= end].copy()


def _norm(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    keep = [c for c in cols if c in df.columns]
    out = df[keep].copy()
    if "SeqNum" in out.columns:
        out["SeqNum"] = pd.to_numeric(out["SeqNum"], errors="coerce").astype("Int64")
        out = out.dropna(subset=["SeqNum"])
        out["SeqNum"] = out["SeqNum"].astype("int64")
        out = out.sort_values("SeqNum", kind="mergesort").drop_duplicates("SeqNum", keep="first")
    return out.reset_index(drop=True)


def _cmp_values(a: pd.Series, b: pd.Series) -> tuple[int, object, object, object]:
    if pd.api.types.is_datetime64_any_dtype(a) or pd.api.types.is_datetime64_any_dtype(b):
        aa = pd.to_datetime(a, errors="coerce").astype("int64").to_numpy()
        bb = pd.to_datetime(b, errors="coerce").astype("int64").to_numpy()
        bad = aa != bb
        if not bad.any():
            return 0, None, None, None
        i = int(np.flatnonzero(bad)[0])
        return int(bad.sum()), a.iloc[i], b.iloc[i], int(aa[i] - bb[i])
    aa = pd.to_numeric(a, errors="coerce")
    bb = pd.to_numeric(b, errors="coerce")
    if not aa.isna().all() or not bb.isna().all():
        av = aa.to_numpy("float64")
        bv = bb.to_numpy("float64")
        bad = ~np.isclose(av, bv, rtol=0, atol=0, equal_nan=True)
        if not bad.any():
            return 0, None, None, None
        i = int(np.flatnonzero(bad)[0])
        diff = av[i] - bv[i] if np.isfinite(av[i]) and np.isfinite(bv[i]) else None
        return int(bad.sum()), a.iloc[i], b.iloc[i], diff
    bad = a.astype(str).to_numpy() != b.astype(str).to_numpy()
    if not bad.any():
        return 0, None, None, None
    i = int(np.flatnonzero(bad)[0])
    return int(bad.sum()), a.iloc[i], b.iloc[i], None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--codes", nargs="+", required=True)
    ap.add_argument("--kinds", nargs="+", default=["deal", "tick"])
    ap.add_argument("--end-time", default="150000")
    ap.add_argument("--data-path", default=os.environ.get("OSS_DATA_PATH", "2026"))
    args = ap.parse_args()

    ne._base_ns = pd.Timestamp(args.date).value
    api = DataAPI(mode="backtest", oss_base_path=args.data_path)
    files = ne.scan_shm_dir(os.environ.get("NATIVE_SHM_DIR", "/data/quant/shm"))

    kind_id = {"tick": ne.KIND_TICK, "deal": ne.KIND_DEAL, "order": ne.KIND_ORDER}
    cols = {
        "deal": [
            "Code", "Time", "UpdateTime", "SaleOrderID", "BuyOrderID", "Side",
            "Price", "Volume", "Money", "Channel", "SeqNum",
        ],
        "tick": [
            "Code", "Time", "UpdateTime", "CurrentPrice", "TotalVolume", "TotalMoney",
            "PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice",
            "AskPrice1", "AskVolume1", "BidPrice1", "BidVolume1", "Channel", "SeqNum",
        ],
        "order": [
            "Code", "Time", "UpdateTime", "OrderID", "Side", "Price", "Volume",
            "OrderType", "Channel", "SeqNum",
        ],
    }

    any_diff = False
    for code in args.codes:
        for kind in args.kinds:
            print(f"\n=== {code} {kind} ===")
            path = files.get((code, kind_id[kind]))
            if not path:
                print("SHM_MISSING")
                any_diff = True
                continue
            shm = _norm(_cut(_build_shm_df(path, kind, args.date, code), args.end_time), cols[kind])
            hist = _norm(_cut(api.load_stock_data(code, args.date, kind), args.end_time), cols[kind])
            sseq = pd.Index(shm["SeqNum"]) if "SeqNum" in shm else pd.Index([])
            hseq = pd.Index(hist["SeqNum"]) if "SeqNum" in hist else pd.Index([])
            common = sseq.intersection(hseq)
            only_shm = sseq.difference(hseq)
            only_hist = hseq.difference(sseq)
            print(
                f"rows shm={len(shm)} hist={len(hist)} common_seq={len(common)} "
                f"only_shm={len(only_shm)} only_hist={len(only_hist)}"
            )
            if len(only_hist):
                extra = hist[hist["SeqNum"].isin(only_hist)]
                print(f"only_hist_seq_sample={only_hist[:10].tolist()}")
                if "Time" in extra:
                    print(f"only_hist_time_range={extra['Time'].min()}..{extra['Time'].max()}")
            if len(only_shm):
                extra = shm[shm["SeqNum"].isin(only_shm)]
                print(f"only_shm_seq_sample={only_shm[:10].tolist()}")
                if "Time" in extra:
                    print(f"only_shm_time_range={extra['Time'].min()}..{extra['Time'].max()}")

            sj = shm[shm["SeqNum"].isin(common)].set_index("SeqNum").sort_index()
            hj = hist[hist["SeqNum"].isin(common)].set_index("SeqNum").sort_index()
            common_cols = [c for c in cols[kind] if c != "SeqNum" and c in sj.columns and c in hj.columns]
            col_diffs = []
            for col in common_cols:
                bad, first_shm, first_hist, delta = _cmp_values(sj[col], hj[col])
                if bad:
                    col_diffs.append((col, bad, first_shm, first_hist, delta))
                    any_diff = True
            if col_diffs:
                print("COMMON_SEQ_FIELD_DIFFS")
                for row in col_diffs:
                    print(row)
            else:
                print("COMMON_SEQ_FIELDS_OK")
            if len(only_hist) or len(only_shm):
                any_diff = True
    return 1 if any_diff else 0


if __name__ == "__main__":
    raise SystemExit(main())
