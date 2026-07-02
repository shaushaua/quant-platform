#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Compare per-code SHM DataFrames with restored OSS archive DataFrames."""

import argparse
import os
import warnings

import numpy as np
import pandas as pd

from quant_platform.data.api import DataAPI
from quant_platform.live_engine import native_engine as ne

warnings.filterwarnings("ignore", category=FutureWarning)


def _build_shm_df(path: str, kind: str, date: str, code: str) -> pd.DataFrame:
    if kind == "tick":
        return ne._build_df_from_native(
            ne.NativeShmReader(path),
            ne._tick_columns,
            ne._tick_buf_cols,
            ne._tick_time_idx,
            ne._tick_updtime_idx,
            date,
            code,
            ne._tick_volume_idx,
            True,
            "tick",
        )
    if kind == "deal":
        return ne._build_df_from_native(
            ne.NativeShmReader(path),
            ne._deal_columns,
            ne._deal_buf_cols,
            ne._deal_time_idx,
            ne._deal_updtime_idx,
            date,
            code,
            ne._deal_volume_idx,
            True,
            "deal",
        )
    if kind == "order":
        return ne._build_df_from_native(
            ne.NativeShmReader(path),
            ne._order_columns,
            ne._order_buf_cols,
            ne._order_time_idx,
            ne._order_updtime_idx,
            date,
            code,
            ne._order_volume_idx,
            True,
            "order",
        )
    raise ValueError(kind)


def _cut(df: pd.DataFrame, end_time: str) -> pd.DataFrame:
    if df.empty or not end_time or "Time" not in df.columns:
        return df
    hh, mm, ss = int(end_time[:2]), int(end_time[2:4]), int(end_time[4:6])
    day = pd.to_datetime(df["Time"].dropna().iloc[0]).normalize() if df["Time"].notna().any() else None
    if day is None:
        return df
    end = day + pd.Timedelta(hours=hh, minutes=mm, seconds=ss)
    return df[df["Time"] <= end].copy()


def _norm_for_compare(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    if "SeqNum" in out.columns:
        out = out.sort_values("SeqNum", kind="mergesort")
    elif "Time" in out.columns:
        out = out.sort_values("Time", kind="mergesort")
    out = out.reset_index(drop=True)
    keep = [c for c in cols if c in out.columns]
    return out[keep]


def _series_equal(a: pd.Series, b: pd.Series) -> tuple[int, object, object, object]:
    if len(a) != len(b):
        n = min(len(a), len(b))
        return abs(len(a) - len(b)), None, None, None
    if pd.api.types.is_datetime64_any_dtype(a) or pd.api.types.is_datetime64_any_dtype(b):
        av = pd.to_datetime(a, errors="coerce").astype("int64").to_numpy()
        bv = pd.to_datetime(b, errors="coerce").astype("int64").to_numpy()
        bad = av != bv
        if bad.any():
            i = int(np.flatnonzero(bad)[0])
            return int(bad.sum()), a.iloc[i], b.iloc[i], int(av[i] - bv[i])
        return 0, None, None, None
    av = pd.to_numeric(a, errors="coerce")
    bv = pd.to_numeric(b, errors="coerce")
    if not av.isna().all() or not bv.isna().all():
        aa = av.to_numpy("float64")
        bb = bv.to_numpy("float64")
        bad = ~np.isclose(aa, bb, rtol=0, atol=0, equal_nan=True)
        if bad.any():
            i = int(np.flatnonzero(bad)[0])
            return int(bad.sum()), a.iloc[i], b.iloc[i], aa[i] - bb[i]
        return 0, None, None, None
    bad = a.astype(str).to_numpy() != b.astype(str).to_numpy()
    if bad.any():
        i = int(np.flatnonzero(bad)[0])
        return int(bad.sum()), a.iloc[i], b.iloc[i], None
    return 0, None, None, None


def _print_seq_stats(label: str, df: pd.DataFrame) -> pd.Index:
    if "SeqNum" not in df.columns or df.empty:
        print(f"{label}_seq_stats unavailable")
        return pd.Index([])
    seq = pd.to_numeric(df["SeqNum"], errors="coerce").dropna().astype("int64")
    dup_rows = int(seq.duplicated(keep=False).sum())
    dup_keys = int(seq[seq.duplicated(keep=False)].nunique()) if dup_rows else 0
    print(
        f"{label}_seq_stats rows={len(seq)} distinct={seq.nunique()} "
        f"dup_rows={dup_rows} dup_keys={dup_keys} min={seq.min()} max={seq.max()}"
    )
    if dup_rows:
        sample = seq[seq.duplicated(keep=False)].head(10).tolist()
        print(f"{label}_dup_seq_sample={sample}")
        if "Time" in df.columns:
            dup_mask = seq.duplicated(keep=False)
            dup_times = pd.to_datetime(df.loc[seq.index[dup_mask], "Time"], errors="coerce")
            if dup_times.notna().any():
                print(f"{label}_dup_time_range={dup_times.min()}..{dup_times.max()}")
    return pd.Index(seq.unique())


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
    compare_cols = {
        "deal": ["Code", "Time", "UpdateTime", "SaleOrderID", "BuyOrderID", "Side", "Price", "Volume", "SeqNum"],
        "order": ["Code", "Time", "UpdateTime", "OrderID", "Side", "Price", "Volume", "OrderType", "SeqNum"],
        "tick": ["Code", "Time", "UpdateTime", "CurrentPrice", "TotalVolume", "PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice", "AskPrice1", "AskVolume1", "BidPrice1", "BidVolume1", "SeqNum"],
    }

    any_bad = False
    for code in args.codes:
        for kind in args.kinds:
            path = files.get((code, kind_id[kind]))
            print(f"\n=== {code} {kind} ===")
            if not path:
                print("SHM_MISSING")
                any_bad = True
                continue
            shm = _cut(_build_shm_df(path, kind, args.date, code), args.end_time)
            hist = _cut(api.load_stock_data(code, args.date, kind), args.end_time)
            cols = compare_cols[kind]
            s = _norm_for_compare(shm, cols)
            h = _norm_for_compare(hist, cols)
            print(f"rows shm={len(s)} hist={len(h)} cols={list(s.columns)}")
            if len(s) and len(h):
                print("shm_time", s["Time"].min(), s["Time"].max())
                print("hist_time", h["Time"].min(), h["Time"].max())
                if "SeqNum" in s.columns:
                    print("shm_seq", s["SeqNum"].min(), s["SeqNum"].max(), "hist_seq", h["SeqNum"].min(), h["SeqNum"].max())
            s_seq = _print_seq_stats("shm", s)
            h_seq = _print_seq_stats("hist", h)
            if len(s_seq) or len(h_seq):
                only_shm = s_seq.difference(h_seq)
                only_hist = h_seq.difference(s_seq)
                print(f"seq_only_shm={len(only_shm)} sample={only_shm[:10].tolist()}")
                print(f"seq_only_hist={len(only_hist)} sample={only_hist[:10].tolist()}")
            if len(s) != len(h):
                any_bad = True
                print("ROW_DIFF", len(s), len(h))
            for col in [c for c in cols if c in s.columns and c in h.columns]:
                bad, first_shm, first_hist, delta = _series_equal(s[col], h[col])
                if bad:
                    any_bad = True
                    print(f"COL_DIFF {col}: bad={bad} first_shm={first_shm!r} first_hist={first_hist!r} delta={delta!r}")
            print("RESULT", "DIFF" if any_bad else "OK_SO_FAR")
    return 1 if any_bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
