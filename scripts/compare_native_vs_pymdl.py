#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare native C++ collector output vs existing pymdl pipeline output.

Supports two reference data formats:
  --pymdl-dir   directory of native SHM files (same format)
  --parquet-dir directory of parquet files: {date}_tick.parquet, {date}_order.parquet, etc.

Usage:
    # Stats only (row counts, per-column min/max/NaN):
    python scripts/compare_native_vs_pymdl.py --shm-dir /data/quant/shm --date 20260605

    # Diff against pymdl SHM files:
    python scripts/compare_native_vs_pymdl.py --shm-dir /data/quant/shm \
        --pymdl-dir /data/quant/shm_pymdl --date 20260605 --diff

    # Diff against parquet reference:
    python scripts/compare_native_vs_pymdl.py --shm-dir /data/quant/shm \
        --parquet-dir /data/oss_data/2026/202606/20260605 --date 20260605 --diff
"""

import argparse
import os
import sys
from typing import Optional

import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_platform.core.constants import TICK_COLUMNS, ORDER_COLUMNS, DEAL_COLUMNS
from quant_platform.data.native_shm_reader import (
    NativeShmReader, scan_shm_dir, get_codes,
    KIND_TICK, KIND_ORDER, KIND_DEAL, COLS_BY_KIND,
)

COLUMNS_BY_KIND = {
    KIND_TICK: TICK_COLUMNS,
    KIND_ORDER: ORDER_COLUMNS,
    KIND_DEAL: DEAL_COLUMNS,
}

PARQUET_FILE_BY_KIND = {
    KIND_TICK: "tick",
    KIND_ORDER: "order",
    KIND_DEAL: "deal",
}

# SeqNum column index in the *buffer* columns (skip TradingDay, Code)
SEQNUM_COL_INDEX = {
    KIND_TICK: 78,   # tick::SeqNum = 78
    KIND_ORDER: 8,   # order::SeqNum = 8
    KIND_DEAL: 9,    # deal::SeqNum = 9
}


def _stats_for_array(arr: np.ndarray, columns: list, label: str) -> None:
    """Print per-column stats for a numpy array."""
    buf_cols = columns[2:]  # skip TradingDay, Code
    if arr.shape[0] == 0:
        print(f"  {label}: empty")
        return
    print(f"  {label}: {arr.shape[0]} rows x {arr.shape[1]} cols")
    for i, col in enumerate(buf_cols):
        if i >= arr.shape[1]:
            break
        col_data = arr[:, i]
        nan_count = np.isnan(col_data).sum()
        finite = col_data[~np.isnan(col_data)]
        if len(finite) > 0:
            print(f"    {col:20s}  min={finite.min():12.4f}  max={finite.max():12.4f}  "
                  f"mean={finite.mean():12.4f}  NaN={nan_count}")
        else:
            print(f"    {col:20s}  ALL NaN ({nan_count})")


def _load_parquet_ref(parquet_dir: str, date_str: str, kind: int) -> Optional[pd.DataFrame]:
    """Load parquet reference file for a given kind."""
    kind_name = PARQUET_FILE_BY_KIND.get(kind)
    if not kind_name:
        return None
    path = os.path.join(parquet_dir, f"{date_str}_{kind_name}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        print(f"    loaded parquet reference: {path} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"    ERROR loading parquet {path}: {e}")
        return None


def _align_and_diff_by_seqnum(native_data: np.ndarray, ref_df: pd.DataFrame,
                              columns: list, kind: int, code: str,
                              tolerance: float) -> list:
    """Align native and reference data by SeqNum, compare matching rows.

    Returns list of (col_name, max_diff, mean_diff) for mismatched columns.
    """
    buf_cols = columns[2:]
    seq_idx = SEQNUM_COL_INDEX.get(kind)
    if seq_idx is None or seq_idx >= native_data.shape[1]:
        return [("SeqNum", float('nan'), float('nan'))]

    # Build SeqNum → row_index mapping for native
    native_seq = native_data[:, seq_idx]
    # Filter out NaN SeqNums
    valid_mask = ~np.isnan(native_seq)
    if valid_mask.sum() == 0:
        return []

    native_seq_int = native_seq[valid_mask].astype(np.int64)
    native_rows = native_data[valid_mask]
    native_by_seq = dict(zip(native_seq_int, native_rows))

    # Get SeqNum column from reference DataFrame
    if 'SeqNum' not in ref_df.columns:
        # Fall back to row-by-row comparison if no SeqNum
        return _simple_diff(native_data, ref_df, columns, tolerance)

    ref_seq_mask = ref_df['SeqNum'].notna()
    ref_work = ref_df.loc[ref_seq_mask].copy()
    ref_work['SeqNum'] = ref_work['SeqNum'].astype(np.int64)
    ref_work = ref_work.drop_duplicates(subset='SeqNum', keep='last').set_index('SeqNum')

    # Compare matching SeqNums
    common_seqs = set(native_by_seq.keys()) & set(ref_work.index.values)
    if not common_seqs:
        print(f"    {code}: 0 common SeqNums (native={len(native_by_seq)}, ref={len(ref_by_seq)})")
        return []

    col_diffs = []
    for i, col_name in enumerate(buf_cols):
        if i >= native_data.shape[1]:
            break
        if col_name in ('TradingDay', 'Code', 'Time', 'UpdateTime', 'SeqNum'):
            continue  # skip non-comparable columns

        diffs = []
        for seq in common_seqs:
            native_val = native_by_seq[seq][i]
            if col_name in ref_df.columns:
                ref_val = ref_work.at[seq, col_name]
                if pd.isna(ref_val) or np.isnan(native_val):
                    continue
                diff = abs(native_val - float(ref_val))
                diffs.append(diff)

        if diffs:
            max_d = max(diffs)
            mean_d = sum(diffs) / len(diffs)
            if max_d > tolerance:
                col_diffs.append((col_name, max_d, mean_d))

    return col_diffs


def _simple_diff(native_data: np.ndarray, ref_df: pd.DataFrame,
                 columns: list, tolerance: float) -> list:
    """Simple row-by-row diff when SeqNum alignment is not possible."""
    buf_cols = columns[2:]
    n_native = native_data.shape[0]
    n_ref = len(ref_df)
    n_compare = min(n_native, n_ref)

    col_diffs = []
    for i, col_name in enumerate(buf_cols):
        if i >= native_data.shape[1]:
            break
        if col_name not in ref_df.columns:
            continue
        native_col = native_data[:n_compare, i]
        ref_col = ref_df[col_name].values[:n_compare].astype(np.float64)

        both_valid = ~(np.isnan(native_col) | np.isnan(ref_col))
        if both_valid.sum() == 0:
            continue

        diff = np.abs(native_col[both_valid] - ref_col[both_valid])
        max_d = diff.max()
        if max_d > tolerance:
            col_diffs.append((col_name, max_d, diff.mean()))

    return col_diffs


def compare_native(shm_dir: str, trading_day: str, tolerance: float = 1e-6,
                   pymdl_dir: str = "", parquet_dir: str = "",
                   do_diff: bool = False) -> None:
    """Scan native SHM files, report statistics, optionally diff vs reference."""
    files = scan_shm_dir(shm_dir)
    if not files:
        print(f"No native mmap files found in {shm_dir}")
        return

    codes = sorted(set(code for code, _ in files.keys()))
    print(f"Found {len(codes)} stocks, {len(files)} files")

    kind_stats = {KIND_TICK: [], KIND_ORDER: [], KIND_DEAL: []}

    for (code, kind), path in sorted(files.items()):
        try:
            reader = NativeShmReader(path)
            n_rows = reader.row_count
            if n_rows > 0:
                data = reader.read_rows()
                kind_stats[kind].append({
                    'code': code,
                    'rows': n_rows,
                    'path': path,
                    'data': data,
                })
            reader.close()
        except Exception as e:
            print(f"  ERROR reading {path}: {e}")

    for kind, kind_name in [(KIND_TICK, "tick"), (KIND_ORDER, "order"), (KIND_DEAL, "deal")]:
        entries = kind_stats[kind]
        if entries:
            total_rows = sum(e['rows'] for e in entries)
            print(f"\n{'='*60}")
            print(f"{kind_name}: {len(entries)} stocks, {total_rows} total rows")
            print(f"{'='*60}")

            # Show top 10 by row count with per-column stats
            entries.sort(key=lambda x: x['rows'], reverse=True)
            columns = COLUMNS_BY_KIND[kind]
            for e in entries[:5]:
                print(f"\n  {e['code']} ({e['rows']} rows):")
                _stats_for_array(e['data'], columns, e['code'])

            if do_diff:
                if pymdl_dir:
                    _diff_against_shm(entries, kind, kind_name, pymdl_dir, columns, tolerance)
                elif parquet_dir:
                    _diff_against_parquet(entries, kind, kind_name, parquet_dir,
                                         trading_day, columns, tolerance)


def _diff_against_shm(native_entries: list, kind: int, kind_name: str,
                      pymdl_dir: str, columns: list, tolerance: float) -> None:
    """Compare native mmap data against pymdl SHM files field by field."""
    pymdl_files = scan_shm_dir(pymdl_dir)
    buf_cols = columns[2:]

    print(f"\n  --- Field diff vs pymdl SHM ({kind_name}) ---")
    matched = 0
    mismatched = 0

    for entry in native_entries:
        code = entry['code']
        pymdl_key = (code, kind)
        if pymdl_key not in pymdl_files:
            continue

        try:
            pymdl_reader = NativeShmReader(pymdl_files[pymdl_key])
            if pymdl_reader.row_count == 0:
                pymdl_reader.close()
                continue
            pymdl_data = pymdl_reader.read_rows()
            pymdl_reader.close()
        except Exception as e:
            print(f"    {code}: ERROR reading pymdl: {e}")
            continue

        native_data = entry['data']
        n_native = native_data.shape[0]
        n_pymdl = pymdl_data.shape[0]

        if n_native != n_pymdl:
            print(f"    {code}: row count mismatch native={n_native} pymdl={n_pymdl}")

        col_diffs, compared_rows = _diff_arrays_by_seqnum(
            native_data, pymdl_data, kind, buf_cols, tolerance)

        if col_diffs:
            mismatched += 1
            print(f"    {code}: {len(col_diffs)} columns differ (compared {compared_rows} SeqNums):")
            for col_name, max_d, mean_d in col_diffs[:10]:
                print(f"      {col_name:20s}  max_diff={max_d:.6f}  mean_diff={mean_d:.6f}")
        else:
            matched += 1

    print(f"\n  Summary: {matched} matched, {mismatched} mismatched, "
          f"{len(native_entries) - matched - mismatched} no pymdl reference")


def _diff_against_parquet(native_entries: list, kind: int, kind_name: str,
                          parquet_dir: str, trading_day: str,
                          columns: list, tolerance: float) -> None:
    """Compare native mmap data against parquet reference."""
    ref_df = _load_parquet_ref(parquet_dir, trading_day, kind)
    if ref_df is None:
        print(f"\n  No parquet reference for {kind_name}")
        return

    print(f"\n  --- Field diff vs parquet ({kind_name}, ref={len(ref_df)} rows) ---")
    matched = 0
    mismatched = 0

    for entry in native_entries:
        code = entry['code']
        native_data = entry['data']

        # Filter reference by code
        # In parquet, Code is SECURITY_ID (int) or string — try both
        code_df = ref_df[ref_df['Code'] == code] if 'Code' in ref_df.columns else pd.DataFrame()
        if code_df.empty:
            # Try code without suffix (e.g., "600000" instead of "600000.XSHG")
            code_short = code.split('.')[0]
            code_df = ref_df[ref_df['Code'] == code_short]
        if code_df.empty:
            # Try as int
            try:
                code_int = int(code.split('.')[0])
                code_df = ref_df[ref_df['Code'] == code_int]
            except (ValueError, TypeError):
                pass

        if code_df.empty:
            continue

        col_diffs = _align_and_diff_by_seqnum(
            native_data, code_df, columns, kind, code, tolerance)

        if col_diffs:
            mismatched += 1
            print(f"    {code}: {len(col_diffs)} columns differ:")
            for col_name, max_d, mean_d in col_diffs[:10]:
                print(f"      {col_name:20s}  max_diff={max_d:.6f}  mean_diff={mean_d:.6f}")
        else:
            matched += 1

    print(f"\n  Summary: {matched} matched, {mismatched} mismatched, "
          f"{len(native_entries) - matched - mismatched} no parquet reference")


def _diff_arrays(a: np.ndarray, b: np.ndarray, buf_cols: list,
                 tolerance: float) -> list:
    """Compare two numpy arrays column by column."""
    col_diffs = []
    for i in range(min(a.shape[1], b.shape[1])):
        if i >= len(buf_cols):
            break
        col_name = buf_cols[i]
        a_col = a[:, i]
        b_col = b[:, i]

        both_valid = ~(np.isnan(a_col) | np.isnan(b_col))
        if both_valid.sum() == 0:
            continue

        diff = np.abs(a_col[both_valid] - b_col[both_valid])
        max_d = diff.max()
        if max_d > tolerance:
            col_diffs.append((col_name, max_d, diff.mean()))

    return col_diffs


def _diff_arrays_by_seqnum(a: np.ndarray, b: np.ndarray, kind: int,
                           buf_cols: list, tolerance: float) -> tuple[list, int]:
    """Compare two native buffers after aligning rows by SeqNum."""
    seq_idx = SEQNUM_COL_INDEX.get(kind)
    if seq_idx is None or seq_idx >= a.shape[1] or seq_idx >= b.shape[1]:
        n_compare = min(a.shape[0], b.shape[0])
        return _diff_arrays(a[:n_compare], b[:n_compare], buf_cols, tolerance), n_compare

    def build_map(arr: np.ndarray) -> dict[int, np.ndarray]:
        seq = arr[:, seq_idx]
        valid = ~np.isnan(seq)
        seq_int = seq[valid].astype(np.int64)
        rows = arr[valid]
        return dict(zip(seq_int, rows))

    a_by_seq = build_map(a)
    b_by_seq = build_map(b)
    common = sorted(set(a_by_seq.keys()) & set(b_by_seq.keys()))
    if not common:
        return [], 0

    a_aligned = np.vstack([a_by_seq[s] for s in common])
    b_aligned = np.vstack([b_by_seq[s] for s in common])
    return _diff_arrays(a_aligned, b_aligned, buf_cols, tolerance), len(common)


def main():
    parser = argparse.ArgumentParser(description="Compare native vs pymdl output")
    parser.add_argument("--shm-dir", default="/data/quant/shm",
                        help="Native SHM directory")
    parser.add_argument("--pymdl-dir", default="",
                        help="Pymdl SHM directory (same native format)")
    parser.add_argument("--parquet-dir", default="",
                        help="Parquet reference directory (e.g. OSS data)")
    parser.add_argument("--date", required=True, help="Trading day YYYYMMDD")
    parser.add_argument("--tolerance", type=float, default=1e-6,
                        help="Float comparison tolerance")
    parser.add_argument("--diff", action="store_true",
                        help="Enable field-by-field diff against reference")
    args = parser.parse_args()

    compare_native(args.shm_dir, args.date, args.tolerance,
                   args.pymdl_dir, args.parquet_dir, args.diff)


if __name__ == "__main__":
    main()
