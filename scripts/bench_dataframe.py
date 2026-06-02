#!/usr/bin/env python3
"""Benchmark: old (insert+reorder) vs new (single-pass dict) DataFrame construction.

Simulates the _build_df_from_path hot path with realistic data sizes.
No OSS credentials or live data needed — runs on any machine.

Usage:
    python scripts/bench_dataframe.py
    python scripts/bench_dataframe.py --rows 5000 --stocks 100
"""

import argparse
import time

import numpy as np
import pandas as pd


def _time_str_to_seconds(s):
    """Convert "09:30:00.500" → 34200.5"""
    parts = s.split(':')
    h = float(parts[0])
    m = float(parts[1])
    sec_parts = parts[2].split('.')
    sec = float(sec_parts[0])
    ms = float(sec_parts[1]) if len(sec_parts) > 1 else 0.0
    return h * 3600.0 + m * 60.0 + sec + ms / 1000.0


def make_fake_mmap_data(n_rows, n_cols=79):
    """Generate f64 array simulating mmap-backed tick data (79 cols).

    Layout: Time(f64), UpdateTime(f64), then 77 float columns.
    Time values are seconds since midnight (e.g., 34200.5 = 09:30:00.500).
    """
    arr = np.random.rand(n_rows, n_cols)
    # Column 0 = Time (seconds since midnight, 09:30 ~ 15:00)
    arr[:, 0] = np.sort(34200 + np.random.rand(n_rows) * 21600)
    # Column 1 = UpdateTime
    arr[:, 1] = arr[:, 0] + np.random.rand(n_rows) * 0.5
    return arr


# Simulated column layout (tick has 81 cols total, 79 after dropping TradingDay/Code)
TICK_COLUMNS_FULL = ['TradingDay', 'Code', 'Time', 'UpdateTime'] + [f'Col{i}' for i in range(4, 81)]
TICK_BUF_COLS = TICK_COLUMNS_FULL[2:]  # 79 cols


def build_df_old(arr, columns, trading_day, code):
    """Old approach: DataFrame + insert + reorder."""
    buf_cols = columns[2:]
    df = pd.DataFrame(arr, columns=buf_cols, copy=False)

    base_ns = pd.Timestamp(trading_day).value
    for col in ('Time', 'UpdateTime'):
        if col in buf_cols:
            idx = buf_cols.index(col)
            df[col] = (base_ns + (arr[:, idx] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')

    df.insert(0, 'TradingDay', trading_day)
    df.insert(1, 'Code', code)
    return df[columns]


def build_df_new(arr, columns, trading_day, code):
    """New approach: single-pass dict construction."""
    buf_cols = columns[2:]
    base_ns = pd.Timestamp(trading_day).value
    data = {'TradingDay': trading_day, 'Code': code}
    for i, col in enumerate(buf_cols):
        if col in ('Time', 'UpdateTime'):
            data[col] = (base_ns + (arr[:, i] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')
        else:
            data[col] = arr[:, i]
    return pd.DataFrame(data, columns=columns)


def bench_one(n_rows, rounds=50):
    arr = make_fake_mmap_data(n_rows)
    trading_day = '20250106'
    code = '000001.SZ'

    # Warmup
    for _ in range(5):
        build_df_old(arr, TICK_COLUMNS_FULL, trading_day, code)
        build_df_new(arr, TICK_COLUMNS_FULL, trading_day, code)

    # Benchmark old
    times_old = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        df_old = build_df_old(arr, TICK_COLUMNS_FULL, trading_day, code)
        times_old.append(time.perf_counter() - t0)

    # Benchmark new
    times_new = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        df_new = build_df_new(arr, TICK_COLUMNS_FULL, trading_day, code)
        times_new.append(time.perf_counter() - t0)

    # Verify correctness
    assert df_old.shape == df_new.shape, f"Shape mismatch: {df_old.shape} vs {df_new.shape}"
    assert list(df_old.columns) == list(df_new.columns), "Column order mismatch"
    assert df_old['Code'].iloc[0] == df_new['Code'].iloc[0]
    assert df_old['TradingDay'].iloc[0] == df_new['TradingDay'].iloc[0]

    return np.array(times_old), np.array(times_new)


def main():
    parser = argparse.ArgumentParser(description="DataFrame construction benchmark")
    parser.add_argument('--rows', type=int, nargs='+', default=[1000, 3000, 5000, 10000],
                        help='Row counts to benchmark')
    parser.add_argument('--stocks', type=int, default=5000,
                        help='Number of stocks to extrapolate')
    parser.add_argument('--workers', type=int, default=40,
                        help='Worker count for extrapolation')
    parser.add_argument('--rounds', type=int, default=50,
                        help='Benchmark rounds per test')
    args = parser.parse_args()

    print("=" * 72)
    print("DataFrame Construction Benchmark: old (insert+reorder) vs new (dict)")
    print("=" * 72)
    print(f"Stocks: {args.stocks}  Workers: {args.workers}  Rounds: {args.rounds}")
    print()

    print(f"{'Rows':>6} | {'Old (ms)':>10} {'New (ms)':>10} {'Speedup':>8} | "
          f"{'Full market old':>16} {'Full market new':>16}")
    print("-" * 72)

    for n_rows in args.rows:
        times_old, times_new = bench_one(n_rows, args.rounds)

        old_ms = np.median(times_old) * 1000
        new_ms = np.median(times_new) * 1000
        speedup = old_ms / new_ms if new_ms > 0 else float('inf')

        # Extrapolate: per_stock_ms * stocks / workers
        full_old = old_ms * args.stocks / args.workers / 1000  # seconds
        full_new = new_ms * args.stocks / args.workers / 1000

        print(f"{n_rows:>6} | {old_ms:>9.3f} {new_ms:>9.3f} {speedup:>7.2f}x | "
              f"{full_old:>14.1f}s {full_new:>14.1f}s")

    print()
    print("Note: 'Full market' = per-stock time × 5000 stocks / 40 workers")
    print("      This is DataFrame construction time only (excludes factor compute)")


if __name__ == '__main__':
    main()
