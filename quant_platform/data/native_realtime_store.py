# -*- coding: utf-8 -*-
"""
Realtime-store adapter over native C++ collector mmap files.

This preserves the DataAPI realtime interface while the hot path writes native
SHM files.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..core.constants import (
    DEAL_COLUMNS,
    DEAL_DTYPE,
    ORDER_COLUMNS,
    ORDER_DTYPE,
    TICK_COLUMNS,
    TICK_DTYPE,
)
from .native_shm_reader import (
    KIND_DEAL,
    KIND_ORDER,
    KIND_TICK,
    NativeShmReader,
    scan_shm_dir,
)


class NativeRealtimeStore:
    """DataAPI-compatible reader for native collector mmap files."""

    def __init__(self, shm_dir: Optional[str] = None, trading_day: Optional[str] = None):
        self.shm_dir = shm_dir or os.getenv("NATIVE_SHM_DIR", "/data/quant/shm")
        self.trading_day = trading_day or datetime.now().strftime("%Y%m%d")

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        return self._read_kind(KIND_TICK, TICK_COLUMNS, code)

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        return self._read_kind(KIND_ORDER, ORDER_COLUMNS, code)

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        return self._read_kind(KIND_DEAL, DEAL_COLUMNS, code)

    def get_quote(self, code: str) -> dict:
        return {}

    def get_all_quotes(self) -> Dict[str, dict]:
        return {}

    def get_daily_basic(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_trading_day(self) -> str:
        return self.trading_day

    def get_all_codes(self, data_type: str = "tick") -> List[str]:
        kind = {"tick": KIND_TICK, "order": KIND_ORDER, "deal": KIND_DEAL}.get(data_type)
        if kind is None:
            return []
        files = scan_shm_dir(self.shm_dir)
        return sorted(code for (code, k) in files.keys() if k == kind)

    def get_kline(self, period: str) -> pd.DataFrame:
        return pd.DataFrame()

    def _read_kind(self, kind: int, columns: list, code: Optional[str]) -> pd.DataFrame:
        files = scan_shm_dir(self.shm_dir)
        if code:
            paths = [(code, files.get((code, kind)))]
        else:
            paths = [(c, path) for (c, k), path in files.items() if k == kind]

        frames = []
        for cur_code, path in paths:
            if not path:
                continue
            try:
                reader = NativeShmReader(path)
                arr = reader.read_rows()
                reader.close()
            except Exception:
                continue
            if arr.shape[0] == 0:
                continue
            frames.append(self._array_to_df(arr, columns, cur_code))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _array_to_df(self, arr: np.ndarray, columns: list, code: str) -> pd.DataFrame:
        buf_cols = columns[2:]
        df = pd.DataFrame(arr, columns=buf_cols, copy=False)
        base_ns = pd.Timestamp(self.trading_day).value
        for col in ("Time", "UpdateTime"):
            if col in df.columns:
                df[col] = (base_ns + (df[col].to_numpy() * 1_000_000_000).astype(np.int64)).astype("datetime64[ns]")
        df.insert(0, "TradingDay", self.trading_day)
        df.insert(1, "Code", code)
        df = df[columns]
        dtype_map = {
            tuple(TICK_COLUMNS): TICK_DTYPE,
            tuple(ORDER_COLUMNS): ORDER_DTYPE,
            tuple(DEAL_COLUMNS): DEAL_DTYPE,
        }.get(tuple(columns), {})
        for col, dtype in dtype_map.items():
            if col in ("TradingDay", "Code", "Time", "UpdateTime") or col not in df.columns:
                continue
            try:
                df[col] = df[col].astype(dtype, copy=False)
            except (TypeError, ValueError):
                pass
        return df
