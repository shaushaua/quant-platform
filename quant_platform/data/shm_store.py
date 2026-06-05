# -*- coding: utf-8 -*-
"""
Realtime store facade.

The production realtime path is native C++ collector mmap files under
NATIVE_SHM_DIR.  This class keeps the existing DataAPI dependency on ShmStore.
"""

from __future__ import annotations

from typing import Dict, List, Optional
from pathlib import Path
import os

import pandas as pd

from .native_realtime_store import NativeRealtimeStore

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))


class ShmStore:
    """DataAPI-compatible facade over native collector mmap files."""

    def __init__(self):
        self._backend = NativeRealtimeStore()

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        return self._backend.get_tick(code)

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        return self._backend.get_order(code)

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        return self._backend.get_deal(code)

    def get_quote(self, code: str) -> dict:
        return self._backend.get_quote(code)

    def get_all_quotes(self) -> Dict[str, dict]:
        return self._backend.get_all_quotes()

    def get_daily_basic(self) -> pd.DataFrame:
        return self._backend.get_daily_basic()

    def get_trading_day(self) -> str:
        return self._backend.get_trading_day()

    def get_all_codes(self, data_type: str = "tick") -> List[str]:
        return self._backend.get_all_codes(data_type)

    def get_kline(self, period: str) -> pd.DataFrame:
        return self._backend.get_kline(period)

    def flush(self) -> None:
        pass

    def flush_dirty(self) -> None:
        pass

    def cleanup_rolling(self) -> int:
        return 0

    def clear_rolling(self) -> int:
        return 0

    def clear_day(self) -> None:
        pass
