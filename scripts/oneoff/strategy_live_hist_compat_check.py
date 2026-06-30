#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Smoke test protected daily strategy with historical vs live-compatible inputs.

Run inside the Linux Python 3.11 image, because protected strategy bundles ship
Linux x86_64 extension modules.
"""

import math
import os
from typing import Any

import numpy as np
import pandas as pd

from quant_platform.core.constants import DEAL_COLUMNS
from quant_platform.factor.engine import _normalize_multi_code_batch_results, _restore_oss_precision
from quant_platform.factor.stock_data_builder import build_stock_data
from quant_platform.live_engine import native_engine as ne


class _FakeReader:
    def __init__(self, arr: np.ndarray):
        self._arr = arr

    def view_rows(self) -> np.ndarray:
        return self._arr


def _make_raw_deal_rows() -> np.ndarray:
    # DEAL_COLUMNS[2:]:
    # Time, UpdateTime, SaleOrderID, BuyOrderID, Side, Price, Volume, Money,
    # Channel, SeqNum. Intentionally unsorted by SeqNum to test compatibility.
    return np.array(
        [
            [9 * 3600 + 31 * 60 + 2.123456, 9 * 3600 + 31 * 60 + 2.223456, 1002, 2002, 0, 10.23, 300, 3069, 1, 102],
            [9 * 3600 + 30 * 60 + 1.123456, 9 * 3600 + 30 * 60 + 1.223456, 1001, 2001, 1, 10.21, 100, 1021, 1, 101],
            [14 * 3600 + 59 * 60 + 59.123456, 14 * 3600 + 59 * 60 + 59.323456, 1003, 2003, 0, 10.25, 200, 2050, 1, 103],
        ],
        dtype=np.float64,
    )


def _live_df(arr: np.ndarray, date: str, code: str, historical_compat: bool) -> pd.DataFrame:
    ne._base_ns = pd.Timestamp(date).value
    return ne._build_df_from_native(
        _FakeReader(arr),
        DEAL_COLUMNS,
        DEAL_COLUMNS[2:],
        DEAL_COLUMNS[2:].index("Time"),
        DEAL_COLUMNS[2:].index("UpdateTime"),
        date,
        code,
        [DEAL_COLUMNS[2:].index("Volume")],
        historical_compat,
    )


def _historical_df(arr: np.ndarray, date: str, code: str) -> pd.DataFrame:
    base_ns = pd.Timestamp(date).value
    time_ns = base_ns + (arr[:, DEAL_COLUMNS[2:].index("Time")] * 1_000_000_000).astype(np.int64)
    update_ns = base_ns + (arr[:, DEAL_COLUMNS[2:].index("UpdateTime")] * 1_000_000_000).astype(np.int64)
    archived = pd.DataFrame(
        {
            "Code": [1] * len(arr),
            "Time": (time_ns // 1000).astype("int64"),
            "UpdateTime": (update_ns // 1000).astype("int64"),
            "SaleOrderID": arr[:, DEAL_COLUMNS[2:].index("SaleOrderID")].astype("int64"),
            "BuyOrderID": arr[:, DEAL_COLUMNS[2:].index("BuyOrderID")].astype("int64"),
            "Side": arr[:, DEAL_COLUMNS[2:].index("Side")].astype("int8"),
            "Price": np.round(arr[:, DEAL_COLUMNS[2:].index("Price")] * 100).astype("int32"),
            "Volume": arr[:, DEAL_COLUMNS[2:].index("Volume")].astype("int64"),
            "SeqNum": arr[:, DEAL_COLUMNS[2:].index("SeqNum")].astype("int32"),
        }
    ).sort_values("SeqNum", kind="mergesort").reset_index(drop=True)
    return _restore_oss_precision(archived, code)


def _same_value(a: Any, b: Any) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        if pd.isna(a) and pd.isna(b):
            return True
        return math.isclose(float(a), float(b), rel_tol=1e-6, abs_tol=1e-6)
    return str(a) == str(b)


def main() -> int:
    module_path = os.environ.get(
        "TEST_STRATEGY_MODULE",
        "quant_platform.strategies.protected_eillen_strategy_v2",
    )
    date = os.environ.get("TEST_DATE", "20260630")
    code = os.environ.get("TEST_CODE", "000001.XSHE")
    end_time = os.environ.get("TEST_END_TIME", "150000")

    mod = __import__(module_path, fromlist=["dummy"])
    factor_info = getattr(mod, "FACTOR_INFO", getattr(mod, "factor_info", {})) or {}
    factor_fn = getattr(mod, "factor_calculation")

    arr = _make_raw_deal_rows()
    hist_deal = _historical_df(arr, date, code)
    live_deal = _live_df(arr, date, code, historical_compat=True)

    compare_cols = ["Code", "Time", "UpdateTime", "SaleOrderID", "BuyOrderID", "Side", "Price", "Volume", "SeqNum"]
    print("input_compare")
    for col in compare_cols:
        hv = hist_deal[col].astype(str).tolist()
        lv = live_deal[col].astype(str).tolist()
        print(col, hv == lv, "hist_dtype=", hist_deal[col].dtype, "live_dtype=", live_deal[col].dtype)
        if hv != lv:
            print("  hist", hv)
            print("  live", lv)

    hist_data = build_stock_data(
        code=code,
        date=date,
        end_time=end_time,
        deal_df=hist_deal,
        factor_info=factor_info,
        validate=True,
    )
    live_data = build_stock_data(
        code=code,
        date=date,
        end_time=end_time,
        deal_df=live_deal,
        factor_info=factor_info,
        validate=True,
    )

    hist_raw = factor_fn({code: hist_data}, [code], date, [end_time])
    live_raw = factor_fn({code: live_data}, [code], date, [end_time])
    hist_norm = _normalize_multi_code_batch_results([code], [end_time], hist_raw)
    live_norm = _normalize_multi_code_batch_results([code], [end_time], live_raw)
    print("hist_raw_type", type(hist_raw).__name__)
    print("live_raw_type", type(live_raw).__name__)
    print("hist_norm", hist_norm)
    print("live_norm", live_norm)

    h = (hist_norm or {}).get(code, {}).get(end_time)
    l = (live_norm or {}).get(code, {}).get(end_time)
    if isinstance(h, dict) and isinstance(l, dict):
        keys = sorted(set(h) | set(l))
        diffs = [k for k in keys if not _same_value(h.get(k), l.get(k))]
        print("result_keys", len(keys), "diffs", len(diffs))
        for k in diffs[:20]:
            print("diff", k, h.get(k), l.get(k))
        return 1 if diffs else 0

    same = str(h) == str(l)
    print("result_same", same)
    return 0 if same else 1


if __name__ == "__main__":
    raise SystemExit(main())
