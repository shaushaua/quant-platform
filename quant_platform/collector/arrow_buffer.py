# -*- coding: utf-8 -*-
"""
Columnar buffer with numpy-backed numeric columns.

Numeric columns (float64, int64, int32, int16, timestamp) are stored in
pre-allocated numpy arrays — values go directly to native C memory,
bypassing Python's object allocator entirely.  Only string columns use
Python lists.

This eliminates ~95% of Python object creation in the hot path,
preventing glibc malloc fragmentation that causes RSS growth.
"""

import logging
import threading
from typing import Dict, Optional

import numpy as np
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)


def _is_numpy_type(arrow_type: pa.DataType) -> bool:
    """Check if this Arrow type maps to a numpy dtype."""
    return (
        pa.types.is_float64(arrow_type)
        or pa.types.is_int64(arrow_type)
        or pa.types.is_int32(arrow_type)
        or pa.types.is_int16(arrow_type)
        or pa.types.is_timestamp(arrow_type)
    )


def _arrow_to_numpy_dtype(arrow_type: pa.DataType) -> np.dtype:
    if pa.types.is_float64(arrow_type):
        return np.dtype(np.float64)
    if pa.types.is_int64(arrow_type):
        return np.dtype(np.int64)
    if pa.types.is_int32(arrow_type):
        return np.dtype(np.int32)
    if pa.types.is_int16(arrow_type):
        return np.dtype(np.int16)
    if pa.types.is_timestamp(arrow_type):
        return np.dtype(np.int64)  # Store nanoseconds as int64
    raise ValueError(f"Cannot map Arrow type to numpy: {arrow_type}")


class ArrowBuffer:
    """
    Columnar buffer with numpy arrays for numeric columns.

    - Numeric columns: pre-allocated numpy arrays, zero Python object overhead
    - String columns: Python lists (unavoidable for variable-length strings)
    - Timestamp columns: stored as int64 nanoseconds in numpy, cast on flush

    Thread-safe via internal lock. One buffer per data type (tick/order/deal).
    """

    HIGH_WATERMARK = 8192
    INITIAL_CAP = 4096
    SHRINK_THRESHOLD = HIGH_WATERMARK * 4

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.lock = threading.Lock()
        self._n = 0

        # Separate storage for numpy vs list columns
        self._np: Dict[str, np.ndarray] = {}   # numpy-backed columns
        self._ls: Dict[str, list] = {}           # list-backed columns (strings)
        self._np_is_ts: Dict[str, bool] = {}     # track which numpy cols are timestamps

        for field in schema:
            if _is_numpy_type(field.type):
                self._np[field.name] = np.empty(self.INITIAL_CAP, dtype=_arrow_to_numpy_dtype(field.type))
                self._np_is_ts[field.name] = pa.types.is_timestamp(field.type)
            else:
                self._ls[field.name] = []

    def _ensure_cap(self, name: str, needed: int) -> None:
        arr = self._np[name]
        if needed < len(arr):
            return
        new_cap = max(len(arr) * 2, needed * 2)
        new_arr = np.empty(new_cap, dtype=arr.dtype)
        new_arr[:self._n] = arr[:self._n]
        self._np[name] = new_arr

    # ------------------------------------------------------------------ #
    # Direct-write API: begin_row / set / commit_row / cancel_row         #
    # Eliminates dict creation entirely in the hot path.                  #
    # ------------------------------------------------------------------ #

    def begin_row(self) -> None:
        """Start a new row. Acquires lock; MUST call commit_row() or cancel_row()."""
        self.lock.acquire()
        idx = self._n
        for name in self._np:
            self._ensure_cap(name, idx + 1)
            self._np[name][idx] = 0  # zero-fill defaults

    def set(self, name: str, value) -> None:
        """Set column value for current row. Must be between begin_row and commit/cancel."""
        if name in self._np:
            if value is None:
                return  # already zeroed
            arr = self._np[name]
            idx = self._n
            if self._np_is_ts.get(name):
                if isinstance(value, pd.Timestamp):
                    arr[idx] = value.value
                elif hasattr(value, "value"):
                    arr[idx] = value.value
                else:
                    arr[idx] = int(pd.Timestamp(value).value)
            else:
                arr[idx] = value
        elif name in self._ls:
            self._ls[name].append(value)

    def commit_row(self) -> bool:
        """Finish row, release lock. Returns True if high watermark hit."""
        try:
            for name in self._ls:
                if len(self._ls[name]) <= self._n:
                    self._ls[name].append("")
            self._n += 1
            return self._n >= self.HIGH_WATERMARK
        finally:
            self.lock.release()

    def cancel_row(self) -> None:
        """Cancel row (undo partial string appends), release lock."""
        try:
            for name in self._ls:
                if len(self._ls[name]) > self._n:
                    self._ls[name].pop()
        finally:
            self.lock.release()

    def append_row(self, row: dict) -> bool:
        """
        Append one row via dict. Kept for backward compat; prefer begin/set/commit.
        Returns True if high watermark hit.
        """
        self.begin_row()
        for name, value in row.items():
            self.set(name, value)
        return self.commit_row()

    @property
    def row_count(self) -> int:
        return self._n

    def flush(self) -> Optional[pa.RecordBatch]:
        """Convert buffers to RecordBatch and reset."""
        with self.lock:
            if self._n == 0:
                return None

            arrays = []
            for field in self.schema:
                if field.name in self._np:
                    # Slice the used portion of the numpy array
                    np_arr = self._np[field.name][:self._n]
                    if self._np_is_ts.get(field.name):
                        # Cast int64 ns → timestamp
                        pa_arr = pa.array(np_arr, type=pa.int64()).cast(field.type)
                    else:
                        pa_arr = pa.array(np_arr, type=field.type)
                    arrays.append(pa_arr)
                else:
                    values = self._ls[field.name]
                    try:
                        pa_arr = pa.array(values, type=field.type)
                    except (pa.ArrowInvalid, pa.ArrowTypeError):
                        pa_arr = pa.array(values).cast(field.type, safe=False)
                    arrays.append(pa_arr)

            try:
                batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
            finally:
                del arrays

            # Reset: keep numpy arrays (just reset index), recreate lists
            self._ls = {name: [] for name in self._ls}
            self._n = 0
            self._shrink_if_needed()
            return batch

    def _shrink_if_needed(self) -> None:
        """Avoid permanently retaining huge numpy buffers after transient bursts."""
        for name, arr in list(self._np.items()):
            if len(arr) <= self.SHRINK_THRESHOLD:
                continue
            self._np[name] = np.empty(self.HIGH_WATERMARK, dtype=arr.dtype)
