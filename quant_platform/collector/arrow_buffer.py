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

    def append_row(self, row: dict) -> bool:
        """
        Append one row. Numeric values go directly to numpy arrays (no Python
        float/int objects created for storage). Returns True if high watermark hit.
        """
        with self.lock:
            idx = self._n

            # Ensure numpy capacity
            for name in self._np:
                self._ensure_cap(name, idx + 1)

            # Write numeric columns (direct to native memory)
            for name, arr in self._np.items():
                val = row.get(name)
                if val is None:
                    arr[idx] = 0
                elif self._np_is_ts.get(name):
                    # pd.Timestamp → int64 nanoseconds
                    if isinstance(val, pd.Timestamp):
                        arr[idx] = val.value
                    elif hasattr(val, "value"):
                        arr[idx] = val.value
                    else:
                        arr[idx] = int(pd.Timestamp(val).value)
                else:
                    arr[idx] = val

            # Write string columns (Python lists)
            for name, lst in self._ls.items():
                lst.append(row.get(name))

            self._n += 1
            return self._n >= self.HIGH_WATERMARK

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

            batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)

            # Reset: keep numpy arrays (just reset index), recreate lists
            self._ls = {name: [] for name in self._ls}
            self._n = 0
            return batch
