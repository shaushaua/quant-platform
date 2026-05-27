# -*- coding: utf-8 -*-
"""
Arrow ArrayBuilder-backed columnar buffer for zero-dict accumulation.

Replaces the queue + List[dict] -> frame_arrow pipeline.
Values go directly from callback into Arrow builders (native memory),
eliminating Python dict creation in the hot path.
"""

import logging
import threading
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

# Detect TimestampBuilder availability (not available in all pyarrow versions)
_USE_TIMESTAMP_BUILDER = True
try:
    _tb = pa.TimestampBuilder(pa.timestamp("ns"))
    del _tb
except Exception:
    _USE_TIMESTAMP_BUILDER = False


def _make_builder(arrow_type: pa.DataType):
    """Create an Arrow ArrayBuilder for the given type."""
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return pa.StringBuilder()
    if pa.types.is_float64(arrow_type):
        return pa.Float64Builder()
    if pa.types.is_int64(arrow_type):
        return pa.Int64Builder()
    if pa.types.is_int32(arrow_type):
        return pa.Int32Builder()
    if pa.types.is_int16(arrow_type):
        return pa.Int16Builder()
    if pa.types.is_timestamp(arrow_type):
        if _USE_TIMESTAMP_BUILDER:
            return pa.TimestampBuilder(arrow_type)
        # Fallback: store as int64 nanoseconds, cast on flush
        return pa.Int64Builder()
    raise ValueError(f"Unsupported Arrow type: {arrow_type}")


def _append_val(builder, value, field_type: pa.DataType) -> None:
    """Append a single value to an Arrow builder with type coercion."""
    if value is None:
        builder.append_null()
        return
    try:
        builder.append(value)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
        # Coerce value to match builder type
        if pa.types.is_timestamp(field_type) and isinstance(builder, pa.Int64Builder):
            if isinstance(value, pd.Timestamp):
                builder.append(value.value)
            elif hasattr(value, "value"):
                builder.append(value.value)
            else:
                builder.append(int(pd.Timestamp(value).value))
        elif pa.types.is_float64(field_type):
            builder.append(float(value))
        elif pa.types.is_int64(field_type):
            builder.append(int(value))
        elif pa.types.is_int32(field_type):
            builder.append(int(value))
        elif pa.types.is_int16(field_type):
            builder.append(int(value))
        elif pa.types.is_string(field_type):
            builder.append(str(value))
        else:
            builder.append_null()


class ArrowBuffer:
    """
    Columnar buffer backed by Arrow ArrayBuilders.

    In the callback, call append_row(dict) to feed values directly into
    Arrow builders (native memory). On flush(), produce a RecordBatch
    and reset the builders.

    Thread-safe via internal lock. One buffer per data type (tick/order/deal).
    """

    HIGH_WATERMARK = 8192  # Auto-flush threshold

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.lock = threading.Lock()
        self._builders: Dict[str, object] = {}
        self._n = 0
        self._init_builders()

    def _init_builders(self) -> None:
        for field in self.schema:
            self._builders[field.name] = _make_builder(field.type)

    def append_row(self, row: dict) -> bool:
        """
        Append one row to the buffer. Values go directly to Arrow builders.
        The dict is consumed immediately and not stored.

        Returns True if buffer exceeds high watermark (caller should signal flush).
        """
        with self.lock:
            for field in self.schema:
                _append_val(
                    self._builders[field.name],
                    row.get(field.name),
                    field.type,
                )
            self._n += 1
            return self._n >= self.HIGH_WATERMARK

    @property
    def row_count(self) -> int:
        return self._n

    def flush(self) -> Optional[pa.RecordBatch]:
        """
        Finish all builders into a RecordBatch and reset.
        Returns None if buffer is empty.
        """
        with self.lock:
            if self._n == 0:
                return None
            arrays = []
            for field in self.schema:
                arr = self._builders[field.name].finish()
                # Cast int64 -> timestamp if using Int64Builder fallback
                if pa.types.is_timestamp(field.type) and pa.types.is_int64(arr.type):
                    arr = arr.cast(field.type)
                arrays.append(arr)
            batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
            # Recreate builders for next batch
            self._init_builders()
            self._n = 0
            return batch
