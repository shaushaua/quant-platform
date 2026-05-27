# -*- coding: utf-8 -*-
"""
Columnar buffer for zero-dict accumulation.

Replaces the queue + List[dict] -> frame_arrow pipeline.
Values go directly into per-column Python lists, then converted to
Arrow arrays on flush via pa.array() (compatible with all pyarrow versions).
"""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow as pa

logger = logging.getLogger(__name__)


class ArrowBuffer:
    """
    Columnar buffer: append values per column, flush to RecordBatch.

    In the callback, call append_row(dict) to feed values into per-column
    lists. On flush(), produce a RecordBatch via pa.array() and reset.

    Thread-safe via internal lock. One buffer per data type (tick/order/deal).
    """

    HIGH_WATERMARK = 8192  # Auto-flush threshold

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self.lock = threading.Lock()
        self._cols: Dict[str, list] = {f.name: [] for f in schema}
        self._n = 0

    def append_row(self, row: dict) -> bool:
        """
        Append one row to the buffer. Values go into per-column lists.
        The dict is consumed immediately and not stored.

        Returns True if buffer exceeds high watermark (caller should signal flush).
        """
        with self.lock:
            for field in self.schema:
                self._cols[field.name].append(row.get(field.name))
            self._n += 1
            return self._n >= self.HIGH_WATERMARK

    @property
    def row_count(self) -> int:
        return self._n

    def flush(self) -> Optional[pa.RecordBatch]:
        """
        Convert all buffered values to a RecordBatch and reset.
        Returns None if buffer is empty.
        """
        with self.lock:
            if self._n == 0:
                return None
            arrays = []
            for field in self.schema:
                values = self._cols[field.name]
                try:
                    arr = pa.array(values, type=field.type)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    arr = pa.array(values).cast(field.type, safe=False)
                arrays.append(arr)
            batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
            # Reset: new empty lists, let old ones be GC'd
            self._cols = {f.name: [] for f in self.schema}
            self._n = 0
            return batch
