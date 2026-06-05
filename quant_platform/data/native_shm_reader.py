# -*- coding: utf-8 -*-
"""
Native SHM Reader: reads mmap files written by C++ native-mdl-collector.

File format (64-byte header + row data):
    [0..8]   magic        uint64  0x514d444c53484d31 ("QMDLSHM1")
    [8..16]  version      uint64  2
    [16..24] kind         uint64  1=tick, 2=order, 3=deal
    [24..32] capacity     uint64
    [32..40] n_cols       uint64
    [40..48] row_count    uint64
    [48..56] trading_day  uint64  YYYYMMDD (e.g. 20260605)
    [56..64] generation   uint64  (odd=writing, even=committed)

Rows: float64[row_count][n_cols] starting at offset 64.
"""

import mmap
import os
import re
import struct
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

# Constants matching C++ shm_writer.h
SHM_MAGIC = 0x514D444C53484D31  # QMDLSHM1
SHM_VERSION = 2
SHM_HEADER_SIZE = 64

KIND_TICK = 1
KIND_ORDER = 2
KIND_DEAL = 3

KIND_NAMES = {KIND_TICK: "tick", KIND_ORDER: "order", KIND_DEAL: "deal"}
KIND_FROM_NAME = {"tick": KIND_TICK, "order": KIND_ORDER, "deal": KIND_DEAL}

# Column counts (must match C++ schema.h)
TICK_COLS = 79
ORDER_COLS = 9
DEAL_COLS = 10

COLS_BY_KIND = {KIND_TICK: TICK_COLS, KIND_ORDER: ORDER_COLS, KIND_DEAL: DEAL_COLS}


class NativeShmHeader:
    """Parsed header from a native SHM file."""

    __slots__ = ("magic", "version", "kind", "capacity", "n_cols", "row_count",
                 "trading_day", "generation")

    def __init__(self, data: bytes):
        if len(data) < SHM_HEADER_SIZE:
            raise ValueError(f"header too small: {len(data)} < {SHM_HEADER_SIZE}")
        (self.magic, self.version, self.kind, self.capacity,
         self.n_cols, self.row_count, self.trading_day,
         self.generation) = struct.unpack_from("<QQQQQQQQ", data, 0)

        if self.magic != SHM_MAGIC:
            raise ValueError(f"bad magic: 0x{self.magic:016x}")
        if self.version != SHM_VERSION:
            raise ValueError(f"unsupported version: {self.version}")

    def __repr__(self):
        return (f"NativeShmHeader(kind={KIND_NAMES.get(self.kind, '?')}, "
                f"rows={self.row_count}, cols={self.n_cols}, "
                f"trading_day={self.trading_day}, gen={self.generation})")


class NativeShmReader:
    """Read-only accessor for a single stock's mmap file."""

    def __init__(self, path: str):
        self.path = path
        self._fd = None
        self._mmap = None
        self._header: Optional[NativeShmHeader] = None
        self._open()

    def _open(self):
        self._fd = open(self.path, "rb")
        self._mmap = mmap.mmap(self._fd.fileno(), 0, access=mmap.ACCESS_READ)
        self._refresh_header()

    def _refresh_header(self):
        self._header = NativeShmHeader(self._mmap[:SHM_HEADER_SIZE])

    @property
    def kind(self) -> int:
        return self._header.kind

    @property
    def kind_name(self) -> str:
        return KIND_NAMES.get(self._header.kind, "unknown")

    @property
    def n_cols(self) -> int:
        return self._header.n_cols

    @property
    def row_count(self) -> int:
        return self._header.row_count

    def read_rows(self, max_retries: int = 10) -> np.ndarray:
        """Read all rows with generation-based consistency check.

        Returns:
            ndarray of shape (row_count, n_cols), dtype float64.
            The array is a copy (safe to use after file closes).
        """
        for attempt in range(max_retries):
            # Read generation (at offset 56)
            gen1 = struct.unpack_from("<Q", self._mmap, 56)[0]
            if gen1 % 2 == 1:
                # Writer is mid-update, retry
                time.sleep(0.000001)
                continue

            # Read header fields
            row_count = struct.unpack_from("<Q", self._mmap, 40)[0]
            n_cols = struct.unpack_from("<Q", self._mmap, 32)[0]

            if row_count == 0:
                return np.empty((0, n_cols), dtype=np.float64)

            # Read row data
            offset = SHM_HEADER_SIZE
            byte_count = row_count * n_cols * 8
            data = np.frombuffer(self._mmap, dtype=np.float64,
                                 offset=offset, count=row_count * n_cols)
            data = data.reshape(row_count, n_cols).copy()

            # Check generation hasn't changed
            gen2 = struct.unpack_from("<Q", self._mmap, 56)[0]
            if gen1 == gen2:
                return data

        raise RuntimeError(f"mmap generation conflict after {max_retries} retries: {self.path}")

    def read_rows_from(self, start_row: int, max_retries: int = 10) -> np.ndarray:
        """Read rows from start_row onwards."""
        for attempt in range(max_retries):
            gen1 = struct.unpack_from("<Q", self._mmap, 56)[0]
            if gen1 % 2 == 1:
                time.sleep(0.000001)
                continue

            row_count = struct.unpack_from("<Q", self._mmap, 40)[0]
            n_cols = struct.unpack_from("<Q", self._mmap, 32)[0]

            if start_row >= row_count:
                return np.empty((0, n_cols), dtype=np.float64)

            n_rows = row_count - start_row
            offset = SHM_HEADER_SIZE + start_row * n_cols * 8
            data = np.frombuffer(self._mmap, dtype=np.float64,
                                 offset=offset, count=n_rows * n_cols)
            data = data.reshape(n_rows, n_cols).copy()

            gen2 = struct.unpack_from("<Q", self._mmap, 56)[0]
            if gen1 == gen2:
                return data

        raise RuntimeError(f"mmap generation conflict after {max_retries} retries: {self.path}")

    def view_rows(self, max_retries: int = 10) -> np.ndarray:
        """Return a zero-copy view of all committed rows.

        Rows are append-only in the native writer.  Existing row bytes are never
        mutated after row_count is published, so factor workers can safely keep a
        read-only view over the mmap while the writer appends later rows.
        """
        for attempt in range(max_retries):
            gen1 = struct.unpack_from("<Q", self._mmap, 56)[0]
            if gen1 % 2 == 1:
                time.sleep(0.000001)
                continue

            row_count = struct.unpack_from("<Q", self._mmap, 40)[0]
            n_cols = struct.unpack_from("<Q", self._mmap, 32)[0]

            if row_count == 0:
                return np.empty((0, n_cols), dtype=np.float64)

            data = np.frombuffer(
                self._mmap,
                dtype=np.float64,
                offset=SHM_HEADER_SIZE,
                count=row_count * n_cols,
            ).reshape(row_count, n_cols)

            gen2 = struct.unpack_from("<Q", self._mmap, 56)[0]
            if gen1 == gen2:
                return data

        raise RuntimeError(f"mmap generation conflict after {max_retries} retries: {self.path}")

    def refresh(self) -> int:
        """Refresh row_count from the file header. Returns new row_count."""
        self._refresh_header()
        return self._header.row_count

    def close(self):
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._fd is not None:
            self._fd.close()
            self._fd = None

    def __del__(self):
        self.close()


def parse_shm_filename(filename: str) -> Optional[Tuple[str, int]]:
    """Parse mmap filename to (code, kind).

    Examples:
        quant_tick_600000_XSHG.mmap → ("600000.XSHG", KIND_TICK)
        quant_order_000001_XSHE.mmap → ("000001.XSHE", KIND_ORDER)
    """
    m = re.match(r"quant_(tick|order|deal)_(\d{6})_(XSHG|XSHE)\.mmap$", filename)
    if not m:
        return None
    kind_name, code, market = m.groups()
    kind = KIND_FROM_NAME.get(kind_name)
    if kind is None:
        return None
    return (f"{code}.{market}", kind)


def scan_shm_dir(shm_dir: str) -> Dict[Tuple[str, int], str]:
    """Scan directory for native SHM files.

    Returns:
        dict mapping (code, kind) → file path
    """
    result = {}
    shm_path = Path(shm_dir)
    if not shm_path.exists():
        return result
    for entry in shm_path.iterdir():
        if not entry.name.endswith(".mmap"):
            continue
        parsed = parse_shm_filename(entry.name)
        if parsed is not None:
            code, kind = parsed
            result[(code, kind)] = str(entry)
    return result


def get_codes(shm_dir: str) -> List[str]:
    """Get sorted list of unique stock codes in the SHM directory."""
    files = scan_shm_dir(shm_dir)
    codes = sorted(set(code for code, _ in files.keys()))
    return codes


def open_all_readers(shm_dir: str) -> Dict[Tuple[str, int], NativeShmReader]:
    """Open all valid SHM files as readers.

    Returns:
        dict mapping (code, kind) → NativeShmReader
    """
    paths = scan_shm_dir(shm_dir)
    readers = {}
    for key, path in paths.items():
        try:
            readers[key] = NativeShmReader(path)
        except Exception as e:
            print(f"[native_shm_reader] warning: failed to open {path}: {e}")
    return readers
