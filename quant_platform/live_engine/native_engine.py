# -*- coding: utf-8 -*-
"""
Native Engine: reads mmap files from C++ native-mdl-collector.
No pymdl import — data collection is entirely in C++.

Data flow:
    C++ native-mdl-collector → /data/quant/shm/*.mmap
    native_engine → read mmap → build DataFrame → factor computation

Environment variables:
    NATIVE_SHM_DIR         mmap directory (default: /data/quant/shm)
    FACTOR_MODULE          factor module path
    COMPUTE_INTERVAL       compute interval seconds (default: 60)
    FACTOR_OUTPUT_PATH     factor output directory
    FACTOR_WORKERS         number of parallel workers (default: 40)
"""

from __future__ import annotations

import copy
import gc
import importlib
import logging
import multiprocessing
import os
import resource
import signal
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..core.constants import (
    TICK_COLUMNS, ORDER_COLUMNS, DEAL_COLUMNS,
    MARKET_SH, MARKET_SZ,
)
from ..data.native_shm_reader import (
    NativeShmReader, scan_shm_dir, get_codes, open_all_readers,
    KIND_TICK, KIND_ORDER, KIND_DEAL, COLS_BY_KIND, SHM_HEADER_SIZE,
)
from ..data.mysql_loader import DailyBasicCache
from ..factor.base import StockData, StockState
from .streaming_engine import (
    _is_trading_hours,
    _time_to_seconds,
    _upload_to_oss,
)
from .pipeline_logger import get_streaming_logger

logger = logging.getLogger(__name__)

# Module-level globals: set before fork, inherited by child processes via COW
_factor_fn: Optional[Callable] = None
_market_df: pd.DataFrame = pd.DataFrame()
_daily_basic_df: pd.DataFrame = pd.DataFrame()
_reader_cache: Dict[str, NativeShmReader] = {}
_base_ns: int = 0  # pd.Timestamp(trading_day).value, set once per day

def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


# ── DataFrame construction from native mmap ────────────────────────

_tick_columns = TICK_COLUMNS
_order_columns = ORDER_COLUMNS
_deal_columns = DEAL_COLUMNS
_tick_buf_cols = TICK_COLUMNS[2:]
_order_buf_cols = ORDER_COLUMNS[2:]
_deal_buf_cols = DEAL_COLUMNS[2:]
_tick_time_idx = _tick_buf_cols.index('Time')       # 0
_tick_updtime_idx = _tick_buf_cols.index('UpdateTime')  # 1
_deal_time_idx = _deal_buf_cols.index('Time')       # 0
_deal_updtime_idx = _deal_buf_cols.index('UpdateTime')  # 1
_order_time_idx = _order_buf_cols.index('Time')       # 0
_order_updtime_idx = _order_buf_cols.index('UpdateTime')  # 1
_columns_by_kind = {
    KIND_TICK: TICK_COLUMNS,
    KIND_ORDER: ORDER_COLUMNS,
    KIND_DEAL: DEAL_COLUMNS,
}
_kind_names = {
    KIND_TICK: "tick",
    KIND_ORDER: "order",
    KIND_DEAL: "deal",
}


def _build_df_from_native(reader: NativeShmReader, columns: list,
                          buf_cols: list, time_idx: int, updtime_idx: int,
                          trading_day: str, code: str) -> pd.DataFrame:
    """Build DataFrame from native SHM reader.

    The C++ writer stores 79/9/10 numeric columns (skip TradingDay and Code).
    Time/UpdateTime are f64 seconds since midnight.

    Reads the full current-day buffer so factor code receives all data
    accumulated up to this round.
    """
    arr = reader.view_rows()
    if arr.shape[0] == 0:
        return pd.DataFrame()

    # Zero-copy DataFrame over numpy array
    df = pd.DataFrame(arr, columns=buf_cols, copy=False)

    # Fast time conversion: f64 seconds → datetime64[ns]
    base_ns = _base_ns
    df.iloc[:, time_idx] = (base_ns + (arr[:, time_idx] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')
    df.iloc[:, updtime_idx] = (base_ns + (arr[:, updtime_idx] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')

    df.insert(0, 'TradingDay', trading_day)
    df.insert(1, 'Code', code)
    return df


def _read_native_df_for_worker(path: str, kind: int, code: str, trading_day: str,
                               start_row: int = 0, end_row: Optional[int] = None) -> pd.DataFrame:
    columns = _columns_by_kind[kind]
    reader = NativeShmReader(path)
    try:
        arr = reader.read_rows_from(start_row) if start_row > 0 else reader.read_rows()
    finally:
        reader.close()
    if end_row is not None:
        arr = arr[:max(0, end_row - start_row)]
    if arr.shape[0] == 0:
        return pd.DataFrame()
    buf_cols = columns[2:]
    df = pd.DataFrame(arr, columns=buf_cols, copy=False)
    base_ns = pd.Timestamp(trading_day).value
    for col in ("Time", "UpdateTime"):
        if col in df.columns:
            df[col] = (base_ns + (df[col].to_numpy() * 1_000_000_000).astype(np.int64)).astype("datetime64[ns]")
    df.insert(0, "TradingDay", trading_day)
    df.insert(1, "Code", code)
    return df[columns]


# ── Worker function for parallel factor computation ────────────────

def _cached_reader(path: str) -> NativeShmReader:
    reader = _reader_cache.get(path)
    if reader is None:
        reader = NativeShmReader(path)
        _reader_cache[path] = reader
    return reader


_df_build_ms = 0.0
_factor_ms = 0.0
_profile_count = 0


def _compute_stock_shm(args):
    """Old combined-engine compute path over native mmap files.

    Each call builds full current-day DataFrames from mmap views and passes the
    main-process StockState snapshot into the factor, matching the old
    _compute_stock_shm contract as closely as possible.
    """
    global _factor_fn, _market_df, _daily_basic_df, _df_build_ms, _factor_ms, _profile_count
    code, date_str, end_time, wall_secs, state_snap, tick_path, deal_path, order_path, trading_day, factor_fn = args
    try:
        t0 = time.perf_counter()
        tick_df = _build_df_from_native(_cached_reader(tick_path), _tick_columns, _tick_buf_cols, _tick_time_idx, _tick_updtime_idx, trading_day, code) if tick_path else pd.DataFrame()
        deal_df = _build_df_from_native(_cached_reader(deal_path), _deal_columns, _deal_buf_cols, _deal_time_idx, _deal_updtime_idx, trading_day, code) if deal_path else pd.DataFrame()
        order_df = _build_df_from_native(_cached_reader(order_path), _order_columns, _order_buf_cols, _order_time_idx, _order_updtime_idx, trading_day, code) if order_path else pd.DataFrame()
        df_ms = (time.perf_counter() - t0) * 1000

        stock_data = StockData(
            code=code, date=date_str, end_time=end_time,
            l1_tick=tick_df, l2_deal=deal_df, l2_order=order_df,
            market=_market_df, daily_basic=_daily_basic_df,
            state=state_snap,
        )

        t1 = time.perf_counter()
        result = factor_fn(stock_data, code, date_str, end_time)
        fn_ms = (time.perf_counter() - t1) * 1000

        _df_build_ms += df_ms
        _factor_ms += fn_ms
        _profile_count += 1
        if _profile_count % 200 == 0:
            logger.info("[profile] worker=%s stocks=%d df=%.1fms factor=%.1fms total=%.1fms",
                        multiprocessing.current_process().name, _profile_count,
                        _df_build_ms / _profile_count, _factor_ms / _profile_count,
                        (_df_build_ms + _factor_ms) / _profile_count)

        if result is not None and state_snap and state_snap.last_market_time:
            market_secs = _time_to_seconds(state_snap.last_market_time)
            if market_secs > 0:
                data_latency_ms = round((wall_secs - market_secs) * 1000, 1)
                if abs(data_latency_ms) < 600_000:
                    result["data_latency_ms"] = data_latency_ms
        return code, result, None
    except Exception as exc:
        return code, None, str(exc)


# ── Native Engine ──────────────────────────────────────────────────

class NativeEngine:
    """Production engine that reads C++ mmap files for factor computation."""

    def __init__(self):
        self.shm_dir = os.environ.get("NATIVE_SHM_DIR", "/data/quant/shm")
        self.compute_interval = int(os.environ.get("COMPUTE_INTERVAL", "60"))
        self.factor_module = os.environ.get("FACTOR_MODULE", "")
        self.factor_output_path = os.environ.get("FACTOR_OUTPUT_PATH", "/data/factors")
        self.output_path = Path(self.factor_output_path) if self.factor_output_path else None
        self.n_workers = int(os.environ.get("FACTOR_WORKERS", "40"))
        self.trading_day: str = ""
        self._pool: Optional[multiprocessing.Pool] = None
        self._daily_cache: Optional[DailyBasicCache] = None
        self._states: Dict[str, StockState] = {}
        self.factor_calculation: Optional[Callable] = None
        self.outfun: Optional[Callable] = None
        self._last_codes: List[str] = []
        self._last_compute: float = 0.0
        self._data_flush_interval: float = float(os.environ.get("DATA_FLUSH_INTERVAL", "0.01"))
        self._round_count: int = 0
        self._raw_archive_enabled = _env_bool("RAW_DATA_ARCHIVE_ENABLED", True)
        self._archive_interval = int(os.environ.get("ARCHIVE_INTERVAL", "300"))
        self._disk_output_dir = Path(os.environ.get("COLLECTOR_DISK_OUTPUT", "/data/collector_output"))
        self._archive_offsets: Dict[Tuple[str, int], int] = {}
        self._disk_chunk_idx: Dict[str, int] = {}
        self._uploaded_today = False
        self._stopped = False
        self._archive_thread: Optional[threading.Thread] = None
        self._compute_lock = threading.Lock()
        self._compute_running = False
        self._state_offsets: Dict[Tuple[str, int], int] = {}
        self._main_readers: Dict[str, NativeShmReader] = {}
        self._cached_shm_files: Optional[Dict[str, Dict[int, str]]] = None

    def _init_pool(self) -> None:
        """Create persistent worker pool."""
        if self._pool is not None:
            return
        logger.info("[native] creating persistent pool with %d workers", self.n_workers)
        self._pool = multiprocessing.Pool(
            processes=self.n_workers,
            maxtasksperchild=None,
            initializer=_worker_init,
            initargs=(self.factor_module, self._daily_basic_df, self._market_df),
        )

    def _release_pool(self) -> None:
        """Release factor workers before memory-heavy post-close archive upload."""
        with self._compute_lock:
            if self._pool is None:
                return
            logger.info("[native] releasing factor pool")
            self._pool.close()
            self._pool.join()
            self._pool = None
            gc.collect()

    def _load_daily_basic(self) -> None:
        """Load daily basic data from MySQL."""
        market_count = int(os.environ.get("DAILY_BASIC_MARKET_COUNT", "1"))
        self._daily_cache = DailyBasicCache(market_count=market_count)
        self._daily_cache.load(self.trading_day)
        self._daily_basic_df = self._daily_cache.get_daily_basic()
        self._market_df = pd.DataFrame()
        logger.info("[native] daily_basic: %d stocks, market: %d entries",
                     len(self._daily_basic_df), len(self._market_df))

    def _scan_shm_files(self) -> Dict[str, Dict[int, str]]:
        """Scan SHM directory, return {code: {kind: path}}.
        Result is cached after first call — files don't change during the day."""
        if self._cached_shm_files is not None:
            return self._cached_shm_files
        files = scan_shm_dir(self.shm_dir)
        by_code: Dict[str, Dict[int, str]] = {}
        for (code, kind), path in files.items():
            if code not in by_code:
                by_code[code] = {}
            by_code[code][kind] = path
        self._cached_shm_files = by_code
        return by_code

    @staticmethod
    def _raw_time_from_seconds(seconds: float) -> str:
        if seconds <= 0:
            return ""
        total_ms = int(seconds * 1000)
        h, rem = divmod(total_ms, 3600_000)
        m, rem = divmod(rem, 60_000)
        s, ms = divmod(rem, 1000)
        return f"{h:02d}{m:02d}{s:02d}{ms:03d}"

    def _get_cached_reader(self, path: str) -> Optional[NativeShmReader]:
        """Get or create a cached NativeShmReader for the main process."""
        reader = self._main_readers.get(path)
        if reader is not None:
            return reader
        try:
            reader = NativeShmReader(path)
            self._main_readers[path] = reader
            return reader
        except Exception:
            return None

    def _sync_states(self, files_by_code: Dict[str, Dict[int, str]],
                     wall_secs: float) -> Tuple[set, Dict[str, StockState]]:
        """Update main-process StockState from native mmap increments.

        Returns (dirty_codes, states_snapshot). dirty_codes contains only codes
        that received new data since last round — callers should only dispatch
        factor tasks for these stocks.

        Uses cached readers and vectorized numpy ops to minimize overhead.
        """
        dirty_codes: set = set()

        for code, kinds in files_by_code.items():
            state = self._states.get(code)
            if state is None:
                state = StockState(code=code)
                self._states[code] = state

            for kind in (KIND_TICK, KIND_DEAL, KIND_ORDER):
                path = kinds.get(kind)
                if not path:
                    continue
                offset_key = (path, kind)
                start = self._state_offsets.get(offset_key, 0)

                reader = self._get_cached_reader(path)
                if reader is None:
                    continue
                try:
                    current = reader.refresh()
                except Exception:
                    continue
                if current <= start:
                    continue

                arr = reader.view_rows()[start:current]
                n = arr.shape[0]
                if n == 0:
                    self._state_offsets[offset_key] = current
                    continue

                dirty_codes.add(code)
                state.last_update_ts = wall_secs

                if kind == KIND_TICK:
                    self._update_tick_vectorized(state, arr)
                elif kind == KIND_DEAL:
                    self._update_deal_vectorized(state, arr)
                elif kind == KIND_ORDER:
                    self._update_order_vectorized(state, arr)

                # Commit offset only after state update succeeds
                self._state_offsets[offset_key] = current

        return dirty_codes, {code: copy.copy(self._states[code]) for code in dirty_codes}

    @staticmethod
    def _update_tick_vectorized(state: StockState, arr: np.ndarray) -> None:
        """Vectorized tick state update from numpy rows (zero-copy view)."""
        state.tick_count += arr.shape[0]

        prices = arr[:, 2]  # CurrentPrice
        nonzero = prices[prices > 0]
        if len(nonzero) > 0:
            if state.open == 0.0:
                state.open = float(nonzero[0])
            state.latest_price = float(nonzero[-1])
            state.high = max(state.high, float(nonzero.max()))
            low_val = float(nonzero.min())
            if low_val < state.low:
                state.low = low_val

        pre_close = float(arr[-1, 5])
        if pre_close > 0:
            state.pre_close = pre_close
        ask1 = float(arr[-1, 17])
        if ask1 > 0:
            state.ask1 = ask1
        bid1 = float(arr[-1, 47])
        if bid1 > 0:
            state.bid1 = bid1
        state.ask_volume1 = int(arr[-1, 27])
        state.bid_volume1 = int(arr[-1, 57])

        raw_time = NativeEngine._raw_time_from_seconds(float(arr[-1, 1]))
        state.last_tick_time = raw_time
        state._update_market_time_raw(raw_time)

    @staticmethod
    def _update_deal_vectorized(state: StockState, arr: np.ndarray) -> None:
        """Vectorized deal state update from numpy rows (zero-copy view)."""
        n = arr.shape[0]
        state.deal_count += n
        prices = arr[:, 5]   # Price
        volumes = arr[:, 6]  # Volume
        state.cum_amount += float(np.dot(prices, volumes))
        state.cum_volume += int(volumes.sum())

        raw_time = NativeEngine._raw_time_from_seconds(float(arr[-1, 1]))
        state.last_deal_time = raw_time
        state._update_market_time_raw(raw_time)

    @staticmethod
    def _update_order_vectorized(state: StockState, arr: np.ndarray) -> None:
        """Vectorized order state update from numpy rows (zero-copy view)."""
        n = arr.shape[0]
        state.order_count += n

        sides = arr[:, 3]   # Side: 0=buy, 1=sell
        volumes = arr[:, 5]  # Volume
        order_types = arr[:, 6]  # OrderType

        buy_mask = sides == 0
        sell_mask = sides == 1
        state.buy_order_count += int(buy_mask.sum())
        state.sell_order_count += int(sell_mask.sum())
        state.buy_order_volume += int(volumes[buy_mask].sum())
        state.sell_order_volume += int(volumes[sell_mask].sum())
        state.cancel_count += int((order_types == 5).sum())

        raw_time = NativeEngine._raw_time_from_seconds(float(arr[-1, 1]))
        state.last_order_time = raw_time
        state._update_market_time_raw(raw_time)

    def _read_native_df(self, path: str, kind: int, code: str,
                        start_row: int = 0, end_row: Optional[int] = None) -> pd.DataFrame:
        columns = _columns_by_kind[kind]
        reader = NativeShmReader(path)
        try:
            arr = reader.read_rows_from(start_row) if start_row > 0 else reader.read_rows()
        finally:
            reader.close()
        if end_row is not None:
            keep_rows = max(0, end_row - start_row)
            arr = arr[:keep_rows]
        if arr.shape[0] == 0:
            return pd.DataFrame()
        buf_cols = columns[2:]
        df = pd.DataFrame(arr, columns=buf_cols, copy=False)
        base_ns = pd.Timestamp(self.trading_day).value
        for col in ("Time", "UpdateTime"):
            if col in df.columns:
                df[col] = (base_ns + (df[col].to_numpy() * 1_000_000_000).astype(np.int64)).astype("datetime64[ns]")
        df.insert(0, "TradingDay", self.trading_day)
        df.insert(1, "Code", code)
        return df[columns]

    def _append_raw_to_disk(self, kind_name: str, df: pd.DataFrame) -> bool:
        if df.empty:
            return True
        out_dir = self._disk_output_dir / self.trading_day / kind_name
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            key = f"{self.trading_day}/{kind_name}"
            chunk_idx = self._disk_chunk_idx.get(key)
            if chunk_idx is None:
                existing = sorted(out_dir.glob("*.parquet"))
                chunk_idx = max((int(p.stem) for p in existing if p.stem.isdigit()), default=-1) + 1
            chunk_file = out_dir / f"{chunk_idx:06d}.parquet"
            df.to_parquet(chunk_file, index=False)
            self._disk_chunk_idx[key] = chunk_idx + 1
            logger.info("[native-archive] %s +%d rows -> %s", kind_name, len(df), chunk_file)
            return True
        except Exception as exc:
            logger.error("[native-archive] write %s parquet failed: %s", kind_name, exc, exc_info=True)
            return False

    @staticmethod
    def _parse_upload_time(raw: str) -> Tuple[int, int]:
        if ":" in raw:
            hour, minute = raw.split(":", 1)
            return int(hour), int(minute)
        return int(raw), 0

    def _get_oss_bucket(self):
        import oss2
        auth = oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"])
        endpoint = os.environ.get("OSS_ENDPOINT", "")
        if endpoint and not endpoint.startswith("http"):
            endpoint = f"https://{endpoint}"
        bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")
        return oss2.Bucket(auth, endpoint, bucket_name)

    def _check_raw_upload_time(self, files_by_code: Dict[str, Dict[int, str]]) -> None:
        if not self._raw_archive_enabled or self._uploaded_today:
            return
        upload_hour, upload_minute = self._parse_upload_time(
            os.environ.get("RAW_DATA_UPLOAD_TIME", os.environ.get("RAW_DATA_UPLOAD_HOUR", "16"))
        )
        now = datetime.now()
        if (now.hour, now.minute) < (upload_hour, upload_minute):
            return
        self._archive_native_incremental(files_by_code)
        self._release_pool()
        if self._upload_raw_day_to_oss():
            self._uploaded_today = True

    def _upload_raw_day_to_oss(self) -> bool:
        date_str = self.trading_day
        year = date_str[:4]
        month = date_str[4:6]
        prefix = f"{year}/{year}{month}/{date_str}"
        disk_dir = self._disk_output_dir / date_str
        if not disk_dir.exists():
            logger.warning("[native-archive] disk dir missing, skip upload: %s", disk_dir)
            return False

        try:
            bucket = self._get_oss_bucket()
        except Exception as exc:
            logger.error("[native-archive] OSS bucket init failed: %s", exc)
            return False

        try:
            import duckdb
        except Exception as exc:
            logger.error("[native-archive] duckdb unavailable, cannot merge parquet: %s", exc)
            return False

        map_tmp_path = disk_dir / "tmp_code_map.parquet"
        try:
            map_df = self._daily_basic_df[["ID_QI", "SECURITY_ID"]].drop_duplicates()
            map_df.to_parquet(map_tmp_path, index=False)
        except Exception as exc:
            logger.error("[native-archive] code map write failed: %s", exc)
            return False

        uploaded_any = False
        had_error = False

        for kind in ("order", "deal", "tick"):
            chunk_dir = disk_dir / kind
            chunks = sorted(chunk_dir.glob("*.parquet")) if chunk_dir.exists() else []
            if not chunks:
                continue
            tmp_file = disk_dir / f"tmp_{kind}.parquet"
            try:
                tmp_file.unlink(missing_ok=True)
                con = duckdb.connect(":memory:")
                con.execute(f"SET memory_limit='{os.environ.get('ARCHIVE_DUCKDB_MEMORY', '8GB')}'")
                con.execute(
                    "CREATE MACRO epoch_us(ts) AS "
                    "(EXTRACT('epoch' FROM ts)::BIGINT * 1000000 + EXTRACT('microseconds' FROM ts)::BIGINT)"
                )
                select_clause = self._archive_select_clause(kind)
                con.execute(f"""
                    COPY (
                        SELECT
                            {select_clause}
                        FROM read_parquet('{chunk_dir}/*.parquet') x
                        JOIN read_parquet('{map_tmp_path}') m
                          ON regexp_extract(x.Code, '^\\d+') = m.ID_QI::VARCHAR
                        ORDER BY m.SECURITY_ID, x.SeqNum
                    ) TO '{tmp_file}' (FORMAT PARQUET, COMPRESSION 'zstd')
                """)
                con.close()
                oss_key = f"{prefix}/{date_str}_{kind}.parquet"
                bucket.put_object_from_file(oss_key, str(tmp_file))
                size_mb = tmp_file.stat().st_size / 1024 / 1024
                logger.info("[native-archive] uploaded %s -> oss://%s/%s (%.1f MB, %d chunks)",
                            kind, os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data"), oss_key, size_mb, len(chunks))
                uploaded_any = True
                tmp_file.unlink(missing_ok=True)
                for chunk in chunks:
                    chunk.unlink()
            except Exception as exc:
                had_error = True
                logger.error("[native-archive] upload %s failed: %s", kind, exc, exc_info=True)
                tmp_file.unlink(missing_ok=True)

        map_tmp_path.unlink(missing_ok=True)
        logger.info("[native-archive] %s upload finished", date_str)
        return uploaded_any and not had_error

    def _archive_select_clause(self, kind: str) -> str:
        if kind == "order":
            exprs = [
                "m.SECURITY_ID::INTEGER AS Code",
                "epoch_us(x.Time) AS Time",
                "(epoch_us(x.UpdateTime) - epoch_us(x.Time)) AS UpdateTime",
                "x.OrderID::INTEGER AS OrderID",
                "x.Side::TINYINT AS Side",
                "ROUND(x.Price * 100)::INTEGER AS Price",
                "x.Volume::BIGINT AS Volume",
                "x.OrderType::TINYINT AS OrderType",
                "x.SeqNum::INTEGER AS SeqNum",
            ]
        elif kind == "deal":
            exprs = [
                "m.SECURITY_ID::INTEGER AS Code",
                "epoch_us(x.Time) AS Time",
                "(epoch_us(x.UpdateTime) - epoch_us(x.Time)) AS UpdateTime",
                "x.SaleOrderID::BIGINT AS SaleOrderID",
                "x.BuyOrderID::BIGINT AS BuyOrderID",
                "x.Side::TINYINT AS Side",
                "ROUND(x.Price * 100)::INTEGER AS Price",
                "x.Volume::BIGINT AS Volume",
                "x.SeqNum::INTEGER AS SeqNum",
            ]
        elif kind == "tick":
            exprs = [
                "m.SECURITY_ID::INTEGER AS Code",
                "epoch_us(x.Time) AS Time",
                "(epoch_us(x.UpdateTime) - epoch_us(x.Time)) AS UpdateTime",
                "ROUND(x.CurrentPrice * 100)::INTEGER AS CurrentPrice",
                "x.TotalVolume::BIGINT AS TotalVolume",
            ]
            exprs += [f"ROUND(x.{c} * 100)::INTEGER AS {c}" for c in
                      ["PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice",
                       "HighLimitPrice", "LowLimitPrice", "IOPV"]]
            exprs += [
                "COALESCE(x.TradeNum, 0)::INTEGER AS TradeNum",
                "x.TotalBidVolume::BIGINT AS TotalBidVolume",
                "x.TotalAskVolume::BIGINT AS TotalAskVolume",
                "ROUND(x.AvgBidPrice * 100)::INTEGER AS AvgBidPrice",
                "ROUND(x.AvgAskPrice * 100)::INTEGER AS AvgAskPrice",
            ]
            exprs += [f"ROUND(x.AskPrice{i} * 100)::INTEGER AS AskPrice{i}" for i in range(1, 11)]
            exprs += [f"COALESCE(x.AskVolume{i}, 0)::BIGINT AS AskVolume{i}" for i in range(1, 11)]
            exprs += [f"COALESCE(x.AskNum{i}, 0)::INTEGER AS AskNum{i}" for i in range(1, 11)]
            exprs += [f"ROUND(x.BidPrice{i} * 100)::INTEGER AS BidPrice{i}" for i in range(1, 11)]
            exprs += [f"COALESCE(x.BidVolume{i}, 0)::BIGINT AS BidVolume{i}" for i in range(1, 11)]
            exprs += [f"COALESCE(x.BidNum{i}, 0)::INTEGER AS BidNum{i}" for i in range(1, 11)]
            exprs += ["x.SeqNum::INTEGER AS SeqNum"]
        else:
            raise ValueError(f"unsupported archive kind: {kind}")
        return ",\n                            ".join(exprs)

    def _archive_native_incremental(self, files_by_code: Dict[str, Dict[int, str]]) -> None:
        if not self._raw_archive_enabled:
            return
        for kind, kind_name in _kind_names.items():
            frames = []
            pending_offsets: Dict[Tuple[str, int], int] = {}
            for code, kinds in files_by_code.items():
                path = kinds.get(kind)
                if not path:
                    continue
                offset_key = (path, kind)
                start = self._archive_offsets.get(offset_key, 0)
                try:
                    reader = NativeShmReader(path)
                    current = reader.refresh()
                    reader.close()
                    if current <= start:
                        continue
                    df = self._read_native_df(path, kind, code, start_row=start, end_row=current)
                    pending_offsets[offset_key] = current
                    if not df.empty:
                        frames.append(df)
                except Exception as exc:
                    logger.warning("[native-archive] snapshot failed kind=%s code=%s: %s", kind_name, code, exc)
            if frames:
                if self._append_raw_to_disk(kind_name, pd.concat(frames, ignore_index=True)):
                    self._archive_offsets.update(pending_offsets)
            elif pending_offsets:
                self._archive_offsets.update(pending_offsets)

    def _archive_loop(self) -> None:
        logger.info("[native-archive] background archive loop started interval=%ds", self._archive_interval)
        last_archive_date = ""
        archived_lunch = False
        archived_close = False
        while not self._stopped:
            try:
                today = date.today().isoformat()
                if today != last_archive_date:
                    last_archive_date = today
                    archived_lunch = False
                    archived_close = False

                files_by_code = self._scan_shm_files()
                if files_by_code:
                    now = datetime.now()
                    h, m = now.hour, now.minute
                    if not archived_lunch and h == 11 and m >= 35:
                        logger.info("[native-archive] lunch break snapshot starting...")
                        self._archive_native_incremental(files_by_code)
                        archived_lunch = True
                        logger.info("[native-archive] lunch break snapshot done")
                    elif not archived_close and h >= 15 and m >= 5:
                        logger.info("[native-archive] post-close snapshot starting...")
                        self._archive_native_incremental(files_by_code)
                        self._check_raw_upload_time(files_by_code)
                        archived_close = True
                        logger.info("[native-archive] post-close snapshot done")
            except Exception as exc:
                logger.error("[native-archive] loop error: %s", exc, exc_info=True)
            for _ in range(max(1, self._archive_interval)):
                if self._stopped:
                    break
                time.sleep(1)

    def _compute_and_output(self) -> None:
        with self._compute_lock:
            self._compute_and_output_locked()

    def _compute_and_output_wrapper(self) -> None:
        """Run _compute_and_output in background, clear flag when done."""
        try:
            self._compute_and_output()
        except Exception as exc:
            logger.error("[combined] background compute failed: %s", exc, exc_info=True)
        finally:
            self._compute_running = False

    def _compute_and_output_locked(self) -> None:
        if not _is_trading_hours():
            return
        pipe_log = get_streaming_logger()
        now_dt = datetime.now()
        date_str = self.trading_day
        end_time = now_dt.strftime("%H%M%S")
        self._round_count += 1

        files_by_code = self._scan_shm_files()
        if not files_by_code:
            return

        snap_t0 = time.perf_counter()
        wall_secs = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
        dirty_codes, states_snapshot = self._sync_states(files_by_code, wall_secs)
        snap_ms = (time.perf_counter() - snap_t0) * 1000

        if not dirty_codes:
            return

        all_codes = sorted(dirty_codes)
        logger.info("[combined] computing: date=%s end_time=%s dirty=%d total=%d",
                    date_str, end_time, len(all_codes), len(files_by_code))

        t0 = time.time()

        global _factor_fn, _market_df, _daily_basic_df, _base_ns
        _factor_fn = self.factor_calculation
        _market_df = self._market_df
        _daily_basic_df = self._daily_basic_df
        _base_ns = pd.Timestamp(date_str).value

        tick_paths = {code: files_by_code[code].get(KIND_TICK, "") for code in all_codes}
        deal_paths = {code: files_by_code[code].get(KIND_DEAL, "") for code in all_codes}
        order_paths = {code: files_by_code[code].get(KIND_ORDER, "") for code in all_codes}

        tasks = [
            (code, date_str, end_time, wall_secs, states_snapshot.get(code),
             tick_paths.get(code, ""), deal_paths.get(code, ""), order_paths.get(code, ""),
             date_str, self.factor_calculation)
            for code in all_codes
        ]

        results = []
        errors = 0
        pool_t0 = time.perf_counter()

        if self._pool is not None:
            try:
                for code, result, err in self._pool.imap_unordered(_compute_stock_shm, tasks, chunksize=32):
                    if err:
                        logger.warning("[%s] compute failed: %s", code, err)
                        errors += 1
                    elif result is not None:
                        results.append(result)
            except Exception as exc:
                logger.error("[combined] persistent pool failed: %s", exc, exc_info=True)
                self._pool = None
        else:
            for task in tasks:
                code, result, err = _compute_stock_shm(task)
                if err:
                    logger.warning("[%s] compute failed: %s", code, err)
                    errors += 1
                elif result is not None:
                    results.append(result)

        pool_ms = (time.perf_counter() - pool_t0) * 1000
        elapsed_ms = (time.time() - t0) * 1000
        pool_type = "persistent+shm" if self._pool is not None else "inline"
        n_stocks = len(all_codes)
        per_stock = pool_ms / max(n_stocks, 1) * self.n_workers
        logger.info(
            "[combined] done (%s): %d results (errors=%d) | "
            "snap=%.0fms pool=%.0fms total=%.0fms | "
            "%d stocks × %dw → ~%.1fms/stock",
            pool_type, len(results), errors,
            snap_ms, pool_ms, elapsed_ms,
            n_stocks, self.n_workers, per_stock,
        )

        if states_snapshot:
            compute_now = datetime.now()
            compute_wall_secs = compute_now.hour * 3600 + compute_now.minute * 60 + compute_now.second
            latency_parts = []
            for sc in list(states_snapshot.keys())[:3]:
                st = states_snapshot.get(sc)
                if st and st.last_market_time:
                    mkt_secs = _time_to_seconds(st.last_market_time)
                    if mkt_secs > 0:
                        latency_parts.append(
                            f"{sc}: 行情={st.last_market_time} "
                            f"因子计算延迟={round((compute_wall_secs - mkt_secs) * 1000, 1)}ms"
                        )
            if latency_parts:
                logger.info("[latency-factor] %s", " | ".join(latency_parts))

        if not results:
            return

        self._write_results(results, date_str, end_time)
        pipe_log.log("factor_compute", date=date_str, end_time=end_time,
                     stocks=len(all_codes), results=len(results),
                     compute_ms=round(elapsed_ms, 1))

    def _write_results(self, results: list, date_str: str, end_time: str) -> None:
        """Write factor results to CSV and upload to OSS."""
        if not results:
            return
        output_path = self.output_path
        outfun = self.outfun
        try:
            os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            pass
        try:
            pid = os.fork()
            if pid == 0:
                try:
                    result_df = pd.DataFrame(results)
                    if output_path and not result_df.empty:
                        output_path.mkdir(parents=True, exist_ok=True)
                        out_file = output_path / f"{date_str}_{end_time}.csv"
                        result_df.to_csv(out_file, index=False)
                        logger.info("[combined] wrote %s", out_file)
                    if not result_df.empty:
                        _upload_to_oss(result_df, date_str, end_time)
                    if outfun is not None:
                        try:
                            outfun(date_str, end_time, result_df)
                        except Exception as exc:
                            logger.error("[combined] outfun failed: %s", exc)
                except Exception as exc:
                    logger.error("[output-child] failed: %s", exc, exc_info=True)
                finally:
                    os._exit(0)
            logger.info("[combined] output forked to child pid=%d", pid)
        except OSError:
            pass

    def run(self) -> None:
        """Main loop."""
        # Set trading day
        today = date.today()
        self.trading_day = today.strftime("%Y%m%d")
        logger.info("[native] starting with trading_day=%s, shm_dir=%s",
                     self.trading_day, self.shm_dir)

        # Load daily basic
        self._load_daily_basic()

        # Set globals for workers
        global _factor_fn, _market_df, _daily_basic_df
        _market_df = self._market_df
        _daily_basic_df = self._daily_basic_df

        # Load factor module
        if self.factor_module:
            mod = importlib.import_module(self.factor_module)
            _factor_fn = mod.factor_calculation
            self.factor_calculation = mod.factor_calculation
            self.outfun = getattr(mod, "outfun", None)
            logger.info("[native] loaded factor: %s", self.factor_module)
        else:
            logger.error("[native] FACTOR_MODULE not set!")
            return

        # Init pool
        self._init_pool()

        if self._raw_archive_enabled:
            self._archive_thread = threading.Thread(
                target=self._archive_loop,
                name="native-raw-archive",
                daemon=True,
            )
            self._archive_thread.start()

        logger.info("[native] entering main loop (interval=%ds)", self.compute_interval)

        while True:
            try:
                now = time.time()
                if now - self._last_compute >= self.compute_interval:
                    if _is_trading_hours() and not self._compute_running:
                        self._compute_running = True
                        t = threading.Thread(target=self._compute_and_output_wrapper, daemon=True)
                        t.start()
                        self._last_compute = now
                    elif self._compute_running:
                        self._last_compute = now
                    else:
                        self._last_compute = now

                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("[native] interrupted")
                break
            except Exception as e:
                logger.error("[native] error in main loop: %s", e, exc_info=True)
                time.sleep(5)

        # Cleanup
        self._stopped = True
        if self._archive_thread is not None:
            self._archive_thread.join(timeout=30)
        try:
            files_by_code = self._scan_shm_files()
            if files_by_code:
                self._archive_native_incremental(files_by_code)
                self._check_raw_upload_time(files_by_code)
        except Exception as exc:
            logger.warning("[native] final archive/upload failed: %s", exc)
        self._release_pool()


def _worker_init(factor_module: str, daily_basic_df: pd.DataFrame,
                 market_df: pd.DataFrame) -> None:
    """Initialize worker process."""
    global _factor_fn, _market_df, _daily_basic_df
    _market_df = market_df
    _daily_basic_df = daily_basic_df
    if factor_module:
        mod = importlib.import_module(factor_module)
        _factor_fn = mod.factor_calculation


def main() -> None:
    """Entry point."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = NativeEngine()
    engine.run()


if __name__ == "__main__":
    main()
