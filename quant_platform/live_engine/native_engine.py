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

logger = logging.getLogger(__name__)

# Module-level globals: set before fork, inherited by child processes via COW
_factor_fn: Optional[Callable] = None
_market_df: pd.DataFrame = pd.DataFrame()
_daily_basic_df: pd.DataFrame = pd.DataFrame()
_worker_frames: Dict[str, Dict[int, pd.DataFrame]] = {}
_worker_offsets: Dict[Tuple[str, int], int] = {}
_worker_states: Dict[str, StockState] = {}

def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


# ── DataFrame construction from native mmap ────────────────────────

_tick_columns = TICK_COLUMNS
_order_columns = ORDER_COLUMNS
_deal_columns = DEAL_COLUMNS
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
                          trading_day: str, code: str) -> pd.DataFrame:
    """Build DataFrame from native SHM reader.

    The C++ writer stores 79/9/10 numeric columns (skip TradingDay and Code).
    Time/UpdateTime are f64 seconds since midnight.

    Reads the full current-day buffer so factor code receives all data
    accumulated up to this round.
    """
    arr = reader.read_rows()
    if arr.shape[0] == 0:
        return pd.DataFrame()

    buf_cols = columns[2:]  # skip TradingDay, Code

    # Zero-copy DataFrame over numpy array
    df = pd.DataFrame(arr, columns=buf_cols, copy=False)

    # Fast time conversion: f64 seconds → datetime64[ns]
    base_ns = pd.Timestamp(trading_day).value
    for col in ('Time', 'UpdateTime'):
        if col in buf_cols:
            idx = buf_cols.index(col)
            df[col] = (base_ns + (arr[:, idx] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')

    df.insert(0, 'TradingDay', trading_day)
    df.insert(1, 'Code', code)
    return df[columns]


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

def _compute_stock_native(args):
    """Compute factor for one stock, reading full current-day native mmap."""
    global _factor_fn, _market_df, _daily_basic_df

    (code, date_str, end_time, wall_secs, state_snap,
     tick_reader_path, order_reader_path, deal_reader_path,
     trading_day) = args[:9]

    try:
        tick_df = pd.DataFrame()
        deal_df = pd.DataFrame()
        order_df = pd.DataFrame()

        if tick_reader_path:
            r = NativeShmReader(tick_reader_path)
            tick_df = _build_df_from_native(r, _tick_columns, trading_day, code)
            r.close()

        if deal_reader_path:
            r = NativeShmReader(deal_reader_path)
            deal_df = _build_df_from_native(r, _deal_columns, trading_day, code)
            r.close()

        if order_reader_path:
            r = NativeShmReader(order_reader_path)
            order_df = _build_df_from_native(r, _order_columns, trading_day, code)
            r.close()

        stock_data = StockData(
            code=code, date=date_str, end_time=end_time,
            l1_tick=tick_df, l2_deal=deal_df, l2_order=order_df,
            market=_market_df, daily_basic=_daily_basic_df,
            state=state_snap,
        )

        result = _factor_fn(stock_data, code, date_str, end_time)
        return code, result, None
    except Exception as exc:
        return code, None, str(exc)


def _append_frame(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if new.empty:
        return old
    if old.empty:
        return new
    return pd.concat([old, new], ignore_index=True, copy=False)


def _update_state_from_increment(code: str, kind: int, df: pd.DataFrame, wall_secs: float) -> StockState:
    state = _worker_states.get(code)
    if state is None:
        state = StockState(code=code)
        _worker_states[code] = state
    if df.empty:
        return state
    if kind == KIND_TICK:
        state.update_tick(df)
    elif kind == KIND_DEAL:
        state.update_deal(df)
    elif kind == KIND_ORDER:
        state.update_order(df)
    state.last_update_ts = wall_secs
    return state


def _compute_shard_cached(args):
    """Compute a fixed code shard using per-process DataFrame caches.

    Each pool has one process, so _worker_frames/_worker_offsets persist for
    the same shard across rounds.  This preserves full-current-day factor
    inputs without rereading every mmap file from row 0 each round.
    """
    global _factor_fn, _market_df, _daily_basic_df, _worker_frames, _worker_offsets
    shard_items, date_str, end_time, wall_secs, trading_day = args

    results = []
    errors = 0
    for code, kinds in shard_items:
        try:
            frames = _worker_frames.setdefault(code, {
                KIND_TICK: pd.DataFrame(),
                KIND_ORDER: pd.DataFrame(),
                KIND_DEAL: pd.DataFrame(),
            })
            state = _worker_states.get(code) or StockState(code=code)
            _worker_states[code] = state

            for kind in (KIND_TICK, KIND_ORDER, KIND_DEAL):
                path = kinds.get(kind)
                if not path:
                    continue
                offset_key = (path, kind)
                start = _worker_offsets.get(offset_key, 0)
                try:
                    reader = NativeShmReader(path)
                    current = reader.refresh()
                    reader.close()
                except Exception:
                    continue
                if current < start:
                    start = 0
                    _worker_offsets[offset_key] = 0
                    frames[kind] = pd.DataFrame()
                if current <= start:
                    continue
                df_new = _read_native_df_for_worker(path, kind, code, trading_day, start, current)
                _worker_offsets[offset_key] = current
                if not df_new.empty:
                    frames[kind] = _append_frame(frames[kind], df_new)
                    state = _update_state_from_increment(code, kind, df_new, wall_secs)

            stock_data = StockData(
                code=code, date=date_str, end_time=end_time,
                l1_tick=frames.get(KIND_TICK, pd.DataFrame()),
                l2_deal=frames.get(KIND_DEAL, pd.DataFrame()),
                l2_order=frames.get(KIND_ORDER, pd.DataFrame()),
                market=_market_df, daily_basic=_daily_basic_df,
                state=state,
            )
            result = _factor_fn(stock_data, code, date_str, end_time)
            if result is not None:
                results.append(result)
        except Exception:
            errors += 1
    return results, errors, len(shard_items)


# ── Native Engine ──────────────────────────────────────────────────

class NativeEngine:
    """Production engine that reads C++ mmap files for factor computation."""

    def __init__(self):
        self.shm_dir = os.environ.get("NATIVE_SHM_DIR", "/data/quant/shm")
        self.compute_interval = int(os.environ.get("COMPUTE_INTERVAL", "60"))
        self.factor_module = os.environ.get("FACTOR_MODULE", "")
        self.factor_output_path = os.environ.get("FACTOR_OUTPUT_PATH", "/data/factors")
        self.n_workers = int(os.environ.get("FACTOR_WORKERS", "40"))
        self.trading_day: str = ""
        self._pool: Optional[multiprocessing.Pool] = None
        self._daily_cache: Optional[DailyBasicCache] = None
        self._states: Dict[str, StockState] = {}
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
        """Scan SHM directory, return {code: {kind: path}}."""
        files = scan_shm_dir(self.shm_dir)
        by_code: Dict[str, Dict[int, str]] = {}
        for (code, kind), path in files.items():
            if code not in by_code:
                by_code[code] = {}
            by_code[code][kind] = path
        return by_code

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
        while not self._stopped:
            try:
                files_by_code = self._scan_shm_files()
                if files_by_code:
                    if not _is_trading_hours():
                        self._archive_native_incremental(files_by_code)
                        self._check_raw_upload_time(files_by_code)
            except Exception as exc:
                logger.error("[native-archive] loop error: %s", exc, exc_info=True)
            for _ in range(max(1, self._archive_interval)):
                if self._stopped:
                    break
                time.sleep(1)

    def _compute_and_output(self) -> None:
        """Run one round of factor computation."""
        with self._compute_lock:
            self._compute_and_output_locked()

    def _compute_and_output_locked(self) -> None:
        if not _is_trading_hours():
            return
        t0 = time.perf_counter()
        wall_secs = time.time()
        end_time = datetime.now().strftime("%H:%M:%S")
        self._round_count += 1

        # Scan mmap files
        files_by_code = self._scan_shm_files()
        codes = sorted(files_by_code.keys())
        if not codes:
            return

        n_shards = max(1, min(self.n_workers, len(codes)))
        shards: List[List[Tuple[str, Dict[int, str]]]] = [[] for _ in range(n_shards)]
        for idx, code in enumerate(codes):
            shards[idx % n_shards].append((code, files_by_code[code]))
        tasks = [(shard, self.trading_day, end_time, wall_secs, self.trading_day) for shard in shards if shard]

        # Parallel compute
        results = []
        errors = 0
        snap_start = time.perf_counter()

        if self._pool is not None:
            for shard_results, shard_errors, _ in self._pool.imap(_compute_shard_cached, tasks, chunksize=1):
                errors += shard_errors
                results.extend(shard_results)
        else:
            for task in tasks:
                shard_results, shard_errors, _ = _compute_shard_cached(task)
                errors += shard_errors
                results.extend(shard_results)

        snap_ms = (time.perf_counter() - snap_start) * 1000
        total_ms = (time.perf_counter() - t0) * 1000

        if results:
            per_stock_ms = total_ms / len(codes) if codes else 0
            logger.info("[native] round=%d done: %d results (errors=%d) | total=%.0fms | "
                         "%d stocks → ~%.1fms/stock",
                         self._round_count, len(results), errors, total_ms,
                         len(codes), per_stock_ms)

            # Write results to CSV
            self._write_results(results, end_time)

    def _write_results(self, results: list, end_time: str) -> None:
        """Write factor results to CSV and upload to OSS."""
        if not results:
            return

        df = pd.DataFrame(results)
        os.makedirs(self.factor_output_path, exist_ok=True)

        ts = end_time.replace(":", "")
        csv_path = os.path.join(self.factor_output_path,
                                 f"{self.trading_day}_{ts}.csv")
        df.to_csv(csv_path, index=False)

        # Upload to OSS in background (fork)
        try:
            pid = os.fork()
            if pid == 0:
                # Child: upload and exit
                try:
                    _upload_to_oss(df, self.trading_day, end_time)
                except Exception as e:
                    print(f"[native-oss] upload error: {e}", file=sys.stderr)
                os._exit(0)
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
                    if _is_trading_hours():
                        self._compute_and_output()
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
