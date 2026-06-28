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
import hashlib
import importlib
import json
import logging
import multiprocessing
import os
import pickle
import queue
import resource
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

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
from ..data.mysql_loader import DailyBasicCache, IdxConsCache
from ..data.oss_loader import OSSDataLoader
from ..factor.base import StockData, StockState
from ..inference.interface import (
    build_trading_universe_df,
    call_inference,
    compute_index_composition,
    compute_trading_universe,
)
from .runtime_utils import (
    _is_trading_hours,
    _time_to_seconds,
    _upload_to_oss,
)
from .pipeline_logger import get_streaming_logger
from .schedule import ComputationSchedule, build_schedules_from_env

logger = logging.getLogger(__name__)

# Module-level globals: set before fork, inherited by child processes via COW
_factor_fn: Optional[Callable] = None
_factor_info: Dict[str, Any] = {}
_market_df: pd.DataFrame = pd.DataFrame()
_daily_basic_df: pd.DataFrame = pd.DataFrame()
_reader_cache: Dict[str, NativeShmReader] = {}
_strategy_cache: Dict[str, tuple[Callable, Dict[str, Any]]] = {}
_base_ns: int = 0  # pd.Timestamp(trading_day).value, set once per day


def _compact_output_copy(df: pd.DataFrame, decimal_places: int = 6) -> pd.DataFrame:
    """Return an output-only copy with float columns rounded and downcast to float32.

    For parquet: float32 halves column storage vs float64.
    For CSV: round(n) limits decimal digits in text representation.
    """
    float_cols = df.select_dtypes(include=["float64", "float32"]).columns
    if len(float_cols) == 0:
        return df
    out_df = df.copy()
    out_df[float_cols] = out_df[float_cols].round(decimal_places).astype("float32")
    return out_df


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("[native] invalid %s=%r, fallback to %d", name, raw, default)
        return default
    if minimum is not None and value < minimum:
        logger.warning("[native] invalid %s=%r < %d, fallback to %d", name, raw, minimum, default)
        return default
    return value


def _get_worker_strategy(module_path: str, fallback_info: Optional[Dict[str, Any]] = None) -> tuple[Callable, Dict[str, Any]]:
    """Import/cache a strategy inside the worker process.

    Avoid sending function objects through multiprocessing queues and let
    persistent workers switch between minute/daily strategy modules by task.
    """
    if not module_path:
        if _factor_fn is None:
            raise RuntimeError("factor module is empty and worker has no default factor")
        return _factor_fn, (fallback_info or _factor_info or {})

    cached = _strategy_cache.get(module_path)
    if cached is not None:
        return cached

    mod = importlib.import_module(module_path)
    fn = mod.factor_calculation
    info = getattr(mod, "FACTOR_INFO", getattr(mod, "factor_info", None)) or fallback_info or {}
    _strategy_cache[module_path] = (fn, info)
    return fn, info


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


def _code_to_id_qi(code: str) -> str:
    """保留：_add_code_key 使用。"""
    return str(code).split(".")[0].zfill(6)


def _add_code_key(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "_ID_QI_PAD" in df.columns:
        return df
    if "ID_QI" in df.columns:
        df = df.copy()
        df["_ID_QI_PAD"] = df["ID_QI"].astype(str).str.split(".").str[0].str.zfill(6)
    elif "TICKER_SYMBOL" in df.columns:
        df = df.copy()
        df["_ID_QI_PAD"] = df["TICKER_SYMBOL"].astype(str).str.split(".").str[0].str.zfill(6)
    return df


def _minute_end_time(now_dt: datetime) -> str:
    """Return minute-boundary end_time to match historical minute slices."""
    return now_dt.replace(second=0, microsecond=0).strftime("%H%M%S")


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
    # NaN/inf/negative seconds → NaT instead of 1970-01-01. np.int64 min is the
    # canonical NaT marker for datetime64[ns].
    base_ns = _base_ns
    _nat_int = np.datetime64('NaT').view('i8')  # int64 sentinel for NaT
    for _tc_idx in (time_idx, updtime_idx):
        _col_secs = arr[:, _tc_idx]
        _valid = np.isfinite(_col_secs) & (_col_secs >= 0)
        _ns = np.full(len(_col_secs), _nat_int, dtype=np.int64)
        _ns[_valid] = base_ns + (_col_secs[_valid] * 1_000_000_000).astype(np.int64)
        df.iloc[:, _tc_idx] = _ns.astype('datetime64[ns]')

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
    _nat_int = np.datetime64('NaT').view('i8')
    for col in ("Time", "UpdateTime"):
        if col in df.columns:
            _secs = df[col].to_numpy()
            _valid = np.isfinite(_secs) & (_secs >= 0)
            _ns = np.full(len(_secs), _nat_int, dtype=np.int64)
            _ns[_valid] = base_ns + (_secs[_valid] * 1_000_000_000).astype(np.int64)
            df[col] = _ns.astype("datetime64[ns]")
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


def _compute_code_batch_shm(args):
    """Worker path: build a per-process code batch and call strategy once."""
    global _base_ns, _df_build_ms, _factor_ms, _profile_count
    codes, date_str, end_time, wall_secs, states_snapshot, tick_paths, deal_paths, order_paths, trading_day, factor_module, factor_info, is_daily = args
    results = []
    errors = 0
    build_t0 = time.perf_counter()
    data_map = {}
    try:
        _base_ns = pd.Timestamp(trading_day).value
        factor_fn, active_factor_info = _get_worker_strategy(factor_module, factor_info)
        from ..factor.stock_data_builder import build_stock_data

        need_tick = active_factor_info.get("need_l1_tick", True)
        need_deal = active_factor_info.get("need_l2_deal", True)
        need_order = active_factor_info.get("need_l2_order", False)

        for code in codes:
            try:
                tick_path = tick_paths.get(code, "")
                deal_path = deal_paths.get(code, "")
                order_path = order_paths.get(code, "")
                tick_df = (
                    _build_df_from_native(_cached_reader(tick_path), _tick_columns, _tick_buf_cols, _tick_time_idx, _tick_updtime_idx, trading_day, code)
                    if tick_path and need_tick
                    else pd.DataFrame()
                )
                deal_df = (
                    _build_df_from_native(_cached_reader(deal_path), _deal_columns, _deal_buf_cols, _deal_time_idx, _deal_updtime_idx, trading_day, code)
                    if deal_path and need_deal
                    else pd.DataFrame()
                )
                order_df = (
                    _build_df_from_native(_cached_reader(order_path), _order_columns, _order_buf_cols, _order_time_idx, _order_updtime_idx, trading_day, code)
                    if order_path and need_order
                    else pd.DataFrame()
                )
                data_map[code] = build_stock_data(
                    code=code, date=date_str, end_time=end_time,
                    tick_df=tick_df, deal_df=deal_df, order_df=order_df,
                    market_df=_market_df,
                    daily_basic_df=_market_df,
                    factor_info=active_factor_info,
                    state=states_snapshot.get(code),
                    validate=True,
                )
            except Exception as exc:
                logger.warning("[%s] build StockData failed: %s", code, exc)
                errors += 1

        build_ms = (time.perf_counter() - build_t0) * 1000
        factor_t0 = time.perf_counter()
        requested_codes = list(data_map.keys())

        from ..factor.engine import (
            _flatten_factor_result,
            _normalize_multi_code_batch_results,
        )

        # DAILY_USE_MULTI_CODE: when true, daily schedule uses the multi-code
        # batch protocol (factor_fn(data_map, codes, date, [end_time])) instead
        # of the legacy single-code loop. Required for strategies like V2 that
        # only expose the batch entry point.
        daily_multicode = _env_bool("DAILY_USE_MULTI_CODE", False)
        if is_daily and not daily_multicode:
            returned_codes = set()
            for code, stock_data in data_map.items():
                try:
                    raw_one = factor_fn(stock_data, code, date_str, end_time)
                except Exception as exc:
                    logger.warning("[%s] daily factor_calculation failed: %s", code, exc)
                    errors += 1
                    continue
                rows = _flatten_factor_result(raw_one)
                if not rows:
                    continue
                returned_codes.add(code)
                results.extend(rows)

            factor_ms = (time.perf_counter() - factor_t0) * 1000
            missing = set(data_map.keys()) - returned_codes
            if missing:
                logger.info("[daily] missing results for %d/%d codes", len(missing), len(data_map))
            _df_build_ms += build_ms
            _factor_ms += factor_ms
            _profile_count += 1
            data_map.clear()
            return results, errors, build_ms, factor_ms, None

        raw = factor_fn(data_map, requested_codes, date_str, [end_time]) if data_map else {}
        factor_ms = (time.perf_counter() - factor_t0) * 1000
        try:
            normalized = _normalize_multi_code_batch_results(
                requested_codes, [end_time], raw)
        except Exception as exc:
            normalized = None
            logger.warning("[batch] normalize result failed: %s", exc)

        if normalized is None:
            return results, len(codes), build_ms, factor_ms, (
                f"batch result protocol mismatch: got {type(raw).__name__}"
            )

        returned_codes = set()
        for code, et_map in normalized.items():
            rows = _flatten_factor_result(et_map.get(end_time))
            if not rows:
                continue
            returned_codes.add(code)
            state = states_snapshot.get(code)
            data_latency_ms = None
            if state and state.last_market_time:
                market_secs = _time_to_seconds(state.last_market_time)
                if market_secs > 0:
                    candidate = round((wall_secs - market_secs) * 1000, 1)
                    if abs(candidate) < 600_000:
                        data_latency_ms = candidate
            for res in rows:
                if data_latency_ms is not None:
                    res["data_latency_ms"] = data_latency_ms
                results.append(res)

        missing = set(data_map.keys()) - returned_codes
        if missing:
            logger.info("[batch] missing results for %d/%d codes", len(missing), len(data_map))

        _df_build_ms += build_ms
        _factor_ms += factor_ms
        _profile_count += len(codes)
        if _profile_count and _profile_count % 1000 < len(codes):
            logger.info("[profile] worker=%s stocks=%d build=%.1fms factor=%.1fms total=%.1fms",
                        multiprocessing.current_process().name, _profile_count,
                        _df_build_ms / max(_profile_count, 1),
                        _factor_ms / max(_profile_count, 1),
                        (_df_build_ms + _factor_ms) / max(_profile_count, 1))

        data_map.clear()
        return results, errors, build_ms, factor_ms, None
    except Exception as exc:
        data_map.clear()
        return results, len(codes), (time.perf_counter() - build_t0) * 1000, 0.0, str(exc)


# ── Native Engine ──────────────────────────────────────────────────

class NativeEngine:
    """Production engine that reads C++ mmap files for factor computation."""

    def __init__(self):
        self.shm_dir = os.environ.get("NATIVE_SHM_DIR", "/data/quant/shm")
        self.compute_interval = int(os.environ.get("COMPUTE_INTERVAL", "60"))
        self.factor_module = os.environ.get("FACTOR_MODULE", "")
        self.daily_factor_module = os.environ.get("DAILY_FACTOR_MODULE", "")
        self._daily_factor_fn: Optional[Callable] = None
        self.factor_output_path = os.environ.get("FACTOR_OUTPUT_PATH", "/data/factors")
        self.output_path = Path(self.factor_output_path) if self.factor_output_path else None
        self.n_workers = int(os.environ.get("FACTOR_WORKERS", "40"))
        self.trading_day: str = ""
        self._pool: Optional[multiprocessing.Pool] = None
        self._daily_cache: Optional[DailyBasicCache] = None
        self._idx_cons_cache: Optional[IdxConsCache] = None
        self._idx_cons_df: pd.DataFrame = pd.DataFrame()
        self._trading_universe_df: Optional[pd.DataFrame] = None
        self.trading_universe_fn: Optional[Callable] = None
        self._states: Dict[str, StockState] = {}
        self.factor_calculation: Optional[Callable] = None
        self.factor_info: Dict[str, Any] = {}
        self._daily_factor_info: Dict[str, Any] = {}
        self.outfun: Optional[Callable] = None
        self.inference_fn: Optional[Callable] = None
        self.portfolio_context_fn: Optional[Callable] = None
        self._order_queue: Optional[multiprocessing.Queue] = None
        self._order_process: Optional[multiprocessing.Process] = None
        self._prev_day_factors: Optional[pd.DataFrame] = None
        self._prev_valid_rate: Optional[float] = None
        # open_position 预计算缓存：8:50 启动时异步跑树模型推理，
        # 把 target positions 缓存到内存 + 磁盘。9:30 schedule 触发时
        # 优先消费这份缓存，跳过 _tree.inference（重），只跑
        # positions_to_orders（轻，需要实时 tick 价）。
        self._open_position_cache: Optional[pd.DataFrame] = None
        self._open_position_cache_lock = threading.Lock()
        # Generation counter：fallback 路径失效 cache 时 +1。预计算线程在写入
        # 内存/磁盘前必须重新检查 generation 是否变化 —— 变化说明它中途被
        # 失效了（fallback 已经走了 inline 推理），不能落盘否则下次重触发
        # 会读到陈旧 target 重复下单。
        self._open_position_cache_generation: int = 0
        self._schedules: list = []  # List[ComputationSchedule]
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
        self._daily_position_lock = threading.Lock()
        self._compute_running = False
        self._state_offsets: Dict[Tuple[str, int], int] = {}
        self._main_readers: Dict[str, NativeShmReader] = {}
        # _cached_shm_files was removed — SHM files are created lazily by the
        # collector, so every cycle must re-scan to pick up new stocks.
        self._sync_warn_ts: Dict[Tuple[str, int], float] = {}  # rate-limited warning log

    def _init_pool(self) -> None:
        """Create persistent worker pool."""
        if self._pool is not None:
            return
        logger.info("[native] creating persistent pool with %d workers", self.n_workers)
        self._pool = multiprocessing.Pool(
            processes=self.n_workers,
            maxtasksperchild=None,
            initializer=_worker_init,
            initargs=(self.factor_module, self._daily_basic_df, self._market_df, self.factor_info),
        )

    def _init_order_worker(self) -> None:
        """Start a persistent order gateway push worker when configured."""
        if self._order_process is not None or not _order_push_configured():
            return
        maxsize = int(os.environ.get("ORDER_QUEUE_MAXSIZE", "32"))
        self._order_queue = multiprocessing.Queue(maxsize=maxsize)
        self._order_process = multiprocessing.Process(
            target=_order_worker_loop,
            args=(self._order_queue,),
            name="order-gateway-worker",
        )
        self._order_process.start()
        logger.info("[order-gateway] started persistent worker pid=%d", self._order_process.pid)

    def _stop_order_worker(self) -> None:
        """Stop the persistent order gateway push worker."""
        if self._order_queue is not None:
            try:
                self._order_queue.put_nowait(None)
            except Exception:
                pass
        if self._order_process is not None:
            self._order_process.join(timeout=10)
            if self._order_process.is_alive():
                logger.warning("[order-gateway] worker did not stop in time; terminating")
                self._order_process.terminate()
                self._order_process.join(timeout=5)
        self._order_queue = None
        self._order_process = None

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
        if "DAILY_BASIC_MARKET_COUNT" in os.environ:
            market_count = int(os.environ["DAILY_BASIC_MARKET_COUNT"])
        else:
            market_count = 1
            if self.factor_module:
                try:
                    mod = importlib.import_module(self.factor_module)
                    factor_info = getattr(mod, "FACTOR_INFO", getattr(mod, "factor_info", {}))
                    market_count = int(factor_info.get("market_count", 1))
                except Exception as exc:
                    logger.warning("[native] failed to resolve factor market_count, fallback to 1: %s", exc)

        self._daily_cache = DailyBasicCache(market_count=market_count)
        # 初始化 OSS loader 并传给 cache (优先 OSS,fallback MySQL)
        oss_loader = None
        if os.environ.get("OSS_ACCESS_KEY_ID") and os.environ.get("OSS_ACCESS_KEY_SECRET"):
            try:
                oss_loader = OSSDataLoader()
                self._daily_cache._oss_loader = oss_loader
            except Exception as exc:
                logger.warning(
                    "[native] OSSDataLoader init failed in _load_daily_basic (MySQL fallback): %s",
                    exc,
                )
        # 走 OSS-first 路径
        self._daily_cache.load(self.trading_day)
        market_df = self._daily_cache.get_daily_basic()
        market_df = _add_code_key(market_df)
        self._market_df = market_df

        if not market_df.empty and "_date" in market_df.columns:
            date_values = market_df["_date"].astype(str).str.replace("-", "", regex=False)
            today_mask = date_values == self.trading_day
            self._daily_basic_df = market_df[today_mask].reset_index(drop=True)
            if self._daily_basic_df.empty:
                latest_date = date_values.max()
                logger.warning(
                    "[native] daily_basic has no rows for trading_day=%s, using latest date=%s as fallback",
                    self.trading_day, latest_date,
                )
                self._daily_basic_df = market_df[date_values == latest_date].reset_index(drop=True)
        else:
            self._daily_basic_df = market_df

        logger.info(
            "[native] daily_basic: %d stocks, market: %d entries, market_count=%d",
            len(self._daily_basic_df), len(self._market_df), market_count,
        )

    def _one_time_close_on_startup(self) -> None:
        """Flatten all broker holdings once per trading day.

        Triggered by env ONE_TIME_CLOSE_ON_STARTUP=1. Idempotent via
        date-stamped flag file. Spawns a daemon thread that sleeps until
        ONE_TIME_CLOSE_TRIGGER_TIME (default 09:15) then pulls broker
        positions and pushes sell orders — pod may boot before broker ATX
        is open, so the actual push is deferred to trading hours.
        Used for sim reset only; production should leave this off.
        """
        if os.environ.get("ONE_TIME_CLOSE_ON_STARTUP", "0") != "1":
            return
        if self.portfolio_context_fn is None:
            logger.warning("[one-time-close] portfolio_context_fn not loaded, skip")
            return
        trigger = os.environ.get("ONE_TIME_CLOSE_TRIGGER_TIME", "09:15")
        try:
            hh, mm = (int(x) for x in trigger.split(":"))
        except Exception:
            logger.warning("[one-time-close] bad ONE_TIME_CLOSE_TRIGGER_TIME=%r, using 09:15", trigger)
            hh, mm = 9, 15
        flag = f"/data/factors/{self.trading_day}_close_done.flag"
        if os.path.exists(flag):
            logger.info("[one-time-close] already done today (flag=%s), skip", flag)
            return

        def _run() -> None:
            # Sleep until trigger time today
            now = datetime.now()
            target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            wait_secs = (target - now).total_seconds()
            if wait_secs > 0:
                logger.info("[one-time-close] scheduled at %02d:%02d, waiting %.0fs",
                            hh, mm, wait_secs)
                time.sleep(wait_secs)
            elif wait_secs < -600:
                # Already past trigger by >10 min — likely late pod boot.
                # Still attempt; broker may still be open.
                logger.info("[one-time-close] trigger %02d:%02d already passed by %.0fs, running now",
                            hh, mm, -wait_secs)
            try:
                ctx = self.portfolio_context_fn(self.trading_day, f"{hh:02d}{mm:02d}00")
            except Exception as exc:
                logger.error("[one-time-close] portfolio_context failed: %s", exc)
                return
            positions = getattr(ctx, "positions", None)
            if positions is None or positions.empty:
                logger.info("[one-time-close] no holdings, nothing to flatten")
                _write_close_flag(flag)
                return
            if "available_volume" not in positions.columns:
                logger.error("[one-time-close] positions missing available_volume: %s",
                             list(positions.columns))
                return
            sellable = positions[positions["available_volume"].astype(int) > 0]
            if sellable.empty:
                logger.info("[one-time-close] %d holdings but 0 available (T+1 lock)",
                            len(positions))
                _write_close_flag(flag)
                return
            orders = pd.DataFrame({
                "code": sellable["code"].astype(str).values,
                "side": "sell",
                "volume": sellable["available_volume"].astype(int).values,
                "price_type": "latest",
                "strategy": "one_time_close",
                "note": f"one_time_close_{self.trading_day}_{hh:02d}{mm:02d}00",
            })
            csv_path = f"/data/factors/{self.trading_day}_{hh:02d}{mm:02d}00_close_orders.csv"
            orders.to_csv(csv_path, index=False)
            logger.info("[one-time-close] sellable=%d/%d, total_shares=%d, csv=%s",
                        len(orders), len(positions), int(orders["volume"].sum()), csv_path)
            try:
                _push_to_order_gateway(orders, self.trading_day, f"{hh:02d}{mm:02d}00")
                _write_close_flag(flag)
                logger.info("[one-time-close] flatten pushed, flag written")
            except Exception as exc:
                logger.error("[one-time-close] push failed: %s", exc)

        t = threading.Thread(target=_run, name="one-time-close", daemon=True)
        t.start()
        logger.info("[one-time-close] daemon thread started, trigger=%02d:%02d", hh, mm)

    def _load_prev_day_factors(self) -> None:
        """Load previous trading day's factor output for inference.
        Priority: OSS daily result > local CSV fallback.
        """
        # Try OSS first
        oss_df = self._load_prev_day_from_oss()
        if oss_df is not None:
            self._prev_day_factors = oss_df
            return

        # Fallback: local CSV
        if self.output_path is None:
            return
        prev_dir = self.output_path
        if not prev_dir.exists():
            return
        prev_csvs = sorted(
            [f for f in prev_dir.glob("*.csv")
             if not f.name.endswith("_positions.csv")],
            key=lambda f: f.name,
            reverse=True,
        )
        today_prefix = self.trading_day + "_"
        prev_csvs = [f for f in prev_csvs if not f.name.startswith(today_prefix)]
        # Prefer daily CSV
        daily_csvs = [f for f in prev_csvs if f.name.endswith("_daily.csv")]
        target = daily_csvs[0] if daily_csvs else (prev_csvs[0] if prev_csvs else None)
        if target:
            try:
                self._prev_day_factors = pd.read_csv(target)
                logger.info("[native] loaded prev day factors from local: %s (%d rows)",
                            target.name, len(self._prev_day_factors))
            except Exception as exc:
                logger.warning("[native] failed to load prev day factors: %s", exc)

    def _load_prev_day_from_oss(self) -> Optional[pd.DataFrame]:
        """Load the latest available daily factor result from OSS live-factors.

        Strategy: list ``{prefix}/{YYYY}/{YYYYMM}/`` (current month first, up
        to 4 months back), enumerate date folders, and pick the newest date
        strictly before today that has ``daily-feature/daily.parquet``.

        Robust to weekends/holidays and OOM gaps: takes the most recent
        successful daily run instead of guessing T-1..T-5 calendar dates.
        Falls back to legacy ``daily.json`` if no parquet exists.
        """
        try:
            import io as _io
            import oss2
            import re as _re
            endpoint = os.environ.get("OSS_ENDPOINT", "")
            ak_id = os.environ.get("OSS_ACCESS_KEY_ID", "")
            ak_secret = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
            if not all([endpoint, ak_id, ak_secret]):
                return None
            prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")
            bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
            auth = oss2.Auth(ak_id, ak_secret)
            ep = endpoint.replace("https://", "").replace("http://", "")
            bucket = oss2.Bucket(auth, ep, bucket_name)

            today = datetime.strptime(self.trading_day, "%Y%m%d")
            # Walk year-month prefixes from current month backwards (up to 4 months)
            ym_candidates: list[str] = []
            cur = today.replace(day=1)
            for _ in range(4):
                ym_candidates.append(cur.strftime("%Y%m"))
                if cur.month == 1:
                    cur = cur.replace(year=cur.year - 1, month=12)
                else:
                    cur = cur.replace(month=cur.month - 1)

            date_re = _re.compile(r"^(\d{8})/?$")
            tried_dates: list[str] = []
            for ym in ym_candidates:
                year = ym[:4]
                list_prefix = f"{prefix}/{year}/{ym}/"
                try:
                    it = oss2.ObjectIterator(bucket, prefix=list_prefix, delimiter="/")
                    dates_found: list[str] = []
                    for obj in it:
                        name = obj.key[len(list_prefix):]
                        m = date_re.match(name)
                        if m:
                            dates_found.append(m.group(1))
                except Exception as exc:
                    logger.warning("[native] OSS list %s failed: %s", list_prefix, exc)
                    continue
                # Only consider dates strictly before today (don't reuse today's run)
                dates_found = [d for d in dates_found if d < self.trading_day]
                dates_found.sort(reverse=True)
                for date_str in dates_found:
                    tried_dates.append(date_str)
                    base = f"{prefix}/{date_str[:4]}/{date_str[:6]}/{date_str}"
                    candidates = [
                        f"{base}/daily-feature/daily.parquet",
                        f"{base}/daily.parquet",
                        f"{base}/daily.json",
                    ]
                    for key in candidates:
                        try:
                            payload = bucket.get_object(key)
                            buf = payload.read()
                            if key.endswith(".parquet"):
                                df = pd.read_parquet(_io.BytesIO(buf))
                            else:
                                import json as _json
                                df = pd.DataFrame(_json.loads(buf))
                            if df is not None and not df.empty:
                                logger.info(
                                    "[native] loaded latest daily factors from OSS: "
                                    "%s (date=%s, %d rows, tried=%s)",
                                    key, date_str, len(df), tried_dates[:5])
                                return df
                        except oss2.exceptions.NoSuchKey:
                            continue
                        except Exception:
                            continue
            logger.info(
                "[native] no daily factor result found on OSS "
                "(searched months=%s, dates_seen=%s)", ym_candidates, tried_dates[:10])
        except Exception as exc:
            logger.warning("[native] OSS prev day load failed: %s", exc)
        return None
        return None

    def _load_idx_cons(self) -> None:
        """Load index constituent data at startup.

        优先使用 IDX_CONS_CODES (TICKER 代码如 "000300,000905"),
        向后兼容 IDX_CONS_IDS (SECURITY_ID 数字)。
        数据源优先级:OSS composition.parquet > MySQL 直查(含日期)。
        """
        codes_str = os.environ.get("IDX_CONS_CODES", "")
        if not codes_str:
            # 向后兼容:把 IDX_CONS_IDS 映射回 TICKER
            legacy_ids = os.environ.get("IDX_CONS_IDS", "")
            codes_str = self._legacy_ids_to_codes(legacy_ids)

        codes = [s.strip() for s in codes_str.split(",") if s.strip()]
        if not codes:
            logger.warning(
                "[native] IDX_CONS_CODES / IDX_CONS_IDS 均未配置,跳过 idx_cons 加载"
            )
            return

        # 懒加载 OSSDataLoader (读取 composition.parquet 优先,失败 fallback MySQL)
        oss_loader = None
        if os.environ.get("OSS_ACCESS_KEY_ID") and os.environ.get("OSS_ACCESS_KEY_SECRET"):
            try:
                oss_loader = OSSDataLoader()
            except Exception as exc:
                logger.warning("[native] OSSDataLoader init failed (MySQL fallback): %s", exc)

        self._idx_cons_cache = IdxConsCache(
            index_codes=codes,
            oss_loader=oss_loader,
        )
        # 启动时预加载当天数据(填充缓存,避免首轮推理时延迟)
        self._idx_cons_df = self._idx_cons_cache.get_by_date(self.trading_day)
        logger.info(
            "[native] idx_cons: %d rows loaded for trading_day=%s (codes=%s)",
            len(self._idx_cons_df), self.trading_day, codes,
        )

    @staticmethod
    def _legacy_ids_to_codes(ids: str) -> str:
        """IDX_CONS_IDS(SECURITY_ID) → IDX_CONS_CODES(TICKER) 映射,向后兼容。

        1782=沪深300, 2103=中证500, 33736=中证1000,
        3800=中证全指, 1200245=中证2000
        """
        if not ids:
            return ""
        mapping = {
            "1782": "000300",
            "2103": "000905",
            "33736": "000852",
            "3800": "000985",
            "1200245": "932000",
        }
        out = []
        for s in ids.split(","):
            s = s.strip()
            if s and s in mapping:
                out.append(mapping[s])
        return ",".join(out)

    def _scan_shm_files(self) -> Dict[str, Dict[int, str]]:
        """Scan SHM directory, return {code: {kind: path}}.
        Not cached — collector creates files lazily throughout the
        trading day (only ~1800 at startup, up to ~5200+ at peak),
        so every cycle must re-scan to pick up newly-created files.
        This is fast: Path.iterdir on 15k entries takes 1-2ms."""
        files = scan_shm_dir(self.shm_dir)
        by_code: Dict[str, Dict[int, str]] = {}
        for (code, kind), path in files.items():
            if code not in by_code:
                by_code[code] = {}
            by_code[code][kind] = path
        return by_code

    @staticmethod
    def _raw_time_from_seconds(seconds: float) -> str:
        # np.isfinite rejects NaN and inf; combined with `> 0` this covers NaN,
        # -inf, +inf and non-positive values uniformly. `seconds <= 0` alone
        # misses NaN (nan <= 0 is False), which would raise ValueError at
        # int(nan * 1000) and abort the whole snapshot.
        if not np.isfinite(seconds) or seconds <= 0:
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
        # Precise wall time at snap start (with ms precision)
        from datetime import datetime as _snap_dt
        _snap_now = _snap_dt.now()
        _snap_wall_secs = (_snap_now.hour * 3600 + _snap_now.minute * 60
                          + _snap_now.second + _snap_now.microsecond / 1_000_000)
        _push_min_delay_ms = float('inf')
        _push_min_code = ""
        _push_min_kind = ""
        _deal_min_delay_ms = float('inf')
        _deal_min_code = ""
        _deal_min_exchange_secs = 0.0

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

                try:
                    arr = reader.view_rows()[start:current]
                except Exception:
                    continue
                n = arr.shape[0]
                if n == 0:
                    self._state_offsets[offset_key] = current
                    continue

                try:
                    if kind == KIND_TICK:
                        self._update_tick_vectorized(state, arr)
                    elif kind == KIND_DEAL:
                        self._update_deal_vectorized(state, arr)
                    elif kind == KIND_ORDER:
                        self._update_order_vectorized(state, arr)

                    # Track push delay: wall time vs exchange timestamp
                    _row_time = float(arr[-1, 1])  # UpdateTime (seconds since midnight)
                    _delay_ms = (_snap_wall_secs - _row_time) * 1000
                    if 0 < _delay_ms < 600_000:
                        if _delay_ms < _push_min_delay_ms:
                            _push_min_delay_ms = _delay_ms
                            _push_min_code = code
                            _push_min_kind = _kind_names.get(kind, str(kind))
                        if kind == KIND_DEAL and _delay_ms < _deal_min_delay_ms:
                            _deal_min_delay_ms = _delay_ms
                            _deal_min_code = code
                            _deal_min_exchange_secs = _row_time
                except Exception as exc:
                    # State update for this kind failed (e.g. malformed rows).
                    # Do NOT mark dirty: the state wasn't updated and the offset
                    # is held for retry, so running factor computation now would
                    # reuse stale state and risk duplicate/expired signals. The
                    # code is simply skipped this round; once the data is healthy
                    # again it re-enters the compute path naturally.
                    # Offset is NOT committed, so these rows are retried next
                    # round. Rate-limit the warning to avoid log flooding.
                    _now_warn = time.monotonic()
                    if _now_warn - self._sync_warn_ts.get(offset_key, 0.0) > 60.0:
                        self._sync_warn_ts[offset_key] = _now_warn
                        logger.warning(
                            "[sync] %s kind=%s update failed (%s); offset held at %d, "
                            "will retry %d rows next round (code skipped this round)",
                            code, _kind_names.get(kind, str(kind)), exc, start, n)
                    continue

                # Commit offset only after state update succeeds
                self._state_offsets[offset_key] = current
                # Mark dirty and stamp update time only after a successful update.
                dirty_codes.add(code)
                state.last_update_ts = wall_secs

        # Log push delay (min = newest data, best indicator of real-time latency)
        if _push_min_delay_ms < float('inf'):
            logger.info(
                "[push-latency] newest=%s/%s delay=%.0fms snap_s=%.3f",
                _push_min_code, _push_min_kind, _push_min_delay_ms, _snap_wall_secs,
            )
        if _deal_min_delay_ms < float('inf'):
            logger.info(
                "[deal-push] %s exchange_deal_s=%.3f snap_s=%.3f delay=%.0fms",
                _deal_min_code, _deal_min_exchange_secs, _snap_wall_secs, _deal_min_delay_ms,
            )

        return dirty_codes, {code: copy.copy(self._states[code]) for code in dirty_codes}

    def _snapshot_latest_prices(self) -> dict:
        """Snapshot {code: latest_price} from in-memory states (realtime tick).

        Used to augment portfolio_context.meta['latest_prices'] so open_position
        inference at 9:30 can size orders off live tick prices instead of
        yesterday close. Returns {} if states are empty.

        Takes a shallow copy of self._states first to avoid
        "dictionary changed size during iteration" if the minute schedule
        (running in parallel under _compute_lock) inserts new codes.
        """
        out: dict = {}
        # GIL makes dict() copy atomic-ish; iteration then runs on private copy
        for code, st in dict(self._states).items():
            try:
                p = getattr(st, "latest_price", 0.0)
            except Exception:
                continue
            if p and float(p) > 0:
                out[code] = float(p)
        return out

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
        _nat_int = np.datetime64('NaT').view('i8')
        for col in ("Time", "UpdateTime"):
            if col in df.columns:
                _secs = df[col].to_numpy()
                _valid = np.isfinite(_secs) & (_secs >= 0)
                _ns = np.full(len(_secs), _nat_int, dtype=np.int64)
                _ns[_valid] = base_ns + (_secs[_valid] * 1_000_000_000).astype(np.int64)
                df[col] = _ns.astype("datetime64[ns]")
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
            # _ID_QI_PAD is the zero-padded 6-digit code added by _add_code_key
            # (e.g. "000001"). Using the raw ID_QI column risks losing leading
            # zeros via `::VARCHAR` when ID_QI is integer-typed, which silently
            # drops all XSHE (Shenzhen) codes from the archived parquet.
            if "_ID_QI_PAD" in self._daily_basic_df.columns:
                map_code_col = "_ID_QI_PAD"
                map_df = self._daily_basic_df[["_ID_QI_PAD", "SECURITY_ID"]].drop_duplicates()
            else:
                map_code_col = "ID_QI"
                map_df = self._daily_basic_df[["ID_QI", "SECURITY_ID"]].drop_duplicates()
            map_df.to_parquet(map_tmp_path, index=False)
        except Exception as exc:
            logger.error("[native-archive] code map write failed: %s", exc)
            return False

        # Upload runs after all compute cycles are done (post-close,
        # active_hours ended). Free ~80 GB of SHM mmap data so DuckDB
        # has enough memory for the JOIN + ORDER BY merge.
        import gc
        logger.info("[native-archive] freeing pre-upload memory ...")
        # 1) Close main-process SHM readers
        for _path, _reader in list(self._main_readers.items()):
            try:
                _reader.close()
            except Exception:
                pass
        self._main_readers.clear()
        self._state_offsets.clear()
        self._states.clear()
        # 3) Release cached DataFrames
        self._daily_basic_df = None
        self._market_df = None
        self._trading_universe_df = None
        # 4) Collect; try a second pass for any cycle references
        gc.collect()
        gc.collect()
        logger.info("[native-archive] pre-upload memory freed, starting upload")

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
                con.execute(f"SET memory_limit='{os.environ.get('ARCHIVE_DUCKDB_MEMORY', '32GB')}'")
                con.execute(
                    "CREATE MACRO epoch_us(ts) AS "
                    "(EXTRACT('epoch' FROM ts)::BIGINT * 1000000 + EXTRACT('microseconds' FROM ts)::BIGINT)"
                )
                select_clause = self._archive_select_clause(kind)
                # map_code_col determined before _daily_basic_df was freed below;
                # map_df carries either "_ID_QI_PAD" (zero-padded str) or "ID_QI".
                con.execute(f"""
                    COPY (
                        SELECT
                            {select_clause}
                        FROM read_parquet('{chunk_dir}/*.parquet') x
                        JOIN read_parquet('{map_tmp_path}') m
                          ON regexp_extract(x.Code, '^\\d+') = m.{map_code_col}::VARCHAR
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
                "epoch_us(x.UpdateTime) AS UpdateTime",
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
                "epoch_us(x.UpdateTime) AS UpdateTime",
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
                "epoch_us(x.UpdateTime) AS UpdateTime",
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
        from datetime import timedelta
        logger.info("[native-archive] background archive loop started (smart-sleep: 11:35/15:05)")
        last_archive_date = ""
        archived_lunch = False
        archived_close = False
        while not self._stopped:
            try:
                today = date.today().isoformat()
                if today != last_archive_date:
                    last_archive_date = today
                    now_init = datetime.now()
                    archived_lunch = now_init.hour > 11 or (now_init.hour == 11 and now_init.minute >= 35)
                    archived_close = False

                now = datetime.now()
                h, m = now.hour, now.minute

                if h == 11 and m >= 35 and not archived_lunch:
                    files_by_code = self._scan_shm_files()
                    if files_by_code:
                        logger.info("[native-archive] lunch break snapshot starting...")
                        self._archive_native_incremental(files_by_code)
                        archived_lunch = True
                        logger.info("[native-archive] lunch break snapshot done")

                if h >= 15 and m >= 5 and not archived_close:
                    files_by_code = self._scan_shm_files()
                    if files_by_code:
                        logger.info("[native-archive] post-close snapshot starting...")
                        self._archive_native_incremental(files_by_code)
                        self._check_raw_upload_time(files_by_code)
                        archived_close = True
                        logger.info("[native-archive] post-close snapshot done")

                if not archived_lunch:
                    target_h, target_m = 11, 35
                elif not archived_close:
                    target_h, target_m = 15, 5
                else:
                    target_h, target_m = 0, 0

                target = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
                if target <= now:
                    target = target + timedelta(days=1)
                sleep_sec = max(1, (target - now).total_seconds())

                step = 60.0
                slept = 0.0
                while slept < sleep_sec and not self._stopped:
                    actual = min(step, sleep_sec - slept)
                    time.sleep(actual)
                    slept += actual
            except Exception as exc:
                logger.error("[native-archive] loop error: %s", exc, exc_info=True)
                time.sleep(60)

    def _mark_targets_consumed_locked(self) -> None:
        """Caller holds _open_position_cache_lock or accepts the race.
        Rename the disk cache to .consumed.csv (or unlink on rename failure)
        so subsequent pop() cannot replay the same targets.
        """
        if self.output_path is None or not self.trading_day:
            return
        p = self.output_path / f"{self.trading_day}_open_position_targets.csv"
        if not p.exists():
            return
        consumed = p.with_suffix(".consumed.csv")
        try:
            p.replace(consumed)
        except Exception as ren_exc:
            logger.warning("[open_position] rename-to-consumed failed %s: %s", p, ren_exc)
            try:
                p.unlink()
            except Exception:
                pass

    def _pop_open_position_targets(self) -> Optional[pd.DataFrame]:
        """One-shot consume of the precomputed open_position targets.

        Returns the cached DataFrame and clears BOTH the in-memory slot and
        the on-disk cache file (rename to .consumed.csv). If memory is empty
        (e.g. pod restart mid-day), tries the on-disk cache; on successful
        read the disk file is also atomically renamed so a repeat pop() or
        a second schedule fire cannot replay the same targets and
        double-dispatch orders.

        Returns None when both caches are empty — caller should fall back to
        inline inference.
        """
        with self._open_position_cache_lock:
            if self._open_position_cache is not None:
                cached = self._open_position_cache
                self._open_position_cache = None
                # 内存命中也要清盘，否则 pod 重启/重触发会再读一次
                self._mark_targets_consumed_locked()
                return cached
        # 内存没命中：尝试磁盘（pod 长跑后内存被清 / pod 重启场景）
        if self.output_path is not None and self.trading_day:
            p = self.output_path / f"{self.trading_day}_open_position_targets.csv"
            if p.exists():
                try:
                    df = pd.read_csv(p)
                    # 原子标记已消费：rename 防止重复消费导致双单
                    self._mark_targets_consumed_locked()
                    logger.info("[open_position] loaded targets from disk cache: %s (%d rows)",
                                p, len(df))
                    return df
                except Exception as exc:
                    logger.warning("[open_position] disk cache read failed %s: %s", p, exc)
        return None

    def _precompute_open_position_targets(self) -> None:
        """启动时异步跑树模型推理，把 target positions 缓存到内存 + 文件。

        9:30 schedule 会优先消费这份缓存，跳过 _tree.inference（重），只跑
        positions_to_orders（轻，需要实时 tick 价）。

        设计要点：
        - 树模型推理的输入只有 T-1 daily_basic / T-1 factors / 静态指数成分 /
          broker T-1 持仓，**完全不依赖实时 tick 价格**，所以 8:50 跑出的
          target weights 与 9:30 现场跑的等价。
        - portfolio_context 在 8:50 拿到的是 T-1 收市持仓，与 9:30 现场再查
          broker 的差异极小（隔夜无变化），可接受。
        - 任何失败都打告警日志后 return；9:30 会自动 fallback 到 inline
          inference，行为同改造前。
        """
        t0 = time.time()
        # 捕获启动时的 generation。fallback 路径会 bump 这个值；写入前必须
        # 重新检查，若变化则放弃写入 —— 防止晚到的预计算结果在被失效后又把
        # 陈旧 target 写回，下次重触发时被读到重复下单。
        with self._open_position_cache_lock:
            my_generation = self._open_position_cache_generation

        def _invalidated() -> bool:
            with self._open_position_cache_lock:
                return self._open_position_cache_generation != my_generation

        try:
            date_str = self.trading_day
            if not date_str:
                logger.warning("[precompute] trading_day empty, skip")
                return

            # 仅当 inference 模块暴露了拆分接口时才走快路径
            inference_module = os.environ.get(
                "INFERENCE_MODULE", "quant_platform.inference.tree_model_orders")
            try:
                import importlib
                tm = importlib.import_module(inference_module)
                targets_fn = getattr(tm, "inference_targets", None)
            except Exception as exc:
                logger.warning("[precompute] cannot import %s: %s", inference_module, exc)
                targets_fn = None
            if targets_fn is None:
                logger.warning(
                    "[precompute] %s has no inference_targets; precompute disabled",
                    inference_module)
                return

            prev = self._prev_day_factors
            if prev is None or prev.empty:
                logger.warning("[precompute] prev_day_factors empty, skip")
                return

            daily_basic_df = self._daily_basic_df
            tu_df = (self._trading_universe_df
                     if self._trading_universe_df is not None else None)

            # portfolio_context：8:50 拿到 T-1 收市持仓。失败不阻断 —— 模型
            # 内部会按"空持仓"处理，9:30 现场再调 portfolio_context_fn 拿最新
            # 持仓喂给 positions_to_orders 做delta。
            portfolio_context = None
            if self.portfolio_context_fn is not None:
                try:
                    portfolio_context = self.portfolio_context_fn(date_str, "093000")
                except Exception as exc:
                    logger.error("[precompute] portfolio_context failed: %s", exc)

            # 计算 idx_comp（与 _write_results 行 1724-1727 等价）
            idx_comp_df = None
            try:
                universe_extra = {
                    "date": date_str,
                    "end_time": "093000",
                    "codes": _result_codes(prev) if hasattr(prev, "columns") else [],
                    "factor_result": prev,
                    "idx_cons_df": self._idx_cons_df,
                }
                idx_comp_df = compute_index_composition(
                    daily_basic_df, universe_extra,
                    trading_day=date_str,
                    idx_cons_cache=self._idx_cons_cache,
                )
            except Exception as exc:
                logger.warning("[precompute] idx_comp failed (continue without): %s", exc)

            logger.info("[precompute] starting tree_model inference for date=%s (gen=%d)",
                        date_str, my_generation)
            positions = targets_fn(
                date_str=date_str,
                end_time="093000",
                prev_day_factors_df=prev,
                intraday_factors_df=prev,  # open_position 用 prev_day 当 intraday
                daily_basic_df=daily_basic_df,
                trading_universe_df=tu_df,
                index_composition_df=idx_comp_df,
                portfolio_context=portfolio_context,
            )

            if positions is None or positions.empty:
                logger.error(
                    "[precompute] targets empty, will fallback at 9:30 (date=%s)", date_str)
                return

            # 重型推理可能耗时数十秒。完成后再检查 generation —— 期间若 fallback
            # 已走（invalidate 被 bump），必须放弃写入，否则下次重触发会读到陈旧
            # target 重复下单。
            if _invalidated():
                logger.warning(
                    "[precompute] cache invalidated during inference (gen moved from %d), "
                    "dropping %d rows to avoid stale-cache replay",
                    my_generation, len(positions))
                return

            # 写盘是慢 I/O，不能放在 cache_lock 里（会阻塞 pop/invalidate）。
            # 先写 tmp 文件，然后拿锁一次性做：再次确认 generation → replace
            # 成正式 cache 名 → 写内存。任何中途失效都把 tmp 删掉，确保磁盘上
            # 不会残留 fallback 之后才落地的陈旧 target。
            cache_path: Optional[Path] = None
            tmp_path: Optional[Path] = None
            if self.output_path is not None:
                cache_path = self.output_path / f"{date_str}_open_position_targets.csv"
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                # 用 pid+threadid+ts 命名避免多个 precompute 实例撞名
                tmp_path = cache_path.with_name(
                    f"{cache_path.name}.tmp.{os.getpid()}.{threading.get_ident()}.{int(time.time()*1000)}"
                )
            try:
                if tmp_path is not None:
                    positions.to_csv(tmp_path, index=False)
            except Exception as exc:
                logger.warning("[precompute] tmp cache write failed: %s", exc)
                tmp_path = None  # 写盘失败仍允许写内存（pop 时由 mark-consumed 兜底）

            # 关键段：再次检查 generation → 原子 replace → 写内存。
            with self._open_position_cache_lock:
                if self._open_position_cache_generation != my_generation:
                    logger.warning(
                        "[precompute] generation changed before publish (gen=%d → %d), "
                        "dropping disk and memory write",
                        my_generation, self._open_position_cache_generation)
                    if tmp_path is not None and tmp_path.exists():
                        try:
                            tmp_path.unlink()
                        except Exception:
                            pass
                    return
                # 原子 rename tmp → 正式 cache。rename 后才对消费端可见。
                if tmp_path is not None and tmp_path.exists():
                    try:
                        tmp_path.replace(cache_path)
                        logger.info("[precompute] published disk cache: %s", cache_path)
                    except Exception as exc:
                        logger.warning("[precompute] rename tmp→cache failed: %s", exc)
                        try:
                            tmp_path.unlink()
                        except Exception:
                            pass
                self._open_position_cache = positions

            logger.info(
                "[precompute] open_position targets ready: %d rows in %.1fs",
                len(positions), time.time() - t0,
            )
        except Exception as exc:
            logger.error("[precompute] failed: %s", exc, exc_info=True)

    def _invalidate_open_position_targets(self) -> None:
        """Drop in-memory + on-disk precompute cache and bump generation.

        Called from the fallback path once we decide to do inline inference.
        Bumps a generation counter so any in-flight precompute thread, which
        captured the generation at start, can detect invalidation and refuse
        to write its result back — otherwise a late-finishing precompute
        could resurrect a stale target that the next schedule fire would
        consume, causing duplicate orders.

        Safe to call multiple times.
        """
        with self._open_position_cache_lock:
            self._open_position_cache = None
            self._open_position_cache_generation += 1
            gen = self._open_position_cache_generation
        if self.output_path is not None and self.trading_day:
            p = self.output_path / f"{self.trading_day}_open_position_targets.csv"
            if p.exists():
                try:
                    p.unlink()
                    logger.info("[open_position] removed stale precompute cache (gen=%d): %s",
                                gen, p)
                except Exception as exc:
                    logger.warning("[open_position] cache unlink failed %s: %s", p, exc)

    def _compute_and_output(self, schedule: Optional[ComputationSchedule] = None) -> None:
        lock = (self._daily_position_lock
                if (schedule is not None and schedule.name in ("daily_position", "open_position"))
                else self._compute_lock)
        try:
            with lock:
                self._compute_and_output_locked(schedule)
        except Exception as exc:
            logger.error("[combined] background compute failed: %s", exc, exc_info=True)
        finally:
            # Only the minute schedule sets _compute_running; reset it only for
            # minute (or unscheduled) runs so a concurrent daily_position run
            # cannot clear minute's guard while minute is still in flight.
            if schedule is None or schedule.name == "minute":
                self._compute_running = False

    def _compute_and_output_locked(self, schedule: Optional[ComputationSchedule] = None) -> None:
        is_daily = schedule is not None and schedule.is_daily_result
        use_daily_module = is_daily or (
            schedule is not None and getattr(schedule, 'use_daily_factor_module', False))
        is_daily_worker = use_daily_module  # worker call signature: daily=single-code, minute=batch

        # open_position: skip factor compute entirely, use prev_day_factors directly
        if schedule is not None and getattr(schedule, 'skip_factor_compute', False):
            date_str = self.trading_day
            end_time_label = schedule.result_label or "093000"

            # 快路径：消费启动时预计算好的 target weights，只跑 positions_to_orders
            # （轻量，需要 9:30 实时 tick 价）。预计算 miss 则走下方 fallback。
            cached_targets = self._pop_open_position_targets()
            if cached_targets is not None and not cached_targets.empty:
                logger.info("[open_position] %s using precomputed targets: %d rows",
                            schedule.name, len(cached_targets))
                positions_df = None
                try:
                    portfolio_context = None
                    if self.portfolio_context_fn is not None:
                        portfolio_context = self.portfolio_context_fn(date_str, end_time_label)
                    if portfolio_context is not None:
                        lp = self._snapshot_latest_prices()
                        if lp:
                            portfolio_context.meta["latest_prices"] = lp
                    import importlib as _il
                    tm = _il.import_module(
                        os.environ.get("INFERENCE_MODULE",
                                       "quant_platform.inference.tree_model_orders"))
                    targets_to_orders = getattr(tm, "targets_to_orders", None)
                    if targets_to_orders is None:
                        # 老版本 inference 模块没有拆分接口 → 用 inference_fn 全跑
                        logger.warning(
                            "[open_position] inference module lacks targets_to_orders, "
                            "precompute cache unusable; falling back")
                    else:
                        positions_df = targets_to_orders(
                            cached_targets, date_str, end_time_label,
                            daily_basic_df=self._daily_basic_df,
                            portfolio_context=portfolio_context,
                        )
                except Exception as exc:
                    logger.error("[open_position] targets_to_orders failed: %s",
                                 exc, exc_info=True)
                    positions_df = None

                if positions_df is not None and not positions_df.empty:
                    # 写一份 _positions.csv 保持兼容（监控/对账依赖）
                    try:
                        if self.output_path is not None:
                            pos_file = self.output_path / f"{date_str}_{end_time_label}_positions.csv"
                            _compact_output_copy(positions_df).to_csv(pos_file, index=False)
                            logger.info("[open_position] wrote %s", pos_file)
                    except Exception:
                        pass
                    if self._order_queue is not None:
                        try:
                            self._order_queue.put_nowait(
                                (positions_df, date_str, end_time_label))
                            logger.info("[order-gateway] enqueued %d rows date=%s end_time=%s",
                                        len(positions_df), date_str, end_time_label)
                        except queue.Full:
                            logger.error("[order-gateway] queue full, drop %d rows",
                                         len(positions_df))
                    else:
                        _push_to_order_gateway(positions_df, date_str, end_time_label)
                    return
                # targets_to_orders 失败 → 继续走 fallback
                logger.warning("[open_position] targets_to_orders returned empty, falling back")

            # fallback: 预计算 miss 或失败 → 退回原同步推理路径
            prev = self._prev_day_factors
            if prev is None or prev.empty:
                logger.error("[open_position] prev_day_factors empty, skip %s",
                             schedule.name)
                return
            logger.info("[open_position] %s cache miss, inline inference with prev_day_factors: %d rows",
                        schedule.name, len(prev))
            # 清理可能晚到的预计算结果：fallback 一旦走了，磁盘 + 内存 cache
            # 都不能再被消费（否则下次 open_position 重复触发时会读到陈旧
            # target 重复下单；mark_run 已防重复，但再加一层主动清理更安全）。
            self._invalidate_open_position_targets()
            results = prev.to_dict("records")
            self._write_results(results, date_str, "", schedule=schedule)
            return

        # daily_result and daily_position are triggered by time_trigger (precise
        # time point) so skip trading-hours check; only minute needs the guard.
        if not is_daily and not use_daily_module and not _is_trading_hours():
            return
        pipe_log = get_streaming_logger()
        now_dt = datetime.now()
        date_str = self.trading_day
        # Keep daily strategy compatible with historical daily factor runs:
        # daily strategies receive end_time="" for full-day calculation.
        end_time = "" if use_daily_module else _minute_end_time(now_dt)
        self._round_count += 1

        files_by_code = self._scan_shm_files()
        if not files_by_code:
            return

        snap_t0 = time.perf_counter()
        wall_secs = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
        if use_daily_module and not is_daily:
            # daily_position: skip _sync_states to avoid _state_offsets racing
            # with the minute schedule running in parallel under another lock;
            # full-market compute does not need dirty tracking. Take a read-only
            # snapshot of the current StockState objects (GIL-protected).
            dirty_codes = set()
            states_snapshot = dict(self._states)
            snap_ms = 0.0
        else:
            dirty_codes, states_snapshot = self._sync_states(files_by_code, wall_secs)
            snap_ms = (time.perf_counter() - snap_t0) * 1000

        # Daily / daily_position: compute ALL stocks (no dirty tracking)
        if use_daily_module:
            all_codes = sorted(files_by_code.keys())
        elif not dirty_codes:
            return
        else:
            all_codes = sorted(dirty_codes)
        logger.info("[combined] computing: date=%s end_time=%s dirty=%d total=%d schedule=%s",
                    date_str, end_time, len(all_codes), len(files_by_code),
                    schedule.name if schedule else "minute")

        t0 = time.time()

        factor_module_path = self.daily_factor_module if (use_daily_module and self.daily_factor_module) else self.factor_module
        active_factor_info = self._daily_factor_info if (use_daily_module and self._daily_factor_info) else self.factor_info
        global _market_df, _daily_basic_df, _base_ns
        _market_df = self._market_df
        _daily_basic_df = self._daily_basic_df
        _base_ns = pd.Timestamp(date_str).value

        tick_paths = {code: files_by_code[code].get(KIND_TICK, "") for code in all_codes}
        deal_paths = {code: files_by_code[code].get(KIND_DEAL, "") for code in all_codes}
        order_paths = {code: files_by_code[code].get(KIND_ORDER, "") for code in all_codes}

        results = []
        errors = 0
        pool_t0 = time.perf_counter()

        batch_size = _env_int("LIVE_FACTOR_BATCH_SIZE", 128, minimum=1)
        code_batches = [all_codes[i:i + batch_size] for i in range(0, len(all_codes), batch_size)]
        tasks = [
            (batch, date_str, end_time, wall_secs,
             {code: states_snapshot.get(code) for code in batch},
             {code: tick_paths.get(code, "") for code in batch},
             {code: deal_paths.get(code, "") for code in batch},
             {code: order_paths.get(code, "") for code in batch},
             date_str, factor_module_path, active_factor_info, is_daily_worker)
            for batch in code_batches
        ]

        build_ms_total = 0.0
        factor_ms_total = 0.0
        if self._pool is not None:
            try:
                for batch_results, batch_errors, build_ms, factor_ms, err in self._pool.imap_unordered(
                    _compute_code_batch_shm, tasks, chunksize=1,
                ):
                    build_ms_total += build_ms
                    factor_ms_total += factor_ms
                    if err:
                        logger.warning("[combined] batch compute failed: %s", err)
                    errors += batch_errors
                    results.extend(batch_results)
            except Exception as exc:
                logger.error("[combined] persistent batch pool failed: %s", exc, exc_info=True)
                self._pool = None

        if self._pool is None:
            for task in tasks:
                batch_results, batch_errors, build_ms, factor_ms, err = _compute_code_batch_shm(task)
                build_ms_total += build_ms
                factor_ms_total += factor_ms
                if err:
                    logger.warning("[combined] inline batch compute failed: %s", err)
                errors += batch_errors
                results.extend(batch_results)

        pool_ms = (time.perf_counter() - pool_t0) * 1000
        elapsed_ms = (time.time() - t0) * 1000
        pool_type = "persistent+shm" if self._pool is not None else "inline"
        n_stocks = len(all_codes)
        wall_ms_per_stock = pool_ms / max(n_stocks, 1)
        worker_sum_ms_per_stock = (build_ms_total + factor_ms_total) / max(n_stocks, 1)
        logger.info(
            "[combined] done (%s): %d results (errors=%d) | "
            "snap=%.0fms pool=%.0fms build(sum)=%.0fms factor(sum)=%.0fms total=%.0fms | "
            "%d stocks/%d batches × %dw | wall=%.1fms/stock worker_sum=%.1fms/stock",
            pool_type, len(results), errors,
            snap_ms, pool_ms, build_ms_total, factor_ms_total, elapsed_ms,
            n_stocks, len(code_batches), self.n_workers,
            wall_ms_per_stock, worker_sum_ms_per_stock,
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

        # Store daily result for next day's prev_day (before fork, in main process)
        if is_daily:
            self._prev_day_factors = pd.DataFrame(results)
            logger.info("[daily] stored daily result (%d rows) as prev_day_factors",
                        len(self._prev_day_factors))

        self._write_results(results, date_str, end_time, schedule=schedule)
        pipe_log.log("factor_compute", date=date_str, end_time=end_time,
                     stocks=len(all_codes), results=len(results),
                     compute_ms=round(elapsed_ms, 1))

        # 因子有效率自检
        self._check_factor_valid_rate(results)

    def _handle_output_child(self, pid: int, read_fd: int, date_str: str, end_time: str) -> None:
        """Read inference output from the output child and enqueue order work in parent."""
        positions_df = None
        try:
            with os.fdopen(read_fd, "rb") as pipe:
                try:
                    positions_df = pickle.load(pipe)
                except EOFError:
                    positions_df = None
        except Exception as exc:
            logger.error("[output-child] failed to read order payload from pid=%d: %s", pid, exc)
        finally:
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass

        if positions_df is None or positions_df.empty:
            return

        if self._order_queue is not None:
            try:
                self._order_queue.put_nowait((positions_df, date_str, end_time))
                logger.info("[order-gateway] enqueued %d rows date=%s end_time=%s",
                            len(positions_df), date_str, end_time)
            except queue.Full:
                logger.error("[order-gateway] queue full, drop %d rows date=%s end_time=%s",
                             len(positions_df), date_str, end_time)
        else:
            _push_to_order_gateway(positions_df, date_str, end_time)

    def _check_factor_valid_rate(self, results: list) -> None:
        """因子有效率自检：有效率骤降时告警。"""
        if not results:
            return
        import math
        total_cells = 0
        nan_cells = 0
        for r in results:
            if not r or not isinstance(r, dict):
                continue
            for k, v in r.items():
                if not str(k).startswith("F_"):
                    continue
                total_cells += 1
                if isinstance(v, float) and math.isnan(v):
                    nan_cells += 1
        if total_cells == 0:
            return
        valid_rate = 1.0 - (nan_cells / total_cells)
        logger.info("[combined] factor valid rate: %.1f%% (%d/%d cells)",
                    valid_rate * 100, total_cells - nan_cells, total_cells)
        if (valid_rate < 0.5
                and self._prev_valid_rate is not None
                and self._prev_valid_rate > 0.8):
            logger.error(
                "[ALERT] factor valid rate dropped from %.1f%% to %.1f%%!",
                self._prev_valid_rate * 100, valid_rate * 100)
        self._prev_valid_rate = valid_rate

    def _write_results(self, results: list, date_str: str, end_time: str,
                        schedule: Optional[ComputationSchedule] = None) -> None:
        """Write factor results to CSV and upload to OSS."""
        if not results:
            return
        is_daily = schedule is not None and schedule.is_daily_result
        run_inference = True if schedule is None else schedule.run_inference
        # Storage label: daily -> "daily", daily_position -> result_label,
        # minute -> actual end_time. Keeps files/oss keys from colliding.
        if is_daily:
            label = "daily"
        elif schedule is not None and getattr(schedule, 'result_label', ''):
            label = schedule.result_label
        else:
            label = end_time
        # Runtime end_time for downstream (inference, portfolio context, order
        # queue, latency query): daily_position passes "" to the factor function
        # but uses label here so _query_deal_latency / file lookups resolve.
        rt_end_time = label if (
            schedule is not None and getattr(schedule, 'result_label', '')) else end_time
        output_path = self.output_path
        outfun = self.outfun
        inference_fn = self.inference_fn
        portfolio_context_fn = self.portfolio_context_fn
        daily_basic_df = self._daily_basic_df
        prev_day_factors = self._prev_day_factors
        idx_cons_df = self._idx_cons_df
        trading_universe_df = self._trading_universe_df
        try:
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                try:
                    order_payload = None
                    result_df = pd.DataFrame(results)
                    if not result_df.empty:
                        compact_df = _compact_output_copy(result_df)
                        if output_path:
                            output_path.mkdir(parents=True, exist_ok=True)
                            out_file = output_path / f"{date_str}_{label}.csv"
                            compact_df.to_csv(out_file, index=False)
                            logger.info("[combined] wrote %s", out_file)
                        _upload_to_oss(
                            compact_df, date_str, label,
                            category="daily" if is_daily else "minutes",
                        )
                    if run_inference and inference_fn is not None and not result_df.empty:
                        try:
                            portfolio_context = None
                            if portfolio_context_fn is not None:
                                portfolio_context = portfolio_context_fn(date_str, rt_end_time)
                            # Augment with engine realtime tick prices (latest_price
                            # from shm sync). Used by open_position at 9:30 to size
                            # orders off live tick instead of daily_basic.close.
                            if portfolio_context is not None:
                                lp = self._snapshot_latest_prices()
                                if lp:
                                    portfolio_context.meta["latest_prices"] = lp
                            universe_extra = {
                                "date": date_str,
                                "end_time": rt_end_time,
                                "codes": _result_codes(result_df),
                                "factor_result": result_df,
                                "idx_cons_df": idx_cons_df,
                            }
                            # Use pre-computed trading_universe from startup if available
                            if trading_universe_df is not None and not trading_universe_df.empty:
                                tu_df = trading_universe_df
                            else:
                                tu_df = compute_trading_universe(
                                    daily_basic_df, universe_extra)
                            idx_comp_df = compute_index_composition(
                                daily_basic_df, universe_extra,
                                trading_day=date_str,
                                idx_cons_cache=self._idx_cons_cache)
                            positions_df = call_inference(
                                inference_fn,
                                date_str,
                                rt_end_time,
                                prev_day_factors,
                                result_df,
                                daily_basic_df,
                                tu_df,
                                idx_comp_df,
                                portfolio_context,
                            )
                            if positions_df is not None and not positions_df.empty:
                                if output_path:
                                    pos_file = output_path / f"{date_str}_{label}_positions.csv"
                                    _compact_output_copy(positions_df).to_csv(pos_file, index=False)
                                    logger.info("[inference] wrote %d positions to %s",
                                                len(positions_df), pos_file)
                                order_payload = positions_df
                        except Exception as exc:
                            logger.error("[inference] failed: %s", exc, exc_info=True)
                    if outfun is not None:
                        try:
                            outfun(date_str, rt_end_time, result_df)
                        except Exception as exc:
                            logger.error("[combined] outfun failed: %s", exc)
                    with os.fdopen(write_fd, "wb") as pipe:
                        pickle.dump(order_payload, pipe, protocol=pickle.HIGHEST_PROTOCOL)
                except Exception as exc:
                    logger.error("[output-child] failed: %s", exc, exc_info=True)
                    try:
                        with os.fdopen(write_fd, "wb") as pipe:
                            pickle.dump(None, pipe, protocol=pickle.HIGHEST_PROTOCOL)
                    except OSError:
                        pass
                finally:
                    os._exit(0)
            os.close(write_fd)
            threading.Thread(
                target=self._handle_output_child,
                args=(pid, read_fd, date_str, rt_end_time),
                name=f"output-child-{pid}",
                daemon=True,
            ).start()
            logger.info("[combined] output forked to child pid=%d", pid)
        except OSError:
            for fd in ("read_fd", "write_fd"):
                try:
                    os.close(locals()[fd])
                except Exception:
                    pass
            pass

    def run(self) -> None:
        """Main loop."""
        # Set trading day
        today = date.today()
        self.trading_day = today.strftime("%Y%m%d")
        logger.info("[native] starting with trading_day=%s, shm_dir=%s",
                     self.trading_day, self.shm_dir)

        # Pod startup data refresh: proactively generate T-1 and T-0 daily_basic +
        # composition parquet and upload to OSS (matches deeptrade Go pipeline).
        # Failures here must not block startup — MySQL fallback covers _load_*.
        try:
            from quant_platform.data.data_refresh import refresh_pod_startup
            refresh_pod_startup(self.trading_day)
        except Exception as exc:
            logger.error(
                "[native] pod startup data refresh failed (continue with fallback): %s",
                exc, exc_info=True,
            )

        # Load daily basic
        self._load_daily_basic()

        # Load index constituents
        self._load_idx_cons()

        # Set globals for workers
        global _factor_fn, _factor_info, _market_df, _daily_basic_df
        _market_df = self._market_df
        _daily_basic_df = self._daily_basic_df

        # Load factor module
        if self.factor_module:
            mod = importlib.import_module(self.factor_module)
            _factor_fn = mod.factor_calculation
            self.factor_info = getattr(mod, "FACTOR_INFO", getattr(mod, "factor_info", {})) or {}
            _factor_info = self.factor_info
            self.factor_calculation = mod.factor_calculation
            self.outfun = getattr(mod, "outfun", None)
            logger.info("[native] loaded factor: %s factor_info=%s", self.factor_module, self.factor_info)
        else:
            logger.error("[native] FACTOR_MODULE not set!")
            return

        # Load daily factor module (optional, for different strategy at daily schedule)
        if self.daily_factor_module:
            try:
                dmod = importlib.import_module(self.daily_factor_module)
                self._daily_factor_fn = dmod.factor_calculation
                self._daily_factor_info = getattr(dmod, "FACTOR_INFO", getattr(dmod, "factor_info", {})) or {}
                logger.info("[native] loaded daily factor: %s factor_info=%s",
                            self.daily_factor_module, self._daily_factor_info)
            except Exception as exc:
                logger.error("[native] failed to load daily factor module %s: %s",
                             self.daily_factor_module, exc)

        # Load inference module (optional)
        inference_module = os.environ.get("INFERENCE_MODULE", "")
        if inference_module:
            try:
                imod = importlib.import_module(inference_module)
                self.inference_fn = getattr(imod, "inference", None)
                if self.inference_fn:
                    logger.info("[native] loaded inference: %s", inference_module)
                else:
                    logger.warning("[native] inference module %s has no 'inference' function", inference_module)
            except Exception as exc:
                logger.error("[native] failed to load inference module %s: %s", inference_module, exc)

        # Load portfolio context provider (optional). The provider owns broker
        # file access and should expose get_portfolio_context(date_str, end_time).
        portfolio_context_module = os.environ.get("PORTFOLIO_CONTEXT_MODULE", "")
        if portfolio_context_module:
            try:
                pmod = importlib.import_module(portfolio_context_module)
                self.portfolio_context_fn = getattr(pmod, "get_portfolio_context", None)
                if self.portfolio_context_fn:
                    logger.info("[native] loaded portfolio context: %s", portfolio_context_module)
                else:
                    logger.warning("[native] portfolio context module %s has no "
                                   "'get_portfolio_context' function",
                                   portfolio_context_module)
            except Exception as exc:
                logger.error("[native] failed to load portfolio context module %s: %s",
                             portfolio_context_module, exc)

        # Load trading universe module (optional, but must succeed if configured).
        # Traders implement trading_universe(daily_basic_df, idx_cons_df) -> list[str]
        # Called once at startup, result cached and passed to inference.
        trading_universe_module = os.environ.get("TRADING_UNIVERSE_MODULE", "")
        if trading_universe_module:
            try:
                tmod = importlib.import_module(trading_universe_module)
                self.trading_universe_fn = getattr(tmod, "trading_universe", None)
                if not self.trading_universe_fn:
                    logger.error("[native] trading universe module %s has no "
                                 "'trading_universe' function", trading_universe_module)
                    return
                logger.info("[native] loaded trading_universe: %s", trading_universe_module)
            except Exception as exc:
                logger.error("[native] failed to load trading universe module %s: %s",
                             trading_universe_module, exc)
                return

            # Call trading_universe at startup
            try:
                universe_codes = self.trading_universe_fn(
                    self._daily_basic_df, self._idx_cons_df,
                )
                if universe_codes is not None:
                    self._trading_universe_df = build_trading_universe_df(
                        universe_codes, self._daily_basic_df,
                    )
                    logger.info("[native] trading_universe returned %d codes", len(self._trading_universe_df))
                else:
                    logger.info("[native] trading_universe() returned None; using default universe")
            except Exception as exc:
                logger.error("[native] trading_universe() call failed: %s", exc)
                return

        # Init pool
        self._init_pool()
        self._init_order_worker()

        # Load previous day factors (for inference)
        if self.inference_fn is not None:
            self._load_prev_day_factors()

        # Build computation schedules from config
        self._schedules = build_schedules_from_env(self.factor_info)
        logger.info("[native] schedules: %s", [s.name for s in self._schedules])

        # open_position 推理前置：pod 启动后异步跑树模型，把 target weights
        # 缓存到内存 + 磁盘。9:30 schedule 触发时优先消费缓存，跳过重的
        # 树模型推理，只跑轻量的 positions_to_orders（需要实时 tick）。
        # daily_position (14:50) 不受影响 —— 走的是 use_daily_factor_module
        # 主路径，不读这份 cache。
        #
        # ⚠️ 安全前提：tree_model_inference 默认 USE_OPTIMIZER=True，target
        # positions 会受 portfolio_context.positions（current_weight）影响。
        # 启用预计算前必须确认：从 pod 启动到 9:30 之间 broker 持仓不变。
        # 适用场景：隔夜 rebalance 策略、ONE_TIME_CLOSE_ON_STARTUP=0、无其他
        # schedule 在窗口内发单。否则 8:50 算出的 target 与 9:30 实际状态不符。
        # 默认 OFF，需要显式设 OPEN_POSITION_PRECOMPUTE=1 开启。
        precompute_enabled = _env_bool("OPEN_POSITION_PRECOMPUTE", False)
        if (precompute_enabled
                and self.inference_fn is not None
                and self._prev_day_factors is not None
                and not self._prev_day_factors.empty):
            logger.info("[native] launching open_position precompute (OPEN_POSITION_PRECOMPUTE=1)")
            threading.Thread(
                target=self._precompute_open_position_targets,
                name="open-position-precompute",
                daemon=True,
            ).start()
        else:
            logger.info(
                "[native] precompute disabled (OPEN_POSITION_PRECOMPUTE=%s, "
                "inference_fn=%s prev_day_factors=%s)",
                "1" if precompute_enabled else "0",
                bool(self.inference_fn),
                bool(self._prev_day_factors is not None
                     and not self._prev_day_factors.empty),
            )

        # One-shot flatten at startup (sim reset; guarded by env + date flag)
        self._one_time_close_on_startup()

        if self._raw_archive_enabled:
            self._archive_thread = threading.Thread(
                target=self._archive_loop,
                name="native-raw-archive",
                daemon=True,
            )
            self._archive_thread.start()

        logger.info("[native] entering main loop")

        while True:
            try:
                now = time.time()
                now_dt = datetime.now()
                for schedule in self._schedules:
                    if schedule.should_run(now, now_dt, self.trading_day):
                        if schedule.name == "minute" and self._compute_running:
                            continue
                        if schedule.name == "minute":
                            self._compute_running = True
                        schedule.mark_run(now, self.trading_day)
                        logger.info("[native] dispatching schedule=%s", schedule.name)
                        t = threading.Thread(
                            target=self._compute_and_output,
                            args=(schedule,),
                            daemon=True,
                        )
                        try:
                            t.start()
                        except Exception:
                            # Thread start failed: roll back the mark_run so the
                            # schedule can fire again next tick (otherwise daily
                            # would be permanently marked as run for the day),
                            # and release the minute guard.
                            schedule.unmark_run()
                            if schedule.name == "minute":
                                self._compute_running = False
                            logger.error("[native] failed to start thread for schedule=%s",
                                         schedule.name, exc_info=True)
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
        self._stop_order_worker()


def _query_deal_latency(orders_df: pd.DataFrame, push_sec: float, date_str: str, end_time: str) -> None:
    """Query DealOrder DBF and log market-data→deal total latency.

    Computes total delay from the exchange timestamp of the market data
    tick that triggered the order to the exchange timestamp of the trade fill.
    """
    try:
        import json
        import os
        import urllib.request
        import pandas as pd
        gw = os.environ.get("ORDER_GATEWAY_URL", "")
        token = os.environ.get("ORDER_GATEWAY_TOKEN", "")
        if not gw:
            return
        headers = {"Authorization": f"Bearer {token}"} if token else {}

        # Load factor result CSV for this cycle to get data_latency_ms
        factor_path = f"/data/factors/{date_str}_{end_time}.csv"
        data_latency = {}
        if os.path.exists(factor_path):
            try:
                fdf = pd.read_csv(factor_path)
                if "data_latency_ms" in fdf.columns and "ID_QI" in fdf.columns:
                    for _, r in fdf.iterrows():
                        lat = r["data_latency_ms"]
                        if pd.notna(lat) and 100 < lat < 60000:
                            data_latency[str(r["ID_QI"]).split(".")[0].zfill(6)] = int(lat)
            except Exception:
                pass

        # Query DealOrder DBF
        req = urllib.request.Request(f"{gw.rstrip('/')}/v1/atx/report?file=DealOrder_{date_str}.dbf", headers=headers)
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            all_deals = json.loads(resp.read()).get("rows", [])
        except Exception:
            all_deals = []

        if not all_deals:
            return

        # Get pushed stock codes
        pushed_codes = set()
        for col in ("code", "symbol"):
            if col in orders_df.columns:
                pushed_codes.update(orders_df[col].astype(str).str.split(".").str[0].str.zfill(6).tolist())

        # For pushed orders, estimate market exchange time = push_sec - 6s(compute+fork) - data_latency_ms
        # Then compute total: deal_exchange_time - market_exchange_time
        deal_latencies = {}
        for d in all_deals:
            sym = str(d.get("Symbol", ""))
            dt_raw = str(d.get("DealTime", ""))
            if not dt_raw or len(dt_raw) < 17:
                continue
            code6 = sym[:6].zfill(6)
            if code6 not in pushed_codes:
                continue
            h = int(dt_raw[8:10])
            m = int(dt_raw[10:12])
            s = int(dt_raw[12:14])
            ms = int(dt_raw[14:17])
            deal_sec = h * 3600 + m * 60 + s + ms / 1000

            # Estimate market data exchange timestamp
            # push_sec - 6s = cycle snap time, minus data_latency_ms = exchange tick time
            lat_ms = data_latency.get(code6, 1000)  # default 1s if no data
            market_exchange_sec = push_sec - 6.0 - (lat_ms / 1000)
            total_lat = deal_sec - market_exchange_sec
            if 0 < total_lat < 180:
                deal_latencies[sym] = (market_exchange_sec, deal_sec, total_lat)

        if deal_latencies:
            total_lats = [v[2] for v in deal_latencies.values()]
            avg_lat = sum(total_lats) / len(total_lats)
            max_lat = max(total_lats)
            codes_str = " ".join(sorted(deal_latencies.keys()))
            # Show one detailed example
            ex = next(iter(deal_latencies.items()))
            ex_sym, (ex_mkt, ex_deal, ex_lat) = ex
            def _s2h(s):
                hh = int(s // 3600); mm = int((s % 3600) // 60); ss = s % 60
                return f"{hh:02d}:{mm:02d}:{ss:05.2f}"
            logger.info(
                "[deal-latency] end_time=%s deals=%d avg=%.1fs max=%.1fs "
                "eg=%s market=%s deal=%s total=%.1fs",
                end_time, len(deal_latencies), avg_lat, max_lat,
                ex_sym, _s2h(ex_mkt), _s2h(ex_deal), ex_lat,
            )
        else:
            logger.info("[deal-latency] end_time=%s deals=0 (pending)", end_time)
    except Exception as exc:
        logger.warning("[deal-latency] query failed: %s", exc)


def _order_worker_loop(order_queue) -> None:
    """Persistent order gateway worker."""
    logger.info("[order-gateway] worker loop started")
    try:
        while True:
            item = order_queue.get()
            if item is None:
                break
            try:
                orders_df, date_str, end_time = item
                _push_to_order_gateway(orders_df, date_str, end_time)
                # Log push-to-deal latency
                from datetime import datetime as _dt
                _now = _dt.now()
                push_sec = _now.hour * 3600 + _now.minute * 60 + _now.second + _now.microsecond / 1_000_000
                _query_deal_latency(orders_df, push_sec, date_str, end_time)
            except Exception as exc:
                logger.error("[order-gateway] worker task failed: %s", exc, exc_info=True)
    finally:
        logger.info("[order-gateway] worker loop stopped")


def _write_close_flag(path: str) -> None:
    """Write the one-time-close completion flag (date-stamped)."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(f"done at {datetime.now().isoformat()}\n")
    except Exception as exc:
        logger.warning("[one-time-close] flag write failed: %s", exc)


def _push_to_order_gateway(orders_df: pd.DataFrame, date_str: str, end_time: str) -> None:
    """Push orders through order-gateway.

    Env vars:
        ORDER_GATEWAY_URL: HTTP gateway base URL, e.g. http://172.16.0.47:18080
        ORDER_GATEWAY_TOKEN: HTTP bearer token
        ORDER_GATEWAY_TIMEOUT: HTTP timeout seconds
        BROKER_ACCOUNT_ID: optional account id
        ORDER_DEFAULT_STRATEGY: default strategy name in rows
    """
    gateway_url = os.environ.get("ORDER_GATEWAY_URL", "")
    if not gateway_url:
        logger.warning("[order-gateway] ORDER_GATEWAY_URL not configured, skip order push")
        return
    _push_to_order_gateway_http(orders_df, date_str, end_time, gateway_url)


def _order_push_configured() -> bool:
    return bool(os.environ.get("ORDER_GATEWAY_URL", ""))


def _push_to_order_gateway_http(orders_df: pd.DataFrame, date_str: str, end_time: str, gateway_url: str) -> None:
    account_id = os.environ.get("BROKER_ACCOUNT_ID", "")
    orders = _build_order_gateway_orders(orders_df, date_str, end_time)
    if not orders:
        logger.warning("[order-gateway] no valid order rows; required columns include code, side, volume")
        return

    payload = {
        "account_id": account_id,
        "orders": orders,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    token = os.environ.get("ORDER_GATEWAY_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    timeout = float(os.environ.get("ORDER_GATEWAY_TIMEOUT", "10"))
    url = f"{gateway_url.rstrip('/')}/v1/orders"

    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            response_text = resp.read().decode("utf-8", errors="replace")
        try:
            response = json.loads(response_text)
        except Exception:
            response = {}
        if response and not response.get("ok", False):
            logger.error("[order-gateway] push rejected: %s", response)
            return
        logger.info("[order-gateway] pushed %d rows to %s file=%s",
                    len(orders), url, response.get("order_file", "") if response else "")
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        logger.error("[order-gateway] HTTP %s: %s", exc.code, body_text[:500])
    except Exception as exc:
        logger.error("[order-gateway] push failed: %s", exc)


def _build_order_gateway_orders(orders_df: pd.DataFrame, date_str: str, end_time: str) -> List[dict]:
    orders = []
    for _, row in orders_df.iterrows():
        code = _order_symbol(row.get("symbol", row.get("code", "")))
        volume = _order_volume(row)
        side = _order_side(row)
        if not code or volume <= 0 or not side:
            continue
        price_type = _order_price_type(row)
        price_raw = _order_price(row, price_type)
        if price_type == "limit" and not price_raw:
            logger.error("[order-gateway] skip %s %s: limit order requires price", code, side)
            continue
        strategy = str(row.get("strategy", "") or os.environ.get("ORDER_DEFAULT_STRATEGY", "quant_platform"))
        note = str(row.get("note", "") or row.get("remark", "") or
                   f"{strategy}_{date_str}{end_time}_{code}_{side}")
        try:
            price = float(price_raw) if price_raw not in {"", None} else 0.0
        except Exception:
            price = 0.0

        # Minimal pre-trade sanity checks (position/capital validation stays in
        # the gateway, which holds live account state).
        #  - limit orders must carry a positive price
        if price_type == "limit" and price <= 0:
            logger.error("[order-gateway] skip %s %s: limit order price must be > 0 (got %s)",
                         code, side, price_raw)
            continue

        explicit_id = str(row.get("order_id", "") or "")
        if explicit_id:
            order_id = explicit_id
        else:
            # Stable idempotent id derived from order content. The same logical
            # order yields the same id across retries/re-runs so the gateway can
            # deduplicate. Includes price/strategy/algo so that two orders with
            # identical code+side+volume but different price/strategy are NOT
            # collapsed into one id (which the gateway would wrongly dedup).
            content = "|".join([
                date_str, end_time, code, side, str(int(volume)),
                price_type, f"{price:.6f}", strategy,
                str(row.get("algo_strategy", "") or ""),
                str(row.get("algo_param", "") or ""),
            ])
            order_id = "ord_" + hashlib.md5(content.encode("utf-8")).hexdigest()[:12]
        order = {
            "order_id": order_id,
            "symbol": code,
            "side": side,
            "volume": int(volume),
            "price_type": price_type,
            "price": price,
            "strategy": strategy,
            "note": note,
        }
        algo_param = row.get("algo_param", row.get("atx_algo_param", ""))
        if algo_param not in {"", None} and not pd.isna(algo_param):
            order["algo_param"] = str(algo_param)
        # 执行算法策略名 → Gateway 侧自动 resolveAlgoStrategy()
        algo_strategy = row.get("algo_strategy", "")
        if algo_strategy not in {"", None} and not pd.isna(algo_strategy):
            order["algo_strategy"] = str(algo_strategy)
        # POV 执行算法参数
        for field in ("max_percent", "up_limit", "down_limit"):
            val = row.get(field)
            if val not in {"", None, 0} and not pd.isna(val):
                order[field] = float(val)
        orders.append(order)
    return orders


def _order_symbol(code: object) -> str:
    raw = str(code or "").strip().upper()
    if not raw:
        return ""
    if raw.endswith(".SH"):
        return raw[:6] + ".SH"
    if raw.endswith(".SZ"):
        return raw[:6] + ".SZ"
    if len(raw) == 8 and raw.startswith("SH"):
        return raw[2:] + ".SH"
    if len(raw) == 8 and raw.startswith("SZ"):
        return raw[2:] + ".SZ"
    if len(raw) == 6 and raw[0] == "6":
        return raw + ".SH"
    if len(raw) == 6 and raw[0] in {"0", "3"}:
        return raw + ".SZ"
    return raw


def _order_volume(row: pd.Series) -> int:
    for col in ("order_volume", "volume", "qty", "quantity"):
        if col in row and pd.notna(row[col]):
            try:
                return int(float(row[col]))
            except Exception:
                return 0
    return 0


def _order_side(row: pd.Series) -> str:
    side = str(row.get("side", row.get("action", "")) or "").strip().lower()
    if side in {"buy", "b", "long", "1", "23", "买", "买入"}:
        return "buy"
    if side in {"sell", "s", "short", "2", "24", "卖", "卖出"}:
        return "sell"
    return ""


def _order_price_type(row: pd.Series) -> str:
    raw = row.get("price_type", row.get("quote_type", ""))
    value = str(raw or os.environ.get("ORDER_DEFAULT_PRICE_TYPE", "latest")).strip()
    mapping = {
        "1": "latest",
        "6": "latest",
        "latest": "latest",
        "market": "latest",
        "last": "latest",
        "3": "limit",
        "0": "limit",
        "limit": "limit",
        "fixed": "limit",
    }
    return mapping.get(value.lower(), value)


def _order_price(row: pd.Series, price_type: str) -> str:
    raw = row.get("order_price", row.get("limit_price", row.get("price", "")))
    if raw == "" or pd.isna(raw):
        return "0" if price_type == "latest" else ""
    try:
        return f"{float(raw):.4f}".rstrip("0").rstrip(".")
    except Exception:
        return str(raw).strip()


def _result_codes(result_df: pd.DataFrame) -> List[str]:
    """Extract code values from a factor result DataFrame."""
    for col in ("code", "Code", "stock_code"):
        if col in result_df.columns:
            return result_df[col].dropna().astype(str).tolist()
    return []


def _worker_init(factor_module: str, daily_basic_df: pd.DataFrame,
                 market_df: pd.DataFrame, factor_info: Dict[str, Any]) -> None:
    """Initialize worker process."""
    global _factor_fn, _factor_info, _market_df, _daily_basic_df, _strategy_cache
    _market_df = market_df
    _daily_basic_df = daily_basic_df
    _factor_info = factor_info or {}
    if factor_module:
        _factor_fn, cached_info = _get_worker_strategy(factor_module, _factor_info)
        _factor_info = cached_info or _factor_info


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
