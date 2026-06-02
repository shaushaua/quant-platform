# -*- coding: utf-8 -*-
"""
内存数据存储
存储当天的实时数据，供策略直接读取

内部使用 per-stock list 存储（而非 DataFrame），热路径 append 极快。
get_* 方法按需惰性转换为 DataFrame。
"""

import os
import time
import threading
import logging
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

import numpy as np
import pandas as pd
import mdl_parser
import glob as glob_mod
import shutil

from ..core.constants import TICK_COLUMNS, ORDER_COLUMNS, DEAL_COLUMNS

logger = logging.getLogger(__name__)


class MemoryStore:
    """
    内存数据存储 - 单例模式
    通联采集进程写入，策略进程直接读取
    无网络开销，毫秒级访问

    使用方式:
        store = MemoryStore.get_instance()
        df_tick = store.get_tick("000001.XSHE")
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    @classmethod
    def get_instance(cls) -> "MemoryStore":
        """获取单例实例"""
        return cls()

    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        self._init_storage()
        self._initialized = True

    def _init_storage(self):
        """初始化存储结构"""
        # 最大每股票存储行数
        self._max_rows = int(os.environ.get("MAX_ROWS_PER_STOCK", "100000"))

        # 当前交易日
        self._trading_day: str = ""

        # Per-stock list 存储: code -> list[tuple] (SDK 回调直接写入)
        self._tick_lists: Dict[str, list] = {}
        self._order_lists: Dict[str, list] = {}
        self._deal_lists: Dict[str, list] = {}

        # Dirty tracking: stocks that received new data since last reset
        self._dirty_codes: set = set()
        # Per-type dirty tracking: only warm data types that actually changed
        self._dirty_tick: set = set()
        self._dirty_deal: set = set()
        self._dirty_order: set = set()

        # Rust pre-allocated buffer: code -> ShmStockBuffer (mmap-backed, shared with workers)
        # tick: 81 - 2(TradingDay,Code) = 79 cols, first 2 are Time/UpdateTime strings
        # order: 11 - 2 = 9 cols, first 2 are Time/UpdateTime strings
        # deal: 12 - 2 = 10 cols, first 2 are Time/UpdateTime strings
        self._tick_buf: Dict[str, mdl_parser.ShmStockBuffer] = {}
        self._order_buf: Dict[str, mdl_parser.ShmStockBuffer] = {}
        self._deal_buf: Dict[str, mdl_parser.ShmStockBuffer] = {}
        self._buf_config = {"tick": (79, 2), "order": (9, 2), "deal": (10, 2)}
        # mmap directory for shared buffers
        self._shm_dir = os.environ.get("SHM_DIR", "/data/quant/shm")

        self._tick_cache_len: Dict[str, int] = {}
        self._order_cache_len: Dict[str, int] = {}
        self._deal_cache_len: Dict[str, int] = {}
        # DataFrame cache: cache the last built DataFrame for cache-hit fast path
        self._tick_df_cache: Dict[str, pd.DataFrame] = {}
        self._order_df_cache: Dict[str, pd.DataFrame] = {}
        self._deal_df_cache: Dict[str, pd.DataFrame] = {}
        # DataFrame cache row tracking: how many rows are in each cached DataFrame
        self._tick_df_rows: Dict[str, int] = {}
        self._order_df_rows: Dict[str, int] = {}
        self._deal_df_rows: Dict[str, int] = {}

        # 最新行情快照
        self._quotes: Dict[str, dict] = {}

        # K线数据（按周期索引）
        self._kline_1min: pd.DataFrame = pd.DataFrame()
        self._kline_5min: pd.DataFrame = pd.DataFrame()
        self._kline_10min: pd.DataFrame = pd.DataFrame()
        self._kline_30min: pd.DataFrame = pd.DataFrame()
        self._kline_60min: pd.DataFrame = pd.DataFrame()

        # K线聚合状态
        self._kline_state: Dict[str, dict] = defaultdict(dict)

        # 日频基础数据
        self._daily_basic: pd.DataFrame = pd.DataFrame()

        # 数据更新时间
        self._last_update: Dict[str, datetime] = defaultdict(lambda: datetime.min)

        # 读写锁 (使用RLock，Python 3.12兼容)
        self._rw_lock = threading.RLock()
        # Per-kind lock for mmap buffer access (SDK callback writes, warm thread reads)
        self._buf_locks = {kind: threading.Lock() for kind in ("tick", "order", "deal")}

        # 数据活性时间戳 — SDK 回调每次 append 时更新，主循环用于检测断线
        self._last_append_ts: float = 0.0

        logger.info("MemoryStore 初始化完成 (per-stock list 存储)")

    # ==================== 交易日管理 ====================

    def set_trading_day(self, trading_day: str):
        """设置当前交易日"""
        with self._rw_lock:
            self._trading_day = trading_day
            # 清空当天数据
            self._tick_lists.clear()
            self._order_lists.clear()
            self._deal_lists.clear()
            self._dirty_codes.clear()
            self._dirty_tick.clear()
            self._dirty_deal.clear()
            self._dirty_order.clear()
            self._quotes.clear()
            self._kline_state.clear()
            self._last_append_ts = 0.0
            self._kline_1min = pd.DataFrame()
            self._kline_5min = pd.DataFrame()
            self._kline_10min = pd.DataFrame()
            self._kline_30min = pd.DataFrame()
            self._kline_60min = pd.DataFrame()
            # Reset archive offsets
            self._archive_offset_tick = {}
            self._archive_offset_order = {}
            self._archive_offset_deal = {}
            # Reset Rust buffer and caches
            self._tick_buf.clear()
            self._order_buf.clear()
            self._deal_buf.clear()
            self._tick_df_cache.clear()
            self._order_df_cache.clear()
            self._deal_df_cache.clear()
            self._tick_cache_len.clear()
            self._order_cache_len.clear()
            self._deal_cache_len.clear()
            self._tick_df_rows.clear()
            self._order_df_rows.clear()
            self._deal_df_rows.clear()
            # Clean up mmap files
            self._cleanup_shm_files()
            logger.info(f"设置交易日: {trading_day}, 已清空历史数据")

    def _shm_path(self, kind: str, code: str) -> str:
        """Get mmap file path for a stock buffer."""
        safe_code = code.replace('.', '_')
        return os.path.join(self._shm_dir, f"quant_{kind}_{safe_code}.mmap")

    def _cleanup_shm_files(self):
        """Remove all quant_* mmap files from shm directory."""
        try:
            for f in glob_mod.glob(os.path.join(self._shm_dir, "quant_*.mmap")):
                try:
                    os.unlink(f)
                except OSError:
                    pass
        except Exception:
            pass

    def get_trading_day(self) -> str:
        """获取当前交易日"""
        return self._trading_day

    # ==================== 热路径写入（SDK callback 调用，~50K/sec）====================

    def drain_dirty(self) -> set:
        """Return and reset the set of codes that received new data.
        Called by factor computation to skip unchanged stocks."""
        dirty = self._dirty_codes
        self._dirty_codes = set()
        return dirty

    def drain_dirty_typed(self) -> tuple:
        """Return and reset per-type dirty sets: (tick_codes, deal_codes, order_codes)."""
        tick = self._dirty_tick
        deal = self._dirty_deal
        order = self._dirty_order
        self._dirty_tick = set()
        self._dirty_deal = set()
        self._dirty_order = set()
        return tick, deal, order

    # --- Parsed per-stock list (SDK callback writes) ---

    def _get_or_create_buf(self, kind: str, code: str):
        """Get or create ShmStockBuffer for a stock. Called from hot path."""
        buf_map = getattr(self, f"_{kind}_buf")
        buf = buf_map.get(code)
        if buf is None:
            n_cols, n_str = self._buf_config[kind]
            path = self._shm_path(kind, code)
            buf = mdl_parser.ShmStockBuffer(path, 10000, n_cols, n_str)
            buf_map[code] = buf
        return buf

    def append_tick(self, code: str, row_tuple: tuple) -> None:
        """Append a single tick tuple — writes to ShmStockBuffer only."""
        try:
            with self._buf_locks["tick"]:
                buf = self._get_or_create_buf("tick", code)
                buf.append_tuples([row_tuple], 2)  # start_col=2
        except Exception:
            pass
        self._dirty_codes.add(code)
        self._dirty_tick.add(code)
        self._last_append_ts = time.time()

    def append_order(self, code: str, row_tuple: tuple) -> None:
        """Append a single order tuple — writes to ShmStockBuffer only."""
        try:
            with self._buf_locks["order"]:
                buf = self._get_or_create_buf("order", code)
                buf.append_tuples([row_tuple], 2)
        except Exception:
            pass
        self._dirty_codes.add(code)
        self._dirty_order.add(code)
        self._last_append_ts = time.time()

    def append_deal(self, code: str, row_tuple: tuple) -> None:
        """Append a single deal tuple — writes to ShmStockBuffer only."""
        try:
            with self._buf_locks["deal"]:
                buf = self._get_or_create_buf("deal", code)
                buf.append_tuples([row_tuple], 2)
        except Exception:
            pass
        self._dirty_codes.add(code)
        self._dirty_deal.add(code)
        self._last_append_ts = time.time()

    # ==================== 兼容写入接口（采集器调用）====================

    def update_tick(self, code: str, df: pd.DataFrame) -> None:
        """Legacy: convert DataFrame to tuples and append."""
        if df.empty:
            return
        for _, row in df.iterrows():
            tup = tuple(row.get(c, 0) for c in TICK_COLUMNS)
            self.append_tick(code, tup)
        self._last_update[f"tick_{code}"] = datetime.now()

    def update_order(self, code: str, df: pd.DataFrame) -> None:
        """Legacy: convert DataFrame to tuples and append."""
        if df.empty:
            return
        for _, row in df.iterrows():
            tup = tuple(row.get(c, 0) for c in ORDER_COLUMNS)
            self.append_order(code, tup)
        self._last_update[f"order_{code}"] = datetime.now()

    def update_deal(self, code: str, df: pd.DataFrame) -> None:
        """Legacy: convert DataFrame to tuples and append."""
        if df.empty:
            return
        for _, row in df.iterrows():
            tup = tuple(row.get(c, 0) for c in DEAL_COLUMNS)
            self.append_deal(code, tup)
        self._last_update[f"deal_{code}"] = datetime.now()

    def update_quote(self, code: str, quote: dict):
        """更新最新行情"""
        with self._rw_lock:
            quote['UpdateTime'] = datetime.now()
            self._quotes[code] = quote

    def update_daily_basic(self, df: pd.DataFrame):
        """更新日频基础数据"""
        with self._rw_lock:
            self._daily_basic = df
            logger.info(f"更新日频基础数据: {len(df)} 条")

    def update_kline(self, freq: str, df: pd.DataFrame):
        """更新K线数据"""
        with self._rw_lock:
            attr_name = f"_kline_{freq}"
            if hasattr(self, attr_name):
                setattr(self, attr_name, df)

    # ==================== 读取接口（策略调用，增量 DataFrame 缓存）====================

    def _get_incremental(self, kind: str, code: Optional[str], columns: tuple) -> pd.DataFrame:
        """Core logic for incremental DataFrame retrieval.
        Uses Rust StockBuffer for fast incremental accumulation.
        Supports incremental DataFrame update: only append new rows instead of full rebuild."""
        lock = self._buf_locks[kind]

        if code is None:
            # All stocks combined — build from ShmStockBuffer (no Python lists)
            buf_map = getattr(self, f"_{kind}_buf")
            dfs = []
            base = pd.Timestamp(self._trading_day)
            with lock:
                for c, buf in buf_map.items():
                    if buf.len() == 0:
                        continue
                    arr = buf.to_numpy()
                    buf_cols = columns[2:]
                    df = pd.DataFrame(arr, columns=buf_cols, copy=False)
                    for col in ('Time', 'UpdateTime'):
                        if col in buf_cols:
                            idx = buf_cols.index(col)
                            df[col] = (base.value + (arr[:, idx] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')
                    df.insert(0, 'TradingDay', self._trading_day)
                    df.insert(1, 'Code', c)
                    dfs.append(df[columns])
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        df_cache = getattr(self, f"_{kind}_df_cache")
        df_rows = getattr(self, f"_{kind}_df_rows")
        buf_map = getattr(self, f"_{kind}_buf")

        with lock:
            buf = buf_map.get(code)
            buf_len = buf.len() if buf else 0

            # Fast path: cached DataFrame is up-to-date with buffer
            if code in df_cache and df_rows.get(code, 0) == buf_len and buf_len > 0:
                return df_cache[code]

            # No data
            if buf_len == 0 or buf is None:
                return pd.DataFrame()

            # Buffer was reset (list truncation) — discard cache, full rebuild
            cached_rows = df_rows.get(code, 0)
            if cached_rows > buf_len:
                cached_rows = 0
                df_cache.pop(code, None)

            base_ns = pd.Timestamp(self._trading_day).value

            # Incremental path: append new rows to cached DataFrame
            if cached_rows > 0 and code in df_cache:
                new_arr = buf.to_numpy_from(cached_rows)  # only new rows
                if new_arr.shape[0] == 0:
                    return df_cache[code]

                # Build fragment DataFrame for new rows
                buf_cols = columns[2:]
                new_data = {'TradingDay': self._trading_day, 'Code': code}
                for i, col in enumerate(buf_cols):
                    if col == 'Time' or col == 'UpdateTime':
                        new_data[col] = (base_ns + (new_arr[:, i] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')
                    else:
                        new_data[col] = new_arr[:, i]
                new_df = pd.DataFrame(new_data, columns=columns)

                # Append to cached DataFrame
                df = pd.concat([df_cache[code], new_df], ignore_index=True)
                df_cache[code] = df
                df_rows[code] = buf_len
                return df

            # Full rebuild: no cache exists
            t0 = time.perf_counter()
            arr = buf.to_numpy()                           # (rows, n_cols) f64
            buf_cols = columns[2:]  # skip TradingDay/Code
            data = {'TradingDay': self._trading_day, 'Code': code}
            for i, col in enumerate(buf_cols):
                if col == 'Time' or col == 'UpdateTime':
                    data[col] = (base_ns + (arr[:, i] * 1_000_000_000).astype(np.int64)).astype('datetime64[ns]')
                else:
                    data[col] = arr[:, i]
            df = pd.DataFrame(data, columns=columns)
            build_ms = (time.perf_counter() - t0) * 1000

            if build_ms > 5.0:  # log slow builds (>5ms)
                logger.debug("[df-build-%s] %s: %d rows, %.1fms", kind, code, len(df), build_ms)

            df_cache[code] = df
            df_rows[code] = buf_len
            return df

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        """Get tick data as DataFrame. Uses incremental cache for per-stock queries."""
        return self._get_incremental("tick", code, TICK_COLUMNS)

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        """Get order data as DataFrame. Uses incremental cache for per-stock queries."""
        return self._get_incremental("order", code, ORDER_COLUMNS)

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        """Get deal data as DataFrame. Uses incremental cache for per-stock queries."""
        return self._get_incremental("deal", code, DEAL_COLUMNS)

    # ==================== 批量预热（逐股，线程池前）====================

    def warm_cache_batch(self, kind: str, codes: list) -> int:
        """Sync warm state with buffer row counts.
        Since callbacks write directly to ShmStockBuffer and there are no Python lists,
        this just syncs cache_len from buffer.len() for dirty tracking."""
        buf_map = getattr(self, f"_{kind}_buf")
        cache_len = getattr(self, f"_{kind}_cache_len")
        lock = self._buf_locks[kind]

        t0 = time.perf_counter()
        updated = 0
        with lock:
            for code in codes:
                buf = buf_map.get(code)
                if buf is None:
                    continue
                cur = buf.len()
                cl = cache_len.get(code, 0)
                if cl == cur:
                    continue
                # Sync cache_len from buffer
                cache_len[code] = cur
                updated += 1
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if updated > 0:
            logger.info(
                "[warm-%s] %d/%d stocks synced | %.1fms",
                kind, updated, len(codes), elapsed_ms,
            )
        return updated

    def warm_tick_batch(self, codes: list) -> int:
        return self.warm_cache_batch("tick", codes)

    def warm_deal_batch(self, codes: list) -> int:
        return self.warm_cache_batch("deal", codes)

    def warm_order_batch(self, codes: list) -> int:
        return self.warm_cache_batch("order", codes)

    def get_buffer_paths(self, kind: str, codes: list) -> Dict[str, str]:
        """Return {code: mmap_path} for stocks that have warm buffers.
        Workers use these paths to open read-only mmap handles."""
        buf_map = getattr(self, f"_{kind}_buf")
        return {code: buf.path for code, buf in buf_map.items() if code in codes}

    def get_quote(self, code: str) -> dict:
        """获取单只股票最新行情"""
        with self._rw_lock:
            return self._quotes.get(code, {}).copy()

    def get_all_quotes(self) -> Dict[str, dict]:
        """获取所有股票最新行情"""
        with self._rw_lock:
            return {k: v.copy() for k, v in self._quotes.items()}

    def get_kline(self, freq: str) -> pd.DataFrame:
        """获取K线数据"""
        with self._rw_lock:
            attr_name = f"_kline_{freq}"
            df = getattr(self, attr_name, pd.DataFrame())
            return df.copy() if not df.empty else pd.DataFrame()

    def get_daily_basic(self) -> pd.DataFrame:
        """获取日频基础数据"""
        with self._rw_lock:
            return self._daily_basic.copy()

    def get_all_stocks_1min(self) -> pd.DataFrame:
        """获取所有股票1分钟K线"""
        return self.get_kline("1min")

    def get_all_stocks_5min(self) -> pd.DataFrame:
        """获取所有股票5分钟K线"""
        return self.get_kline("5min")

    def get_all_stocks_10min(self) -> pd.DataFrame:
        """获取所有股票10分钟K线"""
        return self.get_kline("10min")

    # ==================== 快照与清理（磁盘归档用）====================

    def snapshot_and_clear(self, kind: str) -> Dict[str, list]:
        """Swap out all per-stock lists for a data kind, returning the old data.
        Caller gets all the data and internal lists are reset to empty."""
        with self._rw_lock:
            attr = f"_{kind}_lists"
            old = getattr(self, attr)
            result = {}
            for code, lst in old.items():
                if lst:
                    result[code] = lst
            # Reset all lists to empty (keep the same stock keys)
            setattr(self, attr, {code: [] for code in old})
            return result

    def snapshot_incremental(self, kind: str) -> Dict[str, pd.DataFrame]:
        """Return new rows since last snapshot as per-stock DataFrames.
        Reads from ShmStockBuffer (no Python lists). Tracks per-stock buffer offset."""
        buf_map = getattr(self, f"_{kind}_buf")
        offset_attr = f"_archive_offset_{kind}"
        offsets = getattr(self, offset_attr, {})
        columns = {"tick": TICK_COLUMNS, "order": ORDER_COLUMNS, "deal": DEAL_COLUMNS}[kind]
        buf_cols = columns[2:]

        base = pd.Timestamp(self._trading_day)
        result = {}
        for code, buf in buf_map.items():
            current = buf.len()
            start = offsets.get(code, 0)
            if current <= start:
                continue
            arr = buf.to_numpy_from(start)
            if arr.shape[0] == 0:
                continue
            df = pd.DataFrame(arr, columns=buf_cols, copy=False)
            for col in ('Time', 'UpdateTime'):
                if col in buf_cols:
                    df[col] = base + pd.to_timedelta(df[col], unit='s')
            df.insert(0, 'TradingDay', self._trading_day)
            df.insert(1, 'Code', code)
            result[code] = df[columns]
            offsets[code] = current

        setattr(self, offset_attr, offsets)
        return result

    def clear_archive_offset(self, kind: str) -> None:
        """Reset incremental snapshot tracking. Called on trading day rollover."""
        offset_attr = f"_archive_offset_{kind}"
        setattr(self, offset_attr, {})

    # ==================== K线聚合 ====================

    def aggregate_kline(self, code: str, tick_data: dict, freq_minutes: int = 1):
        """聚合K线"""
        with self._rw_lock:
            current_time = datetime.now()
            minute_offset = current_time.minute % freq_minutes
            period_start = current_time.replace(
                minute=current_time.minute - minute_offset,
                second=0, microsecond=0
            )

            state_key = f"{code}_{freq_minutes}"
            state = self._kline_state.get(state_key, {})

            # 如果是新周期
            if state.get("period_start") != period_start:
                # 保存上一根K线
                if "kline" in state:
                    self._save_kline_to_df(state["kline"], freq_minutes)

                # 初始化新K线
                state = {
                    "period_start": period_start,
                    "kline": {
                        "Code": code,
                        "Time": period_start,
                        "Open": tick_data.get("price", 0),
                        "High": tick_data.get("price", 0),
                        "Low": tick_data.get("price", 0),
                        "Close": tick_data.get("price", 0),
                        "Volume": 0,
                        "Amount": 0,
                    }
                }
                self._kline_state[state_key] = state
            else:
                # 更新当前K线
                kline = state["kline"]
                price = tick_data.get("price", 0)
                kline["High"] = max(kline["High"], price)
                kline["Low"] = min(kline["Low"], price) if price > 0 else kline["Low"]
                kline["Close"] = price
                kline["Volume"] += tick_data.get("volume", 0)
                kline["Amount"] += tick_data.get("amount", 0)

    def _save_kline_to_df(self, kline: dict, freq_minutes: int):
        """将K线保存到DataFrame"""
        freq_map = {1: "1min", 5: "5min", 10: "10min", 30: "30min", 60: "60min"}
        freq_name = freq_map.get(freq_minutes, f"{freq_minutes}min")
        attr_name = f"_kline_{freq_name}"

        current_df = getattr(self, attr_name, pd.DataFrame())
        new_row = pd.DataFrame([kline])
        new_df = pd.concat([current_df, new_row], ignore_index=True)
        setattr(self, attr_name, new_df)

    # ==================== 统计信息 ====================

    def get_stats(self) -> dict:
        """获取存储统计信息"""
        with self._rw_lock:
            tick_total = sum(buf.len() for buf in self._tick_buf.values())
            order_total = sum(buf.len() for buf in self._order_buf.values())
            deal_total = sum(buf.len() for buf in self._deal_buf.values())
            tick_stocks = len(self._tick_buf)
            order_stocks = len(self._order_buf)
            deal_stocks = len(self._deal_buf)

            return {
                "trading_day": self._trading_day,
                "tick_stocks": tick_stocks,
                "tick_rows": tick_total,
                "order_stocks": order_stocks,
                "order_rows": order_total,
                "deal_stocks": deal_stocks,
                "deal_rows": deal_total,
                "quote_count": len(self._quotes),
                "kline_1min_count": len(self._kline_1min),
                "kline_5min_count": len(self._kline_5min),
                "kline_10min_count": len(self._kline_10min),
                "daily_basic_count": len(self._daily_basic),
                "last_update": dict(self._last_update),
            }

    def get_memory_usage(self) -> dict:
        """获取内存使用情况（基于 list 长度估算）"""
        # Rough estimate: each tuple row ~ N floats/integers
        # tick: ~80 columns * 8 bytes, order: ~10 * 8, deal: ~12 * 8
        TICK_ROW_BYTES = len(TICK_COLUMNS) * 8
        ORDER_ROW_BYTES = len(ORDER_COLUMNS) * 8
        DEAL_ROW_BYTES = len(DEAL_COLUMNS) * 8

        with self._rw_lock:
            tick_rows = sum(buf.len() for buf in self._tick_buf.values())
            order_rows = sum(buf.len() for buf in self._order_buf.values())
            deal_rows = sum(buf.len() for buf in self._deal_buf.values())

            tick_mem = tick_rows * TICK_ROW_BYTES
            order_mem = order_rows * ORDER_ROW_BYTES
            deal_mem = deal_rows * DEAL_ROW_BYTES

            def get_df_memory(df: pd.DataFrame) -> int:
                return df.memory_usage(deep=True).sum() if not df.empty else 0

            kline_mem = (
                get_df_memory(self._kline_1min) +
                get_df_memory(self._kline_5min) +
                get_df_memory(self._kline_10min)
            )

            return {
                "tick_mb": tick_mem / 1024 / 1024,
                "order_mb": order_mem / 1024 / 1024,
                "deal_mb": deal_mem / 1024 / 1024,
                "kline_mb": kline_mem / 1024 / 1024,
                "total_mb": (tick_mem + order_mem + deal_mem + kline_mem) / 1024 / 1024,
            }
