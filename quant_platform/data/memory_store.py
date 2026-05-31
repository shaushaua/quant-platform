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

        # Incremental numpy cache: accumulate as ndarray (fast), convert to DataFrame at read time
        self._tick_np_cache: Dict[str, np.ndarray] = {}
        self._order_np_cache: Dict[str, np.ndarray] = {}
        self._deal_np_cache: Dict[str, np.ndarray] = {}
        self._tick_cache_len: Dict[str, int] = {}
        self._order_cache_len: Dict[str, int] = {}
        self._deal_cache_len: Dict[str, int] = {}
        # DataFrame cache: cache the last built DataFrame for cache-hit fast path
        self._tick_df_cache: Dict[str, pd.DataFrame] = {}
        self._order_df_cache: Dict[str, pd.DataFrame] = {}
        self._deal_df_cache: Dict[str, pd.DataFrame] = {}

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
            # Reset incremental numpy cache
            self._tick_np_cache.clear()
            self._order_np_cache.clear()
            self._deal_np_cache.clear()
            self._tick_df_cache.clear()
            self._order_df_cache.clear()
            self._deal_df_cache.clear()
            self._tick_cache_len.clear()
            self._order_cache_len.clear()
            self._deal_cache_len.clear()
            logger.info(f"设置交易日: {trading_day}, 已清空历史数据")

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

    # --- Parsed per-stock list (SDK callback writes) ---

    def append_tick(self, code: str, row_tuple: tuple) -> None:
        """Append a single tick tuple (very fast)."""
        lst = self._tick_lists.get(code)
        if lst is None:
            lst = []
            self._tick_lists[code] = lst
        lst.append(row_tuple)
        self._dirty_codes.add(code)
        if len(lst) > self._max_rows:
            # CAS: only trim if no other thread replaced the list
            new_lst = lst[-(self._max_rows // 2):]
            if self._tick_lists.get(code) is lst:
                self._tick_lists[code] = new_lst
        self._last_append_ts = time.time()

    def append_order(self, code: str, row_tuple: tuple) -> None:
        """Append a single order tuple (very fast)."""
        lst = self._order_lists.get(code)
        if lst is None:
            lst = []
            self._order_lists[code] = lst
        lst.append(row_tuple)
        self._dirty_codes.add(code)
        if len(lst) > self._max_rows:
            new_lst = lst[-(self._max_rows // 2):]
            if self._order_lists.get(code) is lst:
                self._order_lists[code] = new_lst
        self._last_append_ts = time.time()

    def append_deal(self, code: str, row_tuple: tuple) -> None:
        """Append a single deal tuple (very fast)."""
        lst = self._deal_lists.get(code)
        if lst is None:
            lst = []
            self._deal_lists[code] = lst
        lst.append(row_tuple)
        self._dirty_codes.add(code)
        if len(lst) > self._max_rows:
            new_lst = lst[-(self._max_rows // 2):]
            if self._deal_lists.get(code) is lst:
                self._deal_lists[code] = new_lst
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
        Uses numpy ndarray for fast incremental accumulation,
        converts to DataFrame only at read time.
        After warm_cache_batch(), only DataFrame build remains (thread-safe)."""
        if code is None:
            # All stocks combined — no cache, build from scratch (rare path)
            lists = getattr(self, f"_{kind}_lists")
            all_rows = []
            for lst in lists.values():
                all_rows.extend(lst)
            return pd.DataFrame(all_rows, columns=columns) if all_rows else pd.DataFrame()

        lst = getattr(self, f"_{kind}_lists").get(code, [])
        if not lst:
            return pd.DataFrame()

        np_cache = getattr(self, f"_{kind}_np_cache")
        df_cache = getattr(self, f"_{kind}_df_cache")
        cache_len = getattr(self, f"_{kind}_cache_len")

        cached_len = cache_len.get(code, 0)
        current_len = len(lst)

        # No new data → return cached DataFrame directly (fast path)
        if cached_len == current_len and code in df_cache:
            return df_cache[code]

        # Numpy already warmed (cache_len matches) but DataFrame not built yet
        if cached_len == current_len and code in np_cache:
            df = pd.DataFrame(np_cache[code], columns=columns)
            df_cache[code] = df
            return df

        # Numpy cache needs update (not pre-warmed, or new data arrived after warm)
        if cached_len > current_len:
            cached_len = 0

        new_rows = lst[cached_len:]
        new_np = np.array(new_rows)

        if cached_len == 0 or code not in np_cache:
            np_cache[code] = new_np
        else:
            np_cache[code] = np.concatenate([np_cache[code], new_np])

        cache_len[code] = current_len
        df = pd.DataFrame(np_cache[code], columns=columns)
        df_cache[code] = df
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

    # ==================== 批量预热（一次 numpy 转换）====================

    def warm_cache_batch(self, kind: str, codes: list) -> int:
        """Pre-warm numpy caches for all codes in batch.
        Collects ALL new rows into one list, converts with ONE np.array() call,
        then splits back per stock. Avoids per-stock np.array() overhead.
        After this, get_*() only needs pd.DataFrame build (thread-safe)."""
        np_cache = getattr(self, f"_{kind}_np_cache")
        cache_len = getattr(self, f"_{kind}_cache_len")
        df_cache = getattr(self, f"_{kind}_df_cache")
        lists = getattr(self, f"_{kind}_lists")

        # Phase 1: Collect all new rows, track per-stock boundaries
        all_new_rows = []
        code_meta = {}  # code -> (start, end, is_rebuild)
        offset = 0

        for code in codes:
            lst = lists.get(code, [])
            if not lst:
                continue
            cl = cache_len.get(code, 0)
            cur = len(lst)
            if cl == cur and code in df_cache:
                continue  # Already fully warm
            if cl > cur:
                cl = 0
            new_rows = lst[cl:]
            if not new_rows:
                cache_len[code] = cur
                continue
            is_rebuild = (cl == 0 or code not in np_cache)
            all_new_rows.extend(new_rows)
            code_meta[code] = (offset, offset + len(new_rows), is_rebuild)
            offset += len(new_rows)
            cache_len[code] = cur

        if not all_new_rows:
            return 0

        # Phase 2: ONE np.array conversion for ALL new rows (major speedup)
        big_np = np.array(all_new_rows) if all_new_rows else np.empty((0,))

        # Phase 3: Split back per stock
        for code, (s, e, is_rebuild) in code_meta.items():
            chunk = big_np[s:e]  # View into big_np
            if is_rebuild:
                np_cache[code] = chunk.copy()  # Own its memory
            else:
                np_cache[code] = np.concatenate([np_cache[code], chunk])

        return len(code_meta)

    def warm_tick_batch(self, codes: list) -> int:
        return self.warm_cache_batch("tick", codes)

    def warm_deal_batch(self, codes: list) -> int:
        return self.warm_cache_batch("deal", codes)

    def warm_order_batch(self, codes: list) -> int:
        return self.warm_cache_batch("order", codes)

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

    def snapshot_incremental(self, kind: str) -> Dict[str, list]:
        """Return new rows since last snapshot for each stock, WITHOUT clearing.
        Tracks per-stock offset so next call only returns rows appended since then.
        Data stays in memory for factor computation (full-day accumulation)."""
        attr = f"_{kind}_lists"
        offset_attr = f"_archive_offset_{kind}"
        lists = getattr(self, attr)
        offsets = getattr(self, offset_attr, {})

        result = {}
        for code, lst in lists.items():
            start = offsets.get(code, 0)
            if len(lst) > start:
                # list[start:] creates a copy of the new portion
                result[code] = list(lst[start:])
                offsets[code] = len(lst)

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
            tick_total = sum(len(lst) for lst in self._tick_lists.values())
            order_total = sum(len(lst) for lst in self._order_lists.values())
            deal_total = sum(len(lst) for lst in self._deal_lists.values())
            tick_stocks = len(self._tick_lists)
            order_stocks = len(self._order_lists)
            deal_stocks = len(self._deal_lists)

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
            tick_rows = sum(len(lst) for lst in self._tick_lists.values())
            order_rows = sum(len(lst) for lst in self._order_lists.values())
            deal_rows = sum(len(lst) for lst in self._deal_lists.values())

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
