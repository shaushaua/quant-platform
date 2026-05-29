# -*- coding: utf-8 -*-
"""
内存数据存储
存储当天的实时数据，供策略直接读取

内部使用 per-stock list 存储（而非 DataFrame），热路径 append 极快。
get_* 方法按需惰性转换为 DataFrame。
"""

import os
import threading
import logging
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

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

        # Per-stock list 存储: code -> list[tuple]
        self._tick_lists: Dict[str, list] = {}
        self._order_lists: Dict[str, list] = {}
        self._deal_lists: Dict[str, list] = {}

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
            self._quotes.clear()
            self._kline_state.clear()
            self._kline_1min = pd.DataFrame()
            self._kline_5min = pd.DataFrame()
            self._kline_10min = pd.DataFrame()
            self._kline_30min = pd.DataFrame()
            self._kline_60min = pd.DataFrame()
            logger.info(f"设置交易日: {trading_day}, 已清空历史数据")

    def get_trading_day(self) -> str:
        """获取当前交易日"""
        return self._trading_day

    # ==================== 热路径写入（SDK callback 调用，~50K/sec）====================

    def append_tick(self, code: str, row_tuple: tuple) -> None:
        """Append a single tick tuple (very fast)."""
        lst = self._tick_lists.get(code)
        if lst is None:
            lst = []
            self._tick_lists[code] = lst
        lst.append(row_tuple)
        if len(lst) > self._max_rows:
            self._tick_lists[code] = lst[-self._max_rows:]

    def append_order(self, code: str, row_tuple: tuple) -> None:
        """Append a single order tuple (very fast)."""
        lst = self._order_lists.get(code)
        if lst is None:
            lst = []
            self._order_lists[code] = lst
        lst.append(row_tuple)
        if len(lst) > self._max_rows:
            self._order_lists[code] = lst[-self._max_rows:]

    def append_deal(self, code: str, row_tuple: tuple) -> None:
        """Append a single deal tuple (very fast)."""
        lst = self._deal_lists.get(code)
        if lst is None:
            lst = []
            self._deal_lists[code] = lst
        lst.append(row_tuple)
        if len(lst) > self._max_rows:
            self._deal_lists[code] = lst[-self._max_rows:]

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

    # ==================== 读取接口（策略调用，惰性转 DataFrame）====================

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        """Get tick data as DataFrame. Converts from internal list on demand."""
        with self._rw_lock:
            if code is not None:
                lst = self._tick_lists.get(code, [])
                if not lst:
                    return pd.DataFrame()
                snapshot = list(lst)  # copy
                return pd.DataFrame(snapshot, columns=TICK_COLUMNS)
            else:
                # All stocks combined
                all_rows = []
                for lst in self._tick_lists.values():
                    all_rows.extend(lst)
                if not all_rows:
                    return pd.DataFrame()
                return pd.DataFrame(all_rows, columns=TICK_COLUMNS)

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        """Get order data as DataFrame. Converts from internal list on demand."""
        with self._rw_lock:
            if code is not None:
                lst = self._order_lists.get(code, [])
                if not lst:
                    return pd.DataFrame()
                snapshot = list(lst)  # copy
                return pd.DataFrame(snapshot, columns=ORDER_COLUMNS)
            else:
                # All stocks combined
                all_rows = []
                for lst in self._order_lists.values():
                    all_rows.extend(lst)
                if not all_rows:
                    return pd.DataFrame()
                return pd.DataFrame(all_rows, columns=ORDER_COLUMNS)

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        """Get deal data as DataFrame. Converts from internal list on demand."""
        with self._rw_lock:
            if code is not None:
                lst = self._deal_lists.get(code, [])
                if not lst:
                    return pd.DataFrame()
                snapshot = list(lst)  # copy
                return pd.DataFrame(snapshot, columns=DEAL_COLUMNS)
            else:
                # All stocks combined
                all_rows = []
                for lst in self._deal_lists.values():
                    all_rows.extend(lst)
                if not all_rows:
                    return pd.DataFrame()
                return pd.DataFrame(all_rows, columns=DEAL_COLUMNS)

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
