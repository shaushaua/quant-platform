# -*- coding: utf-8 -*-
"""
Arrow IPC 共享内存存储

collector 容器写入 /dev/shm/store/*.arrow
live-engine 容器 mmap 零拷贝读取
两个容器通过 hostPath volume 共享同一目录

写入策略：内存缓冲累积 + 每次 batch 后立即刷写脏股票
- update_* 做内存 concat（微秒级），标记脏股票
- flush_dirty() 将脏股票写入 arrow 文件（/dev/shm 写入 ~0.1ms/股票）
- 写入 /dev/shm 是内存拷贝，不涉及磁盘 I/O，延迟极低
"""

import gc
import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Set

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

logger = logging.getLogger(__name__)

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))

# order/deal 每只股票最多保留行数，防止数据撑爆共享内存
MAX_ROWS_PER_SYMBOL = int(os.environ.get("SHM_MAX_ROWS_PER_SYMBOL", "10000"))

# 三类缓冲（tick+order+deal）的总行数上限
# 5000 只 × 6000 行 × ~100 bytes ≈ 3GB，4Gi 容器下安全
MAX_TOTAL_BUFFER_ROWS = int(os.environ.get("SHM_MAX_TOTAL_ROWS", "30000000"))


class ShmStore:
    """
    Arrow IPC 共享内存存储。

    写入策略（低延迟）：
    - update_* 做内存 concat（微秒级），标记脏股票
    - flush_dirty() 由 collector 在每个 chunk 处理完后调用
    - 只写脏股票（增量刷写），不是全量刷写
    - /dev/shm 是 RAM disk，写入延迟 ~0.1ms/股票
    """

    def __init__(self):
        for d in [
            SHM_BASE / "tick",
            SHM_BASE / "order",
            SHM_BASE / "deal",
            SHM_BASE / "kline",
            SHM_BASE / "quote",
            SHM_BASE / "daily_basic",
        ]:
            d.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        # 内存缓冲：code -> DataFrame（累积最近 max_rows 行）
        self._tick_buffer: Dict[str, pd.DataFrame] = {}
        self._order_buffer: Dict[str, pd.DataFrame] = {}
        self._deal_buffer: Dict[str, pd.DataFrame] = {}

        # 脏集：记录自上次 flush_dirty 以来哪些股票被更新
        self._dirty_tick: Set[str] = set()
        self._dirty_order: Set[str] = set()
        self._dirty_deal: Set[str] = set()

        self._flush_count = 0
        self._total_buffer_rows = 0  # 跟踪三类缓冲总行数

    # ------------------------------------------------------------------ #
    # 内部读写                                                              #
    # ------------------------------------------------------------------ #

    def _write_arrow(self, path: Path, df: pd.DataFrame) -> None:
        """将 DataFrame 原子写为 Arrow IPC 文件。"""
        table = pa.Table.from_pandas(df, preserve_index=True)
        tmp = path.with_suffix(".tmp")
        with ipc.new_file(str(tmp), table.schema) as writer:
            writer.write_table(table)
        tmp.replace(path)  # 原子替换

    def _read_arrow(self, path: Path) -> Optional[pd.DataFrame]:
        """mmap 零拷贝读取 Arrow IPC 文件。"""
        if not path.exists():
            return None
        try:
            mmap = ipc.MemoryMappedFile(str(path), mode="r")
            with ipc.open_file(mmap) as reader:
                return reader.read_pandas()
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # 缓冲管理                                                              #
    # ------------------------------------------------------------------ #

    def _buffer_update(
        self, buffer: Dict[str, pd.DataFrame], dirty: Set[str],
        code: str, df: pd.DataFrame
    ) -> None:
        """将新数据累积到内存缓冲，标记脏（调用者需持有 _lock）。"""
        incoming_rows = len(df)
        old_rows = len(buffer.get(code, []))

        if code in buffer:
            old = buffer[code]
            new_total = old_rows + incoming_rows
            if new_total > MAX_ROWS_PER_SYMBOL:
                # 超限：只保留最新 max_rows 行
                merged = pd.concat([old, df.iloc[-(MAX_ROWS_PER_SYMBOL):]], ignore_index=True)
                merged = merged.iloc[-MAX_ROWS_PER_SYMBOL:]
                self._total_buffer_rows += MAX_ROWS_PER_SYMBOL - old_rows
            else:
                merged = pd.concat([old, df], ignore_index=True)
                self._total_buffer_rows += incoming_rows
            del old
            buffer[code] = merged
        else:
            if incoming_rows > MAX_ROWS_PER_SYMBOL:
                buffer[code] = df.iloc[-MAX_ROWS_PER_SYMBOL:].copy()
                self._total_buffer_rows += MAX_ROWS_PER_SYMBOL
            else:
                buffer[code] = df
                self._total_buffer_rows += incoming_rows
        dirty.add(code)

    # ------------------------------------------------------------------ #
    # 写接口（collector 调用）                                               #
    # ------------------------------------------------------------------ #

    def update_tick(self, code: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._buffer_update(self._tick_buffer, self._dirty_tick, code, df)

    def update_order(self, code: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._buffer_update(self._order_buffer, self._dirty_order, code, df)

    def update_deal(self, code: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._buffer_update(self._deal_buffer, self._dirty_deal, code, df)

    def update_kline(self, period: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._write_arrow(SHM_BASE / "kline" / f"{period}.arrow", df)

    def update_quote(self, code: str, quote: dict) -> None:
        df = pd.DataFrame([quote])
        with self._lock:
            self._write_arrow(SHM_BASE / "quote" / f"{code}.arrow", df)

    def update_daily_basic(self, df: pd.DataFrame) -> None:
        with self._lock:
            self._write_arrow(SHM_BASE / "daily_basic" / "daily_basic.arrow", df)

    def set_trading_day(self, trading_day: str) -> None:
        (SHM_BASE / "trading_day").write_text(trading_day)

    def flush_dirty(self) -> None:
        """
        将脏股票的缓冲刷写到 arrow 文件。
        由 collector 在每个 chunk 处理完后调用。
        /dev/shm 是 RAM disk，写入延迟 ~0.1ms/股票。
        """
        with self._lock:
            total = 0
            for dirty, buffer, subdir in [
                (self._dirty_tick, self._tick_buffer, "tick"),
                (self._dirty_order, self._order_buffer, "order"),
                (self._dirty_deal, self._deal_buffer, "deal"),
            ]:
                for code in dirty:
                    if code in buffer:
                        self._write_arrow(
                            SHM_BASE / subdir / f"{code}.arrow", buffer[code]
                        )
                        total += 1
                dirty.clear()

            if total > 0:
                self._flush_count += 1
                # 总缓冲超限时强制 gc + 日志告警
                if self._total_buffer_rows > MAX_TOTAL_BUFFER_ROWS:
                    gc.collect()
                    logger.warning(
                        "[ShmStore] 缓冲总行数 %d 超过上限 %d，已 gc",
                        self._total_buffer_rows, MAX_TOTAL_BUFFER_ROWS,
                    )
                elif self._flush_count % 100 == 0:
                    gc.collect()
                    logger.info(
                        "[ShmStore] 第 %d 次刷写: %d 只股票, 缓冲总行数 %d",
                        self._flush_count, total, self._total_buffer_rows,
                    )

    def flush(self) -> None:
        """强制刷写所有缓冲到磁盘（停机前调用）。"""
        with self._lock:
            total = 0
            for buffer, subdir in [
                (self._tick_buffer, "tick"),
                (self._order_buffer, "order"),
                (self._deal_buffer, "deal"),
            ]:
                for code, df in buffer.items():
                    self._write_arrow(SHM_BASE / subdir / f"{code}.arrow", df)
                    total += 1
            self._dirty_tick.clear()
            self._dirty_order.clear()
            self._dirty_deal.clear()
            gc.collect()
            logger.info("[ShmStore] 全量刷写: %d 只股票", total)

    # ------------------------------------------------------------------ #
    # 读接口（live_engine / DataAPI 调用）                                   #
    # ------------------------------------------------------------------ #

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "tick" / f"{code}.arrow")
            if df is not None:
                return df
            with self._lock:
                return self._tick_buffer.get(code, pd.DataFrame()).copy()
        return self._concat_dir(SHM_BASE / "tick")

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "order" / f"{code}.arrow")
            if df is not None:
                return df
            with self._lock:
                return self._order_buffer.get(code, pd.DataFrame()).copy()
        return self._concat_dir(SHM_BASE / "order")

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "deal" / f"{code}.arrow")
            if df is not None:
                return df
            with self._lock:
                return self._deal_buffer.get(code, pd.DataFrame()).copy()
        return self._concat_dir(SHM_BASE / "deal")

    def get_kline(self, period: str) -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "kline" / f"{period}.arrow")
        return df if df is not None else pd.DataFrame()

    def get_quote(self, code: str) -> dict:
        df = self._read_arrow(SHM_BASE / "quote" / f"{code}.arrow")
        if df is None or df.empty:
            return {}
        return df.iloc[0].to_dict()

    def get_all_quotes(self) -> Dict[str, dict]:
        result = {}
        quote_dir = SHM_BASE / "quote"
        for f in quote_dir.glob("*.arrow"):
            code = f.stem
            df = self._read_arrow(f)
            if df is not None and not df.empty:
                result[code] = df.iloc[0].to_dict()
        return result

    def get_daily_basic(self) -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "daily_basic" / "daily_basic.arrow")
        return df if df is not None else pd.DataFrame()

    def get_trading_day(self) -> Optional[str]:
        p = SHM_BASE / "trading_day"
        return p.read_text().strip() if p.exists() else None

    # ------------------------------------------------------------------ #
    # 工具方法                                                              #
    # ------------------------------------------------------------------ #

    def _concat_dir(self, directory: Path) -> pd.DataFrame:
        """读取目录下所有 .arrow 文件并 concat。"""
        frames = []
        for f in sorted(directory.glob("*.arrow")):
            df = self._read_arrow(f)
            if df is not None:
                frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def clear_day(self) -> None:
        """每日收市后清空共享内存（collector 调用）。"""
        import shutil
        with self._lock:
            self._tick_buffer.clear()
            self._order_buffer.clear()
            self._deal_buffer.clear()
            self._dirty_tick.clear()
            self._dirty_order.clear()
            self._dirty_deal.clear()
        for subdir in ["tick", "order", "deal", "kline", "quote", "daily_basic"]:
            d = SHM_BASE / subdir
            if d.exists():
                shutil.rmtree(d)
            d.mkdir(parents=True, exist_ok=True)
