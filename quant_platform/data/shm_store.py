# -*- coding: utf-8 -*-
"""
Arrow IPC 共享内存存储

collector 容器写入 /dev/shm/store/*.arrow
live-engine 容器 mmap 零拷贝读取
两个容器通过 hostPath volume 共享同一目录

写入策略（写回缓存）：
- update_* 做内存 concat（微秒级），标记脏股票
- flush_dirty() 先拷贝脏数据，释放锁，再异步写 arrow 文件，写完清缓冲
- 缓冲只保留自上次 flush 以来的增量数据，内存有界
- 下次 update 同一只股票时从 arrow 文件读取历史累积
"""

import gc
import logging
import os
import threading
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

logger = logging.getLogger(__name__)

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))

# order/deal 每只股票最多保留行数
MAX_ROWS_PER_SYMBOL = int(os.environ.get("SHM_MAX_ROWS_PER_SYMBOL", "10000"))


class ShmStore:
    """
    Arrow IPC 共享内存存储。

    写入策略（写回缓存，低延迟 + 低内存）：
    - update_* 在内存中累积（微秒级）
    - flush_dirty() 拷贝脏数据后释放锁，写完清缓冲
    - 缓冲只保留增量，内存有界（~500 只股票 / 批次）
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

        # 内存缓冲：code -> DataFrame（仅保留自上次 flush 以来的增量）
        self._tick_buffer: Dict[str, pd.DataFrame] = {}
        self._order_buffer: Dict[str, pd.DataFrame] = {}
        self._deal_buffer: Dict[str, pd.DataFrame] = {}

        # 脏集
        self._dirty_tick: Set[str] = set()
        self._dirty_order: Set[str] = set()
        self._dirty_deal: Set[str] = set()

        self._flush_count = 0

    # ------------------------------------------------------------------ #
    # 内部读写                                                              #
    # ------------------------------------------------------------------ #

    def _write_arrow(self, path: Path, df: pd.DataFrame) -> None:
        """将 DataFrame 原子写为 Arrow IPC 文件。"""
        table = pa.Table.from_pandas(df, preserve_index=False)
        tmp = path.with_suffix(".tmp")
        with ipc.new_file(str(tmp), table.schema) as writer:
            writer.write_table(table)
        tmp.replace(path)

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
        code: str, df: pd.DataFrame, subdir: str
    ) -> None:
        """
        将新数据累积到内存缓冲（调用者需持有 _lock）。
        如果缓冲中没有该股票，从 arrow 文件读取历史数据累积。
        """
        if code in buffer:
            # 缓冲命中：直接内存 concat（微秒级）
            old = buffer[code]
            new_rows = len(df)
            total = len(old) + new_rows
            if total > MAX_ROWS_PER_SYMBOL:
                # 超限：只保留最新行
                keep = MAX_ROWS_PER_SYMBOL - new_rows
                if keep > 0:
                    merged = pd.concat([old.iloc[-keep:], df], ignore_index=True)
                else:
                    merged = df.iloc[-MAX_ROWS_PER_SYMBOL:].copy()
                del old
            else:
                merged = pd.concat([old, df], ignore_index=True)
                del old
            buffer[code] = merged
        else:
            # 缓冲未命中：从 arrow 文件读取历史，再 concat
            path = SHM_BASE / subdir / f"{code}.arrow"
            existing = self._read_arrow(path)
            if existing is not None and not existing.empty:
                total = len(existing) + len(df)
                if total > MAX_ROWS_PER_SYMBOL:
                    keep = MAX_ROWS_PER_SYMBOL - len(df)
                    if keep > 0:
                        merged = pd.concat([existing.iloc[-keep:], df], ignore_index=True)
                    else:
                        merged = df.iloc[-MAX_ROWS_PER_SYMBOL:].copy()
                else:
                    merged = pd.concat([existing, df], ignore_index=True)
                del existing
                buffer[code] = merged
            else:
                buffer[code] = df if len(df) <= MAX_ROWS_PER_SYMBOL else df.iloc[-MAX_ROWS_PER_SYMBOL:].copy()
        dirty.add(code)

    # ------------------------------------------------------------------ #
    # 写接口（collector 调用）                                               #
    # ------------------------------------------------------------------ #

    def update_tick(self, code: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._buffer_update(self._tick_buffer, self._dirty_tick, code, df, "tick")

    def update_order(self, code: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._buffer_update(self._order_buffer, self._dirty_order, code, df, "order")

    def update_deal(self, code: str, df: pd.DataFrame) -> None:
        with self._lock:
            self._buffer_update(self._deal_buffer, self._dirty_deal, code, df, "deal")

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
        刷写脏股票到 arrow 文件。
        关键：先拷贝脏数据，释放锁，再写文件（不阻塞 update）。
        写完后清空缓冲，下次 update 从 arrow 文件读历史。
        """
        # 1. 快速拷贝脏数据，释放锁
        with self._lock:
            to_flush: List[Tuple[str, str, pd.DataFrame]] = []
            for dirty, buffer, subdir in [
                (self._dirty_tick, self._tick_buffer, "tick"),
                (self._dirty_order, self._order_buffer, "order"),
                (self._dirty_deal, self._deal_buffer, "deal"),
            ]:
                for code in dirty:
                    if code in buffer:
                        to_flush.append((subdir, code, buffer[code]))
                dirty.clear()
            # 清空缓冲（arrow 文件将是数据的唯一来源）
            self._tick_buffer.clear()
            self._order_buffer.clear()
            self._deal_buffer.clear()

        if not to_flush:
            return

        # 2. 释放锁后写 arrow 文件（不阻塞 update）
        for subdir, code, df in to_flush:
            try:
                self._write_arrow(SHM_BASE / subdir / f"{code}.arrow", df)
            except Exception as e:
                logger.warning("[ShmStore] 写 %s/%s 失败: %s", subdir, code, e)

        # 3. 清理
        self._flush_count += 1
        if self._flush_count % 100 == 0:
            gc.collect()
            logger.info("[ShmStore] 第 %d 次刷写: %d 只股票", self._flush_count, len(to_flush))

    def flush(self) -> None:
        """强制刷写所有缓冲（停机前调用）。"""
        with self._lock:
            to_flush: List[Tuple[str, str, pd.DataFrame]] = []
            for buffer, subdir in [
                (self._tick_buffer, "tick"),
                (self._order_buffer, "order"),
                (self._deal_buffer, "deal"),
            ]:
                for code, df in buffer.items():
                    to_flush.append((subdir, code, df))
            self._tick_buffer.clear()
            self._order_buffer.clear()
            self._deal_buffer.clear()
            self._dirty_tick.clear()
            self._dirty_order.clear()
            self._dirty_deal.clear()

        for subdir, code, df in to_flush:
            try:
                self._write_arrow(SHM_BASE / subdir / f"{code}.arrow", df)
            except Exception as e:
                logger.warning("[ShmStore] 写 %s/%s 失败: %s", subdir, code, e)
        gc.collect()
        logger.info("[ShmStore] 全量刷写: %d 只股票", len(to_flush))

    # ------------------------------------------------------------------ #
    # 读接口（live_engine / DataAPI 调用）                                   #
    # ------------------------------------------------------------------ #

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "tick" / f"{code}.arrow")
            if df is not None:
                return df
            return pd.DataFrame()
        return self._concat_dir(SHM_BASE / "tick")

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "order" / f"{code}.arrow")
            if df is not None:
                return df
            return pd.DataFrame()
        return self._concat_dir(SHM_BASE / "order")

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "deal" / f"{code}.arrow")
            if df is not None:
                return df
            return pd.DataFrame()
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
        """每日收市后清空共享内存。"""
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
