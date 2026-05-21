# -*- coding: utf-8 -*-
"""
Arrow IPC 共享内存存储（滚动 chunk 模式）

collector 容器写入 /dev/shm/store/*.arrow
live-engine 容器 mmap 零拷贝读取
两个容器通过 hostPath volume 共享同一目录

存储布局（滚动 chunk，保留最近 N 分钟数据）：
- tick/chunk_{timestamp_ms}.arrow   所有股票的 tick 数据（每个 chunk 一个文件）
- order/chunk_{timestamp_ms}.arrow  所有股票的委托数据
- deal/chunk_{timestamp_ms}.arrow   所有股票的成交数据
- quote/{code}.arrow                每只股票最新行情（单文件覆盖，不需滚动）
- kline/{period}.arrow              K线数据（单文件覆盖）
- daily_basic/daily_basic.arrow     日线基础数据（单文件覆盖）

写入策略：
- 每次 update 写入新的 chunk 文件（时间戳命名）
- 后台定期清理超过 ROLLING_WINDOW_SECONDS 的旧文件
- live_engine 读取时 concat 所有 chunk 文件，按 code 过滤
"""

import itertools
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

logger = logging.getLogger(__name__)

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))

# 滚动窗口时长（秒），默认 5 分钟
ROLLING_WINDOW_SECONDS = int(os.environ.get("ROLLING_WINDOW_SECONDS", "300"))

# 需要滚动 chunk 的数据类型
_ROLLING_TYPES = {"tick", "order", "deal"}


class ShmStore:
    """
    Arrow IPC 共享内存存储（滚动 chunk 模式）。

    tick/order/deal：每次写入新的 timestamped chunk，后台清理旧文件。
    quote/kline/daily_basic：单文件覆盖（数据量小，不需滚动）。
    """

    # 递增序列号，保证 chunk 文件名唯一
    _seq = itertools.count(int(time.time() * 1000))

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
        # 按数据类型分锁，多线程写入不同类型时互不阻塞
        self._locks = {
            "tick": threading.Lock(),
            "order": threading.Lock(),
            "deal": threading.Lock(),
            "kline": threading.Lock(),
            "quote": threading.Lock(),
            "daily_basic": threading.Lock(),
        }

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
        """读取 Arrow IPC 文件。"""
        if not path.exists():
            return None
        try:
            reader = ipc.open_file(str(path))
            return reader.read_all().to_pandas()
        except Exception:
            return None

    def _read_all_chunks(self, data_type: str) -> pd.DataFrame:
        """
        读取某个类型的所有 chunk 文件并合并。
        用于 tick/order/deal 滚动模式。
        """
        chunk_dir = SHM_BASE / data_type
        if not chunk_dir.exists():
            return pd.DataFrame()

        chunks = sorted(chunk_dir.glob("chunk_*.arrow"))
        if not chunks:
            return pd.DataFrame()

        dfs = []
        for f in chunks:
            df = self._read_arrow(f)
            if df is not None and not df.empty:
                dfs.append(df)

        if not dfs:
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        return result

    # ------------------------------------------------------------------ #
    # 写接口（collector 调用）                                               #
    # ------------------------------------------------------------------ #

    def update_tick(self, df: pd.DataFrame) -> Optional[str]:
        """写入新的 tick chunk，返回 chunk 文件名。"""
        with self._locks["tick"]:
            ts = int(time.time() * 1000)
            seq = next(self._seq)
            name = f"chunk_{ts}_{seq:06d}.arrow"
            self._write_arrow(SHM_BASE / "tick" / name, df)
            return name

    def update_order(self, df: pd.DataFrame) -> Optional[str]:
        """写入新的 order chunk，返回 chunk 文件名。"""
        with self._locks["order"]:
            ts = int(time.time() * 1000)
            seq = next(self._seq)
            name = f"chunk_{ts}_{seq:06d}.arrow"
            self._write_arrow(SHM_BASE / "order" / name, df)
            return name

    def update_deal(self, df: pd.DataFrame) -> Optional[str]:
        """写入新的 deal chunk，返回 chunk 文件名。"""
        with self._locks["deal"]:
            ts = int(time.time() * 1000)
            seq = next(self._seq)
            name = f"chunk_{ts}_{seq:06d}.arrow"
            self._write_arrow(SHM_BASE / "deal" / name, df)
            return name

    def update_kline(self, period: str, df: pd.DataFrame) -> None:
        with self._locks["kline"]:
            self._write_arrow(SHM_BASE / "kline" / f"{period}.arrow", df)

    def update_quote(self, code: str, quote: dict) -> None:
        df = pd.DataFrame([quote])
        with self._locks["quote"]:
            self._write_arrow(SHM_BASE / "quote" / f"{code}.arrow", df)

    def update_daily_basic(self, df: pd.DataFrame) -> None:
        with self._locks["daily_basic"]:
            self._write_arrow(SHM_BASE / "daily_basic" / "daily_basic.arrow", df)

    def set_trading_day(self, trading_day: str) -> None:
        (SHM_BASE / "trading_day").write_text(trading_day)

    def flush(self) -> None:
        """无操作（兼容接口）。"""
        pass

    def flush_dirty(self) -> None:
        """无操作（兼容接口）。"""
        pass

    # ------------------------------------------------------------------ #
    # 滚动清理                                                              #
    # ------------------------------------------------------------------ #

    def cleanup_rolling(self) -> int:
        """
        清理超过 ROLLING_WINDOW_SECONDS 的旧 chunk 文件。
        返回清理的文件数量。
        """
        cutoff = time.time() - ROLLING_WINDOW_SECONDS
        cleaned = 0

        for data_type in _ROLLING_TYPES:
            chunk_dir = SHM_BASE / data_type
            if not chunk_dir.exists():
                continue

            with self._locks[data_type]:
                for f in chunk_dir.glob("chunk_*.arrow"):
                    try:
                        if f.stat().st_mtime < cutoff:
                            f.unlink()
                            cleaned += 1
                    except OSError:
                        pass

        if cleaned:
            logger.info("[滚动清理] 已删除 %d 个过期 chunk 文件（窗口=%ds）",
                        cleaned, ROLLING_WINDOW_SECONDS)
        return cleaned

    # ------------------------------------------------------------------ #
    # 读接口（live_engine / DataAPI 调用）                                   #
    # ------------------------------------------------------------------ #

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        df = self._read_all_chunks("tick")
        if df.empty:
            return pd.DataFrame()
        if code:
            return df[df["Code"] == code].reset_index(drop=True)
        return df

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        df = self._read_all_chunks("order")
        if df.empty:
            return pd.DataFrame()
        if code:
            return df[df["Code"] == code].reset_index(drop=True)
        return df

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        df = self._read_all_chunks("deal")
        if df.empty:
            return pd.DataFrame()
        if code:
            return df[df["Code"] == code].reset_index(drop=True)
        return df

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

    def get_all_codes(self, data_type: str = "tick") -> List[str]:
        """
        获取某个数据类型中所有出现过的股票代码。
        扫描所有 chunk 文件提取 unique Code 值。
        """
        chunk_dir = SHM_BASE / data_type
        if not chunk_dir.exists():
            return []

        chunks = sorted(chunk_dir.glob("chunk_*.arrow"))
        if not chunks:
            return []

        codes = set()
        for f in chunks:
            df = self._read_arrow(f)
            if df is not None and not df.empty and "Code" in df.columns:
                codes.update(df["Code"].unique().tolist())

        return sorted(codes)

    def clear_day(self) -> None:
        """每日收市后清空共享内存。"""
        for subdir in ["tick", "order", "deal", "kline", "quote", "daily_basic"]:
            d = SHM_BASE / subdir
            if d.exists():
                shutil.rmtree(d)
            d.mkdir(parents=True, exist_ok=True)
