# -*- coding: utf-8 -*-
"""
Arrow IPC 共享内存存储

collector 容器写入 /dev/shm/store/*.arrow
live-engine 容器 mmap 零拷贝读取
两个容器通过 hostPath volume 共享同一目录

存储布局（批量写入，按类型聚合）：
- tick/latest.arrow    所有股票的最新 tick 快照（~5MB）
- order/latest.arrow   所有股票的最新委托（~5MB）
- deal/latest.arrow    所有股票的最新成交（~5MB）
- quote/{code}.arrow   每只股票最新行情（小额，保留按股票存储）
- kline/{period}.arrow K线数据
- daily_basic/daily_basic.arrow  日线基础数据

写入策略：
- 每次 update 直接覆盖 latest.arrow（不做 groupby 拆分）
- 写入耗时从 3000×0.2ms=600ms 降到 1×5ms=5ms
- live_engine 读取后按 code 过滤即可
"""

import logging
import os
import shutil
import threading
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

logger = logging.getLogger(__name__)

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))


class ShmStore:
    """
    Arrow IPC 共享内存存储。

    写入策略（批量覆盖）：
    - tick/order/deal 写入单个 latest.arrow，不做按股票拆分
    - 内存峰值 = 单次写入的 DataFrame 大小
    - 写入耗时 ~5ms（vs 之前按股票拆分 ~600ms）
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
    # 写接口（collector 调用）                                               #
    # ------------------------------------------------------------------ #

    def update_tick(self, df: pd.DataFrame) -> None:
        """覆盖写入所有股票的 tick 数据。"""
        with self._locks["tick"]:
            self._write_arrow(SHM_BASE / "tick" / "latest.arrow", df)

    def update_order(self, df: pd.DataFrame) -> None:
        """覆盖写入所有股票的委托数据。"""
        with self._locks["order"]:
            self._write_arrow(SHM_BASE / "order" / "latest.arrow", df)

    def update_deal(self, df: pd.DataFrame) -> None:
        """覆盖写入所有股票的成交数据。"""
        with self._locks["deal"]:
            self._write_arrow(SHM_BASE / "deal" / "latest.arrow", df)

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
    # 读接口（live_engine / DataAPI 调用）                                   #
    # ------------------------------------------------------------------ #

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "tick" / "latest.arrow")
        if df is None or df.empty:
            return pd.DataFrame()
        if code:
            return df[df["Code"] == code].reset_index(drop=True)
        return df

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "order" / "latest.arrow")
        if df is None or df.empty:
            return pd.DataFrame()
        if code:
            return df[df["Code"] == code].reset_index(drop=True)
        return df

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        df = self._read_arrow(SHM_BASE / "deal" / "latest.arrow")
        if df is None or df.empty:
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

    def clear_day(self) -> None:
        """每日收市后清空共享内存。"""
        for subdir in ["tick", "order", "deal", "kline", "quote", "daily_basic"]:
            d = SHM_BASE / subdir
            if d.exists():
                shutil.rmtree(d)
            d.mkdir(parents=True, exist_ok=True)
