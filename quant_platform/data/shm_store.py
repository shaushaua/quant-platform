# -*- coding: utf-8 -*-
"""
Arrow IPC 共享内存存储

collector 容器写入 /dev/shm/store/*.arrow
live-engine 容器 mmap 零拷贝读取
两个容器通过 emptyDir(Memory) volume 共享同一目录
"""

import os
import threading
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))


class ShmStore:
    """
    Arrow IPC 共享内存存储，写端（collector）和读端（live_engine）均使用此类。

    写端调用 update_* 方法，读端调用 get_* 方法。
    文件原子替换保证读端不会读到半写状态。
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

    # ------------------------------------------------------------------ #
    # 内部读写                                                              #
    # ------------------------------------------------------------------ #

    def _write_arrow(self, path: Path, df: pd.DataFrame) -> None:
        """将 DataFrame 原子写为 Arrow IPC 文件。"""
        table = pa.Table.from_pandas(df, preserve_index=True)
        tmp = path.with_suffix(".tmp")
        with ipc.new_file(str(tmp), table.schema) as writer:
            writer.write_table(table)
        tmp.replace(path)  # 原子替换，读端不会读到半写状态

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

    def _append_arrow(self, path: Path, df: pd.DataFrame) -> None:
        """追加写：读取已有数据，concat 后原子替换。"""
        with self._lock:
            existing = self._read_arrow(path)
            merged = pd.concat([existing, df], ignore_index=True) if existing is not None else df
            self._write_arrow(path, merged)

    # ------------------------------------------------------------------ #
    # 写接口（collector 调用）                                               #
    # ------------------------------------------------------------------ #

    def update_tick(self, code: str, df: pd.DataFrame) -> None:
        self._append_arrow(SHM_BASE / "tick" / f"{code}.arrow", df)

    def update_order(self, code: str, df: pd.DataFrame) -> None:
        self._append_arrow(SHM_BASE / "order" / f"{code}.arrow", df)

    def update_deal(self, code: str, df: pd.DataFrame) -> None:
        self._append_arrow(SHM_BASE / "deal" / f"{code}.arrow", df)

    def update_kline(self, period: str, df: pd.DataFrame) -> None:
        """整体替换某周期全市场 K 线（聚合后写入）。"""
        with self._lock:
            self._write_arrow(SHM_BASE / "kline" / f"{period}.arrow", df)

    def update_quote(self, code: str, quote: dict) -> None:
        """更新单只股票最新行情快照。"""
        df = pd.DataFrame([quote])
        with self._lock:
            self._write_arrow(SHM_BASE / "quote" / f"{code}.arrow", df)

    def update_daily_basic(self, df: pd.DataFrame) -> None:
        with self._lock:
            self._write_arrow(SHM_BASE / "daily_basic" / "daily_basic.arrow", df)

    def set_trading_day(self, trading_day: str) -> None:
        """将交易日写入标记文件，live-engine 可读取。"""
        (SHM_BASE / "trading_day").write_text(trading_day)

    # ------------------------------------------------------------------ #
    # 读接口（live_engine / DataAPI 调用）                                   #
    # ------------------------------------------------------------------ #

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "tick" / f"{code}.arrow")
            return df if df is not None else pd.DataFrame()
        return self._concat_dir(SHM_BASE / "tick")

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "order" / f"{code}.arrow")
            return df if df is not None else pd.DataFrame()
        return self._concat_dir(SHM_BASE / "order")

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        if code:
            df = self._read_arrow(SHM_BASE / "deal" / f"{code}.arrow")
            return df if df is not None else pd.DataFrame()
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
        for subdir in ["tick", "order", "deal", "kline", "quote", "daily_basic"]:
            d = SHM_BASE / subdir
            if d.exists():
                shutil.rmtree(d)
            d.mkdir(parents=True, exist_ok=True)
