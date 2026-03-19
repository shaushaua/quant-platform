# -*- coding: utf-8 -*-
"""
Collector：监听通联客户端实时写入的 CSV 文件，增量解析后写入 ShmStore

通联文件命名规律：
    /root/mdl/msg_backup/YYYYMMDD/
        YYYYMMDD_mdl_4_16_0.csv   深交所 逐笔委托 (channel 16)
        YYYYMMDD_mdl_4_17_0.csv   深交所 逐笔委托 (channel 17)
        YYYYMMDD_mdl_4_23_0.csv   深交所 逐笔成交 (channel 23)
        YYYYMMDD_mdl_4_24_0.csv   深交所 逐笔成交 (channel 24)
        YYYYMMDD_mdl_6_28_0.csv   上交所 tick快照 (channel 28, shard 0)
        YYYYMMDD_mdl_6_28_1.csv   上交所 tick快照 (channel 28, shard 1)
        YYYYMMDD_mdl_6_50.csv     上交所 逐笔委托
        YYYYMMDD_mdl_6_51.csv     上交所 逐笔成交
        YYYYMMDD_OrderQueue.csv   委托队列（暂不处理）

市场编号：4=深交所(SZ)  6=上交所(SH)
类型编号：
    SZ: 16/17=order  23/24=deal  28-49=tick
    SH: 50/51=order  52=deal     28-49=tick (同上)
"""

import logging
import os
import re
import threading
import time
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from ..data.converter import TonglanceDataConverter
from ..data.shm_store import ShmStore

logger = logging.getLogger(__name__)

# 通联 msg_backup 根目录
MSG_BACKUP_DIR = Path(os.environ.get("MSG_BACKUP_DIR", "/root/mdl/msg_backup"))

# poll 间隔（秒）
POLL_INTERVAL = float(os.environ.get("COLLECTOR_POLL_INTERVAL", "0.5"))

# 启动时是否跳过历史数据（True=只处理启动后的新增行）
SKIP_HISTORY = os.environ.get("COLLECTOR_SKIP_HISTORY", "true").lower() == "true"

# 文件名正则: YYYYMMDD_mdl_MARKET_TYPE_SHARD.csv
_FILE_RE = re.compile(r"^(\d{8})_mdl_(\d+)_(\d+)_(\d+)\.csv$")

# 类型映射
_SZ_ORDER_TYPES = {16, 17}
_SZ_DEAL_TYPES  = {23, 24}
_SZ_TICK_TYPES  = set(range(28, 50))   # 28-49
_SH_ORDER_TYPES = {50, 51}
_SH_DEAL_TYPES  = {52}
_SH_TICK_TYPES  = set(range(28, 50))   # 28-49

Market = str   # "SH" | "SZ"
DataType = str  # "order" | "deal" | "tick"


def classify_file(filename: str) -> Optional[Tuple[Market, DataType]]:
    """
    根据文件名返回 (market, data_type)，无法识别返回 None。
    """
    m = _FILE_RE.match(filename)
    if not m:
        return None
    market_id = int(m.group(2))
    type_id   = int(m.group(3))

    if market_id == 4:   # 深交所
        if type_id in _SZ_ORDER_TYPES: return ("SZ", "order")
        if type_id in _SZ_DEAL_TYPES:  return ("SZ", "deal")
        if type_id in _SZ_TICK_TYPES:  return ("SZ", "tick")
    elif market_id == 6: # 上交所
        if type_id in _SH_ORDER_TYPES: return ("SH", "order")
        if type_id in _SH_DEAL_TYPES:  return ("SH", "deal")
        if type_id in _SH_TICK_TYPES:  return ("SH", "tick")
    return None


class FilePoller:
    """
    单文件增量读取器。
    记录文件字节偏移，每次只读取新增内容。
    处理通联追加写入时文件可能不完整的情况（末尾行不含换行则跳过）。
    """

    def __init__(self, path: Path, skip_existing: bool = True):
        self.path = path
        self._offset = path.stat().st_size if skip_existing else 0
        self._header: Optional[list] = None

    def read_new_rows(self) -> Optional[pd.DataFrame]:
        """
        读取自上次以来的新增行，返回 DataFrame。
        如果没有新数据或读取失败返回 None。
        """
        try:
            current_size = self.path.stat().st_size
        except FileNotFoundError:
            return None

        if current_size <= self._offset:
            return None

        with open(self.path, "rb") as f:
            # 若 offset=0 需要读 header
            if self._offset == 0:
                f.seek(0)
            else:
                f.seek(self._offset)

            chunk = f.read(current_size - self._offset)

        if not chunk:
            return None

        # 保证只处理完整行：截断到最后一个换行符
        last_newline = chunk.rfind(b"\n")
        if last_newline == -1:
            # 没有完整行，等下次
            return None

        complete_chunk = chunk[: last_newline + 1]
        self._offset += last_newline + 1

        # 解码并解析 CSV
        try:
            text = complete_chunk.decode("utf-8", errors="replace")

            if self._header is None:
                # 第一次读取，需要从文件头获取列名
                with open(self.path, "r", encoding="utf-8", errors="replace") as f:
                    header_line = f.readline().strip()
                self._header = header_line.split(",")

            # 如果 offset 从 0 开始，text 本身包含 header 行，用 pd.read_csv
            import io
            if text.startswith(",".join(self._header[:3])):
                # chunk 包含 header 行
                df = pd.read_csv(io.StringIO(text))
            else:
                # chunk 只有数据行，手动指定 header
                df = pd.read_csv(io.StringIO(text), header=None, names=self._header)

            return df if not df.empty else None
        except Exception as e:
            logger.warning("解析文件 %s 失败: %s", self.path.name, e)
            return None


class DayDirWatcher:
    """
    监听当天日期目录下所有符合命名规范的 CSV 文件。
    自动发现新文件，对每个文件做增量读取。
    """

    def __init__(self, day_dir: Path, skip_history: bool = True):
        self.day_dir = day_dir
        self.skip_history = skip_history
        # path -> (FilePoller, market, data_type)
        self._pollers: Dict[Path, Tuple[FilePoller, Market, DataType]] = {}

    def _register_new_files(self):
        """扫描目录，注册尚未跟踪的新文件。"""
        try:
            files = list(self.day_dir.glob("*.csv"))
        except FileNotFoundError:
            return
        for f in files:
            if f in self._pollers:
                continue
            classification = classify_file(f.name)
            if classification is None:
                continue  # OrderQueue 等暂不处理
            market, data_type = classification
            self._pollers[f] = (FilePoller(f, self.skip_history), market, data_type)
            logger.info("[发现] %s -> %s %s", f.name, market, data_type)

    def poll(self):
        """
        扫描新文件 + 读取所有已跟踪文件的新增行。
        返回 list of (path, market, data_type, DataFrame)
        """
        self._register_new_files()
        results = []
        for path, (poller, market, data_type) in list(self._pollers.items()):
            df = poller.read_new_rows()
            if df is not None and not df.empty:
                results.append((path, market, data_type, df))
        return results


class Collector:
    """
    主 Collector：监听当天通联数据目录，解析 CSV 新增行，写入 ShmStore。
    """

    def __init__(self):
        self._converter = TonglanceDataConverter()
        self._store = ShmStore()
        self._stop_event = threading.Event()
        self._trading_day = date.today()
        self._day_dir = MSG_BACKUP_DIR / self._trading_day.strftime("%Y%m%d")
        self._watcher = DayDirWatcher(self._day_dir, SKIP_HISTORY)

    def _handle(self, path: Path, market: str, data_type: str, raw_df: pd.DataFrame):
        """将原始 DataFrame 转换后写入 ShmStore。"""
        try:
            trading_day_dt = pd.Timestamp(self._trading_day)

            if data_type == "order":
                if market == "SZ":
                    df = self._converter.convert_sz_order(raw_df.to_dict("list"), trading_day_dt)
                else:
                    df = self._converter.convert_sh_order(raw_df.to_dict("list"), trading_day_dt)
                for code, sub in df.groupby("Code"):
                    self._store.update_order(code, sub)

            elif data_type == "deal":
                if market == "SZ":
                    df = self._converter.convert_sz_deal(raw_df.to_dict("list"), trading_day_dt)
                else:
                    df = self._converter.convert_sh_deal(raw_df.to_dict("list"), trading_day_dt)
                for code, sub in df.groupby("Code"):
                    self._store.update_deal(code, sub)

            elif data_type == "tick":
                if market == "SZ":
                    df = self._converter.convert_sz_tick(raw_df.to_dict("list"), trading_day_dt)
                else:
                    df = self._converter.convert_sh_tick(raw_df.to_dict("list"), trading_day_dt)
                for code, sub in df.groupby("Code"):
                    self._store.update_tick(code, sub)

            logger.debug("[%s][%s] %s +%d 行", market, data_type, path.name, len(raw_df))

        except Exception as e:
            logger.warning("处理 %s 失败: %s", path.name, e, exc_info=True)

    def _check_day_rollover(self):
        """交易日切换时更新监听目录（跨日不重启的情况）。"""
        today = date.today()
        if today != self._trading_day:
            logger.info("[日切] %s -> %s", self._trading_day, today)
            self._trading_day = today
            self._day_dir = MSG_BACKUP_DIR / today.strftime("%Y%m%d")
            self._watcher = DayDirWatcher(self._day_dir, skip_history=False)

    def _run_loop(self):
        logger.info("[Collector] 启动，监听目录: %s，poll 间隔: %.1fs", self._day_dir, POLL_INTERVAL)
        while not self._stop_event.is_set():
            self._check_day_rollover()
            for path, market, data_type, df in self._watcher.poll():
                self._handle(path, market, data_type, df)
            self._stop_event.wait(POLL_INTERVAL)
        logger.info("[Collector] 已停止")

    def start(self):
        self._day_dir.mkdir(parents=True, exist_ok=True)
        self._run_loop()

    def stop(self):
        self._stop_event.set()


def main():
    import signal
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    c = Collector()
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    signal.signal(signal.SIGINT,  lambda *_: c.stop())
    c.start()


if __name__ == "__main__":
    main()
