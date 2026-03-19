# -*- coding: utf-8 -*-
"""
Collector：监听通联客户端写入的 CSV 文件，增量解析后写入 ShmStore

目录结构（通联客户端写入）：
    /data/tonglian/
        tick/   *.csv   每个文件追加写入 tick 快照
        order/  *.csv   每个文件追加写入逐笔委托
        deal/   *.csv   每个文件追加写入逐笔成交

每类数据可以是多个文件（如按股票分文件，或按通道分文件）。
Collector 监听目录下所有 CSV 文件的变化，记录每个文件的已读行数，
只处理新增行，转换后写入 ShmStore。
"""

import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict

import pandas as pd

from ..data.converter import TonglanceDataConverter
from ..data.shm_store import ShmStore

logger = logging.getLogger(__name__)

# 通联数据根目录，通过环境变量配置
TONGLIAN_DATA_DIR = Path(os.environ.get("TONGLIAN_DATA_DIR", "/data/tonglian"))

# 每次 poll 间隔（秒）
POLL_INTERVAL = float(os.environ.get("COLLECTOR_POLL_INTERVAL", "0.5"))

# 是否在启动时跳过历史数据（True=只处理启动后的新增行）
SKIP_HISTORY = os.environ.get("COLLECTOR_SKIP_HISTORY", "true").lower() == "true"


class FilePoller:
    """
    单文件增量读取器。
    记录文件已读到的字节偏移，每次只读取新增内容。
    """

    def __init__(self, path: Path, skip_existing: bool = True):
        self.path = path
        # 跳过历史数据：直接定位到文件末尾
        self._offset = path.stat().st_size if skip_existing else 0
        self._header: list = None  # CSV 列名缓存

    def read_new_rows(self) -> pd.DataFrame:
        """
        读取自上次以来新增的行，返回 DataFrame。
        若无新数据返回 None。
        """
        try:
            current_size = self.path.stat().st_size
        except FileNotFoundError:
            return None

        if current_size <= self._offset:
            return None

        with open(self.path, "r", encoding="utf-8") as f:
            # 读表头（始终从头读第一行）
            if self._header is None:
                header_line = f.readline().strip()
                self._header = header_line.split(",")
                # 如果 skip_existing，offset 已跳过 header，需要重置
                if self._offset == 0:
                    pass  # 正常继续
            else:
                # 跳过 header 行
                f.readline()

            # 定位到上次读取位置
            f.seek(self._offset)
            new_content = f.read()
            self._offset = f.tell()

        if not new_content.strip():
            return None

        try:
            from io import StringIO
            df = pd.read_csv(
                StringIO(new_content),
                names=self._header,
                header=None,
                on_bad_lines="skip",
            )
            return df if not df.empty else None
        except Exception as e:
            logger.warning("解析 %s 新增行失败: %s", self.path.name, e)
            return None


class DirectoryWatcher:
    """
    监听单个目录下所有 CSV 文件的新增行。
    支持运行中新出现的文件（动态注册）。
    """

    def __init__(self, directory: Path, data_type: str, skip_history: bool = True):
        self.directory = directory
        self.data_type = data_type  # tick / order / deal
        self.skip_history = skip_history
        self._pollers: Dict[Path, FilePoller] = {}

    def _register_new_files(self):
        """扫描目录，对新出现的 CSV 文件创建 FilePoller。"""
        try:
            csv_files = list(self.directory.glob("*.csv"))
        except Exception:
            return

        for f in csv_files:
            if f not in self._pollers:
                logger.info("[%s] 注册新文件: %s", self.data_type, f.name)
                self._pollers[f] = FilePoller(f, skip_existing=self.skip_history)

    def poll(self) -> list:
        """
        扫描所有已注册文件，返回 (path, df) 列表，每项是一个文件的新增行。
        """
        self._register_new_files()
        results = []
        for path, poller in list(self._pollers.items()):
            df = poller.read_new_rows()
            if df is not None:
                results.append((path, df))
        return results


class Collector:
    """
    主 Collector：启动三个 DirectoryWatcher 分别监听 tick/order/deal 目录，
    新增行经 TonglanceDataConverter 转换后写入 ShmStore。
    """

    def __init__(self):
        self._converter = TonglanceDataConverter()
        self._store = ShmStore()
        self._watchers = {
            "tick":  DirectoryWatcher(TONGLIAN_DATA_DIR / "tick",  "tick",  SKIP_HISTORY),
            "order": DirectoryWatcher(TONGLIAN_DATA_DIR / "order", "order", SKIP_HISTORY),
            "deal":  DirectoryWatcher(TONGLIAN_DATA_DIR / "deal",  "deal",  SKIP_HISTORY),
        }
        self._stop_event = threading.Event()

    def _handle_tick(self, path: Path, raw_df: pd.DataFrame):
        """处理 tick 快照新增行。"""
        try:
            # 从文件名推断股票代码（如 000001.XSHE.csv）
            code = path.stem  # 去掉 .csv
            for code_val in raw_df.get("Code", pd.Series([code])).unique():
                sub = raw_df[raw_df["Code"] == code_val] if "Code" in raw_df.columns else raw_df
                # 直接写入 ShmStore（raw_df 已经是通联原始格式，需要判断是否需要 convert）
                self._store.update_tick(code_val, sub)
                logger.debug("[tick] %s +%d 行", code_val, len(sub))
        except Exception as e:
            logger.warning("处理 tick 文件 %s 失败: %s", path.name, e)

    def _handle_order(self, path: Path, raw_df: pd.DataFrame):
        """处理逐笔委托新增行。"""
        try:
            for code_val in raw_df.get("Code", pd.Series([path.stem])).unique():
                sub = raw_df[raw_df["Code"] == code_val] if "Code" in raw_df.columns else raw_df
                self._store.update_order(code_val, sub)
                logger.debug("[order] %s +%d 行", code_val, len(sub))
        except Exception as e:
            logger.warning("处理 order 文件 %s 失败: %s", path.name, e)

    def _handle_deal(self, path: Path, raw_df: pd.DataFrame):
        """处理逐笔成交新增行。"""
        try:
            for code_val in raw_df.get("Code", pd.Series([path.stem])).unique():
                sub = raw_df[raw_df["Code"] == code_val] if "Code" in raw_df.columns else raw_df
                self._store.update_deal(code_val, sub)
                logger.debug("[deal] %s +%d 行", code_val, len(sub))
        except Exception as e:
            logger.warning("处理 deal 文件 %s 失败: %s", path.name, e)

    def _run_loop(self):
        """主轮询循环。"""
        handlers = {
            "tick":  self._handle_tick,
            "order": self._handle_order,
            "deal":  self._handle_deal,
        }
        while not self._stop_event.is_set():
            for data_type, watcher in self._watchers.items():
                for path, df in watcher.poll():
                    handlers[data_type](path, df)
            time.sleep(POLL_INTERVAL)

    def start(self):
        """启动 collector（阻塞）。"""
        logger.info("Collector 启动，监听目录: %s，poll_interval=%.1fs，skip_history=%s",
                    TONGLIAN_DATA_DIR, POLL_INTERVAL, SKIP_HISTORY)
        # 确保目录存在
        for d in ["tick", "order", "deal"]:
            (TONGLIAN_DATA_DIR / d).mkdir(parents=True, exist_ok=True)
        self._run_loop()

    def stop(self):
        self._stop_event.set()


def main():
    import signal
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    c = Collector()
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    signal.signal(signal.SIGINT,  lambda *_: c.stop())
    c.start()


if __name__ == "__main__":
    main()
