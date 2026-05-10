# -*- coding: utf-8 -*-
"""
实盘流式因子计算引擎

常驻 Deployment 进程，增量消费 ShmStore 中的新 chunk，
维护 per-stock 聚合状态（StockState），每分钟触发因子计算并输出。

数据流：
    collector → ShmStore (chunk files)
                      ↓ (只读，增量)
    StreamingEngine → 维护 StockState → 每分钟算因子 → 输出

环境变量：
    SHM_STORE_PATH     共享内存目录（默认 /dev/shm/quant-store）
    FACTOR_MODULE      交易员因子模块路径，须暴露 factor_calculation / outfun
    COMPUTE_INTERVAL   计算间隔秒数（默认 60）
    FACTOR_OUTPUT_PATH 因子结果输出目录
    LOG_LEVEL          日志级别
"""

import importlib
import json
import logging
import os
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

import pandas as pd
import pyarrow.ipc as ipc

from ..data.shm_store import SHM_BASE
from ..factor.base import StockState

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

logger = logging.getLogger(__name__)


def _read_arrow(path: Path) -> Optional[pd.DataFrame]:
    """读取单个 Arrow IPC 文件。"""
    if not path.exists():
        return None
    try:
        reader = ipc.open_file(str(path))
        return reader.read_all().to_pandas()
    except Exception:
        return None


def _upload_to_oss(df: pd.DataFrame, date_str: str, end_time: str) -> None:
    """上传因子结果到 OSS。"""
    try:
        import oss2

        endpoint = os.environ.get("OSS_ENDPOINT", "")
        ak_id = os.environ.get("OSS_ACCESS_KEY_ID", "")
        ak_secret = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
        bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
        prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")

        if not all([endpoint, ak_id, ak_secret]):
            logger.warning("[OSS] 凭据不完整，跳过上传")
            return

        auth = oss2.Auth(ak_id, ak_secret)
        ep_clean = endpoint.replace("https://", "").replace("http://", "")
        bucket = oss2.Bucket(auth, ep_clean, bucket_name)

        year = date_str[:4]
        month = date_str[4:6]
        key = f"{prefix}/{year}/{year}{month}/{date_str}_{end_time}.json"

        records = df.to_dict(orient="records")
        payload = json.dumps(records, ensure_ascii=False, default=str).encode("utf-8")
        bucket.put_object(key, payload)

        logger.info("[OSS] 已上传 oss://%s/%s (%d 条, %d bytes)",
                    bucket_name, key, len(records), len(payload))
    except Exception as exc:
        logger.error("[OSS] 上传失败: %s", exc)


class StreamingEngine:
    """
    流式因子计算引擎。

    增量消费 ShmStore chunk 文件，维护 per-stock StockState，
    每 COMPUTE_INTERVAL 秒触发因子计算并输出结果。
    """

    def __init__(self):
        self.states: Dict[str, StockState] = {}
        self.processed_chunks: Dict[str, Set[str]] = {
            "tick": set(), "deal": set(), "order": set(),
        }
        self._trading_day: str = ""
        self._last_output_ts: float = 0
        self._stopped = False

        # 因子模块
        module_path = os.environ.get("FACTOR_MODULE", "")
        if not module_path:
            logger.error("[streaming] FACTOR_MODULE 未设置")
            sys.exit(1)
        self.factor_module = importlib.import_module(module_path)
        self.factor_calculation: Callable = self.factor_module.factor_calculation
        self.outfun: Optional[Callable] = getattr(self.factor_module, "outfun", None)

        # 计算间隔
        self.compute_interval = int(os.environ.get("COMPUTE_INTERVAL", "60"))

        # 输出目录
        output_path_str = os.environ.get("FACTOR_OUTPUT_PATH", "")
        self.output_path = Path(output_path_str) if output_path_str else None

        logger.info("[streaming] 初始化完成: module=%s interval=%ds",
                    module_path, self.compute_interval)

    # ------------------------------------------------------------------ #
    # 增量消费                                                             #
    # ------------------------------------------------------------------ #

    def _consume_new_chunks(self) -> int:
        """扫描 ShmStore 中未处理的 chunk，增量更新 per-stock 状态。返回新处理数量。"""
        consumed = 0
        for data_type in ("tick", "deal", "order"):
            chunk_dir = SHM_BASE / data_type
            if not chunk_dir.exists():
                continue

            known = self.processed_chunks[data_type]

            for f in chunk_dir.glob("chunk_*.arrow"):
                if f.name in known:
                    continue

                df = _read_arrow(f)
                if df is not None and not df.empty and "Code" in df.columns:
                    for code, group in df.groupby("Code"):
                        code_str = str(code)
                        if code_str not in self.states:
                            self.states[code_str] = StockState(code=code_str)
                        getattr(self.states[code_str], f"update_{data_type}")(group)

                known.add(f.name)
                consumed += 1

        return consumed

    # ------------------------------------------------------------------ #
    # 因子计算与输出                                                        #
    # ------------------------------------------------------------------ #

    def _compute_and_output(self) -> None:
        """遍历所有股票状态，调用因子函数，输出结果。"""
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        end_time = now.strftime("%H%M%S")

        self._trading_day = date_str

        logger.info("[streaming] 开始计算: date=%s end_time=%s stocks=%d",
                    date_str, end_time, len(self.states))

        t0 = time.time()
        results = []
        for code, state in self.states.items():
            try:
                result = self.factor_calculation(state, code, date_str, end_time)
                if result is not None:
                    results.append(result)
            except Exception as exc:
                logger.warning("[%s] 因子计算失败: %s", code, exc)

        elapsed_ms = (time.time() - t0) * 1000

        result_df = pd.DataFrame(results) if results else pd.DataFrame()
        logger.info("[streaming] 计算完成: %d 条结果, 耗时 %.0fms", len(result_df), elapsed_ms)

        # 写 CSV
        if self.output_path and not result_df.empty:
            self.output_path.mkdir(parents=True, exist_ok=True)
            out_file = self.output_path / f"{date_str}_{end_time}.csv"
            result_df.to_csv(out_file, index=False)
            logger.info("[streaming] 已写入 %s", out_file)

        # 上传 OSS
        if not result_df.empty:
            _upload_to_oss(result_df, date_str, end_time)

        # 调用 outfun
        if self.outfun is not None:
            try:
                self.outfun(date_str, end_time, result_df)
            except Exception as exc:
                logger.error("[streaming] outfun 失败: %s", exc)

    # ------------------------------------------------------------------ #
    # 日切                                                                 #
    # ------------------------------------------------------------------ #

    def _check_day_rollover(self) -> None:
        """交易日切换时清空状态。"""
        today = datetime.now().strftime("%Y%m%d")
        if self._trading_day and today != self._trading_day:
            logger.info("[streaming] 日切: %s -> %s，清空状态", self._trading_day, today)
            self.states.clear()
            for s in self.processed_chunks.values():
                s.clear()
            self._trading_day = today

    # ------------------------------------------------------------------ #
    # inotify 监听                                                         #
    # ------------------------------------------------------------------ #

    def _start_watcher(self) -> None:
        """启动 inotify 监听 ShmStore 目录，新 chunk 写入时立即消费。"""
        if not HAS_WATCHDOG:
            logger.warning("[streaming] watchdog 未安装，使用轮询模式")
            return

        handler = _ChunkHandler(self)
        self._observer = Observer()
        for data_type in ("tick", "deal", "order"):
            watch_dir = SHM_BASE / data_type
            watch_dir.mkdir(parents=True, exist_ok=True)
            self._observer.schedule(handler, str(watch_dir), recursive=False)
        self._observer.start()
        logger.info("[streaming] inotify 监听已启动")

    def on_new_chunk(self, path: Path) -> None:
        """inotify 回调：新 chunk 文件写入时触发。"""
        # 判断数据类型
        parent = path.parent.name
        if parent not in ("tick", "deal", "order"):
            return
        if not path.name.startswith("chunk_") or not path.name.endswith(".arrow"):
            return
        if path.name in self.processed_chunks.get(parent, set()):
            return

        df = _read_arrow(path)
        if df is not None and not df.empty and "Code" in df.columns:
            for code, group in df.groupby("Code"):
                code_str = str(code)
                if code_str not in self.states:
                    self.states[code_str] = StockState(code=code_str)
                getattr(self.states[code_str], f"update_{parent}")(group)

        self.processed_chunks[parent].add(path.name)

    # ------------------------------------------------------------------ #
    # 主循环                                                               #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """主循环：inotify 驱动数据消费 + 定时因子计算。"""
        logger.info("[streaming] 引擎启动")
        self._trading_day = datetime.now().strftime("%Y%m%d")

        # 启动时消费已有的 chunk（避免错过启动前的数据）
        self._consume_new_chunks()

        # 启动 inotify 监听
        self._start_watcher()

        while not self._stopped:
            # 定时计算
            now = time.time()
            if now - self._last_output_ts >= self.compute_interval:
                if self.states:
                    self._compute_and_output()
                else:
                    logger.debug("[streaming] 无股票数据，跳过计算")
                self._last_output_ts = now

            # 日切检查
            self._check_day_rollover()

            time.sleep(0.01)  # 10ms，仅用于定时器精度

    def stop(self) -> None:
        self._stopped = True
        if hasattr(self, '_observer'):
            self._observer.stop()
            self._observer.join()


class _ChunkHandler(FileSystemEventHandler):
    """inotify 事件处理器：新 chunk 文件写入时通知 StreamingEngine。"""

    def __init__(self, engine: StreamingEngine):
        super().__init__()
        self._engine = engine

    def on_created(self, event):
        if not event.is_directory:
            self._engine.on_new_chunk(Path(event.src_path))

    def on_modified(self, event):
        if not event.is_directory:
            self._engine.on_new_chunk(Path(event.src_path))


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    engine = StreamingEngine()

    import signal
    signal.signal(signal.SIGTERM, lambda *_: engine.stop())
    signal.signal(signal.SIGINT, lambda *_: engine.stop())

    engine.run()


if __name__ == "__main__":
    main()
