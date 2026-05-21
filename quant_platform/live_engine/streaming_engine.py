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
    FACTOR_OUTPUT_PATH 因子结果输出目录（checkpoint 也存在这里）
    LOG_LEVEL          日志级别
"""

import importlib
import json
import logging
import os
import pickle
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set

from .pipeline_logger import get_streaming_logger

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
    # 提供 stub，避免 class 定义报错
    class FileSystemEventHandler:  # type: ignore[no-redef]
        pass

logger = logging.getLogger(__name__)

# A 股交易时段（留 5 分钟余量）
_TRADING_START = (9, 15)   # 09:15
_TRADING_END = (15, 5)     # 15:05


def _is_trading_hours() -> bool:
    """当前是否在 A 股交易时段内。"""
    now = datetime.now()
    h, m = now.hour, now.minute
    return (h, m) >= _TRADING_START and (h, m) < _TRADING_END


def _read_arrow(path: Path) -> Optional[pd.DataFrame]:
    """读取单个 Arrow IPC 文件。"""
    if not path.exists():
        return None
    try:
        reader = ipc.open_file(str(path))
        return reader.read_all().to_pandas()
    except Exception as exc:
        logger.warning("[read_arrow] 读取失败 %s: %s", path.name, exc)
        return None


def _extract_chunk_ts(filename: str) -> float:
    """从 chunk 文件名提取写入时间戳（秒）。

    chunk_1715312345678_000001.arrow -> 1715312345.678
    """
    try:
        # chunk_{ts_ms}_{seq}.arrow -> 取第一段数字作为时间戳
        body = filename.replace("chunk_", "").replace(".arrow", "")
        ts_ms = int(body.split("_")[0])
        return ts_ms / 1000.0
    except (ValueError, IndexError):
        return 0.0


def _time_to_seconds(time_val) -> float:
    """将 Time 列值转换为自午夜以来的秒数。

    支持格式：
      - datetime / Timestamp 对象
      - "2025-01-03 09:30:01.000" 字符串
      - 93001000 或 "093001000" 整数/字符串 (HHMMSSmmm)
    """
    import pandas as _pd

    # datetime / Timestamp
    if hasattr(time_val, 'hour'):
        return time_val.hour * 3600 + time_val.minute * 60 + time_val.second + time_val.microsecond / 1e6

    s = str(time_val).strip()
    # datetime 字符串 "2025-01-03 09:30:01"
    if '-' in s and ':' in s:
        try:
            dt = _pd.Timestamp(s)
            return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
        except Exception:
            pass

    # 纯数字 HHMMSSmmm
    try:
        raw = s.replace('.', '').replace(':', '')
        raw = raw.zfill(6)  # 至少 HHMMSS
        h = int(raw[0:2])
        m = int(raw[2:4])
        sec = int(raw[4:6])
        ms = int(raw[6:9]) if len(raw) >= 9 else 0
        return h * 3600 + m * 60 + sec + ms / 1000.0
    except (ValueError, IndexError):
        return 0.0


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
        key = f"{prefix}/{year}/{year}{month}/{date_str}/{end_time}.json"

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

    # Checkpoint 文件名
    _CHECKPOINT_NAME = "streaming_checkpoint.pkl"

    def __init__(self):
        self._lock = threading.Lock()
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

        # 输出目录 & checkpoint 路径
        output_path_str = os.environ.get("FACTOR_OUTPUT_PATH", "")
        self.output_path = Path(output_path_str) if output_path_str else None
        if self.output_path:
            self._checkpoint_path = self.output_path / self._CHECKPOINT_NAME
        else:
            self._checkpoint_path = Path("/tmp") / self._CHECKPOINT_NAME

        # 恢复 checkpoint
        self._load_checkpoint()

        logger.info("[streaming] 初始化完成: module=%s interval=%ds checkpoint=%s",
                    module_path, self.compute_interval, self._checkpoint_path)

    # ------------------------------------------------------------------ #
    # Checkpoint                                                           #
    # ------------------------------------------------------------------ #

    def _save_checkpoint(self) -> None:
        """持久化 StockState 到磁盘，Pod 重启后可恢复。"""
        try:
            with self._lock:
                data = {
                    "trading_day": self._trading_day,
                    "states": dict(self.states),
                    "processed_chunks": {k: list(v) for k, v in self.processed_chunks.items()},
                }
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._checkpoint_path.with_suffix(".tmp")
            with open(tmp, "wb") as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            tmp.replace(self._checkpoint_path)
            logger.debug("[checkpoint] 已保存: %d stocks", len(self.states))
        except Exception as exc:
            logger.warning("[checkpoint] 保存失败: %s", exc)

    def _load_checkpoint(self) -> None:
        """从磁盘恢复 StockState。开盘前或跨日时丢弃旧 checkpoint。"""
        if not self._checkpoint_path.exists():
            logger.info("[checkpoint] 无历史 checkpoint，冷启动")
            return
        now = datetime.now()
        # 开盘前（09:15 之前）删除残留 checkpoint，确保每天干净启动
        if now.hour < 9 or (now.hour == 9 and now.minute < 15):
            logger.info("[checkpoint] 开盘前，删除残留 checkpoint")
            try:
                self._checkpoint_path.unlink(missing_ok=True)
            except Exception:
                pass
            return
        try:
            with open(self._checkpoint_path, "rb") as f:
                data = pickle.load(f)
            ckpt_day = data.get("trading_day", "")
            today = now.strftime("%Y%m%d")
            if ckpt_day and ckpt_day != today:
                logger.info("[checkpoint] checkpoint trading_day=%s != 今天=%s，丢弃旧状态",
                            ckpt_day, today)
                return
            self._trading_day = ckpt_day
            self.states = data.get("states", {})
            for k, v in data.get("processed_chunks", {}).items():
                if k in self.processed_chunks:
                    self.processed_chunks[k] = set(v)
            logger.info("[checkpoint] 已恢复: trading_day=%s stocks=%d",
                        self._trading_day, len(self.states))
        except Exception as exc:
            logger.warning("[checkpoint] 恢复失败，冷启动: %s", exc)
            self.states.clear()
            for s in self.processed_chunks.values():
                s.clear()

    # ------------------------------------------------------------------ #
    # 增量消费                                                             #
    # ------------------------------------------------------------------ #

    def _update_states(self, df: pd.DataFrame, data_type: str,
                       chunk_ts: float = 0.0) -> None:
        """将 DataFrame 按 Code 分组更新到对应的 StockState。线程安全。"""
        if df is None or df.empty or "Code" not in df.columns:
            return
        now = time.time()
        # groupby 在锁外完成（CPU 密集，不涉及共享数据）
        groups = list(df.groupby("Code"))
        # 锁内：只做新增 StockState
        with self._lock:
            for code, _ in groups:
                code_str = str(code)
                if code_str not in self.states:
                    self.states[code_str] = StockState(code=code_str)
        # 锁外：更新已有状态（GIL 保护单对象属性写入）
        for code, group in groups:
            state = self.states.get(str(code))
            if state is not None:
                getattr(state, f"update_{data_type}")(group)
                state.last_update_ts = now
                if chunk_ts > 0:
                    state.last_chunk_ts = chunk_ts

    def _consume_new_chunks(self) -> int:
        """扫描 ShmStore 中未处理的 chunk，增量更新 per-stock 状态。返回新处理数量。"""
        consumed = 0
        for data_type in ("tick", "deal", "order"):
            chunk_dir = SHM_BASE / data_type
            if not chunk_dir.exists():
                continue

            with self._lock:
                known = set(self.processed_chunks[data_type])

            # 收集当前存在的文件名，用于清理 set 中已删除的条目
            alive = set()
            new_files = []
            for f in chunk_dir.glob("chunk_*.arrow"):
                alive.add(f.name)
                if f.name not in known:
                    new_files.append(f)

            for f in new_files:
                with self._lock:
                    if f.name in self.processed_chunks[data_type]:
                        continue  # 已被 inotify 占位，跳过
                    self.processed_chunks[data_type].add(f.name)  # 先占位
                chunk_ts = _extract_chunk_ts(f.name)
                chunk_age_ms = round((time.time() - chunk_ts) * 1000, 1) if chunk_ts > 0 else 0

                t0 = time.time()
                df = _read_arrow(f)
                read_ms = round((time.time() - t0) * 1000, 1)
                rows = len(df) if df is not None and not df.empty else 0

                get_streaming_logger().log(
                    "chunk_read", trigger="poll", data_type=data_type, chunk=f.name,
                    chunk_age_ms=chunk_age_ms, rows=rows, read_elapsed_ms=read_ms,
                )

                self._update_states(df, data_type, chunk_ts)
                consumed += 1
                # 消费后删除 chunk，由计算端控制数据生命周期
                try:
                    f.unlink()
                except OSError:
                    pass

            # 清理 set 中已被 collector 滚动删除的文件名
            with self._lock:
                stale = self.processed_chunks[data_type] - alive
                if stale:
                    self.processed_chunks[data_type] -= stale

        return consumed

    # ------------------------------------------------------------------ #
    # 因子计算与输出                                                        #
    # ------------------------------------------------------------------ #

    def _compute_and_output(self) -> None:
        """遍历所有股票状态，调用因子函数，输出结果。"""
        pipe_log = get_streaming_logger()
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        end_time = now.strftime("%H%M%S")

        # 快照 states：持锁拷贝 dict，释放锁后再计算
        with self._lock:
            self._trading_day = date_str
            snapshot = dict(self.states)

        logger.info("[streaming] 开始计算: date=%s end_time=%s stocks=%d",
                    date_str, end_time, len(snapshot))

        t0 = time.time()
        results = []
        for code, state in snapshot.items():
            try:
                result = self.factor_calculation(state, code, date_str, end_time)
                if result is not None:
                    # 数据延迟：行情时间 → 因子计算时刻
                    if state.last_market_time:
                        market_secs = _time_to_seconds(state.last_market_time)
                        if market_secs > 0:
                            wall_secs = now.hour * 3600 + now.minute * 60 + now.second
                            data_latency_ms = round((wall_secs - market_secs) * 1000, 1)
                            # 只在合理范围内纳入统计（实盘 < 10 分钟）
                            if abs(data_latency_ms) < 600_000:
                                result["data_latency_ms"] = data_latency_ms

                    # chunk 写入延迟（ShmStore → 计算）
                    if state.last_chunk_ts > 0:
                        chunk_latency_ms = round((t0 - state.last_chunk_ts) * 1000, 1)
                        if chunk_latency_ms < 600_000:
                            result["chunk_latency_ms"] = chunk_latency_ms
                    results.append(result)
            except Exception as exc:
                logger.warning("[%s] 因子计算失败: %s", code, exc)

        elapsed_ms = (time.time() - t0) * 1000

        result_df = pd.DataFrame(results) if results else pd.DataFrame()
        logger.info("[streaming] 计算完成: %d 条结果, 耗时 %.0fms", len(result_df), elapsed_ms)

        # 延迟统计
        latency_stats = {}
        if not result_df.empty and "data_latency_ms" in result_df.columns:
            valid = result_df["data_latency_ms"].dropna()
            if not valid.empty:
                latency_stats = {
                    "data_latency_avg_ms": round(valid.mean(), 1),
                    "data_latency_max_ms": round(valid.max(), 1),
                    "data_latency_min_ms": round(valid.min(), 1),
                    "data_latency_p50_ms": round(valid.median(), 1),
                }
                logger.info(
                    "[streaming] 数据延迟(行情→计算): avg=%.0fms max=%.0fms min=%.0fms p50=%.0fms",
                    valid.mean(), valid.max(), valid.min(), valid.median(),
                )

        # 记录因子计算耗时日志
        chunk_latency_stats = {}
        if not result_df.empty and "chunk_latency_ms" in result_df.columns:
            valid_chunk = result_df["chunk_latency_ms"].dropna()
            if not valid_chunk.empty:
                chunk_latency_stats = {
                    "chunk_latency_avg_ms": round(valid_chunk.mean(), 1),
                    "chunk_latency_max_ms": round(valid_chunk.max(), 1),
                }

        pipe_log.log("factor_compute", date=date_str, end_time=end_time,
                     stocks=len(snapshot), results=len(result_df),
                     compute_ms=round(elapsed_ms, 1),
                     **latency_stats, **chunk_latency_stats)

        # 写 CSV
        csv_write_ms = 0
        if self.output_path and not result_df.empty:
            t_csv = time.time()
            self.output_path.mkdir(parents=True, exist_ok=True)
            out_file = self.output_path / f"{date_str}_{end_time}.csv"
            result_df.to_csv(out_file, index=False)
            csv_write_ms = round((time.time() - t_csv) * 1000, 1)
            logger.info("[streaming] 已写入 %s", out_file)

        # 上传 OSS
        oss_upload_ms = 0
        if not result_df.empty:
            t_oss = time.time()
            _upload_to_oss(result_df, date_str, end_time)
            oss_upload_ms = round((time.time() - t_oss) * 1000, 1)

        # 记录输出日志
        pipe_log.log("output", date=date_str, end_time=end_time,
                     file=f"{date_str}_{end_time}.csv" if not result_df.empty else "",
                     rows=len(result_df),
                     csv_write_ms=csv_write_ms,
                     oss_upload_ms=oss_upload_ms)

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
        with self._lock:
            if self._trading_day and today != self._trading_day:
                logger.info("[streaming] 日切: %s -> %s，清空状态", self._trading_day, today)
                self.states.clear()
                for s in self.processed_chunks.values():
                    s.clear()
                self._trading_day = today
                try:
                    self._checkpoint_path.unlink(missing_ok=True)
                except Exception:
                    pass

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
        """inotify 回调：新 chunk 文件写入时触发。线程安全。"""
        parent = path.parent.name
        if parent not in ("tick", "deal", "order"):
            return
        if not path.name.startswith("chunk_") or not path.name.endswith(".arrow"):
            return
        with self._lock:
            if path.name in self.processed_chunks.get(parent, set()):
                return
            self.processed_chunks[parent].add(path.name)  # 立即占位，防 TOCTOU

        pipe_log = get_streaming_logger()
        chunk_ts = _extract_chunk_ts(path.name)
        chunk_age_ms = round((time.time() - chunk_ts) * 1000, 1) if chunk_ts > 0 else 0

        t0 = time.time()
        df = _read_arrow(path)
        read_ms = round((time.time() - t0) * 1000, 1)
        rows = len(df) if df is not None and not df.empty else 0

        pipe_log.log("chunk_read", trigger="inotify", data_type=parent, chunk=path.name,
                     chunk_age_ms=chunk_age_ms, rows=rows, read_elapsed_ms=read_ms)

        self._update_states(df, parent, chunk_ts)
        # 消费后删除 chunk
        try:
            path.unlink()
        except OSError:
            pass

    # ------------------------------------------------------------------ #
    # 主循环                                                               #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """主循环：inotify 驱动 + 轮询兜底 + 定时因子计算。"""
        logger.info("[streaming] 引擎启动")
        self._trading_day = datetime.now().strftime("%Y%m%d")

        # 启动时消费已有的 chunk（避免错过启动前的数据）
        self._consume_new_chunks()

        # 启动 inotify 监听
        self._start_watcher()

        # 轮询/计算/checkpoint 时间戳
        last_poll_ts = time.time()
        last_checkpoint_ts = time.time()

        while not self._stopped:
            now = time.time()

            # 轮询兜底：每 1 秒扫描新 chunk（防止 inotify 丢事件）
            if now - last_poll_ts >= 1.0:
                consumed = self._consume_new_chunks()
                if consumed:
                    logger.info("[streaming] 轮询消费 %d 个新 chunk", consumed)
                last_poll_ts = now

            # 定时计算（仅交易时段）
            if now - self._last_output_ts >= self.compute_interval:
                if _is_trading_hours():
                    if self.states:
                        self._compute_and_output()
                    else:
                        logger.debug("[streaming] 无股票数据，跳过计算")
                self._last_output_ts = now

            # 定时 checkpoint：仅交易时段保存
            if now - last_checkpoint_ts >= 30 and self.states and _is_trading_hours():
                self._save_checkpoint()
                last_checkpoint_ts = now

            # 日切检查
            self._check_day_rollover()

            time.sleep(0.01)  # 10ms

    def stop(self) -> None:
        """停止引擎，保存 checkpoint。"""
        self._stopped = True
        if self.states:
            self._save_checkpoint()
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
