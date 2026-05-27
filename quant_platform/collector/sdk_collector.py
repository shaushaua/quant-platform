# -*- coding: utf-8 -*-
"""MDL SDK based real-time collector."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import gc
import ctypes
import logging
import pyarrow as pa
import os
import signal
import sys
import threading
import time
from typing import Dict, List
import resource

from ..core.constants import ARROW_SCHEMA_BY_KIND
from ..data.mysql_loader import DailyBasicCache
from ..data.shm_store import ShmStore
from ..live_engine.pipeline_logger import get_collector_logger
from .arrow_buffer import ArrowBuffer
from .sdk_callback import SequenceTracker, create_callback
from .sdk_config import SDKCollectorConfig, load_config

logger = logging.getLogger(__name__)


class SDKCollector:
    def __init__(self, config: SDKCollectorConfig):
        self.config = config
        self.tracker = SequenceTracker()
        self.store = ShmStore()
        self._stop = threading.Event()
        self._flush_event = threading.Event()
        self._trading_day = date.today()
        self._io_man = None
        self._subscriber = None
        self._callback = None
        self._flush_thread = threading.Thread(target=self._flush_loop, name="sdk-flush", daemon=True)

        # Arrow buffers: values go directly from callback to native memory
        self._buffers = {
            "tick": ArrowBuffer(ARROW_SCHEMA_BY_KIND["tick"]),
            "order": ArrowBuffer(ARROW_SCHEMA_BY_KIND["order"]),
            "deal": ArrowBuffer(ARROW_SCHEMA_BY_KIND["deal"]),
        }

        self._stats: Dict[str, int] = defaultdict(int)
        self._last_rolling_cleanup = 0.0
        self._rolling_cleanup_interval = max(float(os.getenv("SHM_CLEANUP_INTERVAL_SECONDS", "5")), 1.0)
        self._last_pipeline_status_log = 0.0
        self._last_gc = time.time()
        # 内存增长趋势追踪
        self._prev_rss_mb = 0.0
        self._prev_obj_count = 0
        self._start_time = time.time()

    def trading_day(self) -> date:
        return self._trading_day

    def _load_daily_basic(self) -> None:
        try:
            market_count = int(os.environ.get("DAILY_BASIC_MARKET_COUNT", "1"))
            cache = DailyBasicCache(market_count=market_count)
            trade_date = self._trading_day.strftime("%Y%m%d")
            if cache.load(trade_date):
                df = cache.get_daily_basic()
                if not df.empty:
                    self.store.update_daily_basic(df)
                    logger.info("[daily_basic] loaded %d rows into ShmStore", len(df))
                else:
                    logger.warning("[daily_basic] MySQL returned empty data")
            else:
                logger.warning("[daily_basic] load failed")
        except Exception as exc:
            logger.warning("[daily_basic] load error: %s", exc)

    def _connect(self):
        try:
            import pymdl
        except ImportError as exc:
            raise RuntimeError("pymdl is not installed in this image") from exc

        self._io_man = pymdl.CreateIOController(self.config.io_threads)
        log_path = os.environ.get("MDL_LOG_PATH", "/data/quant/mdl_logs/mdl")
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            self._io_man.EnableLog(log_path, False)
        except Exception as exc:
            logger.warning("[sdk] EnableLog failed: %s", exc)

        self._callback = create_callback(
            pymdl, self._buffers, self._flush_event, self.trading_day, self.tracker,
        )
        self._subscriber = self._io_man.CreateSubscriber(self._callback, self.config.callback_multithread)
        self._subscriber.SetServerAddress(self.config.server)
        self._subscriber.SetMessageEncoding(self.config.encoding)
        self._subscriber.EnableMergeMessage(self.config.enable_merge)
        self._subscriber.SetHeartbeatInterval(self.config.heartbeat_interval)
        self._subscriber.SetHeartbeatTimeout(self.config.heartbeat_timeout)

        for service_id, message_id in self.config.subs:
            self._subscriber.AddSubscription(service_id, 101, message_id)
            logger.info("[sdk] subscribe %s.%s", service_id, message_id)

        err = self._subscriber.Connect()
        if err:
            if isinstance(err, bytes):
                err = err.decode("GBK", errors="replace")
            raise RuntimeError(f"MDL Connect failed: {err}")
        logger.info("[sdk] connected to %s", self.config.server)
        get_collector_logger().log(
            "sdk_connected",
            server=self.config.server,
            subs=[f"{service_id}.{message_id}" for service_id, message_id in self.config.subs],
            io_threads=self.config.io_threads,
            callback_multithread=self.config.callback_multithread,
            encoding=self.config.encoding,
            enable_merge=self.config.enable_merge,
        )

    # ------------------------------------------------------------------ #
    # Flush loop: periodically flush Arrow buffers to ShmStore             #
    # ------------------------------------------------------------------ #

    def _flush_loop(self) -> None:
        interval = max(self.config.flush_interval_ms, 1) / 1000.0
        while not self._stop.is_set():
            self._flush_event.wait(timeout=interval)
            self._flush_event.clear()
            self._flush_all()
            now = time.time()
            if now - self._last_pipeline_status_log >= 5:
                self._log_status(now)
                self._last_pipeline_status_log = now
            if now - self._last_rolling_cleanup >= self._rolling_cleanup_interval:
                self.store.cleanup_rolling()
                self._last_rolling_cleanup = now
            if now - self._last_gc >= 60:
                _release_unused_memory()
                self._last_gc = now

    def _flush_all(self) -> None:
        """Flush all Arrow buffers to ShmStore IPC files."""
        pipe_log = get_collector_logger()
        wrote = False
        for kind in ("tick", "order", "deal"):
            buf = self._buffers[kind]
            if buf.row_count == 0:
                continue
            t0 = time.time()
            batch = buf.flush()
            if batch is None:
                continue
            try:
                chunk_name = getattr(self.store, f"update_{kind}")(batch)
                elapsed_ms = round((time.time() - t0) * 1000, 1)
                rows = batch.num_rows
                self._stats[f"written_{kind}"] += rows
                wrote = True
                pipe_log.log(
                    "sdk_shm_write",
                    data_type=kind,
                    chunk=chunk_name or "",
                    rows=rows,
                    elapsed_ms=elapsed_ms,
                )
            finally:
                del batch
        if wrote:
            _quick_release()

    def _log_status(self, now: float) -> None:
        buf_rows = {k: self._buffers[k].row_count for k in ("tick", "order", "deal")}
        snap = self.tracker.snapshot()
        logger.info(
            "[sdk] pending tick=%d order=%d deal=%d written tick=%d order=%d deal=%d gaps=%s",
            buf_rows["tick"], buf_rows["order"], buf_rows["deal"],
            self._stats.get("written_tick", 0),
            self._stats.get("written_order", 0),
            self._stats.get("written_deal", 0),
            snap["gaps"],
        )
        # 每 30 秒输出一次内存详情（含诊断信息）
        if now - self._last_pipeline_status_log < 30:
            return
        self._last_pipeline_status_log = now

        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_mb = rss_kb / 1024

        # RSS 增长速率
        elapsed_since_start = now - self._start_time
        rss_delta = rss_mb - self._prev_rss_mb
        rss_rate = rss_delta / 30.0 if self._prev_rss_mb > 0 else 0.0  # MB/s
        total_rate = rss_mb / (elapsed_since_start / 60.0) if elapsed_since_start > 0 else 0.0  # MB/min

        # Arrow 内存池
        arrow_pool = pa.default_memory_pool()
        arrow_alloc_mb = arrow_pool.bytes_allocated() / 1024 / 1024

        # Python 对象统计
        obj_count = len(gc.get_objects())
        obj_delta = obj_count - self._prev_obj_count

        # 对象类型 Top 10（定位哪种对象在泄漏）
        type_counter = defaultdict(int)
        for obj in gc.get_objects():
            type_counter[type(obj).__name__] += 1
        top_types = sorted(type_counter.items(), key=lambda x: -x[1])[:10]
        top_types_str = " ".join(f"{name}={count}" for name, count in top_types)

        # /dev/shm chunk 文件数和大小
        shm_info = _shm_store_stats()

        logger.warning(
            "[mem] RSS=%.0fMB (+%.1fMB/30s, %.1fMB/min avg) ArrowPool=%.1fMB "
            "PyObj=%d(+%d) pending=%s",
            rss_mb, rss_delta, total_rate, arrow_alloc_mb,
            obj_count, obj_delta, buf_rows,
        )
        logger.warning("[mem-types] %s", top_types_str)
        logger.warning("[mem-shm] %s", shm_info)

        get_collector_logger().log(
            "sdk_status",
            pending_tick=buf_rows["tick"],
            pending_order=buf_rows["order"],
            pending_deal=buf_rows["deal"],
            written_tick=self._stats.get("written_tick", 0),
            written_order=self._stats.get("written_order", 0),
            written_deal=self._stats.get("written_deal", 0),
            seq_received_total=sum(snap["received"].values()),
            seq_gaps_total=sum(snap["gaps"].values()),
            seq_gap_size_total=sum(snap["gap_size"].values()),
            rss_mb=round(rss_mb, 1),
            rss_delta_mb=round(rss_delta, 1),
            rss_rate_mb_per_min=round(total_rate, 1),
            arrow_pool_mb=round(arrow_alloc_mb, 1),
            py_object_count=obj_count,
            py_object_delta=obj_delta,
            top_types=top_types_str,
            shm_info=shm_info,
        )

        self._prev_rss_mb = rss_mb
        self._prev_obj_count = obj_count

        # RSS 自保护：超限主动退出，K8s 会重启 pod（比 OOM 驱逐更优雅）
        rss_limit_mb = int(os.environ.get("COLLECTOR_RSS_LIMIT_MB", "12288"))
        if rss_mb > rss_limit_mb:
            logger.error("[mem] RSS=%.0fMB 超过限制 %dMB，主动退出", rss_mb, rss_limit_mb)
            get_collector_logger().log("sdk_oom_self_kill", rss_mb=round(rss_mb, 1), limit_mb=rss_limit_mb)
            self.stop()

    def start(self) -> None:
        logger.info("[sdk] collector starting config=%s", self.config)
        if os.getenv("SHM_CLEAR_ON_START", "true").lower() in ("1", "true", "yes", "on"):
            self.store.clear_rolling()
        self._load_daily_basic()
        self._flush_thread.start()
        self._connect()
        while not self._stop.is_set():
            self._stop.wait(1.0)

    def stop(self) -> None:
        self._stop.set()
        # Final flush
        self._flush_all()
        if self._subscriber is not None:
            try:
                self._subscriber.ClearSubscriptions()
            except Exception:
                pass
        if self._io_man is not None:
            try:
                self._io_man.Shutdown()
            except Exception:
                pass
        logger.info("[sdk] collector stopped")


def _shm_store_stats() -> str:
    """统计 /dev/shm chunk 文件数和大小，辅助定位内存去向。"""
    from pathlib import Path
    shm_base = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/store"))
    parts = []
    total_files = 0
    total_bytes = 0
    for dtype in ("tick", "order", "deal", "quote", "kline", "daily_basic"):
        d = shm_base / dtype
        if not d.exists():
            continue
        files = list(d.glob("*.arrow"))
        size = sum(f.stat().st_size for f in files) if files else 0
        total_files += len(files)
        total_bytes += size
        if files:
            parts.append(f"{dtype}={len(files)}files/{size / 1024 / 1024:.1f}MB")
    parts.append(f"total={total_files}files/{total_bytes / 1024 / 1024:.1f}MB")
    return " ".join(parts)


def _quick_release() -> None:
    """Lightweight memory release after each write batch — no GC, just Arrow pool + malloc_trim."""
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass


def _release_unused_memory() -> None:
    """Full memory cleanup — GC + Arrow pool + malloc_trim. Called periodically."""
    try:
        gc.collect()
        _quick_release()
    except Exception:
        pass


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    collector = SDKCollector(load_config())
    signal.signal(signal.SIGTERM, lambda *_: collector.stop())
    signal.signal(signal.SIGINT, lambda *_: collector.stop())
    try:
        collector.start()
    except Exception as exc:
        logger.error("[sdk] collector failed: %s", exc, exc_info=True)
        collector.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
