# -*- coding: utf-8 -*-
"""MDL SDK based real-time collector."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import logging
import os
import queue
import signal
import sys
import threading
import time
from typing import Dict, List

from ..core.constants import ORDER_COLUMNS, DEAL_COLUMNS, TICK_COLUMNS
from ..data.mysql_loader import DailyBasicCache
from ..data.shm_store import ShmStore
from ..live_engine.pipeline_logger import get_collector_logger
from .sdk_callback import MappedMessage, SequenceTracker, create_callback
from .sdk_config import SDKCollectorConfig, load_config
from . import sdk_mapper

logger = logging.getLogger(__name__)


class SDKCollector:
    def __init__(self, config: SDKCollectorConfig):
        self.config = config
        maxsize = config.queue_hard_limit if config.queue_hard_limit > 0 else 0
        self.queue: queue.Queue[MappedMessage] = queue.Queue(maxsize=maxsize)
        self.tracker = SequenceTracker()
        self.store = ShmStore()
        self._stop = threading.Event()
        self._trading_day = date.today()
        self._io_man = None
        self._subscriber = None
        self._flush_thread = threading.Thread(target=self._flush_loop, name="sdk-flush", daemon=True)
        self._stats: Dict[str, int] = defaultdict(int)
        self._last_rolling_cleanup = 0.0
        self._last_minute_stat_log = time.time()
        self._minute_write_stats: Dict[tuple[int, int, str, str], int] = defaultdict(int)
        self._minute_stat_lock = threading.Lock()

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

        callback = create_callback(pymdl, self.queue, self.trading_day, self.tracker)
        self._subscriber = self._io_man.CreateSubscriber(callback, self.config.callback_multithread)
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

    def _flush_loop(self) -> None:
        interval = max(self.config.flush_interval_ms, 1) / 1000.0
        last_log = time.time()
        while not self._stop.is_set():
            batch = self._drain_batch(interval)
            if batch:
                self._write_batch(batch)
            now = time.time()
            if now - last_log >= 5:
                self._log_status()
                last_log = now
            if now - self._last_rolling_cleanup >= 30:
                self.store.cleanup_rolling()
                self._last_rolling_cleanup = now
            if now - self._last_minute_stat_log >= 60:
                self._log_minute_write_stats()
                self._last_minute_stat_log = now

    def _drain_batch(self, interval: float) -> List[MappedMessage]:
        batch: List[MappedMessage] = []
        deadline = time.time() + interval
        while len(batch) < self.config.batch_size:
            timeout = max(deadline - time.time(), 0)
            try:
                item = self.queue.get(timeout=timeout)
                batch.append(item)
            except queue.Empty:
                break
            if time.time() >= deadline:
                break
        return batch

    def _write_batch(self, batch: List[MappedMessage]) -> None:
        pipe_log = get_collector_logger()
        by_kind: Dict[str, List[dict]] = {"tick": [], "order": [], "deal": []}
        receive_to_write_ms: Dict[str, List[float]] = {"tick": [], "order": [], "deal": []}
        market_to_receive_ms: Dict[str, List[float]] = {"tick": [], "order": [], "deal": []}
        batch_minute_stats: Dict[tuple[int, int, str, str], int] = defaultdict(int)
        oldest_age_ms = 0.0
        now = time.time()
        for item in batch:
            by_kind[item.kind].append(item.row)
            queue_age_ms = (now - item.receive_ts) * 1000
            oldest_age_ms = max(oldest_age_ms, queue_age_ms)
            receive_to_write_ms[item.kind].append(queue_age_ms)
            market_latency = _market_to_receive_ms(item.row.get("Time"), item.receive_ts)
            if market_latency is not None:
                market_to_receive_ms[item.kind].append(market_latency)
            market_minute = _market_minute(item.row.get("Time"))
            if market_minute:
                key = (item.service_id, item.message_id, item.kind, market_minute)
                batch_minute_stats[key] += 1

        frames = {
            "tick": sdk_mapper.frame(by_kind["tick"], TICK_COLUMNS),
            "order": sdk_mapper.frame(by_kind["order"], ORDER_COLUMNS),
            "deal": sdk_mapper.frame(by_kind["deal"], DEAL_COLUMNS),
        }

        for kind, df in frames.items():
            if df.empty:
                continue
            t0 = time.time()
            chunk_name = getattr(self.store, f"update_{kind}")(df)
            elapsed_ms = round((time.time() - t0) * 1000, 1)
            rows = len(df)
            self._stats[f"written_{kind}"] += rows
            with self._minute_stat_lock:
                for key, stat_rows in batch_minute_stats.items():
                    if key[2] == kind:
                        self._minute_write_stats[key] += stat_rows
            pipe_log.log(
                "sdk_shm_write",
                data_type=kind,
                chunk=chunk_name or "",
                rows=rows,
                elapsed_ms=elapsed_ms,
                queue_size=self.queue.qsize(),
                oldest_queue_age_ms=round(oldest_age_ms, 1),
                **_latency_summary("receive_to_write", receive_to_write_ms[kind]),
                **_latency_summary("market_to_receive", market_to_receive_ms[kind]),
            )

    def _log_status(self) -> None:
        qsize = self.queue.qsize()
        if qsize > self.config.queue_warn_size:
            logger.warning("[sdk] queue backlog=%d warn=%d", qsize, self.config.queue_warn_size)
        snap = self.tracker.snapshot()
        logger.info(
            "[sdk] q=%d written tick=%d order=%d deal=%d gaps=%s",
            qsize,
            self._stats.get("written_tick", 0),
            self._stats.get("written_order", 0),
            self._stats.get("written_deal", 0),
            snap["gaps"],
        )

    def _log_minute_write_stats(self, force: bool = False) -> None:
        pipe_log = get_collector_logger()
        current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
        with self._minute_stat_lock:
            ready = [
                (key, rows)
                for key, rows in sorted(self._minute_write_stats.items())
                if force or key[3] < current_minute
            ]
            for key, _ in ready:
                self._minute_write_stats.pop(key, None)
        for key, rows in ready:
            service_id, message_id, kind, market_minute = key
            pipe_log.log(
                "sdk_minute_write",
                service_id=service_id,
                message_id=message_id,
                data_type=kind,
                market_minute=market_minute,
                rows=rows,
            )

    def start(self) -> None:
        logger.info("[sdk] collector starting config=%s", self.config)
        self._load_daily_basic()
        self._flush_thread.start()
        self._connect()
        while not self._stop.is_set():
            self._stop.wait(1.0)

    def stop(self) -> None:
        self._stop.set()
        self._log_minute_write_stats(force=True)
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


def _market_to_receive_ms(market_time, receive_ts: float) -> float | None:
    if market_time is None:
        return None
    try:
        if hasattr(market_time, "to_pydatetime"):
            dt = market_time.to_pydatetime()
        elif isinstance(market_time, datetime):
            dt = market_time
        else:
            dt = datetime.fromisoformat(str(market_time))
        return round((receive_ts - dt.timestamp()) * 1000, 1)
    except Exception:
        return None


def _market_minute(market_time) -> str | None:
    if market_time is None:
        return None
    try:
        if hasattr(market_time, "to_pydatetime"):
            dt = market_time.to_pydatetime()
        elif isinstance(market_time, datetime):
            dt = market_time
        else:
            dt = datetime.fromisoformat(str(market_time))
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None


def _latency_summary(prefix: str, values: List[float]) -> dict:
    if not values:
        return {}
    ordered = sorted(values)
    mid = len(ordered) // 2
    p50 = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2
    return {
        f"{prefix}_avg_ms": round(sum(values) / len(values), 1),
        f"{prefix}_max_ms": round(max(values), 1),
        f"{prefix}_p50_ms": round(p50, 1),
    }


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
