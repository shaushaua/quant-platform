# -*- coding: utf-8 -*-
"""
合并引擎：pymdl SDK + 因子计算一体化。

pymdl 回调直接更新 StockState，去掉 Arrow buffer / ShmStore / /dev/shm IPC 管线。
解决 collector 无界 RSS 增长问题（预估 ~400MB steady-state vs 之前 5GB+ OOM）。

数据流：
    pymdl SDK → callback → StockState.update_*_scalar()
                                ↓ (每 60s)
                           factor_calculation(state, code, date, end_time) → CSV + OSS

环境变量（合并了 collector + live-engine 的配置）：
    MDL_SERVER             MDL 服务地址（默认 127.0.0.1:9012）
    MDL_SUBS               订阅配置（默认 4.4,4.24,6.28,6.33,6.36）
    FACTOR_MODULE          交易员因子模块路径
    COMPUTE_INTERVAL       计算间隔秒数（默认 60）
    FACTOR_OUTPUT_PATH     因子结果输出目录
    COLLECTOR_MEM_LIMIT_MB 自保护内存阈值 MB（默认 8192，0=禁用）
"""

from __future__ import annotations

import gc
import importlib
import json
import logging
import os
import pickle
import resource
import signal
import sys
import threading
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set, Tuple

import pandas as pd

from ..collector.sdk_callback import SequenceTracker
from ..collector.sdk_config import SDKCollectorConfig, load_config
from ..data.mysql_loader import DailyBasicCache
from ..factor.base import StockState
from .pipeline_logger import get_streaming_logger
from .streaming_engine import (
    _TRADING_END,
    _TRADING_START,
    _extract_chunk_ts,
    _is_trading_hours,
    _time_to_seconds,
    _upload_to_oss,
)

logger = logging.getLogger(__name__)


# ================================================================== #
# Lightweight extraction helpers — no dict, no pd.Timestamp           #
# ================================================================== #

def _f(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except Exception:
        return 0.0


def _i(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _code(raw: Any, market: str) -> str:
    code = str(raw or "").strip().zfill(6)
    suffix = ".XSHG" if market == "SH" else ".XSHE"
    return code + suffix


def _is_stock(raw: Any, market: str) -> bool:
    code = str(raw or "").strip().zfill(6)
    if market == "SH":
        return code.startswith(("6", "9"))
    return code.startswith(("0", "3"))


def _side_from_flag(flag: Any) -> int:
    flag = str(flag or "").strip()
    if flag == "B":
        return 0
    if flag == "S":
        return 1
    return 10


def _raw_time(value: Any) -> str:
    """Extract time as plain string, no pd.Timestamp creation."""
    return str(value or "")


# ================================================================== #
# Direct callback — updates StockState inline, zero IPC               #
# ================================================================== #

def create_direct_callback(
    pymdl,
    states: Dict[str, StockState],
    lock: threading.Lock,
    trading_day_getter,
    tracker: SequenceTracker,
):
    """Create a pymdl callback that updates StockState directly."""

    class DirectCallback(pymdl.MsgCallback):
        def _get_or_create(self, code: str) -> Optional[StockState]:
            state = states.get(code)
            if state is None:
                state = StockState(code=code)
                states[code] = state
            return state

        def _observe(self, hd) -> None:
            gap = tracker.observe(int(hd.ServiceID), int(hd.MessageID), int(hd.SequenceID))
            if gap is not None:
                logger.error(
                    "[sdk-seq-gap] sid=%s mid=%s expected=%s actual=%s",
                    hd.ServiceID, hd.MessageID, gap[0], gap[1],
                )

        def OnMDLAPIMessage(self, hd, buf):
            try:
                msg = pymdl.mdl_api_msg.Read(hd.MessageID, buf)
                logger.info("[sdk-api] sid=%s mid=%s msg=%s", hd.ServiceID, hd.MessageID, msg)
            except Exception:
                pass

        def OnMDLSysMessage(self, hd, buf):
            try:
                msg = pymdl.mdl_sys_msg.Read(hd.MessageID, buf)
                msg_text = str(msg)
                del msg
                if int(hd.MessageID) == 4 and "Reversed" in msg_text:
                    return
                logger.info("[sdk-sys] sid=%s mid=%s msg=%s", hd.ServiceID, hd.MessageID, msg_text)
            except Exception:
                pass

        # ---------------------------------------------------------- #
        # SH messages                                                 #
        # ---------------------------------------------------------- #

        def OnMDLSHL2Message(self, hd, buf):
            try:
                self._observe(hd)
                msg = pymdl.mdl_shl2_msg.Read(hd.MessageID, buf)
                trading_day = trading_day_getter()

                if hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_SHL2MarketData:
                    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
                        del msg
                        return
                    code = _code(msg.SecurityID, "SH")
                    raw_t = _raw_time(getattr(msg, "UpdateTime", ""))
                    asks = list(getattr(msg, "SellLevels", []) or [])
                    bids = list(getattr(msg, "BidLevels", []) or [])
                    ask1 = _f(getattr(asks[0], "OrderPrice", 0)) if asks else 0.0
                    bid1 = _f(getattr(bids[0], "OrderPrice", 0)) if bids else 0.0
                    ask_v1 = _i(getattr(asks[0], "OrderVol", 0)) if asks else 0
                    bid_v1 = _i(getattr(bids[0], "OrderVol", 0)) if bids else 0
                    with lock:
                        self._get_or_create(code).update_tick_scalar(
                            _f(getattr(msg, "LastPrice", 0)),
                            _f(getattr(msg, "PreCloPrice", 0)),
                            _f(getattr(msg, "OpenPrice", 0)),
                            _f(getattr(msg, "HighPrice", 0)),
                            _f(getattr(msg, "LowPrice", 0)),
                            ask1, bid1, ask_v1, bid_v1, raw_t,
                        )
                    del asks, bids

                elif hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_NGTSTick:
                    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
                        del msg
                        return
                    code = _code(msg.SecurityID, "SH")
                    typ = str(getattr(msg, "Type", "")).strip()
                    raw_t = _raw_time(getattr(msg, "TickTime", ""))
                    side = _side_from_flag(getattr(msg, "TickBSFlag", ""))

                    if typ in ("A", "D"):
                        order_type = 2 if typ == "A" else 5
                        with lock:
                            self._get_or_create(code).update_order_scalar(
                                side,
                                _f(getattr(msg, "Qty", 0)),
                                order_type,
                                raw_t,
                            )
                    elif typ == "T":
                        price = _f(getattr(msg, "Price", 0))
                        volume = _f(getattr(msg, "Qty", 0))
                        with lock:
                            self._get_or_create(code).update_deal_scalar(
                                price, volume, raw_t,
                            )

                del msg
            except Exception as exc:
                logger.warning("[callback] SHL2 failed: %s", exc)

        # ---------------------------------------------------------- #
        # SZ messages                                                 #
        # ---------------------------------------------------------- #

        def OnMDLSZL2Message(self, hd, buf):
            try:
                self._observe(hd)
                msg = pymdl.mdl_szl2_msg.Read(hd.MessageID, buf)
                trading_day = trading_day_getter()

                if hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Snapshot300111_v2:
                    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
                        del msg
                        return
                    code = _code(msg.SecurityID, "SZ")
                    raw_t = _raw_time(getattr(msg, "UpdateTime", ""))
                    asks = list(getattr(msg, "AskPriceLevel", []) or [])
                    bids = list(getattr(msg, "BidPriceLevel", []) or [])
                    ask1 = _f(getattr(asks[0], "Price", 0)) if asks else 0.0
                    bid1 = _f(getattr(bids[0], "Price", 0)) if bids else 0.0
                    ask_v1 = _i(getattr(asks[0], "Volume", 0)) if asks else 0
                    bid_v1 = _i(getattr(bids[0], "Volume", 0)) if bids else 0
                    with lock:
                        self._get_or_create(code).update_tick_scalar(
                            _f(getattr(msg, "LastPrice", 0)),
                            _f(getattr(msg, "PreCloPrice", 0)),
                            _f(getattr(msg, "OpenPrice", 0)),
                            _f(getattr(msg, "HighPrice", 0)),
                            _f(getattr(msg, "LowPrice", 0)),
                            ask1, bid1, ask_v1, bid_v1, raw_t,
                        )
                    del asks, bids

                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Order300192_v2:
                    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
                        del msg
                        return
                    code = _code(msg.SecurityID, "SZ")
                    raw_t = _raw_time(getattr(msg, "TransactTime", ""))
                    side = {49: 0, 50: 1}.get(_i(getattr(msg, "Side", 0)), 10)
                    order_type = {49: 1, 50: 2, 85: 3}.get(_i(getattr(msg, "OrdType", 0)), 0)
                    with lock:
                        self._get_or_create(code).update_order_scalar(
                            side,
                            _f(getattr(msg, "OrderQty", 0)),
                            order_type,
                            raw_t,
                        )

                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Transaction300191_v2:
                    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
                        del msg
                        return
                    code = _code(msg.SecurityID, "SZ")
                    raw_t = _raw_time(getattr(msg, "TransactTime", ""))
                    price = _f(getattr(msg, "LastPx", 0))
                    volume = _f(getattr(msg, "LastQty", 0))
                    with lock:
                        self._get_or_create(code).update_deal_scalar(
                            price, volume, raw_t,
                        )

                del msg
            except Exception as exc:
                logger.warning("[callback] SZL2 failed: %s", exc)

    return DirectCallback()


# ================================================================== #
# Combined Engine                                                     #
# ================================================================== #

class CombinedEngine:
    """
    Single-process engine: pymdl SDK → StockState → factor calculation.

    Replaces both sdk_collector + streaming_engine.
    """

    _CHECKPOINT_NAME = "combined_checkpoint.pkl"

    def __init__(self):
        self._lock = threading.Lock()
        self.states: Dict[str, StockState] = {}
        self._stopped = False
        self._trading_day: str = ""
        self._start_time = time.time()

        # pymdl SDK
        self.config: SDKCollectorConfig = load_config()
        self._io_man = None
        self._subscriber = None

        # Sequence tracking
        self.tracker = SequenceTracker()

        # Factor module
        module_path = os.environ.get("FACTOR_MODULE", "")
        if not module_path:
            logger.error("[combined] FACTOR_MODULE not set")
            sys.exit(1)
        self.factor_module = importlib.import_module(module_path)
        self.factor_calculation: Callable = self.factor_module.factor_calculation
        self.outfun: Optional[Callable] = getattr(self.factor_module, "outfun", None)
        self.compute_interval = int(os.environ.get("COMPUTE_INTERVAL", "60"))

        # Output & checkpoint
        output_path_str = os.environ.get("FACTOR_OUTPUT_PATH", "")
        self.output_path = Path(output_path_str) if output_path_str else None
        if self.output_path:
            self._checkpoint_path = self.output_path / self._CHECKPOINT_NAME
        else:
            self._checkpoint_path = Path("/tmp") / self._CHECKPOINT_NAME

        self._load_checkpoint()

        logger.info(
            "[combined] init done: module=%s interval=%ds",
            module_path, self.compute_interval,
        )

    def trading_day(self) -> str:
        return self._trading_day or datetime.now().strftime("%Y%m%d")

    # ------------------------------------------------------------------ #
    # pymdl SDK connection                                                 #
    # ------------------------------------------------------------------ #

    def _connect(self) -> None:
        try:
            import pymdl
        except ImportError as exc:
            raise RuntimeError("pymdl not installed") from exc

        self._io_man = pymdl.CreateIOController(self.config.io_threads)
        log_path = os.environ.get("MDL_LOG_PATH", "/data/quant/mdl_logs/mdl")
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            self._io_man.EnableLog(log_path, False)
        except Exception:
            pass

        callback = create_direct_callback(
            pymdl, self.states, self._lock, self.trading_day, self.tracker,
        )
        self._subscriber = self._io_man.CreateSubscriber(callback, self.config.callback_multithread)
        self._subscriber.SetServerAddress(self.config.server)
        self._subscriber.SetMessageEncoding(self.config.encoding)
        self._subscriber.EnableMergeMessage(self.config.enable_merge)
        self._subscriber.SetHeartbeatInterval(self.config.heartbeat_interval)
        self._subscriber.SetHeartbeatTimeout(self.config.heartbeat_timeout)

        for service_id, message_id in self.config.subs:
            self._subscriber.AddSubscription(service_id, 101, message_id)
            logger.info("[combined] subscribe %s.%s", service_id, message_id)

        err = self._subscriber.Connect()
        if err:
            if isinstance(err, bytes):
                err = err.decode("GBK", errors="replace")
            raise RuntimeError(f"MDL Connect failed: {err}")
        logger.info("[combined] connected to %s", self.config.server)

    # ------------------------------------------------------------------ #
    # Factor computation (reuses streaming_engine logic)                   #
    # ------------------------------------------------------------------ #

    def _compute_and_output(self) -> None:
        """Snapshot states, compute factors, write output."""
        pipe_log = get_streaming_logger()
        now = datetime.now()
        date_str = now.strftime("%Y%m%d")
        end_time = now.strftime("%H%M%S")

        with self._lock:
            self._trading_day = date_str
            snapshot = dict(self.states)

        logger.info("[combined] computing: date=%s end_time=%s stocks=%d",
                     date_str, end_time, len(snapshot))

        t0 = time.time()
        results = []
        for code, state in snapshot.items():
            try:
                result = self.factor_calculation(state, code, date_str, end_time)
                if result is not None:
                    if state.last_market_time:
                        market_secs = _time_to_seconds(state.last_market_time)
                        if market_secs > 0:
                            wall_secs = now.hour * 3600 + now.minute * 60 + now.second
                            data_latency_ms = round((wall_secs - market_secs) * 1000, 1)
                            if abs(data_latency_ms) < 600_000:
                                result["data_latency_ms"] = data_latency_ms
                    results.append(result)
            except Exception as exc:
                logger.warning("[%s] factor failed: %s", code, exc)

        elapsed_ms = (time.time() - t0) * 1000
        result_df = pd.DataFrame(results) if results else pd.DataFrame()
        logger.info("[combined] computed: %d results in %.0fms", len(result_df), elapsed_ms)

        # CSV
        if self.output_path and not result_df.empty:
            self.output_path.mkdir(parents=True, exist_ok=True)
            out_file = self.output_path / f"{date_str}_{end_time}.csv"
            result_df.to_csv(out_file, index=False)
            logger.info("[combined] wrote %s", out_file)

        # OSS
        if not result_df.empty:
            _upload_to_oss(result_df, date_str, end_time)

        # outfun
        if self.outfun is not None:
            try:
                self.outfun(date_str, end_time, result_df)
            except Exception as exc:
                logger.error("[combined] outfun failed: %s", exc)

        pipe_log.log("factor_compute", date=date_str, end_time=end_time,
                      stocks=len(snapshot), results=len(result_df),
                      compute_ms=round(elapsed_ms, 1))

    # ------------------------------------------------------------------ #
    # Checkpoint (same logic as streaming_engine)                          #
    # ------------------------------------------------------------------ #

    def _save_checkpoint(self) -> None:
        try:
            with self._lock:
                data = {"trading_day": self._trading_day, "states": dict(self.states)}
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._checkpoint_path.with_suffix(".tmp")
            with open(tmp, "wb") as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            tmp.replace(self._checkpoint_path)
        except Exception as exc:
            logger.warning("[checkpoint] save failed: %s", exc)

    def _load_checkpoint(self) -> None:
        if not self._checkpoint_path.exists():
            return
        now = datetime.now()
        if now.hour < 9 or (now.hour == 9 and now.minute < 15):
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
                return
            self._trading_day = ckpt_day
            self.states = data.get("states", {})
            logger.info("[checkpoint] restored: day=%s stocks=%d", self._trading_day, len(self.states))
        except Exception:
            self.states.clear()

    def _check_day_rollover(self) -> None:
        today = datetime.now().strftime("%Y%m%d")
        with self._lock:
            if self._trading_day and today != self._trading_day:
                logger.info("[combined] day rollover: %s -> %s", self._trading_day, today)
                self.states.clear()
                self._trading_day = today
                try:
                    self._checkpoint_path.unlink(missing_ok=True)
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    # Memory diagnostics                                                   #
    # ------------------------------------------------------------------ #

    def _log_mem(self) -> None:
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_mb = rss_kb / 1024
        logger.warning(
            "[mem] RSS=%.0fMB stocks=%d tick=%d deal=%d order=%d",
            rss_mb, len(self.states),
            sum(s.tick_count for s in self.states.values()),
            sum(s.deal_count for s in self.states.values()),
            sum(s.order_count for s in self.states.values()),
        )

    # ------------------------------------------------------------------ #
    # Main loop                                                            #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        logger.info("[combined] engine starting")
        self._trading_day = datetime.now().strftime("%Y%m%d")

        # Load daily_basic for factor modules that need it
        try:
            market_count = int(os.environ.get("DAILY_BASIC_MARKET_COUNT", "1"))
            cache = DailyBasicCache(market_count=market_count)
            trade_date = self._trading_day
            if cache.load(trade_date):
                logger.info("[combined] daily_basic loaded")
        except Exception as exc:
            logger.warning("[combined] daily_basic load failed: %s", exc)

        self._connect()

        last_compute = time.time()
        last_checkpoint = time.time()
        last_mem_log = time.time()
        self._start_time = time.time()

        while not self._stopped:
            now = time.time()

            # Factor computation
            if now - last_compute >= self.compute_interval:
                if _is_trading_hours() and self.states:
                    self._compute_and_output()
                last_compute = now

            # Checkpoint
            if now - last_checkpoint >= 30 and self.states and _is_trading_hours():
                self._save_checkpoint()
                last_checkpoint = now

            # Memory log
            if now - last_mem_log >= 30:
                self._log_mem()
                last_mem_log = now

            # Day rollover
            self._check_day_rollover()

            # Self-protection
            mem_limit_mb = int(os.environ.get("COLLECTOR_MEM_LIMIT_MB", "8192"))
            if mem_limit_mb > 0:
                rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
                if rss_mb > mem_limit_mb:
                    logger.error("[mem] RSS=%.0fMB exceeds limit %dMB, exiting", rss_mb, mem_limit_mb)
                    break

            time.sleep(0.5)

    def stop(self) -> None:
        self._stopped = True
        if self.states:
            self._save_checkpoint()
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
        logger.info("[combined] engine stopped")


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = CombinedEngine()
    signal.signal(signal.SIGTERM, lambda *_: engine.stop())
    signal.signal(signal.SIGINT, lambda *_: engine.stop())
    try:
        engine.run()
    except Exception as exc:
        logger.error("[combined] failed: %s", exc, exc_info=True)
        engine.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
