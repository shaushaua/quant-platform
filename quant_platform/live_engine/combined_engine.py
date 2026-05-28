# -*- coding: utf-8 -*-
"""
合并引擎 v2：pymdl SDK + 因子计算一体化（完整 StockData 支持）。

数据流：
    pymdl SDK → callback
                  ├─ sdk_mapper.write_*()  → ArrowBuffer（原始数据，columnar numpy）
                  └─ StockState.update_*_scalar() → 累计聚合值

    每 60 秒：
        1. flush ArrowBuffer → RecordBatch → DataFrame
        2. 追加到滑动窗口（默认 5 分钟），丢弃过期数据
        3. 按 Code 分组 → 构建完整 StockData（含 l1_tick/l2_deal/l2_order DataFrame + StockState）
        4. factor_calculation(stock_data, code, date, end_time) → 结果 → CSV + OSS

环境变量：
    MDL_SERVER             MDL 服务地址（默认 127.0.0.1:9012）
    MDL_SUBS               订阅配置（默认 4.4,4.24,6.28,6.33,6.36）
    FACTOR_MODULE          交易员因子模块路径
    COMPUTE_INTERVAL       计算间隔秒数（默认 60）
    DATA_WINDOW_SECONDS    滑动窗口秒数（默认 300 = 5 分钟）
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
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

import pandas as pd
import pyarrow as pa

from ..collector.arrow_buffer import ArrowBuffer
from ..collector.sdk_callback import SequenceTracker
from ..collector.sdk_config import SDKCollectorConfig, load_config
from ..core.constants import ARROW_SCHEMA_BY_KIND
from ..data.mysql_loader import DailyBasicCache
from ..factor.base import StockData, StockState
from .pipeline_logger import get_streaming_logger
from .streaming_engine import (
    _TRADING_END,
    _TRADING_START,
    _is_trading_hours,
    _time_to_seconds,
    _upload_to_oss,
)

# Re-use sdk_mapper for ArrowBuffer writes and scalar helpers
from ..collector import sdk_mapper
from ..collector.sdk_mapper import _f, _i, _code, _is_stock, _side_from_flag

logger = logging.getLogger(__name__)


def _raw_time(value: Any) -> str:
    return str(value or "")


# ================================================================== #
# Direct callback — ArrowBuffer writes + StockState scalar updates     #
# ================================================================== #

def create_direct_callback(
    pymdl,
    buffers: Dict[str, ArrowBuffer],
    states: Dict[str, StockState],
    lock: threading.Lock,
    trading_day_getter,
    tracker: SequenceTracker,
):
    """Create pymdl callback that writes to ArrowBuffer AND updates StockState."""

    class DirectCallback(pymdl.MsgCallback):
        def _get_or_create(self, code: str) -> StockState:
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
                    # 1. ArrowBuffer write (reuses sdk_mapper)
                    sdk_mapper.write_sh_tick(buffers["tick"], msg, trading_day, int(hd.SequenceID))

                    # 2. StockState scalar update
                    if _is_stock(getattr(msg, "SecurityID", ""), "SH"):
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

                elif hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_NGTSTick:
                    # 1. ArrowBuffer write
                    sdk_mapper.write_sh_ngts_tick(
                        buffers["order"], buffers["deal"], msg, trading_day,
                    )

                    # 2. StockState scalar update
                    if _is_stock(getattr(msg, "SecurityID", ""), "SH"):
                        code = _code(msg.SecurityID, "SH")
                        typ = str(getattr(msg, "Type", "")).strip()
                        raw_t = _raw_time(getattr(msg, "TickTime", ""))
                        side = _side_from_flag(getattr(msg, "TickBSFlag", ""))
                        with lock:
                            if typ in ("A", "D"):
                                order_type = 2 if typ == "A" else 5
                                self._get_or_create(code).update_order_scalar(
                                    side,
                                    _f(getattr(msg, "Qty", 0)),
                                    order_type,
                                    raw_t,
                                )
                            elif typ == "T":
                                self._get_or_create(code).update_deal_scalar(
                                    _f(getattr(msg, "Price", 0)),
                                    _f(getattr(msg, "Qty", 0)),
                                    raw_t,
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
                    # 1. ArrowBuffer write
                    sdk_mapper.write_sz_tick(buffers["tick"], msg, trading_day, int(hd.SequenceID))

                    # 2. StockState scalar update
                    if _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
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

                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Order300192_v2:
                    # 1. ArrowBuffer write
                    sdk_mapper.write_sz_order(buffers["order"], msg, trading_day)

                    # 2. StockState scalar update
                    if _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
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
                    # 1. ArrowBuffer write
                    sdk_mapper.write_sz_deal(buffers["deal"], msg, trading_day)

                    # 2. StockState scalar update
                    if _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
                        code = _code(msg.SecurityID, "SZ")
                        raw_t = _raw_time(getattr(msg, "TransactTime", ""))
                        with lock:
                            self._get_or_create(code).update_deal_scalar(
                                _f(getattr(msg, "LastPx", 0)),
                                _f(getattr(msg, "LastQty", 0)),
                                raw_t,
                            )

                del msg
            except Exception as exc:
                logger.warning("[callback] SZL2 failed: %s", exc)

    return DirectCallback()


# ================================================================== #
# Combined Engine                                                      #
# ================================================================== #

class CombinedEngine:
    """
    Single-process engine: pymdl SDK → ArrowBuffer + StockState → StockData → factor calculation.

    Replaces both sdk_collector + streaming_engine.
    Provides full StockData (with DataFrames) matching the backtest DataAPI.
    """

    _CHECKPOINT_NAME = "combined_checkpoint.pkl"

    def __init__(self):
        self._lock = threading.Lock()
        self.states: Dict[str, StockState] = {}
        self._stopped = False
        self._trading_day: date = date.today()
        self._start_time = time.time()

        # pymdl SDK config
        self.config: SDKCollectorConfig = load_config()
        self._io_man = None
        self._subscriber = None
        self.tracker = SequenceTracker()

        # ArrowBuffers for raw data (same as collector)
        self._buffers = {
            "tick": ArrowBuffer(ARROW_SCHEMA_BY_KIND["tick"]),
            "order": ArrowBuffer(ARROW_SCHEMA_BY_KIND["order"]),
            "deal": ArrowBuffer(ARROW_SCHEMA_BY_KIND["deal"]),
        }

        # Sliding window: accumulated DataFrames from recent flushes
        # Key: "tick"/"order"/"deal" → list of DataFrames
        self._window_dfs: Dict[str, List[pd.DataFrame]] = {
            "tick": [], "order": [], "deal": [],
        }
        self._data_window_seconds = int(os.environ.get("DATA_WINDOW_SECONDS", "300"))

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

        # daily_basic data (loaded once at startup)
        self._market_df: pd.DataFrame = pd.DataFrame()
        self._daily_basic_df: pd.DataFrame = pd.DataFrame()

        self._load_checkpoint()

        logger.info(
            "[combined] init done: module=%s interval=%ds window=%ds",
            module_path, self.compute_interval, self._data_window_seconds,
        )

    def trading_day(self) -> date:
        return self._trading_day

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
            pymdl, self._buffers, self.states, self._lock, self.trading_day, self.tracker,
        )
        self._subscriber = self._io_man.CreateSubscriber(callback, self.config.callback_multithread)
        self._subscriber.SetServerAddress(self.config.server)
        if self.config.username:
            self._subscriber.SetUserName(self.config.username)
        if self.config.password:
            self._subscriber.SetPassword(self.config.password)
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
    # ArrowBuffer flush → DataFrame                                        #
    # ------------------------------------------------------------------ #

    def _flush_buffer(self, kind: str) -> pd.DataFrame:
        """Flush ArrowBuffer to DataFrame and clear the buffer."""
        buf = self._buffers[kind]
        if buf.row_count == 0:
            return pd.DataFrame()
        batch = buf.flush()
        if batch is None:
            return pd.DataFrame()
        try:
            df = batch.to_pandas()
        finally:
            del batch
        return df

    # ------------------------------------------------------------------ #
    # Sliding window management                                            #
    # ------------------------------------------------------------------ #

    def _update_window(self, kind: str, new_df: pd.DataFrame) -> None:
        """Append new flush to window and prune old data."""
        if new_df.empty:
            return
        self._window_dfs[kind].append(new_df)
        self._prune_window(kind)

    def _prune_window(self, kind: str) -> None:
        """Remove DataFrames whose data is entirely older than window."""
        cutoff = time.time() - self._data_window_seconds
        # Keep at least one DataFrame even if it's old
        while len(self._window_dfs[kind]) > 1:
            oldest = self._window_dfs[kind][0]
            if oldest.empty:
                self._window_dfs[kind].pop(0)
                continue
            # Check if the newest row in the oldest df is still within window
            if "Time" in oldest.columns and not oldest.empty:
                # Time column has timestamps — check the last row
                last_time = oldest["Time"].iloc[-1]
                if hasattr(last_time, "timestamp"):
                    try:
                        if last_time.timestamp() < cutoff:
                            self._window_dfs[kind].pop(0)
                            continue
                    except Exception:
                        pass
            break

    def _get_window_df(self, kind: str) -> pd.DataFrame:
        """Concatenate all DataFrames in the window for this kind."""
        dfs = self._window_dfs[kind]
        if not dfs:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0].copy()
        try:
            combined = pd.concat(dfs, ignore_index=True)
            # Filter by time window
            cutoff = pd.Timestamp.now() - pd.Timedelta(seconds=self._data_window_seconds)
            if "Time" in combined.columns:
                try:
                    combined = combined[combined["Time"] >= cutoff].copy()
                except Exception:
                    pass
            return combined
        except Exception:
            return pd.concat(dfs, ignore_index=True)

    def _clear_window(self, kind: str) -> None:
        """Clear all DataFrames from the window."""
        self._window_dfs[kind].clear()

    # ------------------------------------------------------------------ #
    # Factor computation                                                   #
    # ------------------------------------------------------------------ #

    def _compute_and_output(self) -> None:
        """Flush buffers, build StockData per stock, compute factors, output."""
        pipe_log = get_streaming_logger()
        now = datetime.now()
        date_str = self._trading_day.strftime("%Y%m%d")
        end_time = now.strftime("%H%M%S")

        # 1. Flush ArrowBuffers → DataFrame, append to sliding window
        tick_new = self._flush_buffer("tick")
        deal_new = self._flush_buffer("deal")
        order_new = self._flush_buffer("order")

        self._update_window("tick", tick_new)
        self._update_window("deal", deal_new)
        self._update_window("order", order_new)

        del tick_new, deal_new, order_new

        # 2. Get windowed DataFrames
        tick_df = self._get_window_df("tick")
        deal_df = self._get_window_df("deal")
        order_df = self._get_window_df("order")

        # 3. Snapshot StockState
        with self._lock:
            self._trading_day = date_str
            states_snapshot = dict(self.states)

        logger.info("[combined] computing: date=%s end_time=%s stocks=%d "
                     "tick=%d deal=%d order=%d rows",
                     date_str, end_time, len(states_snapshot),
                     len(tick_df), len(deal_df), len(order_df))

        # 4. Collect all stock codes
        all_codes = set(states_snapshot.keys())
        if not tick_df.empty and "Code" in tick_df.columns:
            all_codes.update(tick_df["Code"].unique())
        if not deal_df.empty and "Code" in deal_df.columns:
            all_codes.update(deal_df["Code"].unique())
        if not order_df.empty and "Code" in order_df.columns:
            all_codes.update(order_df["Code"].unique())

        # 5. Per-stock computation
        t0 = time.time()
        results = []
        for code in all_codes:
            try:
                # Extract per-stock DataFrames
                stock_tick = tick_df[tick_df["Code"] == code] if not tick_df.empty else pd.DataFrame()
                stock_deal = deal_df[deal_df["Code"] == code] if not deal_df.empty else pd.DataFrame()
                stock_order = order_df[order_df["Code"] == code] if not order_df.empty else pd.DataFrame()

                stock_data = StockData(
                    code=code,
                    date=date_str,
                    end_time=end_time,
                    l1_tick=stock_tick,
                    l2_deal=stock_deal,
                    l2_order=stock_order,
                    market=self._market_df,
                    daily_basic=self._daily_basic_df,
                    state=states_snapshot.get(code),
                )
                result = self.factor_calculation(stock_data, code, date_str, end_time)
                if result is not None:
                    # Data latency
                    state = states_snapshot.get(code)
                    if state and state.last_market_time:
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
                      stocks=len(all_codes), results=len(result_df),
                      compute_ms=round(elapsed_ms, 1))

        # 6. Release temporary DataFrames
        del tick_df, deal_df, order_df, result_df

    # ------------------------------------------------------------------ #
    # Checkpoint                                                           #
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
            ckpt_day = data.get("trading_day")
            today = date.today()
            if ckpt_day and ckpt_day != today:
                return
            self._trading_day = ckpt_day
            self.states = data.get("states", {})
            logger.info("[checkpoint] restored: day=%s stocks=%d", self._trading_day, len(self.states))
        except Exception:
            self.states.clear()

    def _check_day_rollover(self) -> None:
        today = date.today()
        with self._lock:
            if self._trading_day and today != self._trading_day:
                logger.info("[combined] day rollover: %s -> %s", self._trading_day, today)
                self.states.clear()
                self._trading_day = today
                for kind in ("tick", "order", "deal"):
                    self._clear_window(kind)
                try:
                    self._checkpoint_path.unlink(missing_ok=True)
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    # Memory diagnostics                                                   #
    # ------------------------------------------------------------------ #

    def _log_mem(self) -> None:
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        rss_mb = int(line.split()[1]) / 1024
                        break
                else:
                    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        except Exception:
            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

        window_rows = {}
        for kind in ("tick", "order", "deal"):
            window_rows[kind] = sum(len(df) for df in self._window_dfs[kind])

        logger.warning(
            "[mem] RSS=%.0fMB stocks=%d window_rows=tick:%d deal:%d order:%d "
            "buffer_rows=tick:%d deal:%d order:%d",
            rss_mb, len(self.states),
            window_rows["tick"], window_rows["deal"], window_rows["order"],
            self._buffers["tick"].row_count,
            self._buffers["deal"].row_count,
            self._buffers["order"].row_count,
        )

    # ------------------------------------------------------------------ #
    # Main loop                                                            #
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        logger.info("[combined] engine starting")
        self._trading_day = date.today()

        # Load daily_basic
        try:
            market_count = int(os.environ.get("DAILY_BASIC_MARKET_COUNT", "1"))
            cache = DailyBasicCache(market_count=market_count)
            trade_date = self._trading_day.strftime("%Y%m%d")
            if cache.load(trade_date):
                self._daily_basic_df = cache.get_daily_basic()
                self._market_df = self._daily_basic_df
                logger.info("[combined] daily_basic loaded: %d rows", len(self._daily_basic_df))
        except Exception as exc:
            logger.warning("[combined] daily_basic load failed: %s", exc)

        self._connect()

        last_compute = time.time()
        last_checkpoint = time.time()
        last_mem_log = time.time()
        last_gc = time.time()
        self._start_time = time.time()

        while not self._stopped:
            now = time.time()

            # Factor computation
            if now - last_compute >= self.compute_interval:
                if _is_trading_hours() and (self.states or any(
                    b.row_count > 0 for b in self._buffers.values()
                )):
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

            # GC
            if now - last_gc >= 60:
                gc.collect()
                try:
                    pa.default_memory_pool().release_unused()
                except Exception:
                    pass
                last_gc = now

            # Day rollover
            self._check_day_rollover()

            # Self-protection
            mem_limit_mb = int(os.environ.get("COLLECTOR_MEM_LIMIT_MB", "8192"))
            if mem_limit_mb > 0:
                try:
                    with open("/proc/self/status", "r", encoding="utf-8") as f:
                        for line in f:
                            if line.startswith("VmRSS:"):
                                rss_mb = int(line.split()[1]) / 1024
                                break
                        else:
                            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
                except Exception:
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
