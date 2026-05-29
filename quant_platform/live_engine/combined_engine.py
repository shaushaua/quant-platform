# -*- coding: utf-8 -*-
"""
合并引擎：pymdl SDK + MemoryStore + DataAPI 统一接口。

数据流：
    pymdl SDK → callback
                  ├─ extract_*_tuple() → memory_store.append_*()（per-stock list，~5μs/msg）
                  └─ StockState.update_*_scalar() → 累计聚合值

    因子计算（每 COMPUTE_INTERVAL 秒）：
        MemoryStore.get_tick/get_deal/get_order(code) → list→DataFrame → StockData → factor_calculation

    磁盘归档（每 ARCHIVE_INTERVAL 秒）：
        memory_store.snapshot_and_clear() → DataFrame → parquet 落盘 → 盘后上传 OSS

环境变量：
    MDL_SERVER               MDL 服务地址（默认 mdl-cloud-sh.datayes.com:19012）
    MDL_TOKEN                MDL 认证 Token（32位，从 secret 注入）
    MDL_SUBS                 订阅配置（默认 4.4,4.24,6.28,6.33,6.36）
    FACTOR_MODULE            交易员因子模块路径
    COMPUTE_INTERVAL         因子计算间隔秒数（默认 60）
    ARCHIVE_INTERVAL         磁盘归档间隔秒数（默认 30）
    FACTOR_OUTPUT_PATH       因子结果输出目录
    RAW_DATA_ARCHIVE_ENABLED 是否落盘并在盘后上传 tick/order/deal 原始数据（默认 true）
"""

from __future__ import annotations

import gc
import io
import importlib
import logging
import os
import pickle
import resource
import signal
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import pandas as pd

from ..collector.sdk_callback import SequenceTracker
from ..collector.sdk_config import SDKCollectorConfig, load_config
from ..core.constants import TICK_COLUMNS, ORDER_COLUMNS, DEAL_COLUMNS
from ..data.memory_store import MemoryStore
from ..data.mysql_loader import DailyBasicCache
from ..factor.base import StockData, StockState
from .pipeline_logger import get_streaming_logger
from .streaming_engine import (
    _is_trading_hours,
    _time_to_seconds,
    _upload_to_oss,
)

# extract_*_tuple for fast callback path; scalar helpers for StockState
from ..collector.sdk_mapper import (
    extract_sh_tick_tuple, extract_sz_tick_tuple, extract_sh_ngts_tuple,
    extract_sz_order_tuple, extract_sz_deal_tuple,
    _f, _i, _code, _is_stock, _side_from_flag,
)

logger = logging.getLogger(__name__)


def _raw_time(value: Any) -> str:
    return str(value or "")


# ================================================================== #
# Direct callback — per-stock list append + StockState scalar updates  #
# ================================================================== #

def create_direct_callback(
    pymdl,
    memory_store: MemoryStore,
    states: Dict[str, StockState],
    lock: threading.Lock,
    trading_day_getter,
    tracker: SequenceTracker,
):
    """Create pymdl callback that appends to per-stock lists AND updates StockState.
    Lock-free hot path: CPython GIL guarantees dict.get/set atomicity.
    Wall clock sampled once per callback invocation, not per stock."""

    _datetime = datetime  # local ref for speed

    class DirectCallback(pymdl.MsgCallback):

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
                now = _datetime.now()
                wall_ts = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond * 1e-6

                if hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_SHL2MarketData:
                    result = extract_sh_tick_tuple(msg, trading_day, int(hd.SequenceID))
                    if result:
                        code, tup = result
                        memory_store.append_tick(code, tup)

                        state = states.get(code)
                        if state is None:
                            state = StockState(code=code)
                            states[code] = state
                        state.last_update_ts = wall_ts
                        raw_t = _raw_time(getattr(msg, "UpdateTime", ""))
                        asks = list(getattr(msg, "SellLevels", []) or [])
                        bids = list(getattr(msg, "BidLevels", []) or [])
                        state.update_tick_scalar(
                            _f(getattr(msg, "LastPrice", 0)),
                            _f(getattr(msg, "PreCloPrice", 0)),
                            _f(getattr(msg, "OpenPrice", 0)),
                            _f(getattr(msg, "HighPrice", 0)),
                            _f(getattr(msg, "LowPrice", 0)),
                            _f(getattr(asks[0], "OrderPrice", 0)) if asks else 0.0,
                            _f(getattr(bids[0], "OrderPrice", 0)) if bids else 0.0,
                            _i(getattr(asks[0], "OrderVol", 0)) if asks else 0,
                            _i(getattr(bids[0], "OrderVol", 0)) if bids else 0,
                            raw_t,
                        )

                elif hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_NGTSTick:
                    result = extract_sh_ngts_tuple(msg, trading_day)
                    if result:
                        code, order_tup, deal_tup = result
                        if order_tup is not None:
                            memory_store.append_order(code, order_tup)
                        if deal_tup is not None:
                            memory_store.append_deal(code, deal_tup)

                        state = states.get(code)
                        if state is None:
                            state = StockState(code=code)
                            states[code] = state
                        state.last_update_ts = wall_ts
                        typ = str(getattr(msg, "Type", "")).strip()
                        raw_t = _raw_time(getattr(msg, "TickTime", ""))
                        if typ in ("A", "D"):
                            state.update_order_scalar(
                                _side_from_flag(getattr(msg, "TickBSFlag", "")),
                                _f(getattr(msg, "Qty", 0)),
                                2 if typ == "A" else 5,
                                raw_t,
                            )
                        elif typ == "T":
                            state.update_deal_scalar(
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
                now = _datetime.now()
                wall_ts = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond * 1e-6

                if hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Snapshot300111_v2:
                    result = extract_sz_tick_tuple(msg, trading_day, int(hd.SequenceID))
                    if result:
                        code, tup = result
                        memory_store.append_tick(code, tup)

                        state = states.get(code)
                        if state is None:
                            state = StockState(code=code)
                            states[code] = state
                        state.last_update_ts = wall_ts
                        raw_t = _raw_time(getattr(msg, "UpdateTime", ""))
                        asks = list(getattr(msg, "AskPriceLevel", []) or [])
                        bids = list(getattr(msg, "BidPriceLevel", []) or [])
                        state.update_tick_scalar(
                            _f(getattr(msg, "LastPrice", 0)),
                            _f(getattr(msg, "PreCloPrice", 0)),
                            _f(getattr(msg, "OpenPrice", 0)),
                            _f(getattr(msg, "HighPrice", 0)),
                            _f(getattr(msg, "LowPrice", 0)),
                            _f(getattr(asks[0], "Price", 0)) if asks else 0.0,
                            _f(getattr(bids[0], "Price", 0)) if bids else 0.0,
                            _i(getattr(asks[0], "Volume", 0)) if asks else 0,
                            _i(getattr(bids[0], "Volume", 0)) if bids else 0,
                            raw_t,
                        )

                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Order300192_v2:
                    result = extract_sz_order_tuple(msg, trading_day, int(hd.SequenceID))
                    if result:
                        code, tup = result
                        memory_store.append_order(code, tup)

                        state = states.get(code)
                        if state is None:
                            state = StockState(code=code)
                            states[code] = state
                        state.last_update_ts = wall_ts
                        state.update_order_scalar(
                            {49: 0, 50: 1}.get(_i(getattr(msg, "Side", 0)), 10),
                            _f(getattr(msg, "OrderQty", 0)),
                            {49: 1, 50: 2, 85: 3}.get(_i(getattr(msg, "OrdType", 0)), 0),
                            _raw_time(getattr(msg, "TransactTime", "")),
                        )

                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Transaction300191_v2:
                    result = extract_sz_deal_tuple(msg, trading_day, int(hd.SequenceID))
                    if result:
                        code, tup = result
                        memory_store.append_deal(code, tup)

                        state = states.get(code)
                        if state is None:
                            state = StockState(code=code)
                            states[code] = state
                        state.last_update_ts = wall_ts
                        state.update_deal_scalar(
                            _f(getattr(msg, "LastPx", 0)),
                            _f(getattr(msg, "LastQty", 0)),
                            _raw_time(getattr(msg, "TransactTime", "")),
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
    Single-process engine: pymdl SDK → per-stock lists + StockState → StockData → factor calculation.

    Replaces both sdk_collector + streaming_engine.
    Provides full StockData (with DataFrames) matching the backtest DataAPI.
    No ArrowBuffer — callback writes directly to per-stock lists via MemoryStore.append_*().
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
        self._subscribers = []  # multiple subscribers: SH L2 + SZ L2
        self._callbacks = []    # keep callback references to prevent GC
        self.tracker = SequenceTracker()

        # MemoryStore: callback writes directly via append_* (no ArrowBuffer)
        self._memory_store = MemoryStore.get_instance()

        # Factor module
        module_path = os.environ.get("FACTOR_MODULE", "")
        if not module_path:
            logger.error("[combined] FACTOR_MODULE not set")
            sys.exit(1)
        self.factor_module = importlib.import_module(module_path)
        self.factor_calculation: Callable = self.factor_module.factor_calculation
        self.outfun: Optional[Callable] = getattr(self.factor_module, "outfun", None)
        self.compute_interval = int(os.environ.get("COMPUTE_INTERVAL", "60"))

        # Disk archive interval (default 30s, replaces the old 10ms flush)
        self._archive_interval = int(os.environ.get("ARCHIVE_INTERVAL", "30"))

        # Output & checkpoint
        output_path_str = os.environ.get("FACTOR_OUTPUT_PATH", "")
        self.output_path = Path(output_path_str) if output_path_str else None
        if self.output_path:
            self._checkpoint_path = self.output_path / self._CHECKPOINT_NAME
        else:
            self._checkpoint_path = Path("/tmp") / self._CHECKPOINT_NAME

        self._raw_archive_enabled = os.environ.get(
            "RAW_DATA_ARCHIVE_ENABLED", "true"
        ).lower() in ("1", "true", "yes", "on")
        self._disk_output_dir = Path(os.environ.get("COLLECTOR_DISK_OUTPUT", "/data/collector_output"))
        self._max_disk_queue = int(os.environ.get("COLLECTOR_MAX_DISK_QUEUE", "200"))
        self._disk_queue: Deque[Tuple[date, str, pd.DataFrame]] = deque()
        self._disk_queue_lock = threading.Lock()
        self._disk_active_writes = 0
        self._disk_chunk_idx: Dict[str, int] = {}
        self._disk_thread: Optional[threading.Thread] = None
        self._uploaded_today = False
        if self._raw_archive_enabled:
            self._disk_thread = threading.Thread(
                target=self._disk_writer_loop,
                name="combined-raw-disk",
                daemon=True,
            )
            self._disk_thread.start()

        # daily_basic data (loaded once at startup)
        self._market_df: pd.DataFrame = pd.DataFrame()
        self._daily_basic_df: pd.DataFrame = pd.DataFrame()

        self._load_checkpoint()

        # Thread pool for parallel factor computation
        self._compute_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="factor")

        logger.info(
            "[combined] init done: module=%s interval=%ds archive=%ds",
            module_path, self.compute_interval, self._archive_interval,
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

        # Single callback instance shared by all subscribers (per SDK sample code)
        self._callbacks = [create_direct_callback(
            pymdl, self._memory_store, self.states, self._lock, self.trading_day, self.tracker,
        )]
        callback = self._callbacks[0]

        if self.config.use_local_client:
            # Local mode: connect to feeder_client sidecar on 127.0.0.1:9012
            # Single subscriber, all services on one connection, no token/MAC needed
            sub = self._io_man.CreateSubscriber(callback, True)
            sub.SetServerAddress(self.config.server)  # 127.0.0.1:9012
            sub.SetMessageEncoding(self.config.encoding)  # 1 (uncompressed for local)
            sub.EnableMergeMessage(self.config.enable_merge)  # False for local
            sub.SetHeartbeatInterval(self.config.heartbeat_interval)
            sub.SetHeartbeatTimeout(self.config.heartbeat_timeout)
            for service_id, message_id in self.config.subs:
                sub.AddSubscription(service_id, 101, message_id)
                logger.info("[combined] subscribe %s.%s (local -> %s)", service_id, message_id, self.config.server)
            err = sub.Connect()
            if err:
                if isinstance(err, bytes):
                    err = err.decode("GBK", errors="replace")
                raise RuntimeError(f"MDL Connect local failed: {err}")
            self._subscribers.append(sub)
            logger.info("[combined] connected to local feeder_client %s", self.config.server)
        else:
            # Remote cloud mode: direct connection to MDL cloud servers
            # SH L2 and SZ L2 may need different servers
            logger.info("[combined] token=%s...%s", self.config.token[:4], self.config.token[-4:] if len(self.config.token) > 8 else "")

            sh_subs = [(sid, mid) for sid, mid in self.config.subs if sid == 4]
            sz_subs = [(sid, mid) for sid, mid in self.config.subs if sid == 6]

            conn_groups = [
                ("SH-L2", self.config.server_sh, sh_subs),
                ("SZ-L2", self.config.server, sz_subs),
            ]

            for label, server, subs in conn_groups:
                if not subs:
                    continue
                sub = self._io_man.CreateSubscriber(callback, True)
                sub.SetServerAddress(server)
                sub.SetUserName(self.config.token)
                sub.SetSendMacAuth(True)
                sub.SetMessageEncoding(self.config.encoding)
                sub.EnableMergeMessage(self.config.enable_merge)
                sub.SetHeartbeatInterval(self.config.heartbeat_interval)
                sub.SetHeartbeatTimeout(self.config.heartbeat_timeout)
                for service_id, message_id in subs:
                    sub.AddSubscription(service_id, 101, message_id)
                    logger.info("[combined] subscribe %s.%s (%s -> %s)", service_id, message_id, label, server)
                err = sub.Connect()
                if err:
                    if isinstance(err, bytes):
                        err = err.decode("GBK", errors="replace")
                    logger.warning("[combined] MDL Connect %s failed: %s (continuing with other connections)", label, err)
                    continue
                self._subscribers.append(sub)
                logger.info("[combined] %s connected to %s", label, server)

            if not self._subscribers:
                raise RuntimeError("All MDL connections failed, cannot start engine")
            logger.info("[combined] %d/%d connections established", len(self._subscribers), len(conn_groups))

    # ------------------------------------------------------------------ #
    # Raw data archive: snapshot MemoryStore → parquet → OSS upload        #
    # ------------------------------------------------------------------ #

    def _enqueue_raw_archive(self, trading_day: date, kind: str, df: pd.DataFrame) -> None:
        """Persist raw market data without changing the in-memory calculation path."""
        if not self._raw_archive_enabled or df.empty:
            return
        with self._disk_queue_lock:
            if len(self._disk_queue) < self._max_disk_queue:
                self._disk_queue.append((trading_day, kind, df))
                return

        logger.warning(
            "[raw-archive] queue full (%d), writing %s synchronously",
            self._max_disk_queue,
            kind,
        )
        self._append_raw_to_disk(trading_day, kind, df)

    def _disk_writer_loop(self) -> None:
        while not self._stopped:
            try:
                with self._disk_queue_lock:
                    item = self._disk_queue.popleft() if self._disk_queue else None
                    if item is not None:
                        self._disk_active_writes += 1
                if item is None:
                    time.sleep(0.05)
                    continue
                try:
                    trading_day, kind, df = item
                    self._append_raw_to_disk(trading_day, kind, df)
                    del df
                finally:
                    with self._disk_queue_lock:
                        self._disk_active_writes -= 1
            except Exception as exc:
                logger.warning("[raw-archive] writer failed: %s", exc)

    def _append_raw_to_disk(self, trading_day: date, kind: str, df: pd.DataFrame) -> None:
        try:
            date_str = trading_day.strftime("%Y%m%d")
            out_dir = self._disk_output_dir / date_str / kind
            out_dir.mkdir(parents=True, exist_ok=True)
            key = f"{date_str}/{kind}"
            with self._disk_queue_lock:
                chunk_idx = self._disk_chunk_idx.get(key, 0)
                self._disk_chunk_idx[key] = chunk_idx + 1
                chunk_file = out_dir / f"{chunk_idx:06d}.parquet"
            df.to_parquet(chunk_file, index=False)
            logger.info("[raw-archive] %s +%d rows -> %s", kind, len(df), chunk_file)
        except Exception as exc:
            logger.error("[raw-archive] write %s failed: %s", kind, exc, exc_info=True)

    def _flush_disk_queue(self, timeout_seconds: float = 30.0) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            with self._disk_queue_lock:
                pending = len(self._disk_queue)
                active = self._disk_active_writes
            if pending == 0 and active == 0:
                return
            time.sleep(0.05)
        logger.warning(
            "[raw-archive] disk queue not drained after %.0fs pending=%d active=%d",
            timeout_seconds,
            pending,
            active,
        )

    def _snapshot_to_archive(self) -> None:
        """Snapshot MemoryStore per-stock lists to disk for archiving.
        Uses snapshot_and_clear to swap out data so callback can continue writing."""
        archive_day = self._trading_day
        store = self._memory_store
        for kind in ("tick", "deal", "order"):
            columns = {"tick": TICK_COLUMNS, "order": ORDER_COLUMNS, "deal": DEAL_COLUMNS}[kind]
            snapshots = store.snapshot_and_clear(kind)
            if not snapshots:
                continue
            all_rows = []
            for code, lst in snapshots.items():
                all_rows.extend(lst)
            if all_rows:
                df = pd.DataFrame(all_rows, columns=columns)
                self._enqueue_raw_archive(archive_day, kind, df)

    def _snapshot_for_day(self, trading_day: date) -> None:
        """Snapshot and archive data attributed to a specific trading day."""
        store = self._memory_store
        for kind in ("tick", "deal", "order"):
            columns = {"tick": TICK_COLUMNS, "order": ORDER_COLUMNS, "deal": DEAL_COLUMNS}[kind]
            snapshots = store.snapshot_and_clear(kind)
            if not snapshots:
                continue
            all_rows = []
            for code, lst in snapshots.items():
                all_rows.extend(lst)
            if all_rows:
                df = pd.DataFrame(all_rows, columns=columns)
                self._enqueue_raw_archive(trading_day, kind, df)

    def _get_oss_bucket(self):
        import oss2

        auth = oss2.Auth(
            os.environ["OSS_ACCESS_KEY_ID"],
            os.environ["OSS_ACCESS_KEY_SECRET"],
        )
        endpoint = os.environ.get("OSS_ENDPOINT", "")
        if endpoint and not endpoint.startswith("http"):
            endpoint = f"https://{endpoint}"
        bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")
        return oss2.Bucket(auth, endpoint, bucket_name)

    @staticmethod
    def _parse_upload_time(raw: str) -> Tuple[int, int]:
        if ":" in raw:
            hour, minute = raw.split(":", 1)
            return int(hour), int(minute)
        return int(raw), 0

    def _check_raw_upload_time(self) -> None:
        if not self._raw_archive_enabled or self._uploaded_today:
            return
        upload_hour, upload_minute = self._parse_upload_time(
            os.environ.get("RAW_DATA_UPLOAD_TIME", os.environ.get("RAW_DATA_UPLOAD_HOUR", "16"))
        )
        now = datetime.now()
        if (now.hour, now.minute) < (upload_hour, upload_minute):
            return
        self._snapshot_to_archive()
        self._flush_disk_queue()
        if self._upload_raw_day_to_oss(self._trading_day):
            self._uploaded_today = True

    def _upload_raw_day_to_oss(self, trading_day: date) -> bool:
        """Merge raw parquet chunks, sort like historical data, and upload to OSS."""
        date_str = trading_day.strftime("%Y%m%d")
        year = date_str[:4]
        month = date_str[4:6]
        prefix = f"{year}/{year}{month}/{date_str}"
        disk_dir = self._disk_output_dir / date_str

        if not disk_dir.exists():
            logger.warning("[raw-archive] disk dir missing, skip upload: %s", disk_dir)
            return False

        try:
            bucket = self._get_oss_bucket()
        except Exception as exc:
            logger.error("[raw-archive] OSS bucket init failed: %s", exc)
            return False

        try:
            import duckdb
        except Exception as exc:
            logger.error("[raw-archive] duckdb unavailable, cannot merge parquet: %s", exc)
            return False

        uploaded_any = False
        had_error = False
        for kind in ("order", "deal", "tick"):
            chunk_dir = disk_dir / kind
            if not chunk_dir.exists():
                logger.info("[raw-archive] %s has no chunks, skip", kind)
                continue
            chunks = sorted(chunk_dir.glob("*.parquet"))
            if not chunks:
                continue

            tmp_file = disk_dir / f"tmp_{kind}.parquet"
            try:
                tmp_file.unlink(missing_ok=True)
                con = duckdb.connect(":memory:")
                con.execute("SET memory_limit='2GB'")
                con.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{chunk_dir}/*.parquet')
                        ORDER BY Code, SeqNum
                    ) TO '{tmp_file}' (FORMAT PARQUET)
                """)
                con.close()

                oss_key = f"{prefix}/{date_str}_{kind}.parquet"
                bucket.put_object_from_file(oss_key, str(tmp_file))
                size_mb = tmp_file.stat().st_size / 1024 / 1024
                logger.info(
                    "[raw-archive] uploaded %s -> oss://%s/%s (%.1f MB, %d chunks)",
                    kind,
                    os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data"),
                    oss_key,
                    size_mb,
                    len(chunks),
                )
                uploaded_any = True
                tmp_file.unlink(missing_ok=True)
                for chunk in chunks:
                    chunk.unlink()
            except Exception as exc:
                had_error = True
                logger.error("[raw-archive] upload %s failed: %s", kind, exc, exc_info=True)
                tmp_file.unlink(missing_ok=True)

        if not self._daily_basic_df.empty:
            try:
                buffer = io.BytesIO()
                self._daily_basic_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                oss_key = f"{prefix}/{date_str}_daily_basic_data.parquet"
                bucket.put_object(oss_key, buffer.read())
                logger.info("[raw-archive] uploaded daily_basic -> %s", oss_key)
                uploaded_any = True
            except Exception as exc:
                had_error = True
                logger.error("[raw-archive] upload daily_basic failed: %s", exc)

        logger.info("[raw-archive] %s upload finished", date_str)
        return uploaded_any and not had_error

    # ------------------------------------------------------------------ #
    # Archive loop: MemoryStore snapshot → disk (every ARCHIVE_INTERVAL s)  #
    # ------------------------------------------------------------------ #

    def _archive_loop(self) -> None:
        """Periodically snapshot MemoryStore to disk for archiving."""
        while not self._stopped:
            time.sleep(self._archive_interval)
            try:
                self._snapshot_to_archive()
            except Exception as exc:
                logger.warning("[archive] snapshot failed: %s", exc)

    # ------------------------------------------------------------------ #
    # Latency logging                                                      #
    # ------------------------------------------------------------------ #

    def _log_pipeline_latency(self) -> None:
        """Sample and log SDK→wall latency for a few stocks."""
        now = datetime.now()
        wall_secs = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1e6

        # Pick up to 3 stocks with latest market time
        samples = []
        with self._lock:
            for code, state in list(self.states.items())[:50]:
                if state.last_market_time:
                    samples.append((code, state.last_market_time, state.last_update_ts))
        samples.sort(key=lambda x: x[1], reverse=True)
        samples = samples[:3]

        if not samples:
            return

        parts = []
        for code, mkt_time_str, update_ts in samples:
            mkt_secs = _time_to_seconds(mkt_time_str)
            if mkt_secs <= 0:
                continue
            sdk_latency_ms = round((update_ts - mkt_secs) * 1000, 1) if update_ts > 0 else -1
            pipeline_latency_ms = round((wall_secs - mkt_secs) * 1000, 1)
            parts.append(
                f"{code}: 行情时间={mkt_time_str} "
                f"SDK延迟={sdk_latency_ms}ms "
                f"总延迟={pipeline_latency_ms}ms"
            )

        if parts:
            logger.info("[latency] %s", " | ".join(parts))

    # ------------------------------------------------------------------ #
    # Factor computation                                                   #
    # ------------------------------------------------------------------ #

    def _compute_and_output(self) -> None:
        """Read from MemoryStore, build StockData per stock, compute factors, output."""
        pipe_log = get_streaming_logger()
        now = datetime.now()
        date_str = self._trading_day.strftime("%Y%m%d")
        end_time = now.strftime("%H%M%S")

        # Snapshot StockState
        with self._lock:
            states_snapshot = dict(self.states)

        # Collect all stock codes from MemoryStore + StockState
        store = self._memory_store
        all_codes = set(states_snapshot.keys())
        all_codes.update(store._tick_lists.keys())
        all_codes.update(store._order_lists.keys())
        all_codes.update(store._deal_lists.keys())

        logger.info("[combined] computing: date=%s end_time=%s stocks=%d",
                     date_str, end_time, len(all_codes))

        # Parallel per-stock computation
        t0 = time.time()
        results = []
        wall_secs = now.hour * 3600 + now.minute * 60 + now.second

        futures = {
            self._compute_executor.submit(
                self._compute_stock, code, date_str, end_time, states_snapshot, wall_secs,
            ): code
            for code in all_codes
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as exc:
                logger.warning("[%s] factor failed: %s", futures[future], exc)

        elapsed_ms = (time.time() - t0) * 1000
        result_df = pd.DataFrame(results) if results else pd.DataFrame()
        logger.info("[combined] computed: %d results in %.0fms (parallel, 8 workers)", len(result_df), elapsed_ms)

        # Sample factor compute latency
        if states_snapshot:
            compute_now = datetime.now()
            compute_wall_secs = compute_now.hour * 3600 + compute_now.minute * 60 + compute_now.second
            sample_codes = list(states_snapshot.keys())[:3]
            latency_parts = []
            for sc in sample_codes:
                st = states_snapshot.get(sc)
                if st and st.last_market_time:
                    mkt_secs = _time_to_seconds(st.last_market_time)
                    if mkt_secs > 0:
                        latency_parts.append(
                            f"{sc}: 行情={st.last_market_time} "
                            f"因子计算延迟={round((compute_wall_secs - mkt_secs) * 1000, 1)}ms"
                        )
            if latency_parts:
                logger.info("[latency-factor] %s", " | ".join(latency_parts))

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

    def _compute_stock(self, code: str, date_str: str, end_time: str,
                       states_snapshot: Dict[str, StockState], wall_secs: float) -> Optional[dict]:
        """Compute factor for a single stock (called from thread pool)."""
        store = self._memory_store
        stock_data = StockData(
            code=code,
            date=date_str,
            end_time=end_time,
            l1_tick=store.get_tick(code),
            l2_deal=store.get_deal(code),
            l2_order=store.get_order(code),
            market=self._market_df,
            daily_basic=self._daily_basic_df,
            state=states_snapshot.get(code),
        )
        result = self.factor_calculation(stock_data, code, date_str, end_time)
        if result is not None:
            state = states_snapshot.get(code)
            if state and state.last_market_time:
                market_secs = _time_to_seconds(state.last_market_time)
                if market_secs > 0:
                    data_latency_ms = round((wall_secs - market_secs) * 1000, 1)
                    if abs(data_latency_ms) < 600_000:
                        result["data_latency_ms"] = data_latency_ms
        return result

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
        upload_day: Optional[date] = None
        with self._lock:
            if self._trading_day and today != self._trading_day:
                logger.info("[combined] day rollover: %s -> %s", self._trading_day, today)
                if self._raw_archive_enabled and not self._uploaded_today:
                    upload_day = self._trading_day
                self.states.clear()
                self._trading_day = today
                self._uploaded_today = False
                # Reset MemoryStore for new trading day
                self._memory_store.set_trading_day(today.strftime("%Y%m%d"))
                try:
                    self._checkpoint_path.unlink(missing_ok=True)
                except Exception:
                    pass
        if upload_day is not None:
            self._snapshot_for_day(upload_day)
            self._flush_disk_queue()
            self._upload_raw_day_to_oss(upload_day)

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

        store = self._memory_store
        mem_info = store.get_memory_usage()
        stats = store.get_stats()

        logger.warning(
            "[mem] RSS=%.0fMB stocks=%d store=tick:%.0fMB(%d/%drows) deal:%.0fMB(%d/%drows) order:%.0fMB(%d/%drows)",
            rss_mb, len(self.states),
            mem_info["tick_mb"], stats["tick_stocks"], stats["tick_rows"],
            mem_info["deal_mb"], stats["deal_stocks"], stats["deal_rows"],
            mem_info["order_mb"], stats["order_stocks"], stats["order_rows"],
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

        # Initialize MemoryStore for DataAPI
        self._memory_store.set_trading_day(trade_date)
        if not self._daily_basic_df.empty:
            self._memory_store.update_daily_basic(self._daily_basic_df)
            logger.info("[combined] MemoryStore initialized: trading_day=%s", trade_date)

        # Start archive thread (MemoryStore snapshot → disk, every ARCHIVE_INTERVAL)
        if self._raw_archive_enabled:
            self._archive_thread = threading.Thread(
                target=self._archive_loop, name="archive-snapshot", daemon=True,
            )
            self._archive_thread.start()
            logger.info("[combined] archive thread started: interval=%ds", self._archive_interval)

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
                if _is_trading_hours() and self.states:
                    self._compute_and_output()
                last_compute = now

            # Checkpoint
            if now - last_checkpoint >= 30 and self.states and _is_trading_hours():
                self._save_checkpoint()
                last_checkpoint = now

            # Memory log + latency
            if now - last_mem_log >= 30:
                self._log_mem()
                self._log_pipeline_latency()
                last_mem_log = now

            self._check_raw_upload_time()

            # GC
            if now - last_gc >= 60:
                gc.collect()
                last_gc = now

            # Day rollover
            self._check_day_rollover()

            time.sleep(0.5)

    def stop(self) -> None:
        # Snapshot remaining data to disk archive
        try:
            self._snapshot_to_archive()
        except Exception as exc:
            logger.warning("[combined] final archive snapshot failed: %s", exc)
        if self._raw_archive_enabled:
            self._flush_disk_queue()
        self._stopped = True
        if self.states:
            self._save_checkpoint()
        for sub in self._subscribers:
            try:
                sub.ClearSubscriptions()
            except Exception:
                pass
        if self._io_man is not None:
            try:
                self._io_man.Shutdown()
            except Exception:
                pass
        # Shutdown thread pools
        self._compute_executor.shutdown(wait=False)
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
