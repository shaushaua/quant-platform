# -*- coding: utf-8 -*-
"""
合并引擎：pymdl SDK + MemoryStore + DataAPI 统一接口。

数据流：
    pymdl SDK → callback（~4μs/msg，Rust mdl_parser 直接解析 → per-stock list + StockState）
                     ↓
    DataAPI（get_tick/get_deal/get_order → 已解析的 DataFrame，瞬间返回）

    因子计算（每 COMPUTE_INTERVAL 秒）：
        MemoryStore.get_tick/get_deal/get_order(code) → StockData → factor_calculation

    盘后归档：
        snapshot_incremental → DataFrame → parquet → OSS

环境变量：
    MDL_SERVER               MDL 服务地址
    MDL_TOKEN                MDL 认证 Token
    MDL_SUBS                 订阅配置（默认 4.4,4.24,6.28,6.33,6.36）
    FACTOR_MODULE            交易员因子模块路径
    COMPUTE_INTERVAL         因子计算间隔秒数（默认 60）
    FACTOR_OUTPUT_PATH       因子结果输出目录
    RAW_DATA_ARCHIVE_ENABLED 是否落盘并在盘后上传（默认 true）
"""

from __future__ import annotations

import copy
import gc
import io
import importlib
import logging
import os
import pickle
import resource
import signal
import struct
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

# Rust binary parser (replaces pymdl.Read + extract_*_tuple for ~10x speedup)
try:
    import mdl_parser
except ImportError:
    mdl_parser = None

logger = logging.getLogger(__name__)


# ================================================================== #
# Direct-parse callback — Rust parse in callback (~4μs/msg)          #
# ================================================================== #

def create_direct_callback(
    pymdl,
    memory_store: MemoryStore,
    tracker: SequenceTracker,
    states: Dict[str, StockState],
    state_lock: threading.Lock,
    trading_day_getter,
):
    """Create pymdl callback that directly parses and stores data.
    Rust mdl_parser.parse_*() called inside callback (~2μs parse + ~1μs append).
    trading_day_getter returns 'YYYYMMDD' string for consistent Time format.
    Data is immediately available in per-stock lists — no intermediate queue.
    Total callback cost ~4-5μs/msg, well under feeder_client ~50μs kick threshold."""

    MID_SH_TICK = pymdl.mdl_shl2_msg.MDLMID_SHL2MarketData
    MID_SH_NGTS = pymdl.mdl_shl2_msg.MDLMID_NGTSTick
    MID_SZ_TICK = pymdl.mdl_szl2_msg.MDLMID_Snapshot300111_v2
    MID_SZ_ORDER = pymdl.mdl_szl2_msg.MDLMID_Order300192_v2
    MID_SZ_DEAL = pymdl.mdl_szl2_msg.MDLMID_Transaction300191_v2

    # PML recording: dump raw binary for later replay testing
    _rec_dir = os.environ.get("PML_RECORD_DIR", "")
    _rec_seconds = int(os.environ.get("PML_RECORD_SECONDS", "60"))
    _rec_file = None
    _rec_count = 0
    _rec_deadline = 0.0

    if _rec_dir:
        os.makedirs(_rec_dir, exist_ok=True)
        _rec_path = os.path.join(_rec_dir, f"pml_capture_{time.strftime('%Y%m%d_%H%M%S')}.bin")
        _rec_file = open(_rec_path, "wb")
        _rec_deadline = time.time() + _rec_seconds
        import struct
        logger.info("[pml-rec] recording to %s for %ds", _rec_path, _rec_seconds)

    def _record_buf(mid, seq_id, buf):
        """Write one raw PML frame to capture file."""
        f = DirectCallback._rec_file
        if f is None or time.time() > DirectCallback._rec_deadline:
            return
        raw = bytes(buf)
        frame = struct.pack("<dIII", time.time(), mid, seq_id, len(raw)) + raw
        f.write(frame)
        DirectCallback._rec_count += 1

    class DirectCallback(pymdl.MsgCallback):

        # Cached values — refreshed periodically to avoid per-message allocation
        _cached_td: str = ""
        _cached_td_ts: float = 0.0
        _cached_wall: float = 0.0
        _cached_wall_ts: float = 0.0

        def _observe(self, hd) -> None:
            gap = tracker.observe(int(hd.ServiceID), int(hd.MessageID), int(hd.SequenceID))
            if gap is not None:
                logger.error(
                    "[sdk-seq-gap] sid=%s mid=%s expected=%s actual=%s",
                    hd.ServiceID, hd.MessageID, gap[0], gap[1],
                )

        def _get_or_create(self, code: str) -> StockState:
            state = states.get(code)
            if state is None:
                with state_lock:
                    state = states.get(code)
                    if state is None:
                        state = StockState(code=code)
                        states[code] = state
            return state

        def _wall_ts(self) -> float:
            """Get wall-clock seconds since midnight, cached for 100ms."""
            now = time.time()
            if now - self._cached_wall_ts > 0.1:
                dt = datetime.now()
                self._cached_wall = dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond * 1e-6
                self._cached_wall_ts = now
            return self._cached_wall

        def _get_td(self) -> str:
            """Get trading day string, cached for 1s."""
            now = time.time()
            if now - self._cached_td_ts > 1.0:
                self._cached_td = trading_day_getter()
                self._cached_td_ts = now
            return self._cached_td

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

        def OnMDLSHL2Message(self, hd, buf):
            try:
                if DirectCallback._rec_file:
                    _record_buf(int(hd.MessageID), int(hd.SequenceID), buf)
                self._observe(hd)
                mid = int(hd.MessageID)
                seq_id = int(hd.SequenceID)
                trading_day = self._get_td()
                wall_ts = self._wall_ts()

                if mid == MID_SH_TICK:
                    result = mdl_parser.parse_sh_tick(buf, trading_day, seq_id)
                    if result:
                        code, tup = result
                        memory_store.append_tick(code, tup)
                        state = self._get_or_create(code)
                        state.last_update_ts = wall_ts
                        state.update_tick_scalar(
                            float(tup[4]), float(tup[7]), float(tup[8]),
                            float(tup[9]), float(tup[10]),
                            float(tup[19]), float(tup[49]),
                            int(tup[29]), int(tup[59]),
                            str(tup[3]),
                        )
                elif mid == MID_SH_NGTS:
                    result = mdl_parser.parse_sh_ngts(buf, trading_day)
                    if result:
                        code, order_tup, deal_tup = result
                        state = self._get_or_create(code)
                        state.last_update_ts = wall_ts
                        if order_tup is not None:
                            memory_store.append_order(code, order_tup)
                            state.update_order_scalar(
                                int(order_tup[5]), float(order_tup[7]),
                                int(order_tup[8]), str(order_tup[3]),
                            )
                        if deal_tup is not None:
                            memory_store.append_deal(code, deal_tup)
                            state.update_deal_scalar(
                                float(deal_tup[7]), float(deal_tup[8]),
                                str(deal_tup[3]),
                            )
            except Exception as exc:
                logger.warning("[callback] SHL2 failed: %s", exc)

        def OnMDLSZL2Message(self, hd, buf):
            try:
                if DirectCallback._rec_file:
                    _record_buf(int(hd.MessageID), int(hd.SequenceID), buf)
                self._observe(hd)
                mid = int(hd.MessageID)
                seq_id = int(hd.SequenceID)
                trading_day = self._get_td()
                wall_ts = self._wall_ts()

                if mid == MID_SZ_TICK:
                    result = mdl_parser.parse_sz_tick(buf, trading_day, seq_id)
                    if result:
                        code, tup = result
                        memory_store.append_tick(code, tup)
                        state = self._get_or_create(code)
                        state.last_update_ts = wall_ts
                        state.update_tick_scalar(
                            float(tup[4]), float(tup[7]), float(tup[8]),
                            float(tup[9]), float(tup[10]),
                            float(tup[19]), float(tup[49]),
                            int(tup[29]), int(tup[59]),
                            str(tup[3]),
                        )
                elif mid == MID_SZ_ORDER:
                    result = mdl_parser.parse_sz_order(buf, trading_day, seq_id)
                    if result:
                        code, tup = result
                        memory_store.append_order(code, tup)
                        state = self._get_or_create(code)
                        state.last_update_ts = wall_ts
                        state.update_order_scalar(
                            int(tup[5]), float(tup[7]),
                            int(tup[8]), str(tup[3]),
                        )
                elif mid == MID_SZ_DEAL:
                    result = mdl_parser.parse_sz_deal(buf, trading_day, seq_id)
                    if result:
                        code, tup = result
                        memory_store.append_deal(code, tup)
                        state = self._get_or_create(code)
                        state.last_update_ts = wall_ts
                        state.update_deal_scalar(
                            float(tup[7]), float(tup[8]),
                            str(tup[3]),
                        )
            except Exception as exc:
                logger.warning("[callback] SZL2 failed: %s", exc)

    return DirectCallback()


# ================================================================== #
# Combined Engine                                                      #
# ================================================================== #

class CombinedEngine:
    """
    Single-process engine: pymdl SDK → Rust parse in callback → per-stock lists → StockData.

    Architecture:
      - SDK callback: Rust mdl_parser.parse_*() directly (~4μs/msg), appends to per-stock lists + StockState
      - Factor computation: reads from already-parsed MemoryStore (instant)
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
        self._subscribers = []
        self._callbacks = []
        self.tracker = SequenceTracker()

        # MemoryStore
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

        # Archive interval
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

        # daily_basic data
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

    def trading_day_str(self) -> str:
        """Trading day as 'YYYYMMDD' string (matches OSS format and Rust parser expectation)."""
        return self._trading_day.strftime("%Y%m%d")

    # ------------------------------------------------------------------ #
    # pymdl SDK connection                                                 #
    # ------------------------------------------------------------------ #

    def _connect(self) -> None:
        try:
            import pymdl
        except ImportError as exc:
            raise RuntimeError("pymdl not installed") from exc

        if mdl_parser is None:
            raise RuntimeError("mdl_parser (Rust extension) not installed — build with maturin")

        self._io_man = pymdl.CreateIOController(self.config.io_threads)
        log_path = os.environ.get("MDL_LOG_PATH", "/data/quant/mdl_logs/mdl")
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            self._io_man.EnableLog(log_path, False)
        except Exception:
            pass

        # Direct-parse callback — Rust parse inside callback, data available immediately
        self._callbacks = [create_direct_callback(
            pymdl, self._memory_store, self.tracker,
            self.states, self._lock, self.trading_day_str,
        )]
        callback = self._callbacks[0]

        if self.config.use_local_client:
            sub = self._io_man.CreateSubscriber(callback, True)
            sub.SetServerAddress(self.config.server)
            sub.SetMessageEncoding(self.config.encoding)
            sub.EnableMergeMessage(self.config.enable_merge)
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
    # Raw data archive: snapshot → parquet → OSS                          #
    # ------------------------------------------------------------------ #

    def _enqueue_raw_archive(self, trading_day: date, kind: str, df: pd.DataFrame) -> None:
        if not self._raw_archive_enabled or df.empty:
            return
        with self._disk_queue_lock:
            if len(self._disk_queue) < self._max_disk_queue:
                self._disk_queue.append((trading_day, kind, df))
                return
        logger.warning("[raw-archive] queue full (%d), writing %s synchronously", self._max_disk_queue, kind)
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
        logger.warning("[raw-archive] disk queue not drained after %.0fs pending=%d active=%d", timeout_seconds, pending, active)

    def _snapshot_to_archive(self) -> None:
        """Snapshot per-stock lists to disk for archiving."""
        archive_day = self._trading_day
        store = self._memory_store
        t0 = time.time()
        for kind in ("tick", "deal", "order"):
            columns = {"tick": TICK_COLUMNS, "order": ORDER_COLUMNS, "deal": DEAL_COLUMNS}[kind]
            # Incremental snapshot: get new rows without clearing (full-day data stays in memory)
            snapshots = store.snapshot_incremental(kind)
            if not snapshots:
                continue
            all_rows = []
            row_count = 0
            for code, lst in snapshots.items():
                all_rows.extend(lst)
                row_count += len(lst)
            if all_rows:
                t1 = time.time()
                df = pd.DataFrame(all_rows, columns=columns)
                t2 = time.time()
                self._enqueue_raw_archive(archive_day, kind, df)
                logger.info("[archive] %s: %d stocks %d rows | snapshot=%.0fms df_build=%.0fms",
                            kind, len(snapshots), row_count, (t1 - t0) * 1000, (t2 - t1) * 1000)

    def _snapshot_for_day(self, trading_day: date) -> None:
        store = self._memory_store
        for kind in ("tick", "deal", "order"):
            columns = {"tick": TICK_COLUMNS, "order": ORDER_COLUMNS, "deal": DEAL_COLUMNS}[kind]
            # Final incremental snapshot for the day (gets any remaining un-archived rows)
            snapshots = store.snapshot_incremental(kind)
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
        auth = oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"])
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
                duckdb_mem = os.environ.get("ARCHIVE_DUCKDB_MEMORY", "2GB")
                con.execute(f"SET memory_limit='{duckdb_mem}'")
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
                logger.info("[raw-archive] uploaded %s -> oss://%s/%s (%.1f MB, %d chunks)",
                            kind, os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data"), oss_key, size_mb, len(chunks))
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
    # Archive loop                                                         #
    # ------------------------------------------------------------------ #

    def _archive_loop(self) -> None:
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
        now = datetime.now()
        wall_secs = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1e6

        samples = []
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
                f"解析延迟={sdk_latency_ms}ms "
                f"总延迟={pipeline_latency_ms}ms"
            )

        if parts:
            logger.info("[latency] %s", " | ".join(parts))

    # ------------------------------------------------------------------ #
    # Factor computation                                                   #
    # ------------------------------------------------------------------ #

    def _compute_and_output(self) -> None:
        pipe_log = get_streaming_logger()
        now = datetime.now()
        date_str = self._trading_day.strftime("%Y%m%d")
        end_time = now.strftime("%H%M%S")

        # Snapshot StockState (shallow copy each state so compute thread sees consistent values)
        with self._lock:
            states_snapshot = {code: copy.copy(st) for code, st in self.states.items()}

        # Only compute stocks that received new data since last cycle
        store = self._memory_store
        dirty = store.drain_dirty()
        if not dirty:
            return

        # Per-type dirty: only warm data types that actually changed
        dirty_tick, dirty_deal, dirty_order = store.drain_dirty_typed()

        all_codes = dirty & states_snapshot.keys() if states_snapshot else dirty
        codes_list = list(all_codes)

        # Filter per-type dirty to only stocks we'll compute
        tick_codes = list(dirty_tick & all_codes)
        deal_codes = list(dirty_deal & all_codes)
        order_codes = list(dirty_order & all_codes)

        logger.info("[combined] computing: date=%s end_time=%s stocks=%d (tick=%d deal=%d order=%d)",
                     date_str, end_time, len(all_codes), len(tick_codes), len(deal_codes), len(order_codes))

        t0 = time.time()

        # Pre-warm only the data types that changed
        warm_t0 = time.perf_counter()
        if tick_codes: store.warm_tick_batch(tick_codes)
        if deal_codes: store.warm_deal_batch(deal_codes)
        if order_codes: store.warm_order_batch(order_codes)
        warm_ms = (time.perf_counter() - warm_t0) * 1000

        results = []
        wall_secs = now.hour * 3600 + now.minute * 60 + now.second

        # Aggregate timing from all factor workers
        total_df_us = 0
        total_factor_us = 0

        futures = {
            self._compute_executor.submit(
                self._compute_stock, code, date_str, end_time, states_snapshot, wall_secs,
            ): code
            for code in all_codes
        }
        for future in as_completed(futures):
            try:
                result, df_us, factor_us = future.result()
                total_df_us += df_us
                total_factor_us += factor_us
                if result is not None:
                    results.append(result)
            except Exception as exc:
                logger.warning("[%s] factor failed: %s", futures[future], exc)

        elapsed_ms = (time.time() - t0) * 1000
        result_df = pd.DataFrame(results) if results else pd.DataFrame()
        logger.info(
            "[combined] computed: %d results in %.0fms | warm=%.0fms df_build=%.0fms factor=%.0fms",
            len(result_df), elapsed_ms, warm_ms, total_df_us / 1000, total_factor_us / 1000,
        )

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

        if self.output_path and not result_df.empty:
            self.output_path.mkdir(parents=True, exist_ok=True)
            out_file = self.output_path / f"{date_str}_{end_time}.csv"
            result_df.to_csv(out_file, index=False)
            logger.info("[combined] wrote %s", out_file)

        if not result_df.empty:
            _upload_to_oss(result_df, date_str, end_time)

        if self.outfun is not None:
            try:
                self.outfun(date_str, end_time, result_df)
            except Exception as exc:
                logger.error("[combined] outfun failed: %s", exc)

        pipe_log.log("factor_compute", date=date_str, end_time=end_time,
                      stocks=len(all_codes), results=len(result_df),
                      compute_ms=round(elapsed_ms, 1))

    def _compute_stock(self, code: str, date_str: str, end_time: str,
                       states_snapshot: Dict[str, StockState], wall_secs: float) -> Optional[Tuple[Optional[dict], int, int]]:
        store = self._memory_store

        # Time DataFrame creation (list → DataFrame conversion)
        t0 = time.perf_counter()
        tick_df = store.get_tick(code)
        t1 = time.perf_counter()
        deal_df = store.get_deal(code)
        t2 = time.perf_counter()
        order_df = store.get_order(code)
        t3 = time.perf_counter()
        df_us = int((t3 - t0) * 1e6)

        stock_data = StockData(
            code=code,
            date=date_str,
            end_time=end_time,
            l1_tick=tick_df,
            l2_deal=deal_df,
            l2_order=order_df,
            market=self._market_df,
            daily_basic=self._daily_basic_df,
            state=states_snapshot.get(code),
        )

        # Time factor computation
        t4 = time.perf_counter()
        result = self.factor_calculation(stock_data, code, date_str, end_time)
        t5 = time.perf_counter()
        factor_us = int((t5 - t4) * 1e6)

        if result is not None:
            state = states_snapshot.get(code)
            if state and state.last_market_time:
                market_secs = _time_to_seconds(state.last_market_time)
                if market_secs > 0:
                    data_latency_ms = round((wall_secs - market_secs) * 1000, 1)
                    if abs(data_latency_ms) < 600_000:
                        result["data_latency_ms"] = data_latency_ms
        return result, df_us, factor_us

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
        stats = store.get_stats()

        logger.info(
            "[pipeline] RSS=%.0fMB stocks=%d | parsed: tick=%d order=%d deal=%d",
            rss_mb, len(self.states),
            stats["tick_rows"], stats["order_rows"], stats["deal_rows"],
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

        # Initialize MemoryStore
        self._memory_store.set_trading_day(trade_date)
        if not self._daily_basic_df.empty:
            self._memory_store.update_daily_basic(self._daily_basic_df)
            logger.info("[combined] MemoryStore initialized: trading_day=%s", trade_date)

        # Start archive thread
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
        last_upload_check = time.time()
        self._start_time = time.time()

        while not self._stopped:
            now = time.time()

            if now - last_compute >= self.compute_interval:
                if _is_trading_hours() and self.states:
                    self._compute_and_output()
                last_compute = now

            if now - last_checkpoint >= 30 and self.states and _is_trading_hours():
                self._save_checkpoint()
                last_checkpoint = now

            if now - last_mem_log >= 30:
                self._log_mem()
                self._log_pipeline_latency()
                last_mem_log = now

            if now - last_upload_check >= 60:
                self._check_raw_upload_time()
                last_upload_check = now

            if now - last_gc >= 60:
                gc.collect()
                last_gc = now

            self._check_day_rollover()

            # Health check: if no data arrives for 120s during trading hours, exit for K8s restart
            if _is_trading_hours() and self.states:
                last_ts = self._memory_store._last_append_ts
                if last_ts > 0 and (now - last_ts) > 120:
                    logger.error("[combined] no data for %.0fs during trading hours, exiting for restart",
                                 now - last_ts)
                    self.stop()
                    sys.exit(1)

            time.sleep(0.1)

    def stop(self) -> None:
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
