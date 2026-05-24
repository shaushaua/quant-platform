# -*- coding: utf-8 -*-
"""pymdl callback adapter for the SDK collector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
import queue
import threading
import time
from typing import Dict, Optional, Tuple

from ..live_engine.pipeline_logger import get_collector_logger
from . import sdk_mapper

logger = logging.getLogger(__name__)


@dataclass
class MappedMessage:
    kind: str
    row: dict
    service_id: int
    message_id: int
    sequence_id: int
    receive_ts: float


class SequenceTracker:
    """Track MDL SequenceID continuity per service/message type."""

    def __init__(self):
        self._lock = threading.Lock()
        self._last: Dict[Tuple[int, int], int] = {}
        self.received: Dict[Tuple[int, int], int] = {}
        self.gaps: Dict[Tuple[int, int], int] = {}
        self.gap_size: Dict[Tuple[int, int], int] = {}

    def observe(self, service_id: int, message_id: int, sequence_id: int) -> Optional[Tuple[int, int]]:
        key = (service_id, message_id)
        with self._lock:
            self.received[key] = self.received.get(key, 0) + 1
            prev = self._last.get(key)
            self._last[key] = sequence_id
            if prev is None:
                return None
            expected = prev + 1
            if sequence_id != expected:
                gap = max(sequence_id - expected, 0)
                self.gaps[key] = self.gaps.get(key, 0) + 1
                self.gap_size[key] = self.gap_size.get(key, 0) + gap
                return expected, sequence_id
        return None

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "last": dict(self._last),
                "received": dict(self.received),
                "gaps": dict(self.gaps),
                "gap_size": dict(self.gap_size),
            }


def create_callback(pymdl, out_queue: queue.Queue, trading_day_getter, tracker: SequenceTracker):
    """Create a pymdl.MsgCallback subclass bound to the imported pymdl module."""

    class SDKMessageCallback(pymdl.MsgCallback):
        def _put(self, mapped: Optional[MappedMessage]) -> None:
            if mapped is not None:
                out_queue.put(mapped)

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
            except Exception as exc:
                logger.warning("[sdk-api] parse failed: %s", exc)

        def OnMDLSysMessage(self, hd, buf):
            try:
                msg = pymdl.mdl_sys_msg.Read(hd.MessageID, buf)
                msg_text = str(msg)
                if int(hd.MessageID) == 4 and "Reversed" in msg_text:
                    logger.debug("[sdk-sys-heartbeat] sid=%s mid=%s msg=%s", hd.ServiceID, hd.MessageID, msg)
                    return
                logger.info("[sdk-sys] sid=%s mid=%s msg=%s", hd.ServiceID, hd.MessageID, msg)
                get_collector_logger().log(
                    "sdk_system_message",
                    service_id=int(hd.ServiceID),
                    message_id=int(hd.MessageID),
                    sequence_id=int(hd.SequenceID),
                    message=msg_text,
                )
            except Exception as exc:
                logger.warning("[sdk-sys] parse failed: %s", exc)

        def OnMDLSHL2Message(self, hd, buf):
            receive_ts = time.time()
            try:
                self._observe(hd)
                msg = pymdl.mdl_shl2_msg.Read(hd.MessageID, buf)
                trading_day = trading_day_getter()
                if hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_SHL2MarketData:
                    row = sdk_mapper.map_sh_tick(msg, trading_day, int(hd.SequenceID))
                    self._put(_mapped("tick", row, hd, receive_ts))
                elif hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_NGTSTick:
                    order_row, deal_row = sdk_mapper.map_sh_ngts_tick(msg, trading_day)
                    self._put(_mapped("order", order_row, hd, receive_ts))
                    self._put(_mapped("deal", deal_row, hd, receive_ts))
            except Exception as exc:
                logger.warning(
                    "[sdk-callback] SHL2 parse/map failed sid=%s mid=%s seq=%s: %s",
                    hd.ServiceID, hd.MessageID, hd.SequenceID, exc,
                    exc_info=True,
                )

        def OnMDLSZL2Message(self, hd, buf):
            receive_ts = time.time()
            try:
                self._observe(hd)
                msg = pymdl.mdl_szl2_msg.Read(hd.MessageID, buf)
                trading_day = trading_day_getter()
                if hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Snapshot300111_v2:
                    row = sdk_mapper.map_sz_tick(msg, trading_day, int(hd.SequenceID))
                    self._put(_mapped("tick", row, hd, receive_ts))
                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Order300192_v2:
                    row = sdk_mapper.map_sz_order(msg, trading_day)
                    self._put(_mapped("order", row, hd, receive_ts))
                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Transaction300191_v2:
                    row = sdk_mapper.map_sz_deal(msg, trading_day)
                    self._put(_mapped("deal", row, hd, receive_ts))
            except Exception as exc:
                logger.warning(
                    "[sdk-callback] SZL2 parse/map failed sid=%s mid=%s seq=%s: %s",
                    hd.ServiceID, hd.MessageID, hd.SequenceID, exc,
                    exc_info=True,
                )

    return SDKMessageCallback()


def _mapped(kind: str, row: Optional[dict], hd, receive_ts: float) -> Optional[MappedMessage]:
    if row is None:
        return None
    return MappedMessage(
        kind=kind,
        row=row,
        service_id=int(hd.ServiceID),
        message_id=int(hd.MessageID),
        sequence_id=int(hd.SequenceID),
        receive_ts=receive_ts,
    )
