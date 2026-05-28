# -*- coding: utf-8 -*-
"""pymdl callback adapter for the SDK collector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging
import threading
import time
from typing import Dict, Optional, Tuple

from ..live_engine.pipeline_logger import get_collector_logger
from . import sdk_mapper
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .arrow_buffer import ArrowBuffer

logger = logging.getLogger(__name__)


@dataclass
class MappedMessage:
    """Legacy message class — kept for backward compat, no longer used in hot path."""
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


def create_callback(
    pymdl,
    buffers: Dict[str, "ArrowBuffer"],
    flush_event: threading.Event,
    trading_day_getter,
    tracker: SequenceTracker,
):
    """Create a pymdl.MsgCallback subclass bound to the imported pymdl module."""

    class SDKMessageCallback(pymdl.MsgCallback):
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
                del msg
                if int(hd.MessageID) == 4 and "Reversed" in msg_text:
                    logger.debug("[sdk-sys-heartbeat] sid=%s mid=%s msg=%s", hd.ServiceID, hd.MessageID, msg_text)
                    return
                logger.info("[sdk-sys] sid=%s mid=%s msg=%s", hd.ServiceID, hd.MessageID, msg_text)
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
            try:
                self._observe(hd)
                msg = pymdl.mdl_shl2_msg.Read(hd.MessageID, buf)
                trading_day = trading_day_getter()
                if hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_SHL2MarketData:
                    if sdk_mapper.write_sh_tick(buffers["tick"], msg, trading_day, int(hd.SequenceID)):
                        flush_event.set()
                elif hd.MessageID == pymdl.mdl_shl2_msg.MDLMID_NGTSTick:
                    order_flush, deal_flush = sdk_mapper.write_sh_ngts_tick(
                        buffers["order"], buffers["deal"], msg, trading_day,
                    )
                    if order_flush or deal_flush:
                        flush_event.set()
                del msg
            except Exception as exc:
                logger.warning(
                    "[sdk-callback] SHL2 parse/map failed sid=%s mid=%s seq=%s: %s",
                    hd.ServiceID, hd.MessageID, hd.SequenceID, exc,
                    exc_info=True,
                )

        def OnMDLSZL2Message(self, hd, buf):
            try:
                self._observe(hd)
                msg = pymdl.mdl_szl2_msg.Read(hd.MessageID, buf)
                trading_day = trading_day_getter()
                if hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Snapshot300111_v2:
                    if sdk_mapper.write_sz_tick(buffers["tick"], msg, trading_day, int(hd.SequenceID)):
                        flush_event.set()
                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Order300192_v2:
                    if sdk_mapper.write_sz_order(buffers["order"], msg, trading_day):
                        flush_event.set()
                elif hd.MessageID == pymdl.mdl_szl2_msg.MDLMID_Transaction300191_v2:
                    if sdk_mapper.write_sz_deal(buffers["deal"], msg, trading_day):
                        flush_event.set()
                del msg
            except Exception as exc:
                logger.warning(
                    "[sdk-callback] SZL2 parse/map failed sid=%s mid=%s seq=%s: %s",
                    hd.ServiceID, hd.MessageID, hd.SequenceID, exc,
                    exc_info=True,
                )

    return SDKMessageCallback()
