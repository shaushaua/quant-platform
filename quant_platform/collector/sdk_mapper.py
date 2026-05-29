# -*- coding: utf-8 -*-
"""Map pymdl SDK messages into quant-platform's standard DataFrame rows."""

from __future__ import annotations

from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa

from ..core.constants import ORDER_COLUMNS, DEAL_COLUMNS, TICK_COLUMNS, ARROW_SCHEMA_BY_KIND


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


def _code(raw: str, market: str) -> str:
    code = str(raw or "").strip().zfill(6)
    suffix = ".XSHG" if market == "SH" else ".XSHE"
    return f"{code}{suffix}"


def _is_stock(raw: str, market: str) -> bool:
    code = str(raw or "").strip().zfill(6)
    if market == "SH":
        return code.startswith(("6", "9"))
    return code.startswith(("0", "3"))


def _parse_mdl_time(value: Any, trading_day: date) -> pd.Timestamp:
    """Parse MDL HHMMSSmmm/HHMMSSuu style time values into pandas Timestamp."""
    raw = str(value or "").strip()
    if not raw:
        return pd.NaT
    raw = raw.replace(":", "").replace(".", "")
    if not raw.isdigit():
        return pd.NaT
    raw = raw.zfill(9)
    hour = _i(raw[0:2])
    minute = _i(raw[2:4])
    second = _i(raw[4:6])
    millis = _i(raw[6:9])
    extra_min, second = divmod(second, 60)
    minute += extra_min
    extra_hour, minute = divmod(minute, 60)
    hour = (hour + extra_hour) % 24
    try:
        return pd.Timestamp(datetime.combine(trading_day, dt_time(hour, minute, second, millis * 1000)))
    except Exception:
        return pd.NaT


def _side_from_flag(flag: Any) -> int:
    flag = str(flag or "").strip()
    if flag == "B":
        return 0
    if flag == "S":
        return 1
    return 10


def _level(levels: List[Any], idx: int, price_attr: str, volume_attr: str, num_attr: str) -> Tuple[float, float, float]:
    if idx >= len(levels):
        return 0.0, 0.0, 0.0
    item = levels[idx]
    return _f(getattr(item, price_attr, 0)), _f(getattr(item, volume_attr, 0)), _f(getattr(item, num_attr, 0))


def _empty_tick_row() -> Dict[str, Any]:
    return {col: 0.0 for col in TICK_COLUMNS}


# Pre-computed bid/ask column names — avoid f-string allocation per call
_LEVEL_COLS = tuple(
    (f"BidPrice{i}", f"BidVolume{i}", f"BidNum{i}",
     f"AskPrice{i}", f"AskVolume{i}", f"AskNum{i}")
    for i in range(1, 11)
)


def _set_levels(buf, bids: list, asks: list, price_attr: str, vol_attr: str, num_attr: str) -> None:
    """Write 10-level bid/ask data directly to buffer."""
    for i in range(10):
        bp, bv, bn = _level(bids, i, price_attr, vol_attr, num_attr)
        ap, av, an = _level(asks, i, price_attr, vol_attr, num_attr)
        bp_c, bv_c, bn_c, ap_c, av_c, an_c = _LEVEL_COLS[i]
        buf.set(bp_c, bp)
        buf.set(bv_c, bv)
        buf.set(bn_c, bn)
        buf.set(ap_c, ap)
        buf.set(av_c, av)
        buf.set(an_c, an)


def map_sh_tick(msg: Any, trading_day: date, sequence_id: int) -> Optional[Dict[str, Any]]:
    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
        return None
    row = _empty_tick_row()
    row.update({
        "TradingDay": trading_day,
        "Code": _code(msg.SecurityID, "SH"),
        "Time": _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day),
        "UpdateTime": _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day),
        "CurrentPrice": _f(getattr(msg, "LastPrice", 0)),
        "TotalVolume": _f(getattr(msg, "TradVolume", 0)),
        "TotalMoney": _f(getattr(msg, "Turnover", 0)),
        "PreClosePrice": _f(getattr(msg, "PreCloPrice", 0)),
        "OpenPrice": _f(getattr(msg, "OpenPrice", 0)),
        "HighestPrice": _f(getattr(msg, "HighPrice", 0)),
        "LowestPrice": _f(getattr(msg, "LowPrice", 0)),
        "IOPV": _f(getattr(msg, "IOPV", 0)),
        "TradeNum": _f(getattr(msg, "TradNumber", 0)),
        "TotalBidVolume": _f(getattr(msg, "TotalBidVol", 0)),
        "TotalAskVolume": _f(getattr(msg, "TotalAskVol", 0)),
        "AvgBidPrice": _f(getattr(msg, "WAvgBidPri", 0)),
        "AvgAskPrice": _f(getattr(msg, "WAvgAskPri", 0)),
        "Channel": 0,
        "SeqNum": sequence_id,
    })
    bids = list(getattr(msg, "BidLevels", []) or [])
    asks = list(getattr(msg, "SellLevels", []) or [])
    for i in range(10):
        bp, bv, bn = _level(bids, i, "OrderPrice", "OrderVol", "OrderNum")
        ap, av, an = _level(asks, i, "OrderPrice", "OrderVol", "OrderNum")
        n = i + 1
        row[f"BidPrice{n}"] = bp
        row[f"BidVolume{n}"] = bv
        row[f"BidNum{n}"] = bn
        row[f"AskPrice{n}"] = ap
        row[f"AskVolume{n}"] = av
        row[f"AskNum{n}"] = an
    return row


def map_sz_tick(msg: Any, trading_day: date, sequence_id: int) -> Optional[Dict[str, Any]]:
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return None
    row = _empty_tick_row()
    row.update({
        "TradingDay": trading_day,
        "Code": _code(msg.SecurityID, "SZ"),
        "Time": _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day),
        "UpdateTime": _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day),
        "CurrentPrice": _f(getattr(msg, "LastPrice", 0)),
        "TotalVolume": _f(getattr(msg, "Volume", 0)),
        "TotalMoney": _f(getattr(msg, "Turnover", 0)),
        "PreClosePrice": _f(getattr(msg, "PreCloPrice", 0)),
        "OpenPrice": _f(getattr(msg, "OpenPrice", 0)),
        "HighestPrice": _f(getattr(msg, "HighPrice", 0)),
        "LowestPrice": _f(getattr(msg, "LowPrice", 0)),
        "HighLimitPrice": _f(getattr(msg, "HighLimitPrice", 0)),
        "LowLimitPrice": _f(getattr(msg, "LowLimitPrice", 0)),
        "IOPV": _f(getattr(msg, "IOPV", 0)),
        "TradeNum": _f(getattr(msg, "TurnNum", 0)),
        "TotalBidVolume": _f(getattr(msg, "TotalBidQty", 0)),
        "TotalAskVolume": _f(getattr(msg, "TotalOfferQty", 0)),
        "AvgBidPrice": _f(getattr(msg, "WeightedAvgBidPx", 0)),
        "AvgAskPrice": _f(getattr(msg, "WeightedAvgOfferPx", 0)),
        "Channel": _i(getattr(msg, "ChannelNo", 0)),
        "SeqNum": sequence_id,
    })
    bids = list(getattr(msg, "BidPriceLevel", []) or [])
    asks = list(getattr(msg, "AskPriceLevel", []) or [])
    for i in range(10):
        bp, bv, bn = _level(bids, i, "Price", "Volume", "NumOrders")
        ap, av, an = _level(asks, i, "Price", "Volume", "NumOrders")
        n = i + 1
        row[f"BidPrice{n}"] = bp
        row[f"BidVolume{n}"] = bv
        row[f"BidNum{n}"] = bn
        row[f"AskPrice{n}"] = ap
        row[f"AskVolume{n}"] = av
        row[f"AskNum{n}"] = an
    return row


def map_sh_ngts_tick(msg: Any, trading_day: date) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
        return None, None
    typ = str(getattr(msg, "Type", "")).strip()
    code = _code(msg.SecurityID, "SH")
    event_time = _parse_mdl_time(getattr(msg, "TickTime", 0), trading_day)
    side = _side_from_flag(getattr(msg, "TickBSFlag", ""))
    if typ in ("A", "D"):
        return {
            "TradingDay": trading_day,
            "Code": code,
            "Time": event_time,
            "UpdateTime": event_time,
            "OrderID": _i(getattr(msg, "BuyOrderNO", 0)) + _i(getattr(msg, "SellOrderNO", 0)),
            "Side": side,
            "Price": _f(getattr(msg, "Price", 0)),
            "Volume": _f(getattr(msg, "Qty", 0)),
            "OrderType": 2 if typ == "A" else 5,
            "Channel": _i(getattr(msg, "Channel", 0)),
            "SeqNum": _i(getattr(msg, "BizIndex", 0)),
        }, None
    if typ == "T":
        price = _f(getattr(msg, "Price", 0))
        volume = _f(getattr(msg, "Qty", 0))
        money = _f(getattr(msg, "TradeMoney", 0)) or price * volume
        return None, {
            "TradingDay": trading_day,
            "Code": code,
            "Time": event_time,
            "UpdateTime": event_time,
            "SaleOrderID": _i(getattr(msg, "SellOrderNO", 0)),
            "BuyOrderID": _i(getattr(msg, "BuyOrderNO", 0)),
            "Side": side,
            "Price": price,
            "Volume": volume,
            "Money": money,
            "Channel": _i(getattr(msg, "Channel", 0)),
            "SeqNum": _i(getattr(msg, "BizIndex", 0)),
        }
    return None, None


def map_sz_order(msg: Any, trading_day: date) -> Optional[Dict[str, Any]]:
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return None
    side = {49: 0, 50: 1}.get(_i(getattr(msg, "Side", 0)), 10)
    order_type = {49: 1, 50: 2, 85: 3}.get(_i(getattr(msg, "OrdType", 0)), 0)
    return {
        "TradingDay": trading_day,
        "Code": _code(msg.SecurityID, "SZ"),
        "Time": _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day),
        "UpdateTime": _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day),
        "OrderID": _i(getattr(msg, "ApplSeqNum", 0)),
        "Side": side,
        "Price": _f(getattr(msg, "Price", 0)),
        "Volume": _f(getattr(msg, "OrderQty", 0)),
        "OrderType": order_type,
        "Channel": _i(getattr(msg, "ChannelNo", 0)),
        "SeqNum": _i(getattr(msg, "ApplSeqNum", 0)),
    }


def map_sz_deal(msg: Any, trading_day: date) -> Optional[Dict[str, Any]]:
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return None
    buy_id = _i(getattr(msg, "BidApplSeqNum", 0))
    sell_id = _i(getattr(msg, "OfferApplSeqNum", 0))
    side = 0 if buy_id > sell_id else 1
    if _i(getattr(msg, "ExecType", 0)) == 52:
        side = 4
    price = _f(getattr(msg, "LastPx", 0))
    volume = _f(getattr(msg, "LastQty", 0))
    return {
        "TradingDay": trading_day,
        "Code": _code(msg.SecurityID, "SZ"),
        "Time": _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day),
        "UpdateTime": _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day),
        "SaleOrderID": sell_id,
        "BuyOrderID": buy_id,
        "Side": side,
        "Price": price,
        "Volume": volume,
        "Money": price * volume,
        "Channel": _i(getattr(msg, "ChannelNo", 0)),
        "SeqNum": _i(getattr(msg, "ApplSeqNum", 0)),
    }


# ================================================================== #
# Direct-write functions: write to ArrowBuffer without creating dicts #
# ================================================================== #

def write_sh_tick(buf, msg: Any, trading_day: date, sequence_id: int) -> bool:
    """Write SH tick directly to buffer. Returns True if watermark hit."""
    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
        return False
    buf.begin_row()
    try:
        code = _code(msg.SecurityID, "SH")
        event_time = _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day)
        buf.set("TradingDay", str(trading_day))
        buf.set("Code", code)
        buf.set("Time", event_time)
        buf.set("UpdateTime", event_time)
        buf.set("CurrentPrice", _f(getattr(msg, "LastPrice", 0)))
        buf.set("TotalVolume", _f(getattr(msg, "TradVolume", 0)))
        buf.set("TotalMoney", _f(getattr(msg, "Turnover", 0)))
        buf.set("PreClosePrice", _f(getattr(msg, "PreCloPrice", 0)))
        buf.set("OpenPrice", _f(getattr(msg, "OpenPrice", 0)))
        buf.set("HighestPrice", _f(getattr(msg, "HighPrice", 0)))
        buf.set("LowestPrice", _f(getattr(msg, "LowPrice", 0)))
        buf.set("IOPV", _f(getattr(msg, "IOPV", 0)))
        buf.set("TradeNum", _f(getattr(msg, "TradNumber", 0)))
        buf.set("TotalBidVolume", _f(getattr(msg, "TotalBidVol", 0)))
        buf.set("TotalAskVolume", _f(getattr(msg, "TotalAskVol", 0)))
        buf.set("AvgBidPrice", _f(getattr(msg, "WAvgBidPri", 0)))
        buf.set("AvgAskPrice", _f(getattr(msg, "WAvgAskPri", 0)))
        buf.set("Channel", 0)
        buf.set("SeqNum", sequence_id)
        bids = list(getattr(msg, "BidLevels", []) or [])
        asks = list(getattr(msg, "SellLevels", []) or [])
        _set_levels(buf, bids, asks, "OrderPrice", "OrderVol", "OrderNum")
    except Exception:
        buf.cancel_row()
        return False
    return buf.commit_row()


def write_sz_tick(buf, msg: Any, trading_day: date, sequence_id: int) -> bool:
    """Write SZ tick directly to buffer. Returns True if watermark hit."""
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return False
    buf.begin_row()
    try:
        code = _code(msg.SecurityID, "SZ")
        event_time = _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day)
        buf.set("TradingDay", str(trading_day))
        buf.set("Code", code)
        buf.set("Time", event_time)
        buf.set("UpdateTime", event_time)
        buf.set("CurrentPrice", _f(getattr(msg, "LastPrice", 0)))
        buf.set("TotalVolume", _f(getattr(msg, "Volume", 0)))
        buf.set("TotalMoney", _f(getattr(msg, "Turnover", 0)))
        buf.set("PreClosePrice", _f(getattr(msg, "PreCloPrice", 0)))
        buf.set("OpenPrice", _f(getattr(msg, "OpenPrice", 0)))
        buf.set("HighestPrice", _f(getattr(msg, "HighPrice", 0)))
        buf.set("LowestPrice", _f(getattr(msg, "LowPrice", 0)))
        buf.set("HighLimitPrice", _f(getattr(msg, "HighLimitPrice", 0)))
        buf.set("LowLimitPrice", _f(getattr(msg, "LowLimitPrice", 0)))
        buf.set("IOPV", _f(getattr(msg, "IOPV", 0)))
        buf.set("TradeNum", _f(getattr(msg, "TurnNum", 0)))
        buf.set("TotalBidVolume", _f(getattr(msg, "TotalBidQty", 0)))
        buf.set("TotalAskVolume", _f(getattr(msg, "TotalOfferQty", 0)))
        buf.set("AvgBidPrice", _f(getattr(msg, "WeightedAvgBidPx", 0)))
        buf.set("AvgAskPrice", _f(getattr(msg, "WeightedAvgOfferPx", 0)))
        buf.set("Channel", _i(getattr(msg, "ChannelNo", 0)))
        buf.set("SeqNum", sequence_id)
        bids = list(getattr(msg, "BidPriceLevel", []) or [])
        asks = list(getattr(msg, "AskPriceLevel", []) or [])
        _set_levels(buf, bids, asks, "Price", "Volume", "NumOrders")
    except Exception:
        buf.cancel_row()
        return False
    return buf.commit_row()


def write_sh_ngts_tick(order_buf, deal_buf, msg: Any, trading_day: date) -> Tuple[bool, bool]:
    """Write SH NGTS order/deal directly to buffers. Returns (order_flush, deal_flush)."""
    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
        return False, False
    typ = str(getattr(msg, "Type", "")).strip()
    code = _code(msg.SecurityID, "SH")
    event_time = _parse_mdl_time(getattr(msg, "TickTime", 0), trading_day)
    side = _side_from_flag(getattr(msg, "TickBSFlag", ""))

    if typ in ("A", "D"):
        order_buf.begin_row()
        try:
            buf = order_buf
            buf.set("TradingDay", str(trading_day))
            buf.set("Code", code)
            buf.set("Time", event_time)
            buf.set("UpdateTime", event_time)
            buf.set("OrderID", _i(getattr(msg, "BuyOrderNO", 0)) + _i(getattr(msg, "SellOrderNO", 0)))
            buf.set("Side", side)
            buf.set("Price", _f(getattr(msg, "Price", 0)))
            buf.set("Volume", _f(getattr(msg, "Qty", 0)))
            buf.set("OrderType", 2 if typ == "A" else 5)
            buf.set("Channel", _i(getattr(msg, "Channel", 0)))
            buf.set("SeqNum", _i(getattr(msg, "BizIndex", 0)))
        except Exception:
            order_buf.cancel_row()
            return False, False
        return order_buf.commit_row(), False

    if typ == "T":
        price = _f(getattr(msg, "Price", 0))
        volume = _f(getattr(msg, "Qty", 0))
        money = _f(getattr(msg, "TradeMoney", 0)) or price * volume
        deal_buf.begin_row()
        try:
            buf = deal_buf
            buf.set("TradingDay", str(trading_day))
            buf.set("Code", code)
            buf.set("Time", event_time)
            buf.set("UpdateTime", event_time)
            buf.set("SaleOrderID", _i(getattr(msg, "SellOrderNO", 0)))
            buf.set("BuyOrderID", _i(getattr(msg, "BuyOrderNO", 0)))
            buf.set("Side", side)
            buf.set("Price", price)
            buf.set("Volume", volume)
            buf.set("Money", money)
            buf.set("Channel", _i(getattr(msg, "Channel", 0)))
            buf.set("SeqNum", _i(getattr(msg, "BizIndex", 0)))
        except Exception:
            deal_buf.cancel_row()
            return False, False
        return False, deal_buf.commit_row()

    return False, False


def write_sz_order(buf, msg: Any, trading_day: date) -> bool:
    """Write SZ order directly to buffer. Returns True if watermark hit."""
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return False
    side = {49: 0, 50: 1}.get(_i(getattr(msg, "Side", 0)), 10)
    order_type = {49: 1, 50: 2, 85: 3}.get(_i(getattr(msg, "OrdType", 0)), 0)
    buf.begin_row()
    try:
        buf.set("TradingDay", str(trading_day))
        buf.set("Code", _code(msg.SecurityID, "SZ"))
        event_time = _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day)
        buf.set("Time", event_time)
        buf.set("UpdateTime", event_time)
        buf.set("OrderID", _i(getattr(msg, "ApplSeqNum", 0)))
        buf.set("Side", side)
        buf.set("Price", _f(getattr(msg, "Price", 0)))
        buf.set("Volume", _f(getattr(msg, "OrderQty", 0)))
        buf.set("OrderType", order_type)
        buf.set("Channel", _i(getattr(msg, "ChannelNo", 0)))
        buf.set("SeqNum", _i(getattr(msg, "ApplSeqNum", 0)))
    except Exception:
        buf.cancel_row()
        return False
    return buf.commit_row()


def write_sz_deal(buf, msg: Any, trading_day: date) -> bool:
    """Write SZ deal directly to buffer. Returns True if watermark hit."""
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return False
    buy_id = _i(getattr(msg, "BidApplSeqNum", 0))
    sell_id = _i(getattr(msg, "OfferApplSeqNum", 0))
    side = 0 if buy_id > sell_id else 1
    if _i(getattr(msg, "ExecType", 0)) == 52:
        side = 4
    price = _f(getattr(msg, "LastPx", 0))
    volume = _f(getattr(msg, "LastQty", 0))
    buf.begin_row()
    try:
        buf.set("TradingDay", str(trading_day))
        buf.set("Code", _code(msg.SecurityID, "SZ"))
        event_time = _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day)
        buf.set("Time", event_time)
        buf.set("UpdateTime", event_time)
        buf.set("SaleOrderID", sell_id)
        buf.set("BuyOrderID", buy_id)
        buf.set("Side", side)
        buf.set("Price", price)
        buf.set("Volume", volume)
        buf.set("Money", price * volume)
        buf.set("Channel", _i(getattr(msg, "ChannelNo", 0)))
        buf.set("SeqNum", _i(getattr(msg, "ApplSeqNum", 0)))
    except Exception:
        buf.cancel_row()
        return False
    return buf.commit_row()


def frame(rows: List[Dict[str, Any]], columns: List[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows, columns=columns)


def frame_arrow(rows: List[Dict[str, Any]], kind: str) -> Optional[pa.RecordBatch]:
    """Build Arrow RecordBatch directly from row dicts, skipping pandas entirely."""
    if not rows:
        return None
    schema = ARROW_SCHEMA_BY_KIND[kind]
    arrays = []
    for field in schema:
        col_name = field.name
        col_type = field.type
        values = [r.get(col_name) for r in rows]
        try:
            arr = pa.array(values, type=col_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # fallback: coerce via safe cast
            arr = pa.array(values).cast(col_type, safe=False)
        arrays.append(arr)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# ================================================================== #
# Tuple-extraction functions: return (code, tuple) from SDK messages #
# ================================================================== #


def _tick_levels_tuple(
    bids: list, asks: list,
    bid_price_attr: str, bid_vol_attr: str, bid_num_attr: str,
    ask_price_attr: str, ask_vol_attr: str, ask_num_attr: str,
) -> tuple:
    """Build 60-element tuple for 10-level ask/bid data.

    Order matches TICK_COLUMNS: AskPrice1-10, AskVolume1-10, AskNum1-10,
    BidPrice1-10, BidVolume1-10, BidNum1-10.
    """
    ask_prices: list = [0.0] * 10
    ask_volumes: list = [0.0] * 10
    ask_nums: list = [0.0] * 10
    bid_prices: list = [0.0] * 10
    bid_volumes: list = [0.0] * 10
    bid_nums: list = [0.0] * 10
    for i in range(10):
        ap, av, an = _level(asks, i, ask_price_attr, ask_vol_attr, ask_num_attr)
        ask_prices[i] = ap
        ask_volumes[i] = av
        ask_nums[i] = an
        bp, bv, bn = _level(bids, i, bid_price_attr, bid_vol_attr, bid_num_attr)
        bid_prices[i] = bp
        bid_volumes[i] = bv
        bid_nums[i] = bn
    return tuple(ask_prices + ask_volumes + ask_nums +
                 bid_prices + bid_volumes + bid_nums)


def extract_sh_tick_tuple(
    msg: Any, trading_day: date, seq_id: int,
) -> Optional[Tuple[str, tuple]]:
    """Extract SH tick fields into an 81-element tuple matching TICK_COLUMNS order.

    Returns ``(code, tuple)`` or ``None`` if the message is not a valid SH stock.
    HighLimitPrice and LowLimitPrice are set to 0.0 for SH ticks.
    """
    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
        return None
    code = _code(msg.SecurityID, "SH")
    event_time = _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day)
    td = str(trading_day)
    row = (
        # (0) TradingDay
        td,
        # (1) Code
        code,
        # (2) Time
        event_time,
        # (3) UpdateTime
        event_time,
        # (4) CurrentPrice
        _f(getattr(msg, "LastPrice", 0)),
        # (5) TotalVolume
        _f(getattr(msg, "TradVolume", 0)),
        # (6) TotalMoney
        _f(getattr(msg, "Turnover", 0)),
        # (7) PreClosePrice
        _f(getattr(msg, "PreCloPrice", 0)),
        # (8) OpenPrice
        _f(getattr(msg, "OpenPrice", 0)),
        # (9) HighestPrice
        _f(getattr(msg, "HighPrice", 0)),
        # (10) LowestPrice
        _f(getattr(msg, "LowPrice", 0)),
        # (11) HighLimitPrice
        0.0,
        # (12) LowLimitPrice
        0.0,
        # (13) IOPV
        _f(getattr(msg, "IOPV", 0)),
        # (14) TradeNum
        _f(getattr(msg, "TradNumber", 0)),
        # (15) TotalBidVolume
        _f(getattr(msg, "TotalBidVol", 0)),
        # (16) TotalAskVolume
        _f(getattr(msg, "TotalAskVol", 0)),
        # (17) AvgBidPrice
        _f(getattr(msg, "WAvgBidPri", 0)),
        # (18) AvgAskPrice
        _f(getattr(msg, "WAvgAskPri", 0)),
    )
    bids = list(getattr(msg, "BidLevels", []) or [])
    asks = list(getattr(msg, "SellLevels", []) or [])
    levels = _tick_levels_tuple(
        bids, asks,
        "OrderPrice", "OrderVol", "OrderNum",
        "OrderPrice", "OrderVol", "OrderNum",
    )
    # (19-78) 60-level elements + (79) Channel + (80) SeqNum
    return code, row + levels + (0, seq_id)


def extract_sz_tick_tuple(
    msg: Any, trading_day: date, seq_id: int,
) -> Optional[Tuple[str, tuple]]:
    """Extract SZ tick fields into an 81-element tuple matching TICK_COLUMNS order.

    Returns ``(code, tuple)`` or ``None`` if the message is not a valid SZ stock.
    Uses SZ-specific field names (Volume, HighLimitPrice, LowLimitPrice, etc.).
    """
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return None
    code = _code(msg.SecurityID, "SZ")
    event_time = _parse_mdl_time(getattr(msg, "UpdateTime", 0), trading_day)
    td = str(trading_day)
    row = (
        # (0) TradingDay
        td,
        # (1) Code
        code,
        # (2) Time
        event_time,
        # (3) UpdateTime
        event_time,
        # (4) CurrentPrice
        _f(getattr(msg, "LastPrice", 0)),
        # (5) TotalVolume
        _f(getattr(msg, "Volume", 0)),
        # (6) TotalMoney
        _f(getattr(msg, "Turnover", 0)),
        # (7) PreClosePrice
        _f(getattr(msg, "PreCloPrice", 0)),
        # (8) OpenPrice
        _f(getattr(msg, "OpenPrice", 0)),
        # (9) HighestPrice
        _f(getattr(msg, "HighPrice", 0)),
        # (10) LowestPrice
        _f(getattr(msg, "LowPrice", 0)),
        # (11) HighLimitPrice
        _f(getattr(msg, "HighLimitPrice", 0)),
        # (12) LowLimitPrice
        _f(getattr(msg, "LowLimitPrice", 0)),
        # (13) IOPV
        _f(getattr(msg, "IOPV", 0)),
        # (14) TradeNum
        _f(getattr(msg, "TurnNum", 0)),
        # (15) TotalBidVolume
        _f(getattr(msg, "TotalBidQty", 0)),
        # (16) TotalAskVolume
        _f(getattr(msg, "TotalOfferQty", 0)),
        # (17) AvgBidPrice
        _f(getattr(msg, "WeightedAvgBidPx", 0)),
        # (18) AvgAskPrice
        _f(getattr(msg, "WeightedAvgOfferPx", 0)),
    )
    bids = list(getattr(msg, "BidPriceLevel", []) or [])
    asks = list(getattr(msg, "AskPriceLevel", []) or [])
    levels = _tick_levels_tuple(
        bids, asks,
        "Price", "Volume", "NumOrders",
        "Price", "Volume", "NumOrders",
    )
    # (19-78) 60-level elements + (79) Channel + (80) SeqNum
    return code, row + levels + (_i(getattr(msg, "ChannelNo", 0)), seq_id)


def extract_sh_ngts_tuple(
    msg: Any, trading_day: date,
) -> Optional[Tuple[str, Optional[tuple], Optional[tuple]]]:
    """Extract SH NGTS fields into order and/or deal tuples.

    Returns ``(code, order_tuple_or_None, deal_tuple_or_None)`` where
    ``order_tuple`` has 11 elements matching ORDER_COLUMNS and
    ``deal_tuple`` has 12 elements matching DEAL_COLUMNS.
    SH NGTS ``Type`` field: ``"A"``/``"D"`` = order, ``"T"`` = deal.
    Returns ``None`` if the message is not a valid SH stock.
    """
    if not _is_stock(getattr(msg, "SecurityID", ""), "SH"):
        return None
    typ = str(getattr(msg, "Type", "")).strip()
    code = _code(msg.SecurityID, "SH")
    event_time = _parse_mdl_time(getattr(msg, "TickTime", 0), trading_day)
    side = _side_from_flag(getattr(msg, "TickBSFlag", ""))
    td = str(trading_day)

    if typ in ("A", "D"):
        order_tuple = (
            # ORDER_COLUMNS: TradingDay, Code, Time, UpdateTime,
            # OrderID, Side, Price, Volume, OrderType, Channel, SeqNum
            td,
            code,
            event_time,
            event_time,
            _i(getattr(msg, "BuyOrderNO", 0)) + _i(getattr(msg, "SellOrderNO", 0)),
            side,
            _f(getattr(msg, "Price", 0)),
            _f(getattr(msg, "Qty", 0)),
            2 if typ == "A" else 5,
            _i(getattr(msg, "Channel", 0)),
            _i(getattr(msg, "BizIndex", 0)),
        )
        return code, order_tuple, None

    if typ == "T":
        price = _f(getattr(msg, "Price", 0))
        volume = _f(getattr(msg, "Qty", 0))
        money = _f(getattr(msg, "TradeMoney", 0)) or price * volume
        deal_tuple = (
            # DEAL_COLUMNS: TradingDay, Code, Time, UpdateTime,
            # SaleOrderID, BuyOrderID, Side, Price, Volume, Money, Channel, SeqNum
            td,
            code,
            event_time,
            event_time,
            _i(getattr(msg, "SellOrderNO", 0)),
            _i(getattr(msg, "BuyOrderNO", 0)),
            side,
            price,
            volume,
            money,
            _i(getattr(msg, "Channel", 0)),
            _i(getattr(msg, "BizIndex", 0)),
        )
        return code, None, deal_tuple

    return code, None, None


def extract_sz_order_tuple(
    msg: Any, trading_day: date, seq_id: int,
) -> Optional[Tuple[str, tuple]]:
    """Extract SZ order fields into an 11-element tuple matching ORDER_COLUMNS.

    Returns ``(code, tuple)`` or ``None`` if the message is not a valid SZ stock.
    Side mapping: 49=buy(0), 50=sell(1).
    OrderType mapping: 49=1, 50=2, 85=3.
    """
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return None
    side = {49: 0, 50: 1}.get(_i(getattr(msg, "Side", 0)), 10)
    order_type = {49: 1, 50: 2, 85: 3}.get(_i(getattr(msg, "OrdType", 0)), 0)
    code = _code(msg.SecurityID, "SZ")
    event_time = _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day)
    # ORDER_COLUMNS: TradingDay, Code, Time, UpdateTime,
    # OrderID, Side, Price, Volume, OrderType, Channel, SeqNum
    return code, (
        str(trading_day),
        code,
        event_time,
        event_time,
        _i(getattr(msg, "ApplSeqNum", 0)),
        side,
        _f(getattr(msg, "Price", 0)),
        _f(getattr(msg, "OrderQty", 0)),
        order_type,
        _i(getattr(msg, "ChannelNo", 0)),
        _i(getattr(msg, "ApplSeqNum", 0)),
    )


def extract_sz_deal_tuple(
    msg: Any, trading_day: date, seq_id: int,
) -> Optional[Tuple[str, tuple]]:
    """Extract SZ deal fields into a 12-element tuple matching DEAL_COLUMNS.

    Returns ``(code, tuple)`` or ``None`` if the message is not a valid SZ stock.
    Side logic: buy_id > sell_id -> 0 (buy), else 1 (sell).
    If ExecType == 52, side is set to 4.
    """
    if not _is_stock(getattr(msg, "SecurityID", ""), "SZ"):
        return None
    buy_id = _i(getattr(msg, "BidApplSeqNum", 0))
    sell_id = _i(getattr(msg, "OfferApplSeqNum", 0))
    side = 0 if buy_id > sell_id else 1
    if _i(getattr(msg, "ExecType", 0)) == 52:
        side = 4
    price = _f(getattr(msg, "LastPx", 0))
    volume = _f(getattr(msg, "LastQty", 0))
    code = _code(msg.SecurityID, "SZ")
    event_time = _parse_mdl_time(getattr(msg, "TransactTime", 0), trading_day)
    # DEAL_COLUMNS: TradingDay, Code, Time, UpdateTime,
    # SaleOrderID, BuyOrderID, Side, Price, Volume, Money, Channel, SeqNum
    return code, (
        str(trading_day),
        code,
        event_time,
        event_time,
        sell_id,
        buy_id,
        side,
        price,
        volume,
        price * volume,
        _i(getattr(msg, "ChannelNo", 0)),
        _i(getattr(msg, "ApplSeqNum", 0)),
    )
