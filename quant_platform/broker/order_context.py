# -*- coding: utf-8 -*-
"""Read account snapshots from order-gateway and build PortfolioContext."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

import pandas as pd

from ..inference.interface import PortfolioContext

logger = logging.getLogger(__name__)


def get_portfolio_context(date_str: str, end_time: str) -> PortfolioContext:
    gateway_url = os.environ.get("ORDER_GATEWAY_URL", "")
    account_id = os.environ.get("BROKER_ACCOUNT_ID", "")
    timeout = float(os.environ.get("ORDER_GATEWAY_TIMEOUT", "10"))
    token = os.environ.get("ORDER_GATEWAY_TOKEN", "")
    broker = os.environ.get("BROKER_TYPE", "atx")
    max_stale_seconds = float(os.environ.get("ORDER_POSITION_MAX_STALE_SECONDS", "15"))

    positions = pd.DataFrame()
    as_of = ""
    source = gateway_url
    meta = {
        "date": date_str,
        "end_time": end_time,
        "gateway_url": gateway_url.rstrip("/") if gateway_url else "",
        "positions_usable": False,
        "positions_stale": False,
    }

    if not gateway_url:
        logger.warning("[order-context] ORDER_GATEWAY_URL not configured")
    else:
        params = urllib.parse.urlencode({"account_id": account_id}) if account_id else ""
        url = f"{gateway_url.rstrip('/')}/v1/positions"
        if params:
            url = f"{url}?{params}"

        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if not payload.get("ok", False):
                logger.warning("[order-context] gateway returned not ok: %s", payload.get("error", ""))
            else:
                stale_seconds = payload.get("stale_seconds", None)
                stale = bool(payload.get("stale", False))
                try:
                    stale_by_age = stale_seconds is not None and float(stale_seconds) > max_stale_seconds
                except Exception:
                    stale_by_age = False
                if stale or stale_by_age:
                    meta["positions_stale"] = True
                    logger.error(
                        "[order-context] stale positions ignored: stale=%s stale_seconds=%s max=%s source=%s",
                        stale, stale_seconds, max_stale_seconds, payload.get("source_file", ""),
                    )
                else:
                    positions = _standardize_gateway_positions(pd.DataFrame(payload.get("positions") or []))
                    meta["positions_usable"] = True
                as_of = str(payload.get("source_mtime", "") or "")
                source = str(payload.get("source_file", "") or url)
                meta.update({
                    "source_file": payload.get("source_file", ""),
                    "source_mtime": payload.get("source_mtime", ""),
                    "stale_seconds": payload.get("stale_seconds", None),
                    "stale": payload.get("stale", None),
                    "raw_rows": payload.get("raw_rows", None),
                    "position_count": payload.get("position_count", len(positions)),
                })
                logger.debug("[order-context] gateway positions=%d stale=%s source=%s",
                             len(positions), payload.get("stale", None), source)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            logger.warning("[order-context] gateway HTTP %s: %s", exc.code, body[:300])
        except Exception as exc:
            logger.warning("[order-context] gateway read failed: %s", exc)

    return PortfolioContext(
        account_id=account_id,
        broker=broker,
        account_type="",
        as_of=as_of,
        source=source,
        positions=positions,
        account=pd.DataFrame(),
        orders=pd.DataFrame(),
        deals=pd.DataFrame(),
        meta=meta,
    )


def _standardize_gateway_positions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = pd.DataFrame()
    out["code"] = df.get("symbol", pd.Series(dtype=object)).map(_normalize_code)
    out["current_volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
    out["available_volume"] = pd.to_numeric(df.get("available_volume", 0), errors="coerce").fillna(0).astype(int)
    for src, dst in (
        ("name", "name"),
        ("market_value", "market_value"),
        ("cost_price", "cost_price"),
        ("last_price", "last_price"),
        ("pnl", "pnl"),
    ):
        if src in df.columns:
            out[dst] = df[src]
    return out[out["code"] != ""].reset_index(drop=True)


def _normalize_code(raw: object) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        return ""
    if value.endswith(".SH") or value.endswith(".SZ"):
        return value
    if value.startswith("SH") and len(value) == 8:
        return f"{value[2:]}.SH"
    if value.startswith("SZ") and len(value) == 8:
        return f"{value[2:]}.SZ"
    if len(value) == 6 and value[0] == "6":
        return f"{value}.SH"
    if len(value) == 6 and value[0] in {"0", "3"}:
        return f"{value}.SZ"
    return value
