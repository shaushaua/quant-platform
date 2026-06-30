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
                source_kind = str(payload.get("source", "") or "")
                # Stale detection differs by source:
                #   - shadow: per-position `price_stale` flag (quote > 60s old).
                #     Whole snapshot is considered stale if all positions have
                #     stale prices.
                #   - raw (legacy DBF): use top-level `stale`/`stale_seconds`
                #     derived from DBF mtime vs cfg.PositionMaxAge.
                if source_kind == "shadow":
                    raw_positions = payload.get("positions") or []
                    positions = _standardize_gateway_positions(pd.DataFrame(raw_positions))
                    meta["positions_usable"] = True
                    # Aggregate price_stale: if any non-stale position exists,
                    # consider the snapshot fresh.
                    stale_flags = [p.get("price_stale", False) for p in raw_positions
                                   if isinstance(p, dict)]
                    any_fresh = any(not f for f in stale_flags) if stale_flags else True
                    if not any_fresh and stale_flags:
                        meta["positions_stale"] = True
                        logger.error(
                            "[order-context] shadow positions all stale (quotes >60s old): n=%d",
                            len(stale_flags),
                        )
                    meta["source_kind"] = "shadow"
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
                    meta["source_kind"] = "raw"
                as_of = str(payload.get("source_mtime", "") or payload.get("date", "") or "")
                source = str(payload.get("source_file", "") or url)
                meta.update({
                    "source_file": payload.get("source_file", ""),
                    "source_mtime": payload.get("source_mtime", ""),
                    "stale_seconds": payload.get("stale_seconds", None),
                    "stale": payload.get("stale", None),
                    "raw_rows": payload.get("raw_rows", None),
                    "position_count": payload.get("position_count", len(positions)),
                    "total_market_value": payload.get("total_market_value", None),
                    "total_unrealized": payload.get("total_unrealized", None),
                })
                logger.debug("[order-context] gateway positions=%d kind=%s stale=%s",
                             len(positions), source_kind, meta.get("positions_stale"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            logger.warning("[order-context] gateway HTTP %s: %s", exc.code, body[:300])
        except Exception as exc:
            logger.warning("[order-context] gateway read failed: %s", exc)

    # Fetch balance to populate account
    account = _fetch_balance_to_df(gateway_url, account_id, token, timeout)

    return PortfolioContext(
        account_id=account_id,
        broker=broker,
        account_type="",
        as_of=as_of,
        source=source,
        positions=positions,
        account=account,
        orders=pd.DataFrame(),
        deals=pd.DataFrame(),
        meta=meta,
    )


def _standardize_gateway_positions(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize gateway positions to schema defined in INFERENCE_INTERFACE.md.

    Supports two upstream sources via the same `/v1/positions` endpoint:
      - `source=shadow` (default): order-gateway shadow ledger returns
        qty / available_qty / avg_cost / market_value / last_price /
        unrealized_pnl / realized_pnl / strategies / price_stale.
      - `source=raw`: legacy ATX DBF returns volume / available_volume /
        cost_price / market_value / last_price / pnl.

    Always returns columns: code, current_volume, available_volume,
    market_value, last_price, cost_price (and `name`/`pnl` if present in source).
    Missing numeric columns become NaN Series (not absent), so downstream code
    can safely call `.fillna(...)` on them. Without this, intraday DBF (which
    only carries symbol/volume/available_volume) would cause
    `df.get('market_value', 0.0).fillna(...)` to crash with
    `'float' object has no attribute 'fillna'`.
    """
    if df.empty:
        return df
    out = pd.DataFrame()
    out["code"] = df.get("symbol", pd.Series(dtype=object)).map(_normalize_code)
    # current_volume: shadow uses `qty`, legacy uses `volume`.
    vol_src = df.get("qty", df.get("volume", 0))
    out["current_volume"] = pd.to_numeric(vol_src, errors="coerce").fillna(0).astype(int)
    # available_volume: shadow uses `available_qty`, legacy uses `available_volume`.
    avail_src = df.get("available_qty", df.get("available_volume", 0))
    out["available_volume"] = pd.to_numeric(avail_src, errors="coerce").fillna(0).astype(int)
    # cost_price: shadow uses `avg_cost`, legacy uses `cost_price`.
    cost_src = df.get("avg_cost", df.get("cost_price", pd.Series(dtype=float)))
    # pnl: shadow uses `unrealized_pnl`, legacy uses `pnl`.
    pnl_src = df.get("unrealized_pnl", df.get("pnl", pd.Series(dtype=float)))
    # Optional enrichment columns — always emit as Series (NaN if absent) so
    # downstream .fillna works regardless of intraday vs EOD DBF schema.
    name_src = df.get("name", pd.Series(dtype=object))
    if not name_src.empty:
        out["name"] = name_src
    for src, dst in (
        (cost_src, "cost_price"),
        (df.get("last_price", pd.Series(dtype=float)), "last_price"),
        (df.get("market_value", pd.Series(dtype=float)), "market_value"),
        (pnl_src, "pnl"),
    ):
        if src is not None and len(src) > 0:
            out[dst] = pd.to_numeric(src, errors="coerce")
        else:
            out[dst] = pd.Series([float("nan")] * len(out), dtype=float)
    # Carry shadow-only diagnostic fields if present (price_stale, price_source,
    # strategies, realized_pnl). Downstream may use them for telemetry; they're
    # not part of INFERENCE_INTERFACE.md schema but harmless to pass through.
    for passthrough in ("price_stale", "price_source", "strategies", "realized_pnl"):
        if passthrough in df.columns:
            out[passthrough] = df[passthrough]
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


def _fetch_balance(gateway_url: str, account_id: str, token: str, timeout: float) -> dict:
    """Fetch account balance from order-gateway."""
    params = urllib.parse.urlencode({"account_id": account_id}) if account_id else ""
    url = f"{gateway_url.rstrip('/')}/v1/balance"
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
            logger.warning("[order-context] balance query not ok: %s", payload.get("error", ""))
            return {}
        return payload.get("balance", {}) or {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        logger.warning("[order-context] balance HTTP %s: %s", exc.code, body[:300])
    except Exception as exc:
        logger.warning("[order-context] balance query failed: %s", exc)
    return {}


def _fetch_balance_to_df(gateway_url: str, account_id: str, token: str, timeout: float) -> pd.DataFrame:
    """Fetch balance and return as single-row DataFrame for PortfolioContext.account.

    Two upstream paths exist via `/v1/balance`:
      - default (`source=shadow`): order-gateway shadow ledger returns Balance
        struct directly with total_asset/available_cash/market_value/
        realized_pnl/unrealized_pnl. No DBF field translation needed.
      - legacy (`source=raw`): ATX DBF raw map. Translates AssetAmt/MarketAmt/
        EnBalance/CrBalance to the standardized schema.

    Standardized fields are NaN if DBF has no data (intraday DBF returns
    '0.000' for AssetAmt/MarketAmt before settlement).
    """
    balance_info = _fetch_balance(gateway_url, account_id, token, timeout)
    if not balance_info:
        return pd.DataFrame()
    row = dict(balance_info)
    row.setdefault("account_id", account_id)
    # Shadow ledger already provides standardized fields. Only run the DBF
    # field translation when total_asset is absent (legacy raw path).
    if "total_asset" not in row:
        asset = row.get("AssetAmt")
        if asset is not None:
            try:
                row["total_asset"] = float(asset)
            except (TypeError, ValueError):
                row["total_asset"] = float("nan")
    if "market_value" not in row:
        mkt = row.get("MarketAmt")
        if mkt is not None:
            try:
                row["market_value"] = float(mkt)
            except (TypeError, ValueError):
                row["market_value"] = float("nan")
    if "available_cash" not in row:
        # EnBalance = enabled balance (available cash); fall back to CrBalance.
        cash_src = row.get("EnBalance")
        if cash_src is None:
            cash_src = row.get("CrBalance")
        if cash_src is not None:
            try:
                row["available_cash"] = float(cash_src)
            except (TypeError, ValueError):
                row["available_cash"] = float("nan")
    return pd.DataFrame([row])


def _code_to_latest_key(code: str) -> str:
    """Map positions.code (.SH/.SZ) → latest_prices key (.XSHG/.XSHE)."""
    if not isinstance(code, str):
        return ""
    if code.endswith(".SH"):
        return code[:-3] + ".XSHG"
    if code.endswith(".SZ"):
        return code[:-3] + ".XSHE"
    return code


def enrich_portfolio_context_with_latest_prices(
    portfolio_context, latest_prices: dict
) -> None:
    """Backfill positions/account valuation columns from SHM latest_prices.

    This is now a **fallback path**. The primary source for `market_value` /
    `last_price` / `available_cash` is the order-gateway shadow ledger
    (`GET /v1/positions?source=shadow` and `/v1/balance?source=shadow`),
    which derives these from DealOrder replay + Tencent/Eastmoney HTTP quotes.
    This SHM-based enrichment only fires when shadow values are missing or
    zero (e.g. shadow ledger not configured, or quote source unreachable).

    Mutates `portfolio_context` in place:
      - positions.last_price: NaN/0 → SHM latest price (when available)
      - positions.market_value: NaN/0 → last_price * current_volume
      - account.total_asset / market_value: NaN/0 → sum of position market_value
      - account.available_cash: left at 0 (intraday DBF gives no reliable figure)

    No-op when positions is empty or latest_prices is empty.
    """
    pos = getattr(portfolio_context, "positions", None)
    if pos is None or pos.empty or not latest_prices:
        return

    # 1. last_price: NaN/0 → SHM latest price
    keys = pos["code"].map(_code_to_latest_key)
    shm_px = keys.map(lambda k: latest_prices.get(k, float("nan")))
    need_px = pos["last_price"].isna() | (pos["last_price"] == 0)
    pos.loc[need_px & shm_px.notna() & (shm_px > 0), "last_price"] = \
        shm_px[need_px & shm_px.notna() & (shm_px > 0)]

    # 2. market_value: NaN/0 → last_price * current_volume
    need_mv = pos["market_value"].isna() | (pos["market_value"] == 0)
    if need_mv.any():
        lp = pos.loc[need_mv, "last_price"]
        cv = pos.loc[need_mv, "current_volume"]
        pos.loc[need_mv, "market_value"] = (lp.fillna(0.0) * cv.fillna(0)).astype(float)

    # 3. account: total_asset / market_value = sum of position market_value
    #    when DBF intraday returns 0/NaN.
    total_mv = float(pos["market_value"].fillna(0).sum())
    account = getattr(portfolio_context, "account", None)
    if account is None or account.empty:
        if total_mv > 0:
            portfolio_context.account = pd.DataFrame([{
                "account_id": portfolio_context.account_id,
                "total_asset": total_mv,
                "market_value": total_mv,
                "available_cash": 0.0,
            }])
    elif total_mv > 0:
        for col, val in (("market_value", total_mv), ("total_asset", total_mv)):
            if col not in account.columns:
                account[col] = float("nan")
            cur = account[col].iloc[0]
            try:
                cur_val = float(cur)
            except (TypeError, ValueError):
                cur_val = 0.0
            if cur_val in (0.0, float("nan")) or cur_val != cur_val:
                account.loc[account.index[0], col] = total_mv


# ── ATX 进程管理 ──────────────────────────────────────────────────────


def atx_status(gateway_url: str, token: str = "") -> dict:
    """Query ATX process status via GET /v1/atx/status."""
    url = f"{gateway_url.rstrip('/')}/v1/atx/status"
    return _atx_request(url, token)


def atx_start(gateway_url: str, token: str = "") -> dict:
    """Start ATX process via POST /v1/atx/start."""
    url = f"{gateway_url.rstrip('/')}/v1/atx/start"
    return _atx_request(url, token, method="POST")


def atx_stop(gateway_url: str, token: str = "") -> dict:
    """Stop ATX process via POST /v1/atx/stop."""
    url = f"{gateway_url.rstrip('/')}/v1/atx/stop"
    return _atx_request(url, token, method="POST")


def list_dbf_files(gateway_url: str, token: str = "") -> list[dict]:
    """List all DBF report files via GET /v1/atx/report."""
    url = f"{gateway_url.rstrip('/')}/v1/atx/report"
    result = _atx_request(url, token)
    if isinstance(result, dict) and "files" in result:
        return result["files"]
    return []


def read_dbf_file(gateway_url: str, token: str = "", file: str = "") -> dict:
    """Read a specific DBF file via GET /v1/atx/report?file=xxx.dbf."""
    params = urllib.parse.urlencode({"file": file}) if file else ""
    url = f"{gateway_url.rstrip('/')}/v1/atx/report"
    if params:
        url = f"{url}?{params}"
    return _atx_request(url, token)


def _atx_request(url: str, token: str = "", method: str = "GET") -> dict:
    """Low-level ATX HTTP request helper."""
    timeout = float(os.environ.get("ORDER_GATEWAY_TIMEOUT", "10"))
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        data = None
        if method == "POST":
            data = b"{}"
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        logger.warning("[order-context] ATX HTTP %s: %s", exc.code, body[:300])
    except Exception as exc:
        logger.warning("[order-context] ATX request failed: %s", exc)
    return {"ok": False, "error": str(exc) if "exc" in locals() else "request failed"}
