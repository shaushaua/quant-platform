# -*- coding: utf-8 -*-
"""Read QMT exported account files and build PortfolioContext."""

from __future__ import annotations

import logging
import os
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from ..inference.interface import PortfolioContext

logger = logging.getLogger(__name__)


def get_portfolio_context(date_str: str, end_time: str) -> PortfolioContext:
    """Return the latest QMT account snapshot from the configured export dir.

    Prefer ``QMT_GATEWAY_URL``. Local CSV/TXT reading is kept only for mounted
    export directories in development.
    """
    gateway_url = os.environ.get("QMT_GATEWAY_URL", "")
    if gateway_url:
        return _get_gateway_portfolio_context(gateway_url, date_str, end_time)

    local_dir_raw = os.environ.get("QMT_EXPORT_LOCAL_DIR", "")
    local_dir = Path(local_dir_raw) if local_dir_raw else None
    account_id = os.environ.get("QMT_ACCOUNT_ID", "")
    account_type = os.environ.get("QMT_ACCOUNT_TYPE", "2")

    if local_dir is not None and local_dir.exists():
        positions_file = _latest_file(local_dir, ("position", "positions", "持仓"))
        account_file = _latest_file(local_dir, ("asset", "account", "fund", "资金", "资产"))
        orders_file = _latest_file(local_dir, ("order", "orders", "委托"))
        deals_file = _latest_file(local_dir, ("deal", "deals", "成交"))

        positions = _standardize_positions(_read_table(positions_file))
        account = _standardize_account(_read_table(account_file), account_id)
        orders = _standardize_orders(_read_table(orders_file))
        deals = _standardize_deals(_read_table(deals_file))

        latest_mtime = max(
            [p.stat().st_mtime for p in (positions_file, account_file, orders_file, deals_file) if p],
            default=0,
        )
        as_of = pd.to_datetime(latest_mtime, unit="s").strftime("%Y%m%d %H:%M:%S") if latest_mtime else ""
        source = str(local_dir)
    else:
        positions_file = account_file = orders_file = deals_file = None
        positions = pd.DataFrame()
        account = pd.DataFrame()
        orders = pd.DataFrame()
        deals = pd.DataFrame()
        as_of = ""
        source = ""
        logger.warning("[qmt-context] QMT_GATEWAY_URL not configured and local export dir missing: %s",
                       local_dir_raw)

    return PortfolioContext(
        account_id=account_id,
        broker="qmt",
        account_type=account_type,
        as_of=as_of,
        source=source,
        positions=positions,
        account=account,
        orders=orders,
        deals=deals,
        meta={
            "date": date_str,
            "end_time": end_time,
            "positions_file": str(positions_file) if positions_file else "",
            "account_file": str(account_file) if account_file else "",
            "orders_file": str(orders_file) if orders_file else "",
            "deals_file": str(deals_file) if deals_file else "",
        },
    )


def _get_gateway_portfolio_context(gateway_url: str, date_str: str, end_time: str) -> PortfolioContext:
    account_id = os.environ.get("QMT_ACCOUNT_ID", "")
    account_type = os.environ.get("QMT_ACCOUNT_TYPE", "2")
    timeout = float(os.environ.get("QMT_GATEWAY_TIMEOUT", "10"))
    token = os.environ.get("QMT_GATEWAY_TOKEN", "")
    params = urllib.parse.urlencode({"account_id": account_id}) if account_id else ""
    url = f"{gateway_url.rstrip('/')}/v1/positions"
    if params:
        url = f"{url}?{params}"

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    positions = pd.DataFrame()
    as_of = ""
    source = url
    meta = {
        "date": date_str,
        "end_time": end_time,
        "gateway_url": gateway_url.rstrip("/"),
    }

    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("ok", False):
            logger.warning("[qmt-context] gateway returned not ok: %s", payload.get("error", ""))
        else:
            positions = _standardize_gateway_positions(pd.DataFrame(payload.get("positions") or []))
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
            logger.debug("[qmt-context] gateway positions=%d stale=%s source=%s",
                         len(positions), payload.get("stale", None), source)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        logger.warning("[qmt-context] gateway HTTP %s: %s", exc.code, body[:300])
    except Exception as exc:
        logger.warning("[qmt-context] gateway read failed: %s", exc)

    return PortfolioContext(
        account_id=account_id,
        broker="qmt",
        account_type=account_type,
        as_of=as_of,
        source=source,
        positions=positions,
        account=pd.DataFrame(),
        orders=pd.DataFrame(),
        deals=pd.DataFrame(),
        meta=meta,
    )


def _latest_file(base_dir: Path, prefixes: Iterable[str]) -> Optional[Path]:
    if not base_dir or not base_dir.exists():
        logger.warning("[qmt-context] export dir not found: %s", base_dir)
        return None
    candidates = []
    lowered_prefixes = tuple(p.lower() for p in prefixes)
    for path in base_dir.iterdir():
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in {".csv", ".txt"}:
            continue
        name = path.name.lower()
        if name.endswith("_result.dbf"):
            continue
        if any(name.startswith(prefix) or prefix in name for prefix in lowered_prefixes):
            candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _read_table(path: Optional[Path]) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    for encoding in ("utf-8-sig", "gbk", "gb18030"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
        except Exception as exc:
            logger.warning("[qmt-context] failed to read %s: %s", path, exc)
            return pd.DataFrame()
    logger.warning("[qmt-context] failed to decode %s", path)
    return pd.DataFrame()


def _pick_col(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    if df.empty:
        return None
    exact = {str(c).strip(): c for c in df.columns}
    lower = {str(c).strip().lower(): c for c in df.columns}
    for name in names:
        if name in exact:
            return exact[name]
        key = name.lower()
        if key in lower:
            return lower[key]
    return None


def _series(df: pd.DataFrame, names: Iterable[str], default=0) -> pd.Series:
    col = _pick_col(df, names)
    if col is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[col]


def _standardize_positions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    code_col = _pick_col(out, ("code", "stock_code", "证券代码", "STOCK_CODE"))
    market_col = _pick_col(out, ("market", "市场", "交易市场", "MARKET"))
    if code_col is not None:
        out["code"] = [
            _normalize_code(code, out.at[idx, market_col] if market_col is not None else "")
            for idx, code in out[code_col].items()
        ]
    out["current_volume"] = pd.to_numeric(
        _series(out, ("current_volume", "volume", "持仓数量", "证券数量", "当前持仓", "总持仓")),
        errors="coerce",
    ).fillna(0).astype("int64")
    out["available_volume"] = pd.to_numeric(
        _series(out, ("available_volume", "可用数量", "可卖数量", "可用余额"), 0),
        errors="coerce",
    ).fillna(0).astype("int64")
    out["market_value"] = pd.to_numeric(
        _series(out, ("market_value", "市值", "证券市值"), 0),
        errors="coerce",
    ).fillna(0.0)
    out["cost_price"] = pd.to_numeric(
        _series(out, ("cost_price", "成本价", "持仓成本价"), 0),
        errors="coerce",
    ).fillna(0.0)
    out["last_price"] = pd.to_numeric(
        _series(out, ("last_price", "最新价", "当前价"), 0),
        errors="coerce",
    ).fillna(0.0)
    return out


def _standardize_gateway_positions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "symbol" in out.columns:
        out["code"] = out["symbol"].map(_normalize_code)
    elif "code" in out.columns:
        out["code"] = out["code"].map(_normalize_code)
    volume = _series(out, ("volume", "current_volume"), 0)
    out["current_volume"] = pd.to_numeric(volume, errors="coerce").fillna(0).astype("int64")
    out["available_volume"] = pd.to_numeric(
        _series(out, ("available_volume", "available"), 0),
        errors="coerce",
    ).fillna(0).astype("int64")
    out["market_value"] = pd.to_numeric(
        _series(out, ("market_value",), 0),
        errors="coerce",
    ).fillna(0.0)
    out["cost_price"] = pd.to_numeric(
        _series(out, ("cost_price",), 0),
        errors="coerce",
    ).fillna(0.0)
    out["last_price"] = pd.to_numeric(
        _series(out, ("last_price",), 0),
        errors="coerce",
    ).fillna(0.0)
    return out


def _standardize_account(df: pd.DataFrame, account_id: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "account_id" not in out.columns:
        out["account_id"] = account_id
    out["total_asset"] = pd.to_numeric(
        _series(out, ("total_asset", "总资产", "资产总值"), 0),
        errors="coerce",
    ).fillna(0.0)
    out["cash"] = pd.to_numeric(
        _series(out, ("cash", "可用资金", "可用金额", "资金余额"), 0),
        errors="coerce",
    ).fillna(0.0)
    return out


def _standardize_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    _add_code_column(out)
    return out


def _standardize_deals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    _add_code_column(out)
    return out


def _add_code_column(df: pd.DataFrame) -> None:
    code_col = _pick_col(df, ("code", "stock_code", "证券代码", "STOCK_CODE"))
    market_col = _pick_col(df, ("market", "市场", "交易市场", "MARKET"))
    if code_col is None:
        return
    df["code"] = [
        _normalize_code(code, df.at[idx, market_col] if market_col is not None else "")
        for idx, code in df[code_col].items()
    ]


def _normalize_code(code: object, market: object = "") -> str:
    raw = str(code or "").strip().upper()
    if not raw:
        return ""
    if raw.startswith("SH") and len(raw) >= 8:
        return f"{raw[-6:]}.SH"
    if raw.startswith("SZ") and len(raw) >= 8:
        return f"{raw[-6:]}.SZ"
    if raw.endswith(".SH") or raw.endswith(".SZ"):
        return raw
    raw = raw.zfill(6) if raw.isdigit() and len(raw) <= 6 else raw
    mkt = str(market or "").strip().upper()
    if mkt in {"SH", "XSHG", "上海", "上交所"} or (len(raw) == 6 and raw[0] in {"6", "9"}):
        return f"{raw}.SH"
    if mkt in {"SZ", "XSHE", "深圳", "深交所"} or (len(raw) == 6 and raw[0] in {"0", "3"}):
        return f"{raw}.SZ"
    return raw
