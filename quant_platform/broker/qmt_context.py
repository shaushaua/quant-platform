# -*- coding: utf-8 -*-
"""Read QMT exported account files and build PortfolioContext."""

from __future__ import annotations

import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd

from ..inference.interface import PortfolioContext

logger = logging.getLogger(__name__)


def get_portfolio_context(date_str: str, end_time: str) -> PortfolioContext:
    """Return the latest QMT account snapshot from the configured export dir.

    QMT data export writes CSV/TXT/DBF files such as position/asset/order/deal.
    This adapter currently reads CSV/TXT files from a local path. In Kubernetes,
    mount/sync the Windows export directory into the Pod or keep the SFTP mirror
    path as ``QMT_EXPORT_LOCAL_DIR``.
    """
    local_dir_raw = os.environ.get("QMT_EXPORT_LOCAL_DIR", "")
    local_dir = Path(local_dir_raw) if local_dir_raw else None
    remote_dir = os.environ.get("QMT_EXPORT_REMOTE_DIR", "")
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
        remote_tables = _read_remote_tables(remote_dir)
        positions_file, positions_raw = remote_tables["positions"]
        account_file, account_raw = remote_tables["account"]
        orders_file, orders_raw = remote_tables["orders"]
        deals_file, deals_raw = remote_tables["deals"]

        positions = _standardize_positions(positions_raw)
        account = _standardize_account(account_raw, account_id)
        orders = _standardize_orders(orders_raw)
        deals = _standardize_deals(deals_raw)
        as_of = ""
        source = f"sftp://{os.environ.get('QMT_SFTP_HOST', '')}/{remote_dir}"

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


def _read_remote_tables(remote_dir: str) -> dict[str, Tuple[str, pd.DataFrame]]:
    specs = {
        "positions": ("position", "positions", "持仓"),
        "account": ("asset", "account", "fund", "资金", "资产"),
        "orders": ("order", "orders", "委托"),
        "deals": ("deal", "deals", "成交"),
    }
    empty = {key: ("", pd.DataFrame()) for key in specs}
    if not remote_dir or not os.environ.get("QMT_SFTP_HOST", ""):
        return empty
    sftp = None
    try:
        import paramiko
        sftp = _open_sftp(paramiko)
        result = {}
        for key, prefixes in specs.items():
            remote_path = _latest_remote_path(sftp, remote_dir, prefixes)
            if not remote_path:
                result[key] = ("", pd.DataFrame())
                continue
            with sftp.open(remote_path, "rb") as fp:
                data = fp.read()
            result[key] = (remote_path, _read_csv_bytes(data, remote_path))
        return result
    except ImportError:
        logger.warning("[qmt-context] paramiko not installed; cannot read QMT exports via SFTP")
    except Exception as exc:
        logger.warning("[qmt-context] remote read failed: %s", exc)
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
    return empty


def _open_sftp(paramiko):
    host = os.environ.get("QMT_SFTP_HOST", "")
    port = int(os.environ.get("QMT_SFTP_PORT", "22"))
    user = os.environ.get("QMT_SFTP_USER", "quant")
    key_path = os.environ.get("QMT_SFTP_KEY", "")
    password = os.environ.get("QMT_SFTP_PASS", "")
    timeout = float(os.environ.get("QMT_SFTP_TIMEOUT", "10"))

    connect_kwargs = {
        "hostname": host,
        "port": port,
        "username": user,
        "timeout": timeout,
        "banner_timeout": timeout,
        "auth_timeout": timeout,
        "look_for_keys": False,
        "allow_agent": False,
    }
    if key_path and Path(key_path).exists():
        connect_kwargs["key_filename"] = key_path
        if password:
            connect_kwargs["password"] = password
    elif password:
        connect_kwargs["password"] = password
    else:
        raise RuntimeError("no QMT_SFTP_KEY or QMT_SFTP_PASS configured")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**connect_kwargs)
    return ssh.open_sftp()


def _latest_remote_path(sftp, remote_dir: str, prefixes: Iterable[str]) -> str:
    lowered_prefixes = tuple(p.lower() for p in prefixes)
    candidates = []
    for attr in sftp.listdir_attr(remote_dir):
        name = attr.filename
        lower_name = name.lower()
        suffix = Path(name).suffix.lower()
        if suffix not in {".csv", ".txt"}:
            continue
        if lower_name.endswith("_result.dbf"):
            continue
        if any(lower_name.startswith(prefix) or prefix in lower_name for prefix in lowered_prefixes):
            candidates.append((attr.st_mtime, _remote_join(remote_dir, name)))
    if not candidates:
        return ""
    return max(candidates, key=lambda item: item[0])[1]


def _remote_join(remote_dir: str, filename: str) -> str:
    sep = "\\" if "\\" in remote_dir else "/"
    clean_dir = remote_dir.rstrip("/\\")
    return f"{clean_dir}{sep}{filename}"


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


def _read_csv_bytes(data: bytes, source: str) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "gbk", "gb18030"):
        try:
            return pd.read_csv(BytesIO(data), encoding=encoding)
        except UnicodeDecodeError:
            continue
        except Exception as exc:
            logger.warning("[qmt-context] failed to parse %s: %s", source, exc)
            return pd.DataFrame()
    logger.warning("[qmt-context] failed to decode %s", source)
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
