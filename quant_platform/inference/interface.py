# -*- coding: utf-8 -*-
"""Shared inference interface helpers."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

import pandas as pd


@dataclass
class PortfolioContext:
    """Current account snapshot passed to trader inference modules.

    The engine owns data acquisition. Trader modules should treat these frames
    as read-only inputs and return target positions/orders.
    """

    account_id: str = ""
    broker: str = "qmt"
    account_type: str = ""
    as_of: str = ""
    source: str = ""
    positions: pd.DataFrame = field(default_factory=pd.DataFrame)
    account: pd.DataFrame = field(default_factory=pd.DataFrame)
    orders: pd.DataFrame = field(default_factory=pd.DataFrame)
    deals: pd.DataFrame = field(default_factory=pd.DataFrame)
    meta: dict[str, Any] = field(default_factory=dict)


_TRADING_UNIVERSE_PARAM_NAMES = {
    "trading_universe",
    "trading_universe_df",
    "universe",
    "universe_df",
}
_INDEX_COMPOSITION_PARAM_NAMES = {
    "index_composition",
    "index_composition_df",
}


def _safe_signature(fn: Callable) -> Optional[inspect.Signature]:
    try:
        return inspect.signature(fn)
    except (TypeError, ValueError):
        return None


def _positional_params(sig: inspect.Signature) -> list[inspect.Parameter]:
    return [
        p for p in sig.parameters.values()
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]


def _has_varargs(sig: inspect.Signature) -> bool:
    return any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())


def _has_varkw(sig: inspect.Signature) -> bool:
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())


def _trading_universe_param_name(sig: inspect.Signature) -> Optional[str]:
    for name in _TRADING_UNIVERSE_PARAM_NAMES:
        if name in sig.parameters:
            return name
    return None


def _index_composition_param_name(sig: inspect.Signature) -> Optional[str]:
    for name in _INDEX_COMPOSITION_PARAM_NAMES:
        if name in sig.parameters:
            return name
    return None


def inference_accepts_portfolio_context(inference_fn: Callable) -> bool:
    """Return whether ``inference_fn`` can accept the portfolio context argument."""

    sig = _safe_signature(inference_fn)
    if sig is None:
        return False

    if _has_varargs(sig) or _has_varkw(sig):
        return True
    if "portfolio_context" in sig.parameters:
        return True
    return len(_positional_params(sig)) >= 8


def build_trading_universe_df(
    codes: Iterable,
    daily_basic_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build the standardized trading universe DataFrame passed to inference."""
    code_list = [str(code) for code in codes]
    universe = pd.DataFrame({"code": code_list})
    if universe.empty:
        return universe
    universe["ID_QI"] = universe["code"].map(lambda code: code.split(".")[0].zfill(6))

    if daily_basic_df is None or daily_basic_df.empty:
        return universe
    if "ID_QI" not in daily_basic_df.columns:
        return universe

    cols = ["ID_QI"]
    for col in ("SECURITY_ID", "ts_code", "name", "industry", "list_status"):
        if col in daily_basic_df.columns:
            cols.append(col)
    daily = daily_basic_df[cols].copy()
    daily["ID_QI"] = daily["ID_QI"].astype(str).str.zfill(6)
    daily = daily.drop_duplicates("ID_QI")
    return universe.merge(daily, on="ID_QI", how="left")


def compute_trading_universe(
    daily_basic_df: Optional[pd.DataFrame],
    extra_fields: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """Compute the trading universe passed to inference.

    This is the extension point for custom universe rules. The default keeps the
    interface usable by standardizing the provided ``codes`` field.
    """
    extra_fields = extra_fields or {}
    codes = extra_fields.get("codes")
    if codes is None and daily_basic_df is not None and not daily_basic_df.empty:
        if "code" in daily_basic_df.columns:
            codes = daily_basic_df["code"].tolist()
        elif "ts_code" in daily_basic_df.columns:
            codes = daily_basic_df["ts_code"].tolist()
        elif "ID_QI" in daily_basic_df.columns:
            codes = daily_basic_df["ID_QI"].tolist()
    return build_trading_universe_df(codes or [], daily_basic_df)


def compute_index_composition(
    daily_basic_df: Optional[pd.DataFrame],
    extra_fields: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """Compute per-stock index composition data passed to inference.

    Placeholder for now; fill this with index constituent/weight logic later.
    """
    return pd.DataFrame()


def call_inference(
    inference_fn: Callable,
    date_str: str,
    end_time: str,
    prev_day_factors_df: Optional[pd.DataFrame],
    intraday_factors_df: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame],
    trading_universe_df: Optional[pd.DataFrame] = None,
    index_composition_df: Optional[pd.DataFrame] = None,
    portfolio_context: Optional[PortfolioContext] = None,
) -> pd.DataFrame:
    """Call a trader inference function with backward-compatible arity."""

    base_args = (
        date_str,
        end_time,
        prev_day_factors_df,
        intraday_factors_df,
        daily_basic_df,
    )
    sig = _safe_signature(inference_fn)
    if sig is None:
        return inference_fn(*base_args)

    if _has_varargs(sig):
        return inference_fn(*base_args, trading_universe_df, index_composition_df, portfolio_context)

    positional = _positional_params(sig)
    extra_args = []
    kwargs: dict[str, Any] = {}

    trading_name = _trading_universe_param_name(sig)
    index_name = _index_composition_param_name(sig)
    portfolio_param = sig.parameters.get("portfolio_context")
    accepts_varkw = _has_varkw(sig)

    if trading_name is None and index_name is None and portfolio_param is None:
        if len(positional) >= 6:
            extra_args.append(trading_universe_df)
        if len(positional) >= 7:
            extra_args.append(index_composition_df)
        if len(positional) >= 8:
            extra_args.append(portfolio_context)

    if trading_name is not None:
        kwargs[trading_name] = trading_universe_df
    elif accepts_varkw:
        kwargs["trading_universe"] = trading_universe_df

    if index_name is not None:
        kwargs[index_name] = index_composition_df
    elif accepts_varkw:
        kwargs["index_composition"] = index_composition_df

    if portfolio_param is not None:
        kwargs["portfolio_context"] = portfolio_context
    elif accepts_varkw:
        kwargs["portfolio_context"] = portfolio_context

    return inference_fn(*base_args, *extra_args, **kwargs)
