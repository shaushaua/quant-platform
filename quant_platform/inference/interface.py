# -*- coding: utf-8 -*-
"""Shared inference interface helpers."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

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


def inference_accepts_portfolio_context(inference_fn: Callable) -> bool:
    """Return whether ``inference_fn`` can accept the sixth context argument."""

    try:
        sig = inspect.signature(inference_fn)
    except (TypeError, ValueError):
        return False

    params = list(sig.parameters.values())
    if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
        return True
    if "portfolio_context" in sig.parameters:
        return True

    positional = [
        p for p in params
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    return len(positional) >= 6


def call_inference(
    inference_fn: Callable,
    date_str: str,
    end_time: str,
    prev_day_factors_df: Optional[pd.DataFrame],
    intraday_factors_df: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame],
    portfolio_context: Optional[PortfolioContext] = None,
) -> pd.DataFrame:
    """Call a trader inference function with backward-compatible arity."""

    args = (
        date_str,
        end_time,
        prev_day_factors_df,
        intraday_factors_df,
        daily_basic_df,
    )
    if inference_accepts_portfolio_context(inference_fn):
        try:
            sig = inspect.signature(inference_fn)
        except (TypeError, ValueError):
            sig = None
        if sig is not None:
            context_param = sig.parameters.get("portfolio_context")
            if context_param is not None and context_param.kind in (
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                return inference_fn(*args, portfolio_context=portfolio_context)
        return inference_fn(*args, portfolio_context)
    return inference_fn(*args)
