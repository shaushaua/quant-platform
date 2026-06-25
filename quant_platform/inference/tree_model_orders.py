#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Engine adapter for tree_model_inference.

The underlying ``tree_model_inference.inference`` returns a *positions* frame
(``_code6``, ``position`` ...). The live engine's order-gateway path expects
*order rows* (``code``, ``side``, ``volume``, ``price_type`` ...).

This module exposes ``inference`` that:
    1. Calls ``tree_model_inference.inference`` to get target positions.
    2. Converts position fractions to share volumes using
       :func:`quant_platform.inference.interface.positions_to_orders` with
       total capital from ``portfolio_context.account.total_asset`` (real) or
       ``OPEN_POSITION_SIM_MODE=1`` + ``OPEN_POSITION_TOTAL_CAPITAL=<amount>``
       env (sim), and price from ``portfolio_context.meta['latest_prices']``
       (9:30 realtime tick).

Configure via:
    INFERENCE_MODULE=quant_platform.inference.tree_model_orders
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd

from .interface import positions_to_orders
from . import tree_model_inference as _tree

logger = logging.getLogger(__name__)

STRATEGY_NAME = os.environ.get("TREE_MODEL_STRATEGY_NAME", "tree_model")


def inference(
    date_str: str,
    end_time: str,
    prev_day_factors_df: Optional[pd.DataFrame],
    intraday_factors_df: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame] = None,
    trading_universe_df: Optional[pd.DataFrame] = None,
    index_composition_df: Optional[pd.DataFrame] = None,
    portfolio_context=None,
) -> pd.DataFrame:
    """Run tree model inference and convert positions to order rows."""
    positions = _tree.inference(
        date_str=date_str,
        end_time=end_time,
        prev_day_factors_df=prev_day_factors_df,
        intraday_factors_df=intraday_factors_df,
        daily_basic_df=daily_basic_df,
        trading_universe_df=trading_universe_df,
        index_composition_df=index_composition_df,
        portfolio_context=portfolio_context,
    )

    if positions is None or positions.empty:
        logger.warning("[tree-orders] no positions produced, no orders")
        return pd.DataFrame()

    positions = positions.copy()

    # tree model .so 是按 96 列 schema 编译的,实盘 daily_basic 多了
    # SEC_SHORT_NAME / SEC_FULL_NAME 两列会让内部列选择错位 →
    # ZeroDivisionError / 列读取错乱。在喂给 .so 前丢弃这两列。
    if daily_basic_df is not None and not daily_basic_df.empty:
        drop_cols = [c for c in ("SEC_SHORT_NAME", "SEC_FULL_NAME")
                     if c in daily_basic_df.columns]
        if drop_cols:
            daily_basic_df = daily_basic_df.drop(columns=drop_cols)
            logger.info("[tree-orders] dropped %s from daily_basic for .so compat",
                        drop_cols)

    # Normalize code column: _code6 (6-digit) → gateway-compatible format.
    # _order_symbol in native_engine.py auto-routes 6-digit codes by leading
    # digit (6→SH, 0/3→SZ), so plain 6-digit is accepted as-is.
    if "code" not in positions.columns:
        if "_code6" in positions.columns:
            positions["code"] = positions["_code6"].astype(str).str.zfill(6)
        elif "symbol" in positions.columns:
            positions["code"] = positions["symbol"]
        else:
            logger.error(
                "[tree-orders] positions has no code/_code6/symbol column: %s",
                list(positions.columns),
            )
            return pd.DataFrame()

    # Normalize position column: tree model may output `weight` / `target_weight`
    # instead of `position`. positions_to_orders expects `position`.
    if "position" not in positions.columns:
        for alt in ("weight", "target_weight", "target_position", "optimized_weight"):
            if alt in positions.columns:
                positions["position"] = pd.to_numeric(positions[alt], errors="coerce").fillna(0.0)
                logger.info("[tree-orders] using '%s' as position column", alt)
                break
    if "position" not in positions.columns:
        logger.error(
            "[tree-orders] positions has no position/weight/target_weight column: %s",
            list(positions.columns),
        )
        return pd.DataFrame()

    orders = positions_to_orders(
        positions,
        portfolio_context,
        daily_basic_df,
        price_type=os.environ.get("TREE_MODEL_PRICE_TYPE", "latest"),
        strategy=STRATEGY_NAME,
        note=f"{STRATEGY_NAME}_{date_str}_{end_time}",
    )

    logger.info(
        "[tree-orders] date=%s end_time=%s positions=%d orders=%d",
        date_str, end_time, len(positions), len(orders),
    )
    return orders
