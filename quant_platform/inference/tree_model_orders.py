#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Engine adapter for tree_model_inference.

The underlying ``tree_model_inference.inference`` returns a *positions* frame
(``_code6``, ``position`` ...). The live engine's order-gateway path expects
*order rows* (``code``, ``side``, ``volume``, ``price_type`` ...).

This module exposes three entry points:

- ``inference_targets`` — Stage 1: 树模型推理 → target positions (weights).
  Only depends on T-1 daily_basic / factors / static index composition /
  broker T-1 holdings. **No realtime tick price dependency.**
  Safe to run at pod startup (8:50) before market open.

- ``targets_to_orders`` — Stage 2: target weights → order rows.
  Uses realtime tick price from ``portfolio_context.meta['latest_prices']``
  to size volumes. Must run at 9:30 (or whenever orders are dispatched).

- ``inference`` — Combined Stage 1 + Stage 2 in one call. Kept for backward
  compatibility; used by the fallback path when precompute cache misses.

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


def _prepare_daily_basic_for_tree(
    daily_basic_df: Optional[pd.DataFrame],
    stage: str,
) -> Optional[pd.DataFrame]:
    """Drop live-only string columns before feeding the compiled tree model."""
    if daily_basic_df is None or daily_basic_df.empty:
        return daily_basic_df

    drop_cols = [c for c in ("SEC_SHORT_NAME", "SEC_FULL_NAME")
                 if c in daily_basic_df.columns]
    if not drop_cols:
        return daily_basic_df

    logger.info("[tree-orders] dropped %s from daily_basic for %s .so compat",
                drop_cols, stage)
    return daily_basic_df.drop(columns=drop_cols)


def _normalize_tree_positions(
    positions: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Apply the daily_basic drop-cols + code/position column normalization.

    Extracted from the original ``inference`` so both the combined path and
    the split targets path share identical normalization.

    - tree model .so 是按 96 列 schema 编译的,实盘 daily_basic 多了
      SEC_SHORT_NAME / SEC_FULL_NAME 两列会让内部列选择错位 →
      ZeroDivisionError / 列读取错乱。在喂给 .so 前丢弃这两列。
    - Normalize code column: _code6 (6-digit) → gateway-compatible format.
      _order_symbol in native_engine.py auto-routes 6-digit codes by leading
      digit (6→SH, 0/3→SZ), so plain 6-digit is accepted as-is.
    - Normalize position column: tree model may output `weight` / `target_weight`
      instead of `position`. positions_to_orders expects `position`.
    """
    positions = positions.copy()

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

    return positions


def inference_targets(
    date_str: str,
    end_time: str,
    prev_day_factors_df: Optional[pd.DataFrame],
    intraday_factors_df: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame] = None,
    trading_universe_df: Optional[pd.DataFrame] = None,
    index_composition_df: Optional[pd.DataFrame] = None,
    portfolio_context=None,
) -> pd.DataFrame:
    """Stage 1: 树模型推理 → target positions (weights).

    Heavy step (loads .so, runs full-market prediction). Does NOT depend on
    realtime tick prices — safe to run at pod startup before market open.

    Returns a normalized positions DataFrame with `code` and `position` columns
    suitable for ``targets_to_orders``. Returns empty DataFrame on failure.
    """
    daily_basic_df = _prepare_daily_basic_for_tree(daily_basic_df, "stage 1")

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
        logger.warning("[tree-orders] no positions produced at stage 1 (date=%s end_time=%s)",
                       date_str, end_time)
        return pd.DataFrame()

    return _normalize_tree_positions(positions, daily_basic_df)


def targets_to_orders(
    positions: pd.DataFrame,
    date_str: str,
    end_time: str,
    daily_basic_df: Optional[pd.DataFrame] = None,
    portfolio_context=None,
    diagnostics: Optional[dict] = None,
) -> pd.DataFrame:
    """Stage 2: target weights → order rows.

    Light step (pure arithmetic). Reads realtime tick prices from
    ``portfolio_context.meta['latest_prices']`` to size volumes.

    Args:
        positions: Output of ``inference_targets`` — must contain `code` and
            `position` columns.
        date_str, end_time: Used for the order ``note`` field.
        daily_basic_df: Passed to ``positions_to_orders`` (unused internally
            but kept for signature symmetry).
        portfolio_context: Account/positions snapshot; must contain
            ``meta['latest_prices']`` populated by the engine caller.

    Returns:
        DataFrame with columns
        ``[code, side, volume, price_type, price, strategy, note]``.
    """
    if positions is None or positions.empty:
        return pd.DataFrame()

    daily_basic_df = _prepare_daily_basic_for_tree(daily_basic_df, "stage 2")

    orders = positions_to_orders(
        positions,
        portfolio_context,
        daily_basic_df,
        price_type=os.environ.get("TREE_MODEL_PRICE_TYPE", "latest"),
        strategy=STRATEGY_NAME,
        note=f"{STRATEGY_NAME}_{date_str}_{end_time}",
        diagnostics=diagnostics,
    )

    logger.info(
        "[tree-orders] stage 2 date=%s end_time=%s positions=%d orders=%d",
        date_str, end_time, len(positions), len(orders),
    )
    return orders


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
    """Run tree model inference and convert positions to order rows.

    Combined Stage 1 + Stage 2. Kept for backward compatibility; the engine
    fallback path uses this when the precomputed cache misses.
    """
    positions = inference_targets(
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

    orders = targets_to_orders(
        positions,
        date_str,
        end_time,
        daily_basic_df=daily_basic_df,
        portfolio_context=portfolio_context,
    )

    logger.info(
        "[tree-orders] combined date=%s end_time=%s positions=%d orders=%d",
        date_str, end_time, len(positions), len(orders),
    )
    return orders
