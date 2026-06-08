# -*- coding: utf-8 -*-
"""
统一 StockData 构造层

回测引擎和实盘引擎共用 build_stock_data() 构造 StockData，
确保两边传入策略的数据格式完全一致。

职责：
  1. 按 factor_info 置空不需要的数据（need_l2_order=False → 空 DataFrame）
  2. 校验 Time 列日期 == trading_day（捕获 _base_ns=0 等 bug）
  3. market / daily_basic 按股过滤
  4. 构造 StockData 并返回

第 1 层（数据源适配）在调用方完成：
  - 回测: OSS parquet → _restore_oss_precision() → 标准 DataFrame
  - 实盘: SHM mmap → _build_df_from_native() → 标准 DataFrame
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from .base import StockData, StockState

logger = logging.getLogger(__name__)

_TIME_COL = "Time"


def build_stock_data(
    code: str,
    date: str,
    end_time: str,
    *,
    tick_df: Optional[pd.DataFrame] = None,
    deal_df: Optional[pd.DataFrame] = None,
    order_df: Optional[pd.DataFrame] = None,
    market_df: Optional[pd.DataFrame] = None,
    daily_basic_df: Optional[pd.DataFrame] = None,
    factor_info: Optional[Dict] = None,
    tick_hist: Optional[List[pd.DataFrame]] = None,
    deal_hist: Optional[List[pd.DataFrame]] = None,
    order_hist: Optional[List[pd.DataFrame]] = None,
    state: Optional[StockState] = None,
    validate: bool = True,
) -> StockData:
    """统一构造 StockData，回测和实盘共用。

    Args:
        code: 股票代码，如 "000001.XSHE"
        date: 交易日 YYYYMMDD
        end_time: 时间切片，如 "093500"
        tick_df: L1 tick DataFrame（第 1 层已转换为标准格式）
        deal_df: L2 deal DataFrame
        order_df: L2 order DataFrame
        market_df: 全市场 daily_basic（多日），builder 内部按股过滤
        daily_basic_df: 全市场 daily_basic，builder 内部按股过滤；
                        若不传则 fallback 到 market_df
        factor_info: 策略因子信息，控制 need_l1_tick / need_l2_deal / need_l2_order
        tick_hist: 历史 tick 数据列表（回测 lookback）
        deal_hist: 历史 deal 数据列表
        order_hist: 历史 order 数据列表
        state: 实盘聚合状态（回测时为 None）
        validate: 是否做运行时校验
    """
    fi = factor_info or {}

    # ── 1. 按 factor_info 置空不需要的数据 ──
    tick = _maybe_empty(tick_df, fi.get("need_l1_tick", True))
    deal = _maybe_empty(deal_df, fi.get("need_l2_deal", True))
    order = _maybe_empty(order_df, fi.get("need_l2_order", False))
    tick_hist = _maybe_empty_hist(tick_hist, fi.get("need_l1_tick", True))
    deal_hist = _maybe_empty_hist(deal_hist, fi.get("need_l2_deal", True))
    order_hist = _maybe_empty_hist(order_hist, fi.get("need_l2_order", False))

    # ── 2. market / daily_basic 按股过滤 ──
    if market_df is not None and not market_df.empty:
        market = _filter_daily_for_code(market_df, code)
    else:
        market = pd.DataFrame()

    if daily_basic_df is not None and not daily_basic_df.empty:
        daily_basic = _filter_daily_for_code(daily_basic_df, code)
    else:
        daily_basic = market

    # ── 3. 运行时校验（可选，开销极小）──
    if validate:
        _validate_time_date(tick, date, code, "tick")
        _validate_time_date(deal, date, code, "deal")
        _validate_time_date(order, date, code, "order")
        if not fi.get("need_l2_order", False) and not order.empty:
            logger.warning("[builder] need_l2_order=False but order not empty: %s", code)

    # ── 4. 构造 StockData ──
    return StockData(
        code=code,
        date=date,
        end_time=end_time,
        l1_tick=tick,
        l2_deal=deal,
        l2_order=order,
        market=market,
        daily_basic=daily_basic,
        l2_order_hist=order_hist,
        l2_deal_hist=deal_hist,
        l1_tick_hist=tick_hist,
        state=state,
    )


# ── 内部工具 ──────────────────────────────────────────

def _maybe_empty(df: Optional[pd.DataFrame], needed: bool) -> pd.DataFrame:
    """factor_info 标记不需要时返回空 DataFrame。"""
    if not needed:
        return pd.DataFrame()
    return df if df is not None else pd.DataFrame()


def _maybe_empty_hist(
    dfs: Optional[List[pd.DataFrame]], needed: bool
) -> List[pd.DataFrame]:
    """factor_info 标记不需要时返回空历史窗口。"""
    if not needed:
        return []
    return dfs or []


def _filter_daily_for_code(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """从多日 daily_basic 中过滤出单只股票的数据。

    支持 _ID_QI_PAD（预计算索引列）、ID_QI、TICKER_SYMBOL 等列名。
    """
    if df.empty:
        return df
    key = code.split(".")[0].zfill(6)

    if "_ID_QI_PAD" in df.columns:
        result = df[df["_ID_QI_PAD"] == key].reset_index(drop=True)
        return result.drop(columns=["_ID_QI_PAD"], errors="ignore")

    for col in ("ID_QI", "TICKER_SYMBOL", "code", "Code", "stock_code"):
        if col in df.columns:
            norm = df[col].astype(str).str.split(".").str[0].str.zfill(6)
            return df[norm == key].reset_index(drop=True)
    return df


def _validate_time_date(
    df: pd.DataFrame, trading_day: str, code: str, kind: str
) -> None:
    """校验 Time 列的日期部分，捕获 _base_ns=0 等问题。

    校验失败只打日志，不抛异常，不影响主流程。
    """
    if df.empty or _TIME_COL not in df.columns:
        return
    try:
        if not pd.api.types.is_datetime64_any_dtype(df[_TIME_COL]):
            return
        sample = df[_TIME_COL].iloc[0]
        date_str = str(sample)[:10].replace("-", "")
        if date_str.startswith("1970"):
            logger.error(
                "[builder] %s %s Time date is 1970 — base_ns likely 0! "
                "trading_day=%s",
                code, kind, trading_day,
            )
        elif date_str != trading_day:
            logger.warning(
                "[builder] %s %s Time date mismatch: expected=%s got=%s",
                code, kind, trading_day, date_str,
            )
    except Exception:
        pass
