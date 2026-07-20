#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交易股票池过滤器：在 Pod 启动时调用一次，作为最终选股边界。

约定接口（INFERENCE_INTERFACE.md 第 393-444 行）：

    def trading_universe(daily_basic_df, idx_cons_df) -> list[str]

引擎在 Pod 启动时调用，结果缓存并传给每轮 inference() 调用，作为最终的
股票池边界。返回 None 表示用默认 universe。

本模块的职责：
    过滤 ST/*ST/退市股票，使它们既不参与因子计算之外的"选股/调仓"链路，
    也不进入推理候选池。判定规则与
    `quant_platform.live_engine.native_engine._filter_st_codes` 完全一致：

        当天 daily_basic 的 SEC_SHORT_NAME 含 "ST"（含 *ST）或 "退"。

    必须用当天数据（_daily_basic_df），而非历史窗口 _market_df——
    摘帽的股票当天 SEC_SHORT_NAME 已不含 ST，但历史快照里还有，
    用历史数据会误排摘帽股。

为何需要这一层：
    native_engine._filter_st_codes 只作用于分钟因子计算，不作用于 inference。
    导致 ST 票虽然不参与因子计算，但仍能进入推理候选池被优化器选为目标持仓
    （生产事故：20260720 open_position 148 个目标里有 3 只 ST，实际下了 2 单）。
    trading_universe 是约定接口指定的"最终过滤边界"，在这里过滤是最干净的
    做法——inference 拿到的 trading_universe_df 已经不含 ST。
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


# ST/退市 关键字。大写匹配，覆盖 "ST"、"*ST"、"S*ST"、"退市"、"PT退" 等。
_ST_KEYWORDS = ("ST", "退")


def _normalize_code6(value) -> Optional[str]:
    """提取 6 位股票代码，前导 0 补齐。返回 None 表示无效。"""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    # 只取 "." 之前的纯数字部分（兼容 "000001.SZ" / "000001.XSHE"）
    digits = "".join(ch for ch in text.split(".")[0] if ch.isdigit())
    if not digits:
        return None
    return digits[-6:].zfill(6)


def _pick_name_column(df: pd.DataFrame) -> Optional[str]:
    """返回 DataFrame 里第一个可用的股票名称列。"""
    for c in ("SEC_SHORT_NAME", "SEC_NAME", "name"):
        if c in df.columns:
            return c
    return None


def _pick_id_column(df: pd.DataFrame) -> Optional[str]:
    """返回 DataFrame 里第一个可用的股票代码列。"""
    if "_ID_QI_PAD" in df.columns:
        return "_ID_QI_PAD"
    if "ID_QI" in df.columns:
        return "ID_QI"
    return None


def _current_daily_basic_snapshot(daily_basic_df: pd.DataFrame) -> pd.DataFrame:
    """Return the latest trade_date snapshot from a possibly multi-day frame."""
    if "trade_date" not in daily_basic_df.columns:
        return daily_basic_df
    trade_dates = daily_basic_df["trade_date"].dropna().astype(str)
    if trade_dates.empty:
        return daily_basic_df
    latest = trade_dates.max()
    return daily_basic_df.loc[daily_basic_df["trade_date"].astype(str) == latest]


def _build_st_code6_set(daily_basic_df: pd.DataFrame) -> set[str]:
    """从当天 daily_basic 构建 ST/退市 6 位代码集合。"""
    if daily_basic_df is None or daily_basic_df.empty:
        return set()

    current_df = _current_daily_basic_snapshot(daily_basic_df)
    if current_df.empty:
        return set()

    name_col = _pick_name_column(current_df)
    id_col = _pick_id_column(current_df)
    if name_col is None or id_col is None:
        return set()

    names = current_df[name_col].astype(str).str.upper()
    mask = names.str.contains("ST", na=False) | names.str.contains("退", na=False)
    if not mask.any():
        return set()

    codes = (
        current_df.loc[mask, id_col]
        .astype(str)
        .str.split(".")
        .str[0]
        .str.zfill(6)
    )
    return set(codes)


def trading_universe(
    daily_basic_df: pd.DataFrame,
    idx_cons_df: Optional[pd.DataFrame] = None,
) -> Optional[list[str]]:
    """过滤 ST/退市后的交易股票池。

    参数（约定接口）:
        daily_basic_df: 引擎已加载的 daily_basic DataFrame，至少含
                        ID_QI/_ID_QI_PAD 和 SEC_SHORT_NAME 列。
        idx_cons_df:    指数成分股 DataFrame（本过滤不需要，但接口约定要求接收）。

    返回:
        list[str]: 过滤 ST/退市后的股票代码列表，格式 "000001.SZ"。
        None:     daily_basic 不可用 / 无 ST 时返回 None，让引擎用默认 universe
                  （配合 inference 内部的兼容性过滤，仍能保证 ST 不被选到）。
    """
    if daily_basic_df is None or daily_basic_df.empty:
        print("[trading-universe] daily_basic 为空，跳过 ST 过滤（返回 None 用默认池）")
        return None

    id_col = _pick_id_column(daily_basic_df)
    if id_col is None:
        print("[trading-universe] daily_basic 无 ID_QI 列，跳过 ST 过滤")
        return None

    current_df = _current_daily_basic_snapshot(daily_basic_df)

    # 全量代码（先归一化到 6 位）。只用当前快照，避免多日窗口里历史代码
    # 把当前 ST/退市票重新带回 universe。
    all_codes = (
        current_df[id_col]
        .dropna()
        .astype(str)
        .str.split(".")
        .str[0]
        .str.zfill(6)
        .unique()
        .tolist()
    )

    st_set = _build_st_code6_set(daily_basic_df)
    if not st_set:
        print(f"[trading-universe] daily_basic 无 ST/退市（{len(all_codes)} 只），返回 None 用默认池")
        return None

    filtered = [c for c in all_codes if c not in st_set]
    print(
        f"[trading-universe] ST/退市过滤：{len(all_codes)} → {len(filtered)} "
        f"(排除 {len(st_set)} 只 ST/退市)"
    )

    # 转成 "000001.SZ" / "600000.SH" 格式，与 INFERENCE_INTERFACE.md 示例一致。
    # 0/3 开头 → SZ，6 开头 → SH，8/4 开头（北交所）→ BJ。
    def _to_symbol(code6: str) -> str:
        if code6.startswith("6"):
            return f"{code6}.SH"
        if code6.startswith(("4", "8")):
            return f"{code6}.BJ"
        return f"{code6}.SZ"

    return [_to_symbol(c) for c in filtered]
