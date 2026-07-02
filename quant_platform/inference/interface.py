# -*- coding: utf-8 -*-
"""Shared inference interface helpers."""

from __future__ import annotations

import inspect
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class PortfolioContext:
    """Current account snapshot passed to trader inference modules.

    The engine owns data acquisition. Trader modules should treat these frames
    as read-only inputs and return target positions/orders.
    """

    account_id: str = ""
    broker: str = "atx"
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
    trading_day: Optional[str] = None,
    idx_cons_cache=None,
) -> pd.DataFrame:
    """Compute per-stock index composition data passed to inference.

    Output columns (wide format):
        ID_QI, SECURITY_ID (merged from daily_basic),
        in_idx_{INDEX_CODE} (bool), weight_{INDEX_CODE} (float)

    Two data sources:
        1. (Preferred) idx_cons_cache.get_by_date(trading_day) - OSS first,
           MySQL fallback, supports any historical trading day
        2. (Legacy) extra_fields["idx_cons_df"] - pre-loaded df

    Expected idx_cons_df columns: INDEX_ID, INDEX_CODE, ID_QI, ...
    Optional weight column (from OSS composition.parquet).

    Args:
        daily_basic_df:  当日基础数据(用于补全 SECURITY_ID)
        extra_fields:    额外字段(含 idx_cons_df 或保留兼容旧接口)
        trading_day:     交易日 YYYYMMDD(用于按日切片缓存)
        idx_cons_cache:  IdxConsCache 实例(优先使用,按交易日查询)
    """
    # 优先从 cache 按 trading_day 取(支持历史回测)
    idx_cons_df: Optional[pd.DataFrame] = None
    if idx_cons_cache is not None and trading_day:
        idx_cons_df = idx_cons_cache.get_by_date(trading_day)
    if idx_cons_df is None or idx_cons_df.empty:
        # Fallback: 旧接口 extra_fields["idx_cons_df"]
        idx_cons_df = (extra_fields or {}).get("idx_cons_df")
    if idx_cons_df is None or idx_cons_df.empty:
        return pd.DataFrame()

    # ID_QI 标准化
    idx_cons_df = idx_cons_df.copy()
    if "ID_QI" in idx_cons_df.columns:
        idx_cons_df["ID_QI"] = idx_cons_df["ID_QI"].astype(str).str.zfill(6)

    # 决定用什么列做 pivot 的 columns:
    #   - 新数据(含 INDEX_CODE): 用 TICKER 代码("000300")
    #   - 旧数据(只有 INDEX_ID): 用 SECURITY_ID(1782)
    if "INDEX_CODE" in idx_cons_df.columns and idx_cons_df["INDEX_CODE"].notna().any():
        col_key = "INDEX_CODE"
    else:
        col_key = "INDEX_ID"

    has_weight = "weight" in idx_cons_df.columns and idx_cons_df["weight"].notna().any()

    # 长表 → 宽表:每个指数一列 in_idx_{code},可选 weight_{code}
    # 用 crosstab 做 membership 矩阵
    membership = pd.crosstab(
        idx_cons_df["ID_QI"], idx_cons_df[col_key]
    ).astype(bool)
    membership.columns = [f"in_idx_{c}" for c in membership.columns]
    wide = membership.reset_index()

    if has_weight:
        # weight 列单独 pivot(同一 ID_QI 在不同 INDEX_CODE 有不同权重)
        if "weight" in idx_cons_df.columns:
            weight_wide = idx_cons_df.pivot_table(
                index="ID_QI", columns=col_key,
                values="weight", fill_value=0.0,
            )
            weight_wide.columns = [f"weight_{c}" for c in weight_wide.columns]
            weight_wide = weight_wide.reset_index()
            wide = wide.merge(weight_wide, on="ID_QI", how="left")

    # Merge SECURITY_ID from daily_basic if available
    if daily_basic_df is not None and not daily_basic_df.empty:
        if "ID_QI" in daily_basic_df.columns and "SECURITY_ID" in daily_basic_df.columns:
            id_map = daily_basic_df[["ID_QI", "SECURITY_ID"]].drop_duplicates("ID_QI")
            id_map["ID_QI"] = id_map["ID_QI"].astype(str).str.zfill(6)
            wide = wide.merge(id_map, on="ID_QI", how="left")

    return wide


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


def positions_to_orders(
    positions_df: pd.DataFrame,
    portfolio_context: Optional["PortfolioContext"],
    daily_basic_df: Optional[pd.DataFrame] = None,
    *,
    price_type: str = "latest",
    strategy: str = "open_position",
    note: str = "",
    total_capital_override: Optional[float] = None,
    delta_mode: str = "auto",
) -> pd.DataFrame:
    """Convert position-fraction output to order rows ready for order-gateway.

    The inference module outputs a `position` column as a TARGET weight/fraction
    (e.g. 0.02 = 2% of capital). This helper turns target weights into order
    rows. By default (delta_mode="auto") it computes deltas against current
    holdings so rebalancing doesn't double-buy existing positions:

        current_weight[code] = (held_volume * price) / total_capital
        delta_weight = target_weight - current_weight
        volume = floor(|delta_weight| * total_capital / price / 100) * 100
        side = buy if delta_weight > 0 else sell

    delta_mode:
        "auto"     — delta if portfolio_context.positions non-empty, else absolute
        "delta"    — always delta (requires positions; missing → treat as 0)
        "absolute" — treat position column as the desired trade itself
                     (current behavior; for fresh-deploy sim with no positions)

    volume formula (target → shares):

        主板/创业板/北交所: floor(total_capital * |weight| / price / 100) * 100
        科创板(688/689): buy: raw_vol (≥200 才下单),sell: 零股可全卖

    Price resolution order:
        1. portfolio_context.meta['latest_prices']  (engine realtime tick,
           injected by NativeEngine._snapshot_latest_prices at fork time)
        2. portfolio_context.positions.last_price (broker)
        3. (opt-in) daily_basic_df.close — only when env
           OPEN_POSITION_FALLBACK_CLOSE=1; for pre-open dry-run only
        4. rows with no price dropped

    Total capital resolution order:
        1. total_capital_override (caller-supplied, takes full responsibility)
        2. portfolio_context.account.total_asset (real trading, must be > 0)
        3. Sim mode ONLY: requires OPEN_POSITION_SIM_MODE=1 AND
           OPEN_POSITION_TOTAL_CAPITAL=<explicit amount>. No silent default.

    Args:
        positions_df: Output of inference; must contain `code` and `position`.
        portfolio_context: Account/positions snapshot from order-gateway.
        daily_basic_df: Unused (kept for signature compatibility).
        price_type, strategy, note: Order row metadata.
        total_capital_override: Force a specific capital amount.

    Returns:
        DataFrame with columns [code, side, volume, price_type, strategy, note]
        consumed by `_build_order_gateway_orders` in native_engine.py.
    """
    import math
    import os

    if positions_df is None or positions_df.empty:
        return pd.DataFrame()
    if "position" not in positions_df.columns:
        raise ValueError(
            "positions_to_orders: positions_df missing 'position' column; "
            f"got {list(positions_df.columns)}"
        )

    df = positions_df.copy()
    if "code" not in df.columns:
        df["code"] = df.get("symbol", "")

    # ── total capital ───────────────────────────────────────────────
    # Resolve total capital. NO silent fallback — ambiguous capital is dangerous.
    #
    # Real trading: portfolio_context.account.total_asset must be > 0.
    # Sim mode:     must explicitly set OPEN_POSITION_SIM_MODE=1 AND
    #               OPEN_POSITION_TOTAL_CAPITAL=<amount> (no implicit default).
    #
    # total_capital_override always wins (caller takes responsibility).
    total_capital = 0.0
    if total_capital_override is not None and total_capital_override > 0:
        total_capital = float(total_capital_override)
    elif portfolio_context is not None:
        acct = getattr(portfolio_context, "account", None)
        if acct is not None and not acct.empty:
            for col in ("total_asset", "available_cash", "market_value"):
                if col in acct.columns:
                    val = pd.to_numeric(acct[col].iloc[0], errors="coerce")
                    if math.isfinite(val) and val > 0:
                        total_capital = float(val)
                        break

    if total_capital <= 0:
        sim_mode = os.environ.get("OPEN_POSITION_SIM_MODE", "0").strip() in ("1", "true", "True", "yes")
        sim_cap = os.environ.get("OPEN_POSITION_TOTAL_CAPITAL", "").strip()
        if sim_mode and sim_cap:
            try:
                total_capital = float(sim_cap)
            except ValueError:
                total_capital = 0.0
        if total_capital <= 0:
            raise ValueError(
                "positions_to_orders: cannot resolve total_capital. "
                "Real trading: ensure portfolio_context.account.total_asset > 0 "
                "(check ORDER_GATEWAY_URL / /v1/balance). "
                "Sim mode: set OPEN_POSITION_SIM_MODE=1 AND "
                "OPEN_POSITION_TOTAL_CAPITAL=<amount> explicitly."
            )
        logger.warning(
            "positions_to_orders: SIM MODE active, total_capital=%.0f from env",
            total_capital)

    # ── price per stock ─────────────────────────────────────────────
    # Code format mismatch risk: engine _snapshot_latest_prices keys by full
    # format ("000001.XSHE"), tree_model_orders normalizes to 6-digit
    # ("000001"), broker positions may use either. Normalize both sides to
    # 6-digit for lookup so price maps always hit regardless of source format.
    def _norm_code6(c) -> str:
        s = str(c).strip().upper()
        # strip .SH / .SZ / .XSHG / .XSHE suffix
        for suf in (".XSHE", ".XSHG", ".SH", ".SZ"):
            if s.endswith(suf):
                s = s[: -len(suf)]
                break
        return s.zfill(6)

    prices = pd.Series(0.0, index=df.index)
    df["_code6"] = df["code"].astype(str).map(_norm_code6)
    code6 = df["_code6"]

    # Source 1: engine realtime tick (preferred at 9:30)
    if portfolio_context is not None:
        meta_prices = (portfolio_context.meta or {}).get("latest_prices") or {}
        if meta_prices:
            price_map_6 = {_norm_code6(k): float(v) for k, v in meta_prices.items()}
            prices = code6.map(price_map_6).fillna(0.0)

    # Source 2: broker positions last_price
    if (prices <= 0).any() and portfolio_context is not None:
        pos = getattr(portfolio_context, "positions", None)
        if pos is not None and not pos.empty and "last_price" in pos.columns \
                and "code" in pos.columns:
            pos_c6 = pos["code"].astype(str).map(_norm_code6)
            broker_map = pd.Series(
                pos["last_price"].astype(float).values, index=pos_c6
            ).dropna()
            prices = prices.where(prices > 0, code6.map(broker_map).fillna(0.0))
    # Source 3 (fallback, opt-in): daily_basic_df yesterday close.
    # Enabled via OPEN_POSITION_FALLBACK_CLOSE=1 — for dry-run testing before
    # market open. Production should NOT enable (9:30 tick must be used).
    if (prices <= 0).any() and daily_basic_df is not None \
            and os.environ.get("OPEN_POSITION_FALLBACK_CLOSE", "0") == "1":
        db = daily_basic_df
        code_col = next((c for c in ("ID_QI", "code", "symbol", "SECURITY_CODE")
                         if c in db.columns), None)
        close_col = next((c for c in ("close", "CLOSE", "pre_close", "PRE_CLOSE")
                          if c in db.columns), None)
        if code_col is not None and close_col is not None:
            db_c6 = db[code_col].astype(str).map(_norm_code6)
            close_map = pd.Series(
                pd.to_numeric(db[close_col], errors="coerce").values,
                index=db_c6,
            ).dropna()
            close_map = close_map[close_map > 0]
            filled = code6.map(close_map).fillna(0.0)
            n_filled = ((prices <= 0) & (filled > 0)).sum()
            if n_filled > 0:
                logger.warning(
                    "positions_to_orders: FALLBACK close used for %d rows "
                    "(OPEN_POSITION_FALLBACK_CLOSE=1, not for production)",
                    n_filled,
                )
                prices = prices.where(prices > 0, filled)
    df["_price"] = prices.astype(float)
    dropped = (df["_price"] <= 0).sum()
    if dropped > 0:
        logger.warning(
            "positions_to_orders: dropped %d rows with no realtime price "
            "(check portfolio_context.meta['latest_prices'])", dropped)
    df = df[df["_price"] > 0]
    if df.empty:
        return pd.DataFrame()

    # ── target vs delta weight ─────────────────────────────────────
    # Default: delta against current holdings so rebalancing doesn't
    # double-buy existing positions. Inference `position` is the TARGET
    # weight; current_weight is computed from broker positions; trade the
    # delta.
    df["_position_num"] = pd.to_numeric(df["position"], errors="coerce").fillna(0.0)
    df["_weight"] = df["_position_num"]  # target weight, signed
    df["_current_weight"] = 0.0  # default; overwritten if delta resolves

    positions_df_pos = (
        getattr(portfolio_context, "positions", None)
        if portfolio_context is not None else None
    )
    has_positions = (
        positions_df_pos is not None and not positions_df_pos.empty
        and "code" in positions_df_pos.columns
    )

    if delta_mode == "auto":
        use_delta = has_positions
    elif delta_mode == "delta":
        use_delta = True
    elif delta_mode == "absolute":
        use_delta = False
    else:
        raise ValueError(
            f"positions_to_orders: delta_mode must be 'auto'|'delta'|'absolute', "
            f"got {delta_mode!r}"
        )

    # Build {code6: current_weight}. current_weight uses the same price
    # source (meta latest_prices preferred, broker last_price fallback)
    # so the comparison is consistent with the order price.
    if use_delta and has_positions:
        pos = positions_df_pos
        vol_col = next(
            (c for c in (
                "current_volume", "volume", "total_qty", "qty", "quantity", "position_volume"
            ) if c in pos.columns),
            None,
        )
        if vol_col is None:
            logger.warning(
                "positions_to_orders: delta_mode needs positions volume "
                "column (current_volume/volume/total_qty/qty/quantity/position_volume); "
                "falling back to absolute"
            )
        else:
            pos_c6 = pos["code"].astype(str).map(_norm_code6)
            pos_vol = pd.to_numeric(pos[vol_col], errors="coerce").fillna(0.0)
            # Price for current holdings: prefer broker last_price
            pos_price = pd.Series(0.0, index=pos.index)
            if "last_price" in pos.columns:
                pos_price = pd.to_numeric(
                    pos["last_price"], errors="coerce"
                ).fillna(0.0)
            # Override with engine realtime tick where available (consistent
            # with the price used for sizing the new order)
            if portfolio_context is not None:
                meta_prices = (
                    (portfolio_context.meta or {}).get("latest_prices") or {}
                )
                if meta_prices:
                    price_map_6 = {
                        _norm_code6(k): float(v)
                        for k, v in meta_prices.items()
                    }
                    tick_price = pos_c6.map(price_map_6).fillna(0.0)
                    pos_price = pos_price.where(pos_price > 0, tick_price)
            held_value = (pos_vol * pos_price).where(pos_price > 0, 0.0)
            current_weight = (
                pd.Series(held_value.values, index=pos_c6) / total_capital
            )
            # Aggregate duplicate codes (multiple rows per code possible)
            current_weight = current_weight.groupby(level=0).sum()
            df["_current_weight"] = (
                df["_code6"].map(current_weight).fillna(0.0)
            )

    # delta_weight: + → buy, − → sell
    df["_delta_weight"] = df["_weight"] - df["_current_weight"]

    # ── target volume ───────────────────────────────────────────────
    # Size off |delta_weight|; round_down to lot size.
    # A-share lot rules:
    #   主板/创业板/北交所(0xxxxx / 3xxxxx / 6xxxxx / 8xxxxx / 4xxxxx): 100 股
    #   科创板(688xxx / 689xxx,SH): 200 股起买,之后 1 股递增
    # Buy: 主板 round_down 到 100;科创板 ≥200 才下单,否则丢
    # Sell: 主板 round_down 到 100;科创板可零股全卖(T+1 解锁后)
    def _lot_size(code6: str) -> int:
        c = str(code6).zfill(6)
        return 200 if c.startswith(("688", "689")) else 100

    df["target_value"] = total_capital * df["_delta_weight"].abs()
    df["_raw_vol"] = df["target_value"] / df["_price"]
    lot_sizes = df["_code6"].map(_lot_size)
    is_kcb = lot_sizes == 200
    is_buy = df["_delta_weight"] >= 0

    # 主板/创业板: round_down 到 100
    df["volume"] = (df["_raw_vol"] // 100).astype(int) * 100
    # 科创板买入: 不取整 100,但 < 200 的丢
    kcb_buy_mask = is_kcb & is_buy
    df.loc[kcb_buy_mask, "volume"] = df.loc[kcb_buy_mask, "_raw_vol"].astype(int)
    df.loc[kcb_buy_mask & (df["volume"] < 200), "volume"] = 0
    # 科创板卖出: 零股可全卖(T+1 解锁份额)
    kcb_sell_mask = is_kcb & ~is_buy
    df.loc[kcb_sell_mask, "volume"] = df.loc[kcb_sell_mask, "_raw_vol"].astype(int)

    # Cap sell volume at held volume (delta_mode only). A-share T+1 + no
    # shorting means a sell exceeding holdings is invalid; clamp to held
    # qty (rounded down to 100) and drop the row if it rounds to 0.
    if use_delta and has_positions:
        held_shares = (
            df["_current_weight"] * total_capital / df["_price"]
        ).fillna(0.0)
        held_lot = (held_shares // 100).astype(int) * 100
        # 科创板持仓可卖零股
        held_lot = held_lot.where(~is_kcb, held_shares.astype(int))
        sell_mask = df["_delta_weight"] < 0
        df.loc[sell_mask, "volume"] = df.loc[sell_mask, "volume"].clip(
            upper=held_lot.loc[sell_mask]
        )

    df = df[df["volume"] > 0]
    if df.empty:
        return pd.DataFrame()

    # side from sign(delta_weight)
    df["side"] = df["_delta_weight"].apply(
        lambda w: "buy" if w > 0 else ("sell" if w < 0 else "")
    )
    df = df[df["side"] != ""]

    out = pd.DataFrame({
        "code": df["code"].astype(str),
        "side": df["side"],
        "volume": df["volume"].astype(int),
        "price_type": price_type,
        "strategy": strategy,
        "note": note,
    })
    return out.reset_index(drop=True)

