# -*- coding: utf-8 -*-
"""Robot model inference adapter for the live engine.

The original robot script is a standalone CLI.  This module exposes the
``inference`` callable expected by ``quant_platform.live_engine`` and returns
order rows directly.
"""

from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Callable, Optional

import joblib
import pandas as pd


_ARTIFACT = None


def _model_path() -> Path:
    configured = os.environ.get("ROBOT_MODEL_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).parent / "artifacts" / "robot_model.joblib"


def _normalize_date(value) -> Optional[str]:
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[:8] if len(digits) >= 8 else None


def _normalize_code6(value) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text.split(".")[0])
    if not digits:
        return None
    return digits[-6:].zfill(6)


def _order_code(code6: str) -> str:
    code6 = str(code6).zfill(6)
    return ("SH" if code6.startswith("6") else "SZ") + code6


def _load_feature_transform_callable(value, fmt: Optional[str]) -> Optional[Callable]:
    if value is None:
        return None
    if callable(value):
        return value
    if isinstance(value, (bytes, bytearray)):
        if fmt not in ("cloudpickle", "", None):
            raise RuntimeError(f"Unsupported feature transform format: {fmt}")
        import cloudpickle

        return cloudpickle.loads(value)
    if isinstance(value, str):
        raise RuntimeError(
            "model artifact contains a legacy string feature_transform; "
            "please regenerate the model with an embedded callable"
        )
    raise RuntimeError(f"Unsupported feature_transform type: {type(value).__name__}")


def _load_model_artifact():
    artifact = joblib.load(_model_path())
    if not isinstance(artifact, dict) or "model" not in artifact:
        raise RuntimeError("robot model artifact must be a dict with model/features/medians")

    features = list(artifact.get("features", []))
    if not features:
        raise RuntimeError("robot model artifact has no feature list")

    medians = artifact.get("medians")
    if medians is not None and not isinstance(medians, pd.Series):
        medians = pd.Series(medians)

    transform = None
    if os.environ.get("ROBOT_USE_FEATURE_TRANSFORM", "false").lower() in {"1", "true", "yes", "y"}:
        transform = _load_feature_transform_callable(
            artifact.get("feature_transform"),
            artifact.get("feature_transform_format"),
        )
    return artifact["model"], features, medians, transform


def _artifact():
    global _ARTIFACT
    if _ARTIFACT is None:
        _ARTIFACT = _load_model_artifact()
    return _ARTIFACT


def _normalize_factor_keys(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df = df.copy()
    date_col = "date" if "date" in df.columns else "_date" if "_date" in df.columns else None
    code_candidates = ["code", "ID_QI", "ts_code", "Code", "stock_code", "_code6"]
    code_col = next((col for col in code_candidates if col in df.columns), None)
    if code_col is None:
        raise RuntimeError(f"factor input has no code column: {list(df.columns)}")

    df["_date"] = df[date_col].map(_normalize_date) if date_col else date_str
    df["_code6"] = df[code_col].map(_normalize_code6)
    df["date"] = df["_date"]
    df["code"] = df["_code6"]
    return df.dropna(subset=["_date", "_code6"])


def _merge_daily_basic(df: pd.DataFrame, daily_basic_df: Optional[pd.DataFrame], date_str: str) -> pd.DataFrame:
    if daily_basic_df is None or daily_basic_df.empty:
        return df

    daily = daily_basic_df.copy()
    daily["_date"] = daily["date"].map(_normalize_date) if "date" in daily.columns else date_str
    if "ID_QI" in daily.columns:
        daily["_code6"] = daily["ID_QI"].map(_normalize_code6)
    elif "ts_code" in daily.columns:
        daily["_code6"] = daily["ts_code"].map(_normalize_code6)
    elif "code" in daily.columns:
        daily["_code6"] = daily["code"].map(_normalize_code6)
    elif "TICKER_SYMBOL" in daily.columns:
        daily["_code6"] = daily["TICKER_SYMBOL"].map(_normalize_code6)
    else:
        return df

    daily = daily.dropna(subset=["_date", "_code6"])
    daily = daily[daily["_date"] == date_str]
    if daily.empty:
        return df

    skip = {"date", "code"}
    cols = [c for c in daily.columns if c not in skip and c not in df.columns]
    return df.merge(daily[["_date", "_code6", *cols]], on=["_date", "_code6"], how="left")


def _build_feature_matrix(
    df: pd.DataFrame,
    features: list[str],
    medians: Optional[pd.Series],
    feature_transform: Optional[Callable],
) -> pd.DataFrame:
    if feature_transform is not None:
        x = feature_transform(df=df, features=features, medians=medians)
        if not isinstance(x, pd.DataFrame):
            x = pd.DataFrame(x, index=df.index, columns=features)
        return x.reindex(columns=features).fillna(0.0)

    values = {
        feature: pd.to_numeric(df[feature], errors="coerce")
        if feature in df.columns else pd.Series(float("nan"), index=df.index)
        for feature in features
    }
    x = pd.DataFrame(values, index=df.index)
    if medians is not None:
        x = x.fillna(medians.reindex(features))
    return x.fillna(0.0)


def _target_position_weights(pred: pd.Series, dates: pd.Series, tail_frac: float) -> pd.Series:
    if not 0 < tail_frac <= 0.5:
        raise RuntimeError("ROBOT_POSITION_TAIL_FRAC must be in (0, 0.5]")

    frame = pd.DataFrame({"_date": dates.reindex(pred.index), "pred": pd.to_numeric(pred, errors="coerce")})
    weights = pd.Series(0.0, index=pred.index, dtype="float64")
    valid = frame.dropna(subset=["_date", "pred"])
    for _, group in valid.groupby("_date", sort=False):
        n = len(group)
        if n < 2:
            continue
        k = max(1, math.ceil(n * tail_frac))
        k = min(k, n // 2)
        ranked = group["pred"].sort_values(kind="mergesort")
        weights.loc[ranked.index[-k:]] = 1.0 / k
        if os.environ.get("ROBOT_ENABLE_SELL", "true").lower() in {"1", "true", "yes", "y"}:
            weights.loc[ranked.index[:k]] = -1.0 / k
    return weights


def _current_positions(portfolio_context) -> dict[str, int]:
    positions = getattr(portfolio_context, "positions", pd.DataFrame()) if portfolio_context is not None else pd.DataFrame()
    if positions is None or positions.empty:
        return {}

    result: dict[str, int] = {}
    for _, row in positions.iterrows():
        code = _normalize_code6(row.get("code", row.get("stock_code", "")))
        if not code:
            continue
        raw = row.get("current_volume", row.get("volume", row.get("qty", 0)))
        try:
            volume = int(float(raw))
        except Exception:
            volume = 0
        if volume > 0:
            result[code] = volume
    return result


def _round_lot(volume: float) -> int:
    lot = int(os.environ.get("ROBOT_LOT_SIZE", "100"))
    if lot <= 0:
        return max(0, int(volume))
    return max(0, int(volume // lot) * lot)


def _target_orders(scored: pd.DataFrame, portfolio_context) -> pd.DataFrame:
    capital = float(os.environ.get("ROBOT_TARGET_CAPITAL", "100000"))
    max_orders = int(os.environ.get("ROBOT_MAX_ORDERS", "10"))
    default_price = float(os.environ.get("ROBOT_DEFAULT_PRICE", "10"))
    strategy = os.environ.get("ROBOT_STRATEGY_NAME", "robot_model")
    meta = getattr(portfolio_context, "meta", {}) if portfolio_context is not None else {}
    if meta.get("positions_stale") or meta.get("positions_usable") is False:
        print(
            "[robot-inference] skip orders: portfolio positions unavailable "
            f"stale={meta.get('positions_stale')} source={meta.get('source_file', '')}"
        )
        return pd.DataFrame()

    current = _current_positions(portfolio_context)
    rows = []
    active = scored[scored["position"] != 0.0].copy()
    active["_abs_position"] = active["position"].abs()
    active = active.sort_values(["_abs_position", "pred"], ascending=[False, False]).head(max_orders)

    for _, row in active.iterrows():
        code6 = str(row["_code6"]).zfill(6)
        price_raw = row.get("last_price", row.get("close", row.get("adj_close", default_price)))
        try:
            price = float(price_raw)
        except Exception:
            price = default_price
        if not math.isfinite(price) or price <= 0:
            price = default_price

        target_value = max(float(row["position"]), 0.0) * capital
        target_volume = _round_lot(target_value / price)
        current_volume = current.get(code6, 0)
        delta = target_volume - current_volume
        if delta == 0:
            continue
        side = "buy" if delta > 0 else "sell"
        volume = _round_lot(abs(delta))
        if volume <= 0:
            continue
        if side == "sell":
            volume = min(volume, _round_lot(current_volume))
            if volume <= 0:
                continue
        rows.append({
            "code": _order_code(code6),
            "side": side,
            "volume": volume,
            "price_type": os.environ.get("ORDER_DEFAULT_PRICE_TYPE", "latest"),
            "strategy": strategy,
            "note": f"{strategy}_{row['_date']}_{code6}_{side}",
            "pred": float(row["pred"]),
            "position": float(row["position"]),
            "target_volume": int(target_volume),
            "current_volume": int(current_volume),
        })
    return pd.DataFrame(rows)


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
    """Score latest factors and return order rows."""
    if intraday_factors_df is None or intraday_factors_df.empty:
        return pd.DataFrame()

    model, features, medians, feature_transform = _artifact()
    df = _normalize_factor_keys(intraday_factors_df, date_str)
    if trading_universe_df is not None and not trading_universe_df.empty and "code" in trading_universe_df.columns:
        universe = set(trading_universe_df["code"].astype(str).map(_normalize_code6).dropna())
        df = df[df["_code6"].isin(universe)]
    df = _merge_daily_basic(df, daily_basic_df, date_str)
    if df.empty:
        return pd.DataFrame()

    x = _build_feature_matrix(df, features, medians, feature_transform)
    df["pred"] = model.predict(x)
    tail_frac = float(os.environ.get("ROBOT_POSITION_TAIL_FRAC", "0.10"))
    df["position"] = _target_position_weights(df["pred"], df["_date"], tail_frac)
    orders = _target_orders(df, portfolio_context)
    print(
        f"[robot-inference] date={date_str} end_time={end_time} "
        f"stocks={len(df)} active={(df['position'] != 0).sum()} orders={len(orders)}"
    )
    return orders
