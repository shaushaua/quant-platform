#!/usr/bin/env python3
"""Rebalance once from the live daily-position factor file.

This is an operational fallback for a large intraday deposit.  It does not
recompute factors: it consumes the exact factor CSV produced by the running
engine, reloads current broker holdings, and computes only the remaining delta.
Dry-run is the default; pass --execute to submit orders.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import json
import urllib.request
from datetime import datetime
from pathlib import Path

import pandas as pd

_SCRIPT = Path(__file__).resolve()
ROOT = Path("/app") if Path("/app/quant_platform").is_dir() else _SCRIPT.parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform.broker.order_context import (  # noqa: E402
    enrich_portfolio_context_with_latest_prices,
    get_portfolio_context,
)
from quant_platform.data.mysql_loader import MySQLLoader  # noqa: E402
from quant_platform.data.oss_loader import OSSDataLoader  # noqa: E402
from quant_platform.inference.interface import (  # noqa: E402
    build_trading_universe_df,
    compute_index_composition,
)
from quant_platform.inference import tree_model_inference as tree_model  # noqa: E402
from quant_platform.inference import tree_model_orders  # noqa: E402
from quant_platform.live_engine.native_engine import (  # noqa: E402
    NativeEngine,
    _build_order_gateway_orders,
    _push_to_order_gateway,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--factor-file", required=True)
    parser.add_argument("--end-time", default="145500")
    parser.add_argument("--not-before", default="145400")
    parser.add_argument("--max-turnover", type=float, default=1.0)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--preview-output", default="/tmp/rebalance_preview.csv")
    return parser.parse_args()


def _code6(value: object) -> str:
    return str(value).strip().split(".")[0].zfill(6)


def _load_universe(daily_basic: pd.DataFrame, composition: pd.DataFrame) -> pd.DataFrame:
    module_name = os.environ.get(
        "TRADING_UNIVERSE_MODULE", "quant_platform.inference.trading_universe")
    module = importlib.import_module(module_name)
    codes = module.trading_universe(daily_basic, composition)
    if codes is None:
        codes = daily_basic["ID_QI"].dropna().astype(str).unique().tolist()
    return build_trading_universe_df(codes, daily_basic)


def _refuse_when_original_orders_exist(date_str: str) -> None:
    """Do not overlap the original 14:50 mother orders (AftAction=1)."""
    gateway = os.environ.get("ORDER_GATEWAY_URL", "").rstrip("/")
    token = os.environ.get("ORDER_GATEWAY_TOKEN", "")
    if not gateway:
        raise RuntimeError("ORDER_GATEWAY_URL is required for the order guard")
    url = f"{gateway}/v1/atx/report?file=ReportOrderAlgo_{date_str}.dbf"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not payload.get("ok", False):
        raise RuntimeError("cannot verify original ATX mother orders")
    rows = payload.get("rows") or []
    for row in rows:
        start = str(
            row.get("EffTime", row.get("effectiveTime", row.get("开始时间", "")))
        )
        digits = "".join(ch for ch in start if ch.isdigit())
        hhmmss = digits[-9:-3] if len(digits) >= 17 else digits[-6:]
        if hhmmss >= "145000":
            raise RuntimeError(
                "original 14:50 mother orders exist; refusing overlapping top-up orders")


def main() -> int:
    args = _args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    now = datetime.now()
    if now.strftime("%Y%m%d") != args.date:
        raise RuntimeError("refusing non-current trading date")
    if now.strftime("%H%M%S") < args.not_before:
        raise RuntimeError(
            f"refusing before {args.not_before}; wait for the original VWAP window")

    factor_path = Path(args.factor_file)
    expected_name = f"{args.date}_145000.csv"
    if factor_path.name != expected_name or not factor_path.is_file():
        raise RuntimeError(f"expected live factor file named {expected_name}")
    if factor_path.stat().st_mtime < now.replace(hour=14, minute=50, second=0).timestamp():
        raise RuntimeError("factor file predates today's 14:50 trigger")

    factor_df = pd.read_csv(factor_path)
    if factor_df.empty:
        raise RuntimeError("14:50 factor file is empty")

    oss = OSSDataLoader()
    daily_basic = oss.read_daily_basic(args.date, market_count=60)
    composition = oss.read_composition(args.date)
    if daily_basic.empty or composition.empty:
        raise RuntimeError("daily_basic or composition is unavailable")

    portfolio = get_portfolio_context(args.date, args.end_time)
    if portfolio.account.empty or portfolio.positions.empty:
        raise RuntimeError("current broker account or positions are unavailable")

    helper = NativeEngine()
    helper.trading_day = args.date
    mysql = MySQLLoader()
    try:
        limits = mysql.get_limit_prices(args.date)
    finally:
        mysql.close()
    helper._daily_limit_prices = {
        code: (item.high_limit, item.low_limit) for code, item in limits.items()
    }

    universe = _load_universe(daily_basic, composition)
    model_composition = compute_index_composition(
        daily_basic,
        {"idx_cons_df": composition},
        trading_day=args.date,
    )
    if model_composition.empty:
        raise RuntimeError("standardized model composition is empty")
    universe_codes = set(universe["ID_QI"].astype(str).map(_code6))
    prices = helper._snapshot_target_prices_from_shm(universe_codes)
    portfolio.meta["latest_prices"] = prices
    portfolio.meta["limit_prices"] = helper._snapshot_limit_prices()
    enrich_portfolio_context_with_latest_prices(portfolio, prices)
    universe = helper._filter_open_position_limit_up_universe(
        universe, daily_basic, portfolio, log_tag="deposit-rebalance-targets")

    tree_model.OPTIMIZER_MAX_TURNOVER = float(args.max_turnover)
    targets = tree_model_orders.inference_targets(
        args.date,
        "145000",
        factor_df,
        factor_df,
        daily_basic,
        universe,
        model_composition,
        portfolio,
    )
    if targets is None or targets.empty:
        raise RuntimeError("turnover override produced no target positions")

    target_codes = set(targets["code"].astype(str).map(_code6))
    have_prices = {_code6(code) for code, price in prices.items() if float(price or 0) > 0}
    missing = target_codes - have_prices
    if missing:
        prices.update(helper._snapshot_target_prices_from_external(missing))
        portfolio.meta["latest_prices"] = prices
        enrich_portfolio_context_with_latest_prices(portfolio, prices)

    diagnostics: dict = {}
    orders = tree_model_orders.targets_to_orders(
        targets,
        args.date,
        args.end_time,
        daily_basic_df=daily_basic,
        portfolio_context=portfolio,
        diagnostics=diagnostics,
    )
    if orders.empty:
        print("no remaining delta orders")
        return 0

    preview = Path(args.preview_output)
    orders.to_csv(preview, index=False)
    gateway_orders = _build_order_gateway_orders(orders, args.date, args.end_time)
    buys = sum(1 for row in gateway_orders if row["side"] == "buy")
    sells = sum(1 for row in gateway_orders if row["side"] == "sell")
    print(
        f"preview={preview} targets={len(targets)} orders={len(gateway_orders)} "
        f"buys={buys} sells={sells} missing_prices="
        f"{len(diagnostics.get('missing_price_codes', []))} execute={args.execute}"
    )
    if args.execute:
        _refuse_when_original_orders_exist(args.date)
        _push_to_order_gateway(orders, args.date, args.end_time)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
