import pandas as pd

from quant_platform.inference.interface import PortfolioContext, positions_to_orders


def test_positions_to_orders_first_day_does_not_apply_rebalance_reserve():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 0.0,
            "market_value": 0.0,
        }]),
        positions=pd.DataFrame(),
        meta={"latest_prices": {"000001.XSHE": 10.0}},
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.10}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "buy"
    assert orders.iloc[0]["volume"] == 1000
    assert orders.iloc[0]["price"] == 10.0


def test_positions_to_orders_rebalance_keeps_full_target_buy_size():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 100.0,
            "market_value": 98_900.0,
        }]),
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 100,
            "last_price": 10.0,
        }]),
        meta={
            "latest_prices": {
                "000001.XSHE": 10.0,
                "600000.XSHG": 10.0,
            },
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([
            {"code": "000001", "position": 0.10},
            {"code": "600000", "position": 0.0},
        ]),
        ctx,
    )

    buy = orders[orders["side"] == "buy"].iloc[0]
    sell = orders[orders["side"] == "sell"].iloc[0]
    assert buy["volume"] == 1000
    assert buy["price"] == 10.0
    assert sell["volume"] == 100


def test_positions_to_orders_sell_clamps_to_available_volume():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 1_000.0,
            "market_value": 99_000.0,
        }]),
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 1000,
            "available_volume": 300,
            "last_price": 10.0,
        }]),
        meta={"latest_prices": {"600000.XSHG": 10.0}},
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "600000", "position": 0.0}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "sell"
    assert orders.iloc[0]["volume"] == 300


def test_positions_to_orders_missing_price_without_exit_returns_empty():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 100_000.0,
            "market_value": 0.0,
        }]),
        positions=pd.DataFrame(),
        meta={"latest_prices": {}},
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.10}]),
        ctx,
    )

    assert orders.empty


def test_positions_to_orders_missing_price_target_holding_is_not_liquidated():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 1_000.0,
            "market_value": 99_000.0,
        }]),
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 1000,
            "available_volume": 1000,
            "last_price": 10.0,
        }]),
        meta={"latest_prices": {}},
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "600000", "position": 0.10}]),
        ctx,
    )

    assert orders.empty


def test_positions_to_orders_reports_missing_price_for_held_target():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 99_000.0,
            "market_value": 1_000.0,
        }]),
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 100,
            "available_volume": 100,
            "last_price": 0.0,
        }]),
        meta={"latest_prices": {"000001.XSHE": 10.0}},
    )
    diagnostics = {}

    orders = positions_to_orders(
        pd.DataFrame([
            {"code": "000001", "position": 0.10},
            {"code": "600000", "position": 0.20},
        ]),
        ctx,
        diagnostics=diagnostics,
    )

    assert orders["code"].tolist() == ["000001"]
    assert diagnostics["missing_price_codes"] == ["600000"]


def test_positions_to_orders_empty_zero_lot_has_no_missing_price():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 100_000.0,
            "market_value": 0.0,
        }]),
        positions=pd.DataFrame(),
        meta={"latest_prices": {"000001.XSHE": 10.0}},
    )
    diagnostics = {}

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.00001}]),
        ctx,
        diagnostics=diagnostics,
    )

    assert orders.empty
    assert diagnostics["missing_price_codes"] == []


def test_positions_to_orders_missing_target_prices_still_liquidates_exits():
    ctx = PortfolioContext(
        account=pd.DataFrame([{
            "total_asset": 100_000.0,
            "available_cash": 1_000.0,
            "market_value": 99_000.0,
        }]),
        positions=pd.DataFrame([
            {
                "code": "600000.SH",
                "current_volume": 500,
                "available_volume": 200,
                "last_price": 10.0,
            },
            {
                "code": "600000.SH",
                "current_volume": 500,
                "available_volume": 300,
                "last_price": 10.0,
            },
            {
                "code": "000002.SZ",
                "current_volume": 100,
                "available_volume": 100,
                "last_price": 10.0,
            },
        ]),
        meta={"latest_prices": {}},
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.10}]),
        ctx,
    )

    sell_orders = orders[orders["side"] == "sell"].sort_values("code").reset_index(drop=True)
    assert len(sell_orders) == 2
    assert sell_orders.iloc[0]["code"] == "000002"
    assert sell_orders.iloc[0]["volume"] == 100
    assert sell_orders.iloc[1]["code"] == "600000"
    assert sell_orders.iloc[1]["volume"] == 500


def test_positions_to_orders_drops_buy_at_exchange_high_limit():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame(),
        meta={
            "latest_prices": {"000001.XSHE": 11.0},
            "limit_prices": {"000001.XSHE": (11.0, 9.0)},
        },
    )
    diagnostics = {}

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.10}]),
        ctx,
        diagnostics=diagnostics,
    )

    assert orders.empty
    assert diagnostics["limit_up_buy_codes"] == ["000001"]


def test_positions_to_orders_keeps_buy_below_exchange_high_limit():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame(),
        meta={
            "latest_prices": {"000001.XSHE": 10.99},
            "limit_prices": {"000001.XSHE": (11.0, 9.0)},
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.10}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "buy"


def test_positions_to_orders_keeps_sell_at_exchange_high_limit():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame([{
            "code": "000001.SZ",
            "current_volume": 1000,
            "available_volume": 1000,
            "last_price": 11.0,
        }]),
        meta={
            "latest_prices": {"000001.XSHE": 11.0},
            "limit_prices": {"000001.XSHE": (11.0, 9.0)},
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.0}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "sell"
    assert orders.iloc[0]["volume"] == 1000


def test_positions_to_orders_keeps_held_buy_at_exchange_high_limit():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame([{
            "code": "000001.SZ",
            "current_volume": 100,
            "available_volume": 100,
            "last_price": 11.0,
        }]),
        meta={
            "latest_prices": {"000001.XSHE": 11.0},
            "limit_prices": {"000001.XSHE": (11.0, 9.0)},
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "000001", "position": 0.10}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "buy"


def test_positions_to_orders_keeps_unheld_buy_with_unknown_limit_price():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame(),
        meta={
            "latest_prices": {"600000.XSHG": 10.0},
            "limit_prices": {},
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "600000", "position": 0.10}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "buy"


def test_positions_to_orders_keeps_sell_with_unknown_limit_price():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 1000,
            "available_volume": 1000,
            "last_price": 10.0,
        }]),
        meta={
            "latest_prices": {"600000.XSHG": 10.0},
            "limit_prices": {},
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "600000", "position": 0.0}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "sell"


def test_positions_to_orders_keeps_held_buy_with_unknown_limit_price():
    ctx = PortfolioContext(
        account=pd.DataFrame([{"total_asset": 100_000.0}]),
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 100,
            "available_volume": 100,
            "last_price": 10.0,
        }]),
        meta={
            "latest_prices": {"600000.XSHG": 10.0},
            "limit_prices": {},
        },
    )

    orders = positions_to_orders(
        pd.DataFrame([{"code": "600000", "position": 0.10}]),
        ctx,
    )

    assert len(orders) == 1
    assert orders.iloc[0]["side"] == "buy"
