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
