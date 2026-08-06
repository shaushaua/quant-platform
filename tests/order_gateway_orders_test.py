import hashlib

import pandas as pd

from quant_platform.live_engine.native_engine import _build_order_gateway_orders


def test_atx_vwap_order_omits_execution_price():
    orders = _build_order_gateway_orders(
        pd.DataFrame([{
            "code": "600000",
            "side": "buy",
            "volume": 100,
            "price_type": "latest",
            "price": 12.34,
            "strategy": "tree_model",
            "note": "open_position_600000",
        }]),
        "20260807",
        "093000",
    )

    assert len(orders) == 1
    assert "price_type" not in orders[0]
    assert orders[0]["algo_strategy"] == "vwap"
    assert "price" not in orders[0]


def test_atx_order_id_does_not_change_with_sizing_price():
    base = {
        "code": "000001",
        "side": "buy",
        "volume": 100,
        "strategy": "tree_model",
    }

    first = _build_order_gateway_orders(
        pd.DataFrame([{**base, "price": 10.0}]), "20260807", "093000"
    )
    second = _build_order_gateway_orders(
        pd.DataFrame([{**base, "price": 10.5}]), "20260807", "093000"
    )

    assert first[0]["order_id"] == second[0]["order_id"]
    assert "price" not in first[0]
    assert "price" not in second[0]


def test_order_id_keeps_legacy_market_component_after_field_removal():
    order = _build_order_gateway_orders(
        pd.DataFrame([{
            "code": "000001",
            "side": "buy",
            "volume": 100,
            "strategy": "tree_model",
        }]),
        "20260807",
        "093000",
    )[0]
    legacy_content = "|".join([
        "20260807", "093000", "000001.SZ", "buy", "100",
        "market", "tree_model", "vwap", "",
    ])
    expected = "tree_model_" + hashlib.md5(
        legacy_content.encode("utf-8")
    ).hexdigest()[:12]

    assert order["order_id"] == expected
    assert "price_type" not in order


def test_market_order_drops_algo_param_that_contains_price():
    orders = _build_order_gateway_orders(
        pd.DataFrame([{
            "code": "600000",
            "side": "buy",
            "volume": 100,
            "algo_param": "UpLimitF=1.0:price=12.34",
        }]),
        "20260807",
        "093000",
    )

    assert len(orders) == 1
    assert "price" not in orders[0]
    assert orders[0]["algo_param"] == "UpLimitF=1.0"


def test_order_algo_is_owned_by_engine_not_row_or_environment(monkeypatch):
    monkeypatch.setenv("ORDER_ALGO_STRATEGY", "twap")
    monkeypatch.setenv("ORDER_ALGO_SPLIT_VWAP_TWAP", "1")

    orders = _build_order_gateway_orders(
        pd.DataFrame([
            {"code": "600000", "side": "buy", "volume": 100, "algo_strategy": "twap"},
            {"code": "000001", "side": "buy", "volume": 100, "algo_strategy": "twap"},
        ]),
        "20260807",
        "093000",
    )

    assert [order["algo_strategy"] for order in orders] == ["vwap", "vwap"]
