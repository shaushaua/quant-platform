import pandas as pd

from quant_platform.factor.engine import _DayBundle, _filter_codes_from_bundle


def test_filter_codes_preserves_schema_for_stock_without_rows():
    order = pd.DataFrame(
        {
            "Code": pd.Series([1], dtype="int64"),
            "Time": pd.to_datetime(["2024-01-02 09:30:00"]),
            "SeqNum": pd.Series([10], dtype="int64"),
        }
    )
    deal = pd.DataFrame(
        {
            "Code": pd.Series(dtype="int64"),
            "Time": pd.Series(dtype="datetime64[ns]"),
            "SeqNum": pd.Series(dtype="int64"),
        }
    )
    tick = pd.DataFrame(
        {
            "Code": pd.Series([1, 2], dtype="int64"),
            "Time": pd.to_datetime(
                ["2024-01-02 09:30:00", "2024-01-02 09:30:00"]
            ),
        }
    )
    market = pd.DataFrame(
        {
            "SECURITY_ID": pd.Series([1, 2], dtype="int64"),
            "ID_QI": ["000001", "000002"],
        }
    )
    bundle = _DayBundle("20240102", order, deal, tick, market)

    filtered = _filter_codes_from_bundle(
        bundle,
        ["000001", "000002"],
        {"000001": 1, "000002": 2},
    )

    by_code = {item.code: item for item in filtered}
    assert len(by_code["000001"].l2_order) == 1
    assert by_code["000002"].l2_order.empty
    assert list(by_code["000002"].l2_order.columns) == list(order.columns)
    assert list(by_code["000002"].l2_deal.columns) == list(deal.columns)
    assert by_code["000002"].l2_order.dtypes.equals(order.dtypes)
