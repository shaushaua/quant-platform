import os

import pandas as pd

os.environ.setdefault("START_DATE", "2024-01-02")
os.environ.setdefault("END_DATE", "2024-01-02")
os.environ.setdefault("TASK_ID", "unit-test")

from quant_platform.backtest.worker_entrypoint import _filter_tradable_universe


def test_l2_strategy_excludes_zero_volume_stocks():
    daily_basic = pd.DataFrame(
        {
            "ID_QI": ["000001", "000046", "002776", "600000"],
            "volume": [100, 0, None, "200"],
        }
    )

    result = _filter_tradable_universe(
        daily_basic,
        {"need_l2_order": True, "need_l2_deal": True},
    )

    assert result["ID_QI"].tolist() == ["000001", "600000"]


def test_daily_strategy_keeps_zero_volume_stocks():
    daily_basic = pd.DataFrame(
        {"ID_QI": ["000001", "000046"], "volume": [100, 0]}
    )

    result = _filter_tradable_universe(daily_basic, {"market_count": 1})

    assert result.equals(daily_basic)
