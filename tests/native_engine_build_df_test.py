import numpy as np

from quant_platform.live_engine.native_engine import _build_df_from_native


class _ArrayReader:
    def __init__(self, rows):
        self._rows = np.asarray(rows, dtype=np.float64)

    def view_rows(self):
        return self._rows


def test_build_df_sorts_entire_rows_by_seq_num():
    buf_cols = ["Time", "UpdateTime", "SeqNum", "Price", "Volume", "OrderID"]
    reader = _ArrayReader([
        [1.0, 1.0, 30.0, 300.0, 3.0, 3000.0],
        [2.0, 2.0, 10.0, 100.0, 1.0, 1000.0],
        [3.0, 3.0, 20.0, 200.0, 2.0, 2000.0],
    ])

    result = _build_df_from_native(
        reader,
        buf_cols,
        buf_cols,
        time_idx=0,
        updtime_idx=1,
        trading_day="20260714",
        code="000001.XSHE",
        volume_idx=[4],
        historical_compat=True,
        kind_name="order",
    )

    assert result["SeqNum"].tolist() == [10, 20, 30]
    assert result["Time"].astype("int64").tolist() == [2_000_000_000, 3_000_000_000, 1_000_000_000]
    assert result["Price"].tolist() == [100.0, 200.0, 300.0]
    assert result["Volume"].tolist() == [100.0, 200.0, 300.0]
    assert result["OrderID"].tolist() == [1000, 2000, 3000]
