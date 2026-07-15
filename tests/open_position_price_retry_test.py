import queue
import sys
from types import SimpleNamespace

import pandas as pd

import quant_platform.live_engine.native_engine as native_engine_module
from quant_platform.live_engine.native_engine import NativeEngine


def test_snapshot_target_prices_reads_late_tick_files(monkeypatch):
    rows_by_path = {
        "target.tick": [0.0, 10.0, 0.0, 10.5],
        "other.tick": [20.0],
    }
    closed = []

    class FakeReader:
        def __init__(self, path):
            self.path = path

        def view_rows(self):
            rows = []
            for price in rows_by_path[self.path]:
                row = [0.0] * 79
                row[2] = price
                rows.append(row)
            return native_engine_module.np.asarray(rows, dtype=float)

        def close(self):
            closed.append(self.path)

    monkeypatch.setattr(native_engine_module, "NativeShmReader", FakeReader)
    engine = NativeEngine.__new__(NativeEngine)
    engine._scan_shm_files = lambda: {
        "600000.XSHG": {native_engine_module.KIND_TICK: "target.tick"},
        "000001.XSHE": {native_engine_module.KIND_TICK: "other.tick"},
    }

    prices = engine._snapshot_target_prices_from_shm({"600000"})

    assert prices == {"600000.XSHG": 10.5}
    assert closed == ["target.tick"]


def test_open_position_retries_only_for_reported_missing_prices(monkeypatch):
    calls = []

    def targets_to_orders(*args, diagnostics=None, portfolio_context=None, **kwargs):
        calls.append(len(calls) + 1)
        latest_prices = portfolio_context.meta.get("latest_prices", {})
        if "600000.XSHG" not in latest_prices:
            diagnostics["missing_price_codes"] = ["600000"]
            return pd.DataFrame([{"code": "000001", "side": "buy", "volume": 100}])
        diagnostics["missing_price_codes"] = []
        return pd.DataFrame([
            {"code": "000001", "side": "buy", "volume": 100},
            {"code": "600000", "side": "buy", "volume": 200},
        ])

    module_name = "tests.fake_price_retry_inference"
    monkeypatch.setitem(
        sys.modules,
        module_name,
        SimpleNamespace(targets_to_orders=targets_to_orders),
    )
    monkeypatch.setenv("INFERENCE_MODULE", module_name)
    monkeypatch.setenv("OPEN_POSITION_PRICE_RETRIES", "2")
    monkeypatch.setenv("OPEN_POSITION_PRICE_RETRY_DELAY", "0")

    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260715"
    engine._pop_open_position_targets = lambda: pd.DataFrame([
        {"code": "000001", "position": 0.1},
        {"code": "600000", "position": 0.2},
    ])
    engine.portfolio_context_fn = lambda *_: SimpleNamespace(
        positions=pd.DataFrame(), meta={})
    engine._snapshot_latest_prices = lambda: {}
    shm_reads = []

    def snapshot_target_prices(codes):
        shm_reads.append(set(codes))
        return {} if len(shm_reads) == 1 else {"600000.XSHG": 10.0}

    engine._snapshot_target_prices_from_shm = snapshot_target_prices
    engine._daily_basic_df = pd.DataFrame()
    engine.output_path = None
    engine._order_queue = queue.Queue()

    schedule = SimpleNamespace(
        name="open_position",
        result_label="093000",
        skip_factor_compute=True,
        is_daily_result=False,
    )
    engine._compute_and_output_locked(schedule)

    orders, date_str, end_time = engine._order_queue.get_nowait()
    assert calls == [1, 2]
    assert shm_reads == [{"000001", "600000"}, {"000001", "600000"}]
    assert orders["code"].tolist() == ["000001", "600000"]
    assert (date_str, end_time) == ("20260715", "093000")
