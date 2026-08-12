import queue
import mmap
import sys
import threading
from types import SimpleNamespace

import pandas as pd

import quant_platform.live_engine.native_engine as native_engine_module
from quant_platform.factor.base import StockState
from quant_platform.live_engine.native_engine import NativeEngine


def test_open_position_blocks_minute_compute_until_complete():
    engine = NativeEngine.__new__(NativeEngine)
    engine._compute_lock = threading.Lock()
    engine._compute_running = False
    engine._open_position_running = threading.Event()
    engine._post_close_daily_done = threading.Event()
    observed = []
    engine._compute_and_output_locked = lambda _schedule: observed.append(
        engine._minute_compute_blocked()
    )
    schedule = SimpleNamespace(name="open_position")

    engine._compute_and_output(schedule)

    assert observed == [True]
    assert engine._minute_compute_blocked() is False


def test_open_position_waits_for_active_minute_without_reopening_minute_gate():
    engine = NativeEngine.__new__(NativeEngine)
    engine._compute_lock = threading.Lock()
    engine._compute_running = True
    engine._open_position_running = threading.Event()
    engine._post_close_daily_done = threading.Event()
    observed = []
    engine._compute_and_output_locked = lambda _schedule: observed.append(True)
    schedule = SimpleNamespace(name="open_position")

    engine._compute_lock.acquire()
    worker = threading.Thread(target=engine._compute_and_output, args=(schedule,))
    try:
        worker.start()
        assert engine._open_position_running.wait(timeout=1.0) is True
        assert engine._minute_compute_blocked() is True
        assert observed == []
    finally:
        engine._compute_running = False
        engine._compute_lock.release()
        worker.join(timeout=1.0)

    assert worker.is_alive() is False
    assert observed == [True]
    assert engine._minute_compute_blocked() is False


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
                row[9] = 11.0
                row[10] = 9.0
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

    limit_prices = {}
    prices = engine._snapshot_target_prices_from_shm(
        {"600000"}, limit_prices
    )

    assert prices == {"600000.XSHG": 10.5}
    assert limit_prices == {"600000.XSHG": (11.0, 9.0)}
    assert closed == ["target.tick"]


def test_snapshot_target_prices_releases_mmap_views_before_close(monkeypatch):
    closed = []

    class MmapBackedReader:
        def __init__(self, path):
            self.path = path
            self._mmap = mmap.mmap(-1, 79 * 8)
            row = native_engine_module.np.frombuffer(
                self._mmap, dtype=native_engine_module.np.float64
            ).reshape(1, 79)
            row[0, 2] = 10.5
            row[0, 9] = 11.0
            row[0, 10] = 9.0
            del row

        def view_rows(self):
            return native_engine_module.np.frombuffer(
                self._mmap, dtype=native_engine_module.np.float64
            ).reshape(1, 79)

        def close(self):
            self._mmap.close()
            closed.append(self.path)

    monkeypatch.setattr(native_engine_module, "NativeShmReader", MmapBackedReader)
    engine = NativeEngine.__new__(NativeEngine)
    engine._scan_shm_files = lambda: {
        "600000.XSHG": {native_engine_module.KIND_TICK: "target.tick"},
    }

    limit_prices = {}
    prices = engine._snapshot_target_prices_from_shm({"600000"}, limit_prices)

    assert prices == {"600000.XSHG": 10.5}
    assert limit_prices == {"600000.XSHG": (11.0, 9.0)}
    assert closed == ["target.tick"]


def test_snapshot_target_prices_uses_current_tencent_quote(monkeypatch):
    fields = [""] * 31
    fields[0] = "1"
    fields[1] = "test"
    fields[2] = "601869"
    fields[3] = "9.87"
    fields[5] = "9.80"
    fields[6] = "100"
    fields[30] = "20260812093103"
    payload = ('v_sh601869="' + "~".join(fields) + '";\n').encode("gbk")

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return payload

    requests = []

    def urlopen(req, timeout):
        requests.append((req.full_url, timeout))
        return FakeResponse()

    monkeypatch.setattr(native_engine_module.urllib.request, "urlopen", urlopen)
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260812"

    prices = engine._snapshot_target_prices_from_external({"601869"})

    assert prices == {"601869.XSHG": 9.87}
    assert "sh601869" in requests[0][0]
    assert requests[0][1] == 1.0


def test_snapshot_target_prices_rejects_stale_tencent_quote(monkeypatch):
    fields = [""] * 31
    fields[0] = "0"
    fields[2] = "300333"
    fields[3] = "12.34"
    fields[5] = "12.00"
    fields[6] = "100"
    fields[30] = "20260811093103"
    payload = ('v_sz300333="' + "~".join(fields) + '";\n').encode("gbk")

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return payload

    monkeypatch.setattr(
        native_engine_module.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse(),
    )
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260812"

    assert engine._snapshot_target_prices_from_external({"300333"}) == {}


def test_snapshot_target_prices_rejects_no_trade_tencent_placeholder(monkeypatch):
    fields = [""] * 31
    fields[0] = "0"
    fields[2] = "300333"
    fields[3] = "8.87"
    fields[5] = "0.00"
    fields[6] = "0"
    fields[30] = "20260812093103"
    payload = ('v_sz300333="' + "~".join(fields) + '";\n').encode("gbk")

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return payload

    monkeypatch.setattr(
        native_engine_module.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: FakeResponse(),
    )
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260812"

    assert engine._snapshot_target_prices_from_external({"300333"}) == {}


def test_snapshot_limit_prices_uses_daily_fallback_for_zero_tick_limits():
    engine = NativeEngine.__new__(NativeEngine)
    engine._daily_limit_prices = {"600000.XSHG": (11.0, 9.0)}
    engine._states = {"600000.XSHG": StockState(code="600000.XSHG")}

    assert engine._snapshot_limit_prices() == {
        "600000.XSHG": (11.0, 9.0),
    }


def test_snapshot_limit_prices_keeps_authoritative_daily_limits():
    engine = NativeEngine.__new__(NativeEngine)
    engine._daily_limit_prices = {"000001.XSHE": (11.0, 9.0)}
    state = StockState(code="000001.XSHE")
    state.high_limit = 11.2
    state.low_limit = 8.8
    engine._states = {"000001.XSHE": state}

    assert engine._snapshot_limit_prices() == {
        "000001.XSHE": (11.0, 9.0),
    }


def test_snapshot_limit_prices_uses_realtime_when_daily_row_missing():
    engine = NativeEngine.__new__(NativeEngine)
    engine._daily_limit_prices = {}
    state = StockState(code="000001.XSHE")
    state.high_limit = 11.2
    state.low_limit = 8.8
    engine._states = {"000001.XSHE": state}

    assert engine._snapshot_limit_prices() == {
        "000001.XSHE": (11.2, 8.8),
    }


def test_pre_model_limit_up_filter_excludes_only_unheld_confirmed_limit_up():
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._snapshot_limit_prices = lambda: {
        "000001.XSHE": (11.0, 9.0),
        "600000.XSHG": (10.0, 8.0),
        "300750.XSHE": (12.0, 9.0),
    }
    engine._snapshot_latest_prices = lambda: {
        "000001.XSHE": 11.0,
        "600000.XSHG": 10.0,
        "300750.XSHE": 10.0,
    }
    engine._snapshot_target_prices_from_shm = lambda *_args, **_kwargs: {}
    context = SimpleNamespace(
        positions=pd.DataFrame([{
            "code": "600000.SH",
            "current_volume": 100,
        }]),
        meta={"positions_usable": True},
    )
    universe = pd.DataFrame({
        "code": ["000001.SZ", "600000.SH", "300750.SZ"],
        "ID_QI": ["000001", "600000", "300750"],
    })

    filtered = engine._filter_open_position_limit_up_universe(
        universe, pd.DataFrame(), context)

    assert filtered["ID_QI"].tolist() == ["600000", "300750"]
    assert context.meta["latest_prices"]["000001.XSHE"] == 11.0


def test_pre_model_limit_up_filter_keeps_missing_price_without_waiting():
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._snapshot_limit_prices = lambda: {"000001.XSHE": (11.0, 9.0)}
    attempts = []

    def latest_prices():
        attempts.append(True)
        return {}

    engine._snapshot_latest_prices = latest_prices
    engine._snapshot_target_prices_from_shm = lambda *_args, **_kwargs: {}
    context = SimpleNamespace(
        positions=pd.DataFrame(), meta={"positions_usable": True})
    universe = pd.DataFrame({"code": ["000001.SZ"], "ID_QI": ["000001"]})

    filtered = engine._filter_open_position_limit_up_universe(
        universe, pd.DataFrame(), context)

    assert len(attempts) == 1
    assert filtered["ID_QI"].tolist() == ["000001"]


def test_daily_position_filters_unheld_limit_up_before_inference(monkeypatch):
    engine = NativeEngine.__new__(NativeEngine)
    engine._snapshot_limit_prices = lambda: {"000001.XSHE": (11.0, 9.0)}
    context = SimpleNamespace(
        positions=pd.DataFrame(), meta={"positions_usable": True})
    universe = pd.DataFrame({"code": ["000001.SZ", "600000.SH"]})
    filtered = universe.iloc[[1]].copy()
    filter_calls = []
    inference_calls = []

    def filter_universe(actual_universe, daily_basic, actual_context, log_tag):
        filter_calls.append((actual_universe.copy(), daily_basic, actual_context, log_tag))
        return filtered

    def call_inference(*args):
        inference_calls.append(args)
        return pd.DataFrame()

    engine._filter_open_position_limit_up_universe = filter_universe
    engine._idx_cons_cache = None
    engine.portfolio_context_fn = lambda *_args: context
    monkeypatch.setattr(native_engine_module, "_upload_to_oss", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(native_engine_module, "compute_index_composition", lambda *_args, **_kwargs: pd.DataFrame())
    monkeypatch.setattr(native_engine_module, "call_inference", call_inference)

    engine._write_factor_output_and_infer(
        results=[{"code": "000001.XSHE", "factor": 1.0}],
        date_str="20260812",
        label="145000",
        rt_end_time="145000",
        is_daily=False,
        run_inference=True,
        output_path=None,
        outfun=None,
        inference_fn=lambda *_args: None,
        portfolio_context_fn=lambda *_args: context,
        daily_basic_df=pd.DataFrame(),
        inference_factor_input=None,
        idx_cons_df=pd.DataFrame(),
        trading_universe_df=universe,
        pre_fork_latest_prices={"000001.XSHE": 11.0},
        filter_unheld_limit_up_before_inference=True,
        gc_before_inference=False,
        log_tag="daily_position",
    )

    assert len(filter_calls) == 1
    assert filter_calls[0][2] is context
    assert filter_calls[0][3] == "daily_position-targets"
    assert inference_calls[0][6].equals(filtered)


def test_daily_position_fails_closed_when_holdings_are_unusable(monkeypatch):
    engine = NativeEngine.__new__(NativeEngine)
    engine._idx_cons_cache = None
    engine.portfolio_context_fn = lambda *_args: SimpleNamespace(
        positions=pd.DataFrame(),
        meta={"positions_usable": False, "positions_stale": True},
    )
    inference_calls = []

    monkeypatch.setenv("OPEN_POSITION_CONTEXT_RETRIES", "1")
    monkeypatch.setattr(native_engine_module, "_upload_to_oss", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        native_engine_module, "call_inference",
        lambda *args: inference_calls.append(args),
    )

    result = engine._write_factor_output_and_infer(
        results=[{"code": "000001.XSHE", "factor": 1.0}],
        date_str="20260812",
        label="145000",
        rt_end_time="145000",
        is_daily=False,
        run_inference=True,
        output_path=None,
        outfun=None,
        inference_fn=lambda *_args: None,
        portfolio_context_fn=engine.portfolio_context_fn,
        daily_basic_df=pd.DataFrame(),
        inference_factor_input=None,
        idx_cons_df=pd.DataFrame(),
        trading_universe_df=pd.DataFrame({"code": ["000001.SZ"]}),
        pre_fork_latest_prices={"000001.XSHE": 11.0},
        filter_unheld_limit_up_before_inference=True,
        gc_before_inference=False,
        log_tag="daily_position",
    )

    assert result is None
    assert inference_calls == []


def test_daily_position_uses_two_bounded_holdings_requests(monkeypatch):
    engine = NativeEngine.__new__(NativeEngine)
    requests = []

    def portfolio_context(_date, _end_time, request_timeout=None):
        requests.append(request_timeout)
        return SimpleNamespace(
            positions=pd.DataFrame(), meta={"positions_usable": False})

    engine.portfolio_context_fn = portfolio_context
    monkeypatch.setenv("OPEN_POSITION_CONTEXT_RETRIES", "3")
    monkeypatch.setenv("DAILY_POSITION_CONTEXT_TIMEOUT", "2.0")

    context = engine._load_open_position_portfolio_context(
        "20260812", "145000", "target inference",
        log_tag="daily_position",
        retries_override=2,
        delay_override=0.0,
        request_timeout=2.0,
    )

    assert context is None
    assert requests == [2.0, 2.0]


def test_precompute_limit_prices_retries_until_ready(monkeypatch):
    engine = NativeEngine.__new__(NativeEngine)
    engine._open_position_limit_prices_ready = False
    engine._daily_limit_prices = {}
    attempts = []

    def load_limit_prices():
        attempts.append(len(attempts) + 1)
        if len(attempts) < 2:
            return False
        engine._daily_limit_prices = {"600000.XSHG": (11.0, 9.0)}
        engine._open_position_limit_prices_ready = True
        return True

    engine._load_daily_limit_prices = load_limit_prices
    monkeypatch.setenv("OPEN_POSITION_LIMIT_PRICE_LOAD_RETRIES", "3")
    monkeypatch.setenv("OPEN_POSITION_LIMIT_PRICE_LOAD_RETRY_DELAY", "0")

    assert engine._ensure_open_position_limit_prices() is True
    assert attempts == [1, 2]


def test_open_position_retries_only_for_reported_missing_prices(monkeypatch):
    calls = []
    observed_prices = []

    def targets_to_orders(*args, diagnostics=None, portfolio_context=None, **kwargs):
        calls.append(len(calls) + 1)
        latest_prices = portfolio_context.meta.get("latest_prices", {})
        observed_prices.append(dict(latest_prices))
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
    engine._open_position_limit_prices_ready = True
    engine._open_position_cache = None
    engine._open_position_cache_lock = native_engine_module.threading.Lock()
    engine._open_position_cache_generation = 0

    def calculate_targets(*_args):
        engine._open_position_cache = pd.DataFrame([
            {"code": "000001", "position": 0.1},
            {"code": "600000", "position": 0.2},
        ])

    engine._calculate_open_position_targets = calculate_targets
    engine.portfolio_context_fn = lambda *_: SimpleNamespace(
        positions=pd.DataFrame(), meta={"positions_usable": True})
    engine._snapshot_latest_prices = lambda: {}
    shm_reads = []

    def snapshot_target_prices(codes, limit_prices=None):
        shm_reads.append(set(codes))
        if limit_prices is not None and len(shm_reads) > 1:
            limit_prices["600000.XSHG"] = (11.0, 9.0)
        return {} if len(shm_reads) == 1 else {
            "000001.XSHE": 10.1,
            "600000.XSHG": 10.0,
        }

    engine._snapshot_target_prices_from_shm = snapshot_target_prices
    external_reads = []
    engine._snapshot_target_prices_from_external = lambda codes: (
        external_reads.append(set(codes)) or {"000001.XSHE": 9.5}
    )
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
    assert external_reads == [{"000001", "600000"}]
    assert observed_prices[1]["000001.XSHE"] == 10.1
    assert orders["code"].tolist() == ["000001", "600000"]
    assert (date_str, end_time) == ("20260715", "093000")


def test_open_position_calculates_targets_at_trigger(monkeypatch):
    module_name = "tests.fake_trigger_time_inference"

    def targets_to_orders(*args, diagnostics=None, **kwargs):
        diagnostics["missing_price_codes"] = []
        targets = args[0]
        return pd.DataFrame([{
            "code": targets.iloc[0]["code"],
            "side": "buy",
            "volume": 100,
        }])

    monkeypatch.setitem(
        sys.modules,
        module_name,
        SimpleNamespace(targets_to_orders=targets_to_orders),
    )
    monkeypatch.setenv("INFERENCE_MODULE", module_name)
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._open_position_cache = None
    engine._open_position_cache_lock = native_engine_module.threading.Lock()
    engine._open_position_cache_generation = 0
    engine.output_path = None
    calculated = []

    def calculate_targets(*_args):
        calculated.append(True)
        engine._open_position_cache = pd.DataFrame([{
            "code": "000001",
            "position": 0.1,
        }])

    engine._calculate_open_position_targets = calculate_targets
    engine.portfolio_context_fn = lambda *_: SimpleNamespace(
        positions=pd.DataFrame(), meta={"positions_usable": True}
    )
    engine._snapshot_latest_prices = lambda: {"000001.XSHE": 10.0}
    engine._snapshot_limit_prices = lambda: {}
    engine._snapshot_target_prices_from_shm = lambda *_args, **_kwargs: {}
    engine._daily_basic_df = pd.DataFrame()
    engine._order_queue = queue.Queue()

    schedule = SimpleNamespace(
        name="open_position",
        result_label="093000",
        skip_factor_compute=True,
        is_daily_result=False,
    )
    engine._compute_and_output_locked(schedule)

    orders, date_str, end_time = engine._order_queue.get_nowait()
    assert calculated == [True]
    assert orders["code"].tolist() == ["000001"]
    assert (date_str, end_time) == ("20260807", "093000")


def test_open_position_recalculates_instead_of_consuming_stale_targets(monkeypatch):
    module_name = "tests.fake_cached_target_inference"

    def targets_to_orders(*args, diagnostics=None, **kwargs):
        diagnostics["missing_price_codes"] = []
        targets = args[0]
        return pd.DataFrame([{
            "code": targets.iloc[0]["code"],
            "side": "buy",
            "volume": 100,
        }])

    monkeypatch.setitem(
        sys.modules, module_name, SimpleNamespace(targets_to_orders=targets_to_orders)
    )
    monkeypatch.setenv("INFERENCE_MODULE", module_name)
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._open_position_cache = pd.DataFrame([{
        "code": "000001",
        "position": 0.1,
    }])
    engine._open_position_cache_lock = native_engine_module.threading.Lock()
    engine._open_position_cache_generation = 0
    engine.output_path = None
    calculated = []

    def calculate_targets(end_time):
        calculated.append(end_time)
        engine._open_position_cache = pd.DataFrame([{
            "code": "600000",
            "position": 0.2,
        }])

    engine._calculate_open_position_targets = calculate_targets
    engine.portfolio_context_fn = lambda *_: SimpleNamespace(
        positions=pd.DataFrame(), meta={"positions_usable": True})
    engine._snapshot_latest_prices = lambda: {"600000.XSHG": 10.0}
    engine._snapshot_limit_prices = lambda: {}
    engine._snapshot_target_prices_from_shm = lambda *_args, **_kwargs: {}
    engine._daily_basic_df = pd.DataFrame()
    engine._order_queue = queue.Queue()

    schedule = SimpleNamespace(
        name="open_position",
        result_label="093000",
        skip_factor_compute=True,
        is_daily_result=False,
    )
    engine._compute_and_output_locked(schedule)

    orders, _, _ = engine._order_queue.get_nowait()
    assert calculated == ["093000"]
    assert orders["code"].tolist() == ["600000"]


def test_open_position_fails_closed_when_holdings_are_unusable(monkeypatch):
    module_name = "tests.fake_unusable_holdings_inference"
    conversion_calls = []

    def targets_to_orders(*args, **kwargs):
        conversion_calls.append(True)
        return pd.DataFrame([{"code": "000001", "side": "buy", "volume": 100}])

    monkeypatch.setitem(
        sys.modules, module_name, SimpleNamespace(targets_to_orders=targets_to_orders)
    )
    monkeypatch.setenv("INFERENCE_MODULE", module_name)
    monkeypatch.setenv("OPEN_POSITION_CONTEXT_RETRIES", "1")

    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._open_position_cache = None
    engine._open_position_cache_lock = native_engine_module.threading.Lock()
    engine._open_position_cache_generation = 0
    engine.output_path = None
    engine._calculate_open_position_targets = lambda *_args: setattr(
        engine,
        "_open_position_cache",
        pd.DataFrame([{"code": "000001", "position": 0.1}]),
    )
    engine.portfolio_context_fn = lambda *_: SimpleNamespace(
        positions=pd.DataFrame(),
        meta={"positions_usable": False, "positions_stale": True},
    )
    engine._daily_basic_df = pd.DataFrame()
    engine._order_queue = queue.Queue()

    schedule = SimpleNamespace(
        name="open_position",
        result_label="093000",
        skip_factor_compute=True,
        is_daily_result=False,
    )
    engine._compute_and_output_locked(schedule)

    assert conversion_calls == []
    assert engine._order_queue.empty()


def test_target_inference_fails_closed_when_holdings_are_unusable(monkeypatch):
    module_name = "tests.fake_target_holdings_inference"
    target_calls = []

    def inference_targets(**kwargs):
        target_calls.append(kwargs)
        return pd.DataFrame([{"code": "000001", "position": 0.1}])

    monkeypatch.setitem(
        sys.modules, module_name, SimpleNamespace(inference_targets=inference_targets)
    )
    monkeypatch.setenv("INFERENCE_MODULE", module_name)
    monkeypatch.setenv("OPEN_POSITION_CONTEXT_RETRIES", "1")

    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._open_position_cache = None
    engine._open_position_cache_lock = native_engine_module.threading.Lock()
    engine._open_position_cache_generation = 0
    engine._ensure_open_position_limit_prices = lambda: True
    engine._prev_day_factors = pd.DataFrame([{"ID_QI": "000001", "factor": 1.0}])
    engine._daily_basic_df = pd.DataFrame()
    engine._trading_universe_df = None
    engine.portfolio_context_fn = lambda *_: SimpleNamespace(
        positions=pd.DataFrame(), meta={"positions_usable": False}
    )

    engine._calculate_open_position_targets()

    assert target_calls == []
    assert engine._open_position_cache is None


def test_open_position_does_not_use_legacy_fallback_when_targets_fail():
    engine = NativeEngine.__new__(NativeEngine)
    engine.trading_day = "20260807"
    engine._open_position_cache = None
    engine._open_position_cache_lock = native_engine_module.threading.Lock()
    engine._open_position_cache_generation = 0
    engine.output_path = None
    engine._calculate_open_position_targets = lambda *_args: None
    fallback_calls = []
    engine._write_results = lambda *args, **kwargs: fallback_calls.append(True)

    schedule = SimpleNamespace(
        name="open_position",
        result_label="093000",
        skip_factor_compute=True,
        is_daily_result=False,
    )
    engine._compute_and_output_locked(schedule)

    assert fallback_calls == []
