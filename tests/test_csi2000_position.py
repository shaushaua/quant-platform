# -*- coding: utf-8 -*-
"""csi2000_position(14:50 盘中调仓)核心链路测试。

Mock 掉 OSS / inference / 收盘价 / 飞书, 只验证编排逻辑:
  1. 因子来自本地 {td}_145000.csv(不调 OSS load_daily_factors), 现场推理
     后恰好一次 push_orders(不双重推送)
  2. 资金预检: 买额超预算 → 按金额降序截断买单
  3. 因子文件未出现 → 超时告警放弃(全指 fail 则 2000 同步 fail)
  4. run_csi2000_open 抽出共用执行链后行为不变(同一实盘链推单/落盘)
"""
from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ── Linux .so / 云依赖 stub(与 tests/test_risk_validation.py 同一套) ──
# calc_predict_tree_model.so 是 Linux 二进制, macOS 无法 dlopen;
# oss2 在单测里禁网。先种进 sys.modules 再 import 被测模块。
for _k in ("OSS_ACCESS_KEY_ID", "OSS_ACCESS_KEY_SECRET"):
    os.environ.setdefault(_k, "test-stub")
sys.modules["oss2"] = types.ModuleType("oss2")
_stub_oss2 = sys.modules["oss2"]


class _StubAuth:
    def __init__(self, *a, **k):
        pass


class _StubObject:
    def __init__(self, key):
        self.key = key

    def read(self):
        raise AssertionError("network disabled in tests")


class _StubBucket:
    def __init__(self, *a, **k):
        pass

    def get_object(self, key):
        return _StubObject(key)

    def head_object(self, key):
        raise AssertionError("network disabled in tests")


_stub_oss2.Auth = _StubAuth
_stub_oss2.Bucket = _StubBucket

if not (Path(__file__).resolve().parent / "turnover_budget.py").exists():
    _budget = types.ModuleType("turnover_budget")
    _budget.dynamic_optimizer_max_turnover = lambda *a, **k: 0.35
    sys.modules["turnover_budget"] = _budget

_calc = types.ModuleType("calc_predict_tree_model")
_calc.POSITION_COLUMNS = ["code", "position"]
for _name in ["build_optimizer_exposure_frame", "extract_current_position_weights",
              "normalize_daily_feature_czhou1_keys", "normalize_date_column",
              "normalize_universe_codes", "predict_tree_model", "resolve_signal_date"]:
    setattr(_calc, _name, lambda *a, **k: None)
_calc.normalize_benchmark_weights = lambda w: None
sys.modules["calc_predict_tree_model"] = _calc

from quant_platform.inference import csi2000_product as cp  # noqa: E402
from quant_platform.inference import tree_model_inference  # noqa: E402,F401  (patch 目标需先可寻址)
from quant_platform.utils import notify as _notify  # noqa: E402,F401  (同上)


class _Ctx:
    def __init__(self, account: pd.DataFrame, positions: pd.DataFrame):
        self.account = account
        self.positions = positions


class _CountingBroker:
    """记录 push 次数的实盘 broker 替身(name 非 virtual → 跳过虚盘写持仓)。"""

    name = "yinhe"

    def __init__(self, ctx: _Ctx):
        self.ctx = ctx
        self.pushed: list[pd.DataFrame] = []

    def get_positions(self, trading_day: str) -> _Ctx:
        return self.ctx

    def push_orders(self, orders: pd.DataFrame, trading_day: str) -> bool:
        self.pushed.append(orders.copy())
        return True


def _factor_file(tmp: Path, td: str, rows: int = 150) -> Path:
    df = pd.DataFrame({
        "code": [f"{600000 + i}" for i in range(rows)],
        "F_TEST_150000": [float(i) for i in range(rows)],
    })
    path = tmp / f"{td}_145000.csv"
    df.to_csv(path, index=False)
    return path


def _account(total: float, avail: float) -> pd.DataFrame:
    return pd.DataFrame({"total_asset": [total], "available_cash": [avail]})


def _positions(rows: dict) -> pd.DataFrame:
    return pd.DataFrame({"code": list(rows), "qty": list(rows.values())})


@mock.patch("quant_platform.utils.notify.feishu_alert")
@mock.patch("quant_platform.inference.tree_model_inference.inference")
@mock.patch("quant_platform.inference.csi2000_product.load_daily_factors")
@mock.patch("quant_platform.inference.csi2000_product.load_composition_csi2000")
@mock.patch("quant_platform.inference.csi2000_product._load_close_prices")
class TestCsi2000Position(unittest.TestCase):

    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp())
        self.td = datetime.now().strftime("%Y%m%d")
        # 引擎侧环境变量(仅构造 OSS 对象所需, 网络访问全部被 mock)
        self._env = mock.patch.dict(os.environ, {
            "OSS_ACCESS_KEY_ID": "test", "OSS_ACCESS_KEY_SECRET": "test",
            "OSS_ENDPOINT": "oss-test", "CSI2000_BROKER": "yinhe",
        })
        self._env.start()
        self.addCleanup(self._env.stop)
        self._outdir = mock.patch.object(cp, "OUT_DIR", self.tmp)
        self._outdir.start()
        self.addCleanup(self._outdir.stop)

    def test_position_pushes_once_with_local_factor(self, m_px, m_comp, m_load, m_inf, m_feishu):
        m_px.return_value = {"000001": 10.0, "000002": 20.0,
                             "300003": 5.0, "600519": 100.0}
        m_comp.return_value = pd.DataFrame({"ID_QI": ["000001", "000002", "300003"]})
        targets = pd.DataFrame({
            "code": ["000001.XSHE", "000002.XSHE", "300003.XSHE"],
            "position": [0.10, 0.20, 0.05],
        })
        m_inf.return_value = targets

        fpath = _factor_file(self.tmp, self.td)
        broker = _CountingBroker(_Ctx(
            _account(10_000_000, 1_000_000),
            _positions({"000001": 50_000, "000002": 130_000, "600519": 2_000}),
        ))

        orders = cp.run_csi2000_position(self.td, broker=broker,
                                         factor_file=str(fpath), wait_seconds=5)

        # 推送恰好一次
        self.assertEqual(len(broker.pushed), 1)
        got = {(r.code, r.side, int(r.volume)) for r in broker.pushed[0].itertuples()}
        self.assertEqual(got, {
            ("000001", "buy", 50_000),
            ("300003", "buy", 100_000),
            ("000002", "sell", 30_000),
            ("600519", "sell", 2_000),
        })
        # 推的是同一份订单
        pd.testing.assert_frame_equal(orders, broker.pushed[0])
        # 因子来自本地文件: OSS 加载未被调用, 文件内容原样传给 inference
        m_load.assert_not_called()
        _, kwargs = m_inf.call_args
        self.assertEqual(len(kwargs["prev_day_factors_df"]), 150)
        self.assertEqual(kwargs["benchmark_code"], "932000")
        # 订单落盘
        self.assertTrue((self.tmp / f"csi2000_position_orders_{self.td}.csv").is_file())
        # 资金充足 → 无截断告警
        m_feishu.assert_not_called()

    def test_position_truncates_buys_when_budget_short(self, m_px, m_comp, m_load, m_inf, m_feishu):
        m_px.return_value = {"000001": 10.0, "000002": 20.0}
        m_comp.return_value = pd.DataFrame({"ID_QI": ["000001", "000002"]})
        m_inf.return_value = pd.DataFrame({
            "code": ["000001.XSHE", "000002.XSHE"],
            "position": [0.50, 0.30],   # 买额 500万 + 300万, 预算仅 400万
        })

        fpath = _factor_file(self.tmp, self.td)
        broker = _CountingBroker(_Ctx(_account(10_000_000, 4_000_000),
                                      _positions({})))

        cp.run_csi2000_position(self.td, broker=broker,
                                factor_file=str(fpath), wait_seconds=5)

        self.assertEqual(len(broker.pushed), 1)
        orders = broker.pushed[0]
        buys = orders[orders["side"] == "buy"]
        # 只保留金额大的那一笔(按笔数截断, 与 9:30 open 同一规则)
        self.assertEqual(len(buys), 1)
        self.assertEqual(buys.iloc[0]["code"], "000001")
        m_feishu.assert_called_once()   # 截断告警

    def test_position_aborts_when_factor_missing(self, m_px, m_comp, m_load, m_inf, m_feishu):
        missing = self.tmp / f"{self.td}_145000.csv"
        broker = _CountingBroker(_Ctx(_account(10_000_000, 1_000_000), _positions({})))

        with self.assertRaises(RuntimeError):
            cp.run_csi2000_position(self.td, broker=broker,
                                    factor_file=str(missing), wait_seconds=1)

        self.assertEqual(len(broker.pushed), 0)      # 未推任何单
        m_inf.assert_not_called()                     # 未做推理
        m_feishu.assert_called_once()                 # 超时告警发出

    def test_open_refactor_unchanged(self, m_px, m_comp, m_load, m_inf, m_feishu):
        """run_csi2000_open 抽出 _push_targets_live 后: T-1 因子 → 推一次 → 落盘 open 订单。"""
        m_px.return_value = {"000001": 10.0}
        m_comp.return_value = pd.DataFrame({"ID_QI": ["000001"]})
        m_inf.return_value = pd.DataFrame({"code": ["000001.XSHE"], "position": [0.10]})
        m_load.return_value = pd.DataFrame({"code": ["000001"], "F_TEST_150000": [1.0]})

        broker = _CountingBroker(_Ctx(_account(10_000_000, 1_000_000), _positions({})))

        # OSS T-1 探测: stub 桶 head_object 必抛 → factor_date 回退 7 天
        orders = cp.run_csi2000_open(self.td, broker=broker)

        self.assertEqual(len(broker.pushed), 1)
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders.iloc[0]["code"], "000001")
        self.assertEqual(orders.iloc[0]["side"], "buy")
        # T-1 因子确实经 load_daily_factors 加载(带回退后的日期)
        m_load.assert_called_once()
        self.assertTrue((self.tmp / f"csi2000_open_orders_{self.td}.csv").is_file())

    def test_open_override_skips_inference_and_drops_limitup(self, m_px, m_comp, m_load, m_inf, m_feishu):
        """预计算 targets 直传: 跳过现场推理; 未持有已涨停的买单被剔除。"""
        m_px.return_value = {"000001": 10.0, "000002": 9.5}
        override = pd.DataFrame({
            "code": ["000001.XSHE", "000002.XSHE"],
            "position": [0.50, 0.10],   # 000001 实时价 10.0 ≥ 涨停 10.0 → 剔除
        })
        broker = _CountingBroker(_Ctx(_account(10_000_000, 1_000_000), _positions({})))

        orders = cp.run_csi2000_open(
            self.td, broker=broker, targets_override=override,
            latest_prices={"000001": 10.0, "000002": 9.5},
            limit_prices={"000001": (10.0, 9.0), "000002": (10.5, 9.5)})

        m_inf.assert_not_called()       # 预计算已算, 未做现场推理
        m_load.assert_not_called()      # 未加载 OSS 因子
        self.assertEqual(len(broker.pushed), 1)
        got = {(r.code, r.side, int(r.volume)) for r in broker.pushed[0].itertuples()}
        self.assertEqual(got, {("000002", "buy", 105_200)})

    def test_resolve_factor_date_falls_back(self, *mocks):
        """OSS 全部 404 时回退 7 天(stub 桶 head 必抛)。"""
        from datetime import datetime as dt, timedelta
        fd = cp.resolve_recent_factor_date(self.td)
        expect = (dt.strptime(self.td, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        self.assertEqual(fd, expect)

    def test_schedule_registered(self, *mocks):
        """COMPUTE_SCHEDULES 含 csi2000_position 时按 14:50/15:00 注册。"""
        from quant_platform.live_engine.schedule import build_schedules_from_env
        with mock.patch.dict(os.environ, {
            "COMPUTE_SCHEDULES": "minute,csi2000_signal,csi2000_open,csi2000_position",
            "CSI2000_POSITION_TRIGGER_TIME": "14:50",
            "CSI2000_POSITION_DEADLINE": "15:00",
        }):
            scheds = [s for s in build_schedules_from_env()
                      if s.name == "csi2000_position"]
        self.assertEqual(len(scheds), 1)
        s = scheds[0]
        self.assertEqual(s.trigger_times, [(14, 50)])
        self.assertEqual(s.trigger_deadline, (15, 0))
        self.assertFalse(s.run_inference)


if __name__ == "__main__":
    unittest.main()
