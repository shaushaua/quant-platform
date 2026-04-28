# -*- coding: utf-8 -*-
"""
因子基类

交易员有两种使用方式：

方式一（函数式，推荐）：
    直接写 factor_calculation 函数 + outfun 函数，配合
    calc_factors_by_date_range 使用，无需继承 BaseFactor。

    示例见 engine.py 顶部注释。

方式二（面向对象）：
    继承 BaseFactor，实现 compute() 方法。
    compute() 返回 float（单只股票单时点的因子值）。

    class MomentumFactor(BaseFactor):
        name = "momentum_20d"
        market_count = 21

        def compute(self, data, code, date, end_time):
            df = data["market"]
            if len(df) < 21:
                return float("nan")
            return float(df["close"].iloc[-1] / df["close"].iloc[-21] - 1)
"""

from dataclasses import dataclass, field
import pandas as pd


@dataclass
class StockData:
    """
    单只股票、单个时间切片的全量数据。
    由引擎在每次调用 factor_calculation / compute() 前填充。

    支持字典式访问：data["l2_order"]，也支持属性访问：data.l2_order。

    历史窗口模式：
        当 factor_info 设置 lookback_days > 0 时，l2_order_hist/l2_deal_hist/l1_tick_hist
        包含历史 N 天的数据列表，列表索引 0 为最早日期，索引 -1 为当天。
        例如 lookback_days=5，计算 20250110：
            l2_deal_hist = [df_20250106, df_20250107, df_20250108, df_20250109, df_20250110]
    """
    code: str
    date: str
    end_time: str

    # L2 逐笔委托
    l2_order: pd.DataFrame = field(default_factory=pd.DataFrame)
    # L2 逐笔成交
    l2_deal: pd.DataFrame = field(default_factory=pd.DataFrame)
    # L1 Tick 快照
    l1_tick: pd.DataFrame = field(default_factory=pd.DataFrame)
    # 历史日频行情（用于 market_count 日回溯窗口）
    market: pd.DataFrame = field(default_factory=pd.DataFrame)
    # 当日日频基础数据（daily_basic：close / turnover / volume 等）
    daily_basic: pd.DataFrame = field(default_factory=pd.DataFrame)

    # 历史窗口列表（当 lookback_days > 0 时启用）
    # 每个字段都是 list[pd.DataFrame]，按日期升序排列
    l2_order_hist: list = field(default_factory=list)
    l2_deal_hist: list = field(default_factory=list)
    l1_tick_hist: list = field(default_factory=list)

    def __getitem__(self, key: str) -> pd.DataFrame:
        """
        支持 data["l2_order"] 写法，与函数式接口兼容。
        """
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def get(self, key: str, default=None):
        """类似 dict.get，键不存在时返回 default。"""
        try:
            return getattr(self, key)
        except AttributeError:
            return default


class BaseFactor:
    """
    面向对象因子基类（可选）。

    类属性（在子类中覆盖）：
        name            因子唯一名称
        market_count    需要几日历史日频数据（0 = 不需要）
        l2_order_count  是否需要 L2 委托数据（0/1）
        l2_deal_count   是否需要 L2 成交数据（0/1）
        l2_tick_count   是否需要 L1 Tick 数据（0/1）
    """

    name: str = ""
    market_count: int = 0
    l2_order_count: int = 0
    l2_deal_count: int = 0
    l2_tick_count: int = 0

    @property
    def factor_info(self) -> dict:
        """生成可直接传入 calc_factors_by_date_range 的 factor_info 字典。"""
        return {
            "func": self.compute,
            "market_count": self.market_count,
            "l2_order_count": self.l2_order_count,
            "l2_deal_count": self.l2_deal_count,
            "l2_tick_count": self.l2_tick_count,
        }

    def on_init(self) -> None:
        """可选钩子：引擎启动时调用一次，用于加载模型等初始化操作。"""

    def compute(self, data: StockData, code: str, date: str, end_time: str) -> float:
        """
        计算单只股票、单个时间切片的因子值。

        Args:
            data:     StockData，包含 l2_order / l2_deal / l1_tick / market / daily_basic
            code:     股票代码，如 "000001.SZ"
            date:     交易日，如 "20240105"
            end_time: 时间切片，如 "0925-0925"

        Returns:
            float，因子值；无法计算时返回 float("nan"）。
            也可以返回 dict，此时该股票贡献多个因子列。
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 compute()")

    def handle_output(self, date: str, end_time: str, results: pd.DataFrame) -> None:
        """
        可选钩子：每个 date × end_time 批次结束后调用。
        默认空实现；子类可覆盖以自定义持久化或推送。

        Args:
            date:     交易日
            end_time: 时间切片
            results:  该批次所有股票结果合并的 DataFrame
        """
