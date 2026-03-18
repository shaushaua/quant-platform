# -*- coding: utf-8 -*-
"""
因子基类

交易员继承 BaseFactor，实现 compute() 方法。
历史批量计算和实盘计算使用相同接口。

示例:
    from quant_platform.factor.base import BaseFactor, FactorData
    import pandas as pd

    class MomentumFactor(BaseFactor):
        name = "momentum_20d"

        def compute(self, date: str, data: FactorData) -> pd.Series:
            df = data.daily_basic
            # index=stock_code, value=因子值
            return df["close"] / df["close"].shift(20) - 1
"""

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd


@dataclass
class FactorData:
    """
    单个交易日的四份数据。
    字段均为 DataFrame，index 为 stock_code。
    未订阅或当日无数据时为空 DataFrame。
    """
    date: str
    daily_basic: pd.DataFrame = field(default_factory=pd.DataFrame)
    tick: pd.DataFrame = field(default_factory=pd.DataFrame)
    order: pd.DataFrame = field(default_factory=pd.DataFrame)
    deal: pd.DataFrame = field(default_factory=pd.DataFrame)


class BaseFactor:
    """
    因子基类。

    子类必须：
    - 设置类属性 name（唯一标识，用于 OSS 路径）
    - 实现 compute() 方法

    子类可选：
    - 覆盖 on_init() 做初始化（如加载外部参数）
    - 设置 required_data 声明需要哪几份数据（减少加载开销）
    """

    # 因子唯一名称，写入 OSS 路径，必须设置
    name: str = ""

    # 声明需要哪些数据源，减少不必要的 IO
    # 可选值: "daily_basic", "tick", "order", "deal"
    required_data: tuple = ("daily_basic",)

    def on_init(self) -> None:
        """可选：因子初始化，在第一次 compute 前调用一次。"""

    def compute(self, date: str, data: FactorData) -> pd.Series:
        """
        计算单个交易日的因子值。

        Args:
            date: 交易日，格式 YYYY-MM-DD
            data: 当日四份数据

        Returns:
            pd.Series，index 为 stock_code（如 "000001.XSHE"），
            value 为因子值（float）。缺失用 NaN 填充。
        """
        raise NotImplementedError(f"{self.__class__.__name__} 必须实现 compute()")
