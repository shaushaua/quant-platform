# -*- coding: utf-8 -*-
"""
示例因子：20日动量因子

交易员上传的因子代码模板。
文件中必须包含一个 BaseFactor 子类。
"""

import pandas as pd
from quant_platform.factor import BaseFactor, FactorData


class MomentumFactor(BaseFactor):
    """
    20 日动量因子
    定义：当日收盘价 / 20 日前收盘价 - 1
    """

    name = "momentum_20d"
    required_data = ("daily_basic",)  # 只需要日线数据

    def compute(self, date: str, data: FactorData) -> pd.Series:
        """
        Args:
            date: 当前计算日期 'YYYY-MM-DD'
            data: FactorData，包含 daily_basic / tick / order / deal

        Returns:
            pd.Series，index=stock_code，value=因子值
        """
        df = data.daily_basic
        if df.empty:
            return pd.Series(dtype=float)

        # daily_basic 预期列：stock_code, close, ...
        if "close" not in df.columns or "stock_code" not in df.columns:
            return pd.Series(dtype=float)

        # 这里简化处理：直接用当日收盘价作为动量代理
        # 生产环境应从 OSS 加载 T-20 的数据做比值
        result = df.set_index("stock_code")["close"]
        return result.rename(date)
