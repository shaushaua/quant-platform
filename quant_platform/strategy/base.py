# -*- coding: utf-8 -*-
"""
策略基类
交易员继承此类实现自己的策略
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from datetime import datetime

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class BaseStrategy(ABC):
    """
    策略基类

    交易员需要实现以下方法:
    - on_init(): 初始化时调用
    - on_bar(): 每根K线时调用
    - on_end(): 结束时调用

    可用方法:
    - self.data: DataAPI 实例
    - self.context: 回测上下文 (仅回测模式)
    - self.buy(): 买入
    - self.sell(): 卖出
    - self.get_position(): 获取持仓
    """

    def __init__(self):
        self.data = None  # DataAPI
        self.context = None  # BacktestContext (仅回测模式)
        self._initialized = False

    def set_data_api(self, data_api):
        """设置数据API"""
        self.data = data_api

    def set_context(self, context):
        """设置回测上下文"""
        self.context = context

    def init(self):
        """初始化策略"""
        if not self._initialized:
            self.on_init()
            self._initialized = True
            logger.info(f"策略初始化完成: {self.__class__.__name__}")

    @abstractmethod
    def on_init(self):
        """
        初始化时调用
        用于设置策略参数、加载初始数据等
        """
        pass

    @abstractmethod
    def on_bar(self, bar_data: pd.DataFrame):
        """
        每根K线时调用

        Args:
            bar_data: K线数据，包含所有股票的当期数据
        """
        pass

    def on_end(self):
        """
        结束时调用
        用于生成最终信号、清理资源等
        """
        pass

    # ==================== 交易方法 ====================

    def buy(self, code: str, volume: float, price: Optional[float] = None) -> bool:
        """
        买入股票

        Args:
            code: 股票代码
            volume: 买入数量
            price: 买入价格 (回测模式可省略)

        Returns:
            是否成功
        """
        if self.context is None:
            logger.warning("非回测模式，无法执行买入")
            return False

        return self.context.buy(code, volume, price)

    def sell(self, code: str, volume: Optional[float] = None, price: Optional[float] = None) -> bool:
        """
        卖出股票

        Args:
            code: 股票代码
            volume: 卖出数量，None表示全部卖出
            price: 卖出价格 (回测模式可省略)

        Returns:
            是否成功
        """
        if self.context is None:
            logger.warning("非回测模式，无法执行卖出")
            return False

        if volume is None:
            volume = self.get_position(code)

        return self.context.sell(code, volume, price)

    def get_position(self, code: str) -> float:
        """获取当前持仓"""
        if self.context is None:
            return 0.0
        return self.context.positions.get(code, 0.0)

    def get_all_positions(self) -> Dict[str, float]:
        """获取所有持仓"""
        if self.context is None:
            return {}
        return self.context.positions.copy()

    # ==================== 辅助方法 ====================

    def log(self, message: str, level: str = "info"):
        """记录日志"""
        if level == "debug":
            logger.debug(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)
        else:
            logger.info(message)


class AlphaStrategy(BaseStrategy):
    """
    Alpha因子策略基类
    用于生成Alpha信号的策略

    需要实现:
    - calculate_alpha(): 计算Alpha因子
    """

    def __init__(self):
        super().__init__()
        self._alpha_cache: Dict[str, pd.DataFrame] = {}

    @abstractmethod
    def calculate_alpha(self, data: pd.DataFrame) -> pd.Series:
        """
        计算Alpha因子

        Args:
            data: 输入数据

        Returns:
            Alpha值序列 (index为股票代码)
        """
        pass

    def on_bar(self, bar_data: pd.DataFrame):
        """处理K线数据"""
        # 计算Alpha
        alpha = self.calculate_alpha(bar_data)

        if alpha is None or alpha.empty:
            return

        # 根据Alpha生成交易信号
        self._generate_signals(alpha)

    def _generate_signals(self, alpha: pd.Series):
        """
        根据Alpha生成交易信号

        Args:
            alpha: Alpha值序列
        """
        # 获取当前持仓
        positions = self.get_all_positions()

        # 简单的顶部/底部策略
        # 买入Alpha最高的N只股票
        # 卖出Alpha最低的持仓

        top_n = 10  # 买入前10只

        # Alpha排序
        alpha_sorted = alpha.sort_values(ascending=False)

        # 买入信号
        buy_codes = alpha_sorted.head(top_n).index.tolist()

        # 卖出信号
        sell_codes = [
            code for code in positions.keys()
            if code not in buy_codes
        ]

        # 执行交易
        for code in sell_codes:
            self.sell(code)

        # 分配资金买入
        if buy_codes:
            capital_per_stock = self.context.capital / len(buy_codes) if self.context else 0
            for code in buy_codes:
                if self.get_position(code) == 0:
                    # 简单估计股数
                    self.buy(code, 1000)


class MomentumStrategy(AlphaStrategy):
    """
    动量策略示例
    基于过去N天的收益率进行选股
    """

    def __init__(self, lookback_days: int = 5, top_n: int = 10):
        super().__init__()
        self.lookback_days = lookback_days
        self.top_n = top_n

    def on_init(self):
        """初始化"""
        self.log(f"动量策略初始化: lookback={self.lookback_days}, top_n={self.top_n}")

    def calculate_alpha(self, data: pd.DataFrame) -> pd.Series:
        """
        计算动量因子

        Args:
            data: 当日K线数据

        Returns:
            动量值 (过去N天累计收益率)
        """
        if self.data is None:
            return pd.Series()

        # 获取历史数据
        current_date = self.context.current_date if self.context else datetime.now().strftime("%Y%m%d")

        # 简化: 使用当日涨跌幅作为动量代理
        if "Close" in data.columns and "PreClose" in data.columns:
            momentum = (data["Close"] / data["PreClose"] - 1).fillna(0)
            momentum.index = data["Code"]
            return momentum

        return pd.Series()


class MeanReversionStrategy(AlphaStrategy):
    """
    均值回归策略示例
    基于偏离均值的程度进行选股
    """

    def __init__(self, window: int = 20, top_n: int = 10):
        super().__init__()
        self.window = window
        self.top_n = top_n

    def on_init(self):
        """初始化"""
        self.log(f"均值回归策略初始化: window={self.window}, top_n={self.top_n}")

    def calculate_alpha(self, data: pd.DataFrame) -> pd.Series:
        """
        计算均值回归因子

        Args:
            data: 当日K线数据

        Returns:
            偏离度 (负值表示超跌，正值表示超涨)
        """
        if data.empty:
            return pd.Series()

        # 简化: 使用价格相对位置
        if "Close" in data.columns and "High" in data.columns and "Low" in data.columns:
            # 计算价格在当日范围内的位置
            price_position = (data["Close"] - data["Low"]) / (data["High"] - data["Low"] + 1e-10)
            # 反转信号 (低位置 = 超跌 = 买入)
            alpha = 0.5 - price_position
            alpha.index = data["Code"]
            return alpha

        return pd.Series()
