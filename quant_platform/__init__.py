# -*- coding: utf-8 -*-
"""
量化策略平台

精简版量化策略平台，专注于实盘交易和策略回测
"""

__version__ = "2.0.0"
__author__ = "Quant Platform Team"

# 导出新平台的所有接口
from .quant_platform import *

__all__ = [
    "DataAPI",
    "MemoryStore",
    "OSSDataLoader",
    "TonglanceDataConverter",
    "TonglanceCollector",
    "BacktestEngine",
    "BaseStrategy",
    "AlphaStrategy",
    "run_backtest",
    "get_config",
    "create_realtime_api",
    "create_backtest_api",
    "create_collector",
]
