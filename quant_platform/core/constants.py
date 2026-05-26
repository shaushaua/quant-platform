# -*- coding: utf-8 -*-
"""
数据列定义
参考 convert_data.py 的数据格式标准
"""

import numpy as np
import pyarrow as pa

# ==================== 逐笔委托列定义 ====================
ORDER_COLUMNS = [
    'TradingDay',   # 交易日期
    'Code',         # 股票代码 (000001.XSHE / 600000.XSHG)
    'Time',         # 委托时间 (datetime)
    'UpdateTime',   # 更新时间 (datetime)
    'OrderID',      # 委托编号
    'Side',         # 买卖方向 (0=买, 1=卖)
    'Price',        # 委托价格
    'Volume',       # 委托数量
    'OrderType',    # 委托类型 (深市:1/2/3, 沪市:2/5)
    'Channel',      # 通道编号
    'SeqNum',       # 序列号
]

ORDER_DTYPE = {
    "TradingDay": np.dtype('object'),
    "Code": np.dtype('object'),
    "Time": np.dtype('datetime64[ns]'),
    "UpdateTime": np.dtype('datetime64[ns]'),
    "OrderID": np.dtype('int64'),
    "Side": np.dtype('int16'),
    "Price": np.dtype('float64'),
    "Volume": np.dtype('float64'),
    "OrderType": np.dtype('int16'),
    "Channel": np.dtype('int64'),
    "SeqNum": np.dtype('int64'),
}

# ==================== 逐笔成交列定义 ====================
DEAL_COLUMNS = [
    'TradingDay',       # 交易日期
    'Code',             # 股票代码
    'Time',             # 成交时间 (datetime)
    'UpdateTime',       # 更新时间 (datetime)
    'SaleOrderID',      # 卖方委托编号
    'BuyOrderID',       # 买方委托编号
    'Side',             # 买卖方向 (0=买, 1=卖, 10=未知, 4=深市融资)
    'Price',            # 成交价格
    'Volume',           # 成交数量
    'Money',            # 成交金额
    'Channel',          # 通道编号
    'SeqNum',           # 序列号
]

DEAL_DTYPE = {
    "TradingDay": np.dtype('object'),
    "Code": np.dtype('object'),
    "Time": np.dtype('datetime64[ns]'),
    "UpdateTime": np.dtype('datetime64[ns]'),
    "SaleOrderID": np.dtype('int64'),
    "BuyOrderID": np.dtype('int64'),
    "Side": np.dtype('int16'),
    "Price": np.dtype('float64'),
    "Volume": np.dtype('float64'),
    "Money": np.dtype('float64'),
    "Channel": np.dtype('int64'),
    "SeqNum": np.dtype('int64'),
}

# ==================== Tick快照列定义 ====================
TICK_COLUMNS = [
    'TradingDay',       # 交易日期
    'Code',             # 股票代码
    'Time',             # 快照时间 (datetime)
    'UpdateTime',       # 更新时间 (datetime)
    'CurrentPrice',     # 当前价
    'TotalVolume',      # 成交总量
    'TotalMoney',       # 成交总金额
    'PreClosePrice',    # 昨收价
    'OpenPrice',        # 开盘价
    'HighestPrice',     # 最高价
    'LowestPrice',      # 最低价
    'HighLimitPrice',   # 涨停价
    'LowLimitPrice',    # 跌停价
    'IOPV',             # IOPV净值估值
    'TradeNum',         # 成交笔数
    'TotalBidVolume',   # 委托买入总量
    'TotalAskVolume',   # 委托卖出总量
    'AvgBidPrice',      # 加权平均委买价
    'AvgAskPrice',      # 加权平均委卖价
    # 卖价档位 (10档)
    'AskPrice1', 'AskPrice2', 'AskPrice3', 'AskPrice4', 'AskPrice5',
    'AskPrice6', 'AskPrice7', 'AskPrice8', 'AskPrice9', 'AskPrice10',
    # 卖量档位
    'AskVolume1', 'AskVolume2', 'AskVolume3', 'AskVolume4', 'AskVolume5',
    'AskVolume6', 'AskVolume7', 'AskVolume8', 'AskVolume9', 'AskVolume10',
    # 卖委托笔数
    'AskNum1', 'AskNum2', 'AskNum3', 'AskNum4', 'AskNum5',
    'AskNum6', 'AskNum7', 'AskNum8', 'AskNum9', 'AskNum10',
    # 买价档位 (10档)
    'BidPrice1', 'BidPrice2', 'BidPrice3', 'BidPrice4', 'BidPrice5',
    'BidPrice6', 'BidPrice7', 'BidPrice8', 'BidPrice9', 'BidPrice10',
    # 买量档位
    'BidVolume1', 'BidVolume2', 'BidVolume3', 'BidVolume4', 'BidVolume5',
    'BidVolume6', 'BidVolume7', 'BidVolume8', 'BidVolume9', 'BidVolume10',
    # 买委托笔数
    'BidNum1', 'BidNum2', 'BidNum3', 'BidNum4', 'BidNum5',
    'BidNum6', 'BidNum7', 'BidNum8', 'BidNum9', 'BidNum10',
    "Channel",          # 通道编号
    "SeqNum",           # 序列号
]

# Tick数据类型映射
TICK_DTYPE_COMMON = {
    "TradingDay": np.dtype('object'),
    "Code": np.dtype('object'),
    "Time": np.dtype('datetime64[ns]'),
    "UpdateTime": np.dtype('datetime64[ns]'),
    "Channel": np.dtype('int64'),
    "SeqNum": np.dtype('int64'),
}

# 其他字段都是 float64
TICK_DTYPE = TICK_DTYPE_COMMON.copy()
for col in TICK_COLUMNS:
    if col not in TICK_DTYPE:
        TICK_DTYPE[col] = np.dtype('float64')

# ==================== Arrow Schema（collector 热路径直写，跳过 pandas） ====================

def _build_arrow_schema(columns: list, dtype_map: dict) -> pa.Schema:
    _ARROW_TYPE_MAP = {
        np.dtype('object'): pa.string(),
        np.dtype('datetime64[ns]'): pa.timestamp('ns'),
        np.dtype('float64'): pa.float64(),
        np.dtype('int64'): pa.int64(),
        np.dtype('int32'): pa.int32(),
        np.dtype('int16'): pa.int16(),
    }
    fields = []
    for col in columns:
        arrow_type = _ARROW_TYPE_MAP.get(dtype_map[col], pa.float64())
        fields.append(pa.field(col, arrow_type))
    return pa.schema(fields)


ORDER_ARROW_SCHEMA = _build_arrow_schema(ORDER_COLUMNS, ORDER_DTYPE)
DEAL_ARROW_SCHEMA = _build_arrow_schema(DEAL_COLUMNS, DEAL_DTYPE)
TICK_ARROW_SCHEMA = _build_arrow_schema(TICK_COLUMNS, TICK_DTYPE)

# schema by kind，方便 collector 按类型查找
ARROW_SCHEMA_BY_KIND = {
    "tick": TICK_ARROW_SCHEMA,
    "order": ORDER_ARROW_SCHEMA,
    "deal": DEAL_ARROW_SCHEMA,
}

# ==================== 日频基础数据列定义 ====================
DAILY_BASIC_COLUMNS = [
    'TradingDay',       # 交易日期
    'Code',             # 股票代码
    'Open',             # 开盘价
    'High',             # 最高价
    'Low',              # 最低价
    'Close',            # 收盘价
    'Volume',           # 成交量
    'Amount',           # 成交额
    'Turnover',         # 换手率
    'PreClose',         # 昨收价
    'HighLimit',        # 涨停价
    'LowLimit',         # 跌停价
    'TotalShares',      # 总股本
    'FloatShares',      # 流通股本
    'PE',               # 市盈率
    'PB',               # 市净率
    'MarketCap',        # 总市值
    'CirculationCap',   # 流通市值
]

# ==================== K线列定义 ====================
KLINE_COLUMNS = [
    'TradingDay',       # 交易日期
    'Code',             # 股票代码
    'Time',             # K线时间
    'Open',             # 开盘价
    'High',             # 最高价
    'Low',              # 最低价
    'Close',            # 收盘价
    'Volume',           # 成交量
    'Amount',           # 成交额
    'Vwap',             # 成交均价
]

# ==================== 数据类型常量 ====================
DATA_TYPES = {
    'TICK': 'tick',
    'ORDER': 'order',
    'DEAL': 'deal',
    'DAILY_BASIC': 'daily_basic',
    'KLINE_1MIN': '1min',
    'KLINE_5MIN': '5min',
    'KLINE_10MIN': '10min',
    'KLINE_30MIN': '30min',
    'KLINE_DAY': 'day',
}

# ==================== 市场代码 ====================
MARKET_SH = 'XSHG'  # 上海
MARKET_SZ = 'XSHE'  # 深圳

# ==================== 委托类型映射 ====================
# 深市委托类型
SZ_ORDER_TYPES = {
    49: 1,   # 限价委托
    50: 2,   # 市价委托
    85: 3,   # 本方最优
}

# 沪市委托类型
SH_ORDER_TYPES = {
    "A": 2,  # 增加委托
    "D": 5,  # 删除委托
}

# ==================== 买卖方向映射 ====================
# 深市买卖方向
SZ_SIDES = {
    49: 0,   # 买
    50: 1,   # 卖
}

# 沪市买卖方向
SH_SIDES = {
    "B": 0,  # 买
    "S": 1,  # 卖
}

def format_code(raw_code: str, market: str = None) -> str:
    """
    格式化股票代码为标准格式

    Args:
        raw_code: 原始代码 (如 000001, 600000)
        market: 市场 (XSHG/XSHE)，为None时自动判断

    Returns:
        标准格式代码 (如 000001.XSHE, 600000.XSHG)
    """
    code = str(raw_code).zfill(6)

    if market is None:
        # 自动判断市场
        if code.startswith(('6', '9', '68')):
            market = MARKET_SH
        else:
            market = MARKET_SZ

    return f"{code}.{market}"


def parse_code(formatted_code: str) -> tuple:
    """
    解析股票代码

    Args:
        formatted_code: 标准格式代码 (如 000001.XSHE)

    Returns:
        (code, market) 元组 (如 ('000001', 'XSHE'))
    """
    parts = formatted_code.split('.')
    if len(parts) == 2:
        return parts[0], parts[1]
    return formatted_code, None
