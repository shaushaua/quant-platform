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
from typing import Optional
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

    # 实盘模式：累计聚合状态（回测时为 None）
    state: Optional['StockState'] = None

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


@dataclass
class StockState:
    """
    单只股票的聚合状态（用于实盘流式计算）。

    由 StreamingEngine 维护，每只股票约 200 bytes。
    交易员的 factor_calculation(state, code, date, end_time) 直接读取此对象。

    回测兼容：通过 from_stock_data() 从 StockData 提取聚合值。
    """
    code: str

    # --- VWAP ---
    cum_amount: float = 0.0    # sum(Price * Volume)
    cum_volume: int = 0        # 累计成交量

    # --- 价格 ---
    latest_price: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = float('inf')
    pre_close: float = 0.0

    # --- 买卖盘（最新快照）---
    ask1: float = 0.0
    bid1: float = 0.0
    ask_volume1: int = 0
    bid_volume1: int = 0

    # --- 计数 ---
    deal_count: int = 0
    tick_count: int = 0
    order_count: int = 0

    # --- 委托聚合 ---
    buy_order_count: int = 0       # 买方委托笔数 (Side=0)
    sell_order_count: int = 0      # 卖方委托笔数 (Side=1)
    buy_order_volume: int = 0      # 买方委托总量
    sell_order_volume: int = 0     # 卖方委托总量
    cancel_count: int = 0          # 撤单笔数 (OrderType=5)

    # --- 时间（数据时间）---
    last_deal_time: str = ""
    last_tick_time: str = ""
    last_order_time: str = ""

    # --- 时间（系统时间，用于延迟统计）---
    last_update_ts: float = 0.0     # wall clock of last chunk update
    last_chunk_ts: float = 0.0      # chunk write timestamp (from filename, seconds)
    _last_market_time_raw: str = ""  # latest Time value across tick/deal/order

    def _update_market_time(self, time_val) -> None:
        """更新最新行情时间（取所有数据类型的最大值）。"""
        t = str(time_val)
        current = getattr(self, '_last_market_time_raw', '')
        if t.zfill(20) > current.zfill(20):
            self._last_market_time_raw = t

    def _update_market_time_raw(self, raw_time: str) -> None:
        """Update market time using raw string (HHMMSSmmm format). No allocation."""
        current = self._last_market_time_raw
        if len(raw_time) > len(current) or (len(raw_time) == len(current) and raw_time > current):
            self._last_market_time_raw = raw_time

    # ------------------------------------------------------------------ #
    # Scalar update methods (for combined engine, zero Python objects)    #
    # ------------------------------------------------------------------ #

    def update_tick_scalar(self, price: float, pre_close: float, open_p: float,
                           high: float, low: float, ask1: float, bid1: float,
                           ask_vol1: int, bid_vol1: int, raw_time: str) -> None:
        """Incremental tick update from scalar values. No DataFrame/Arrow allocation."""
        self.tick_count += 1
        if price > 0:
            if self.open == 0.0:
                self.open = price
            self.latest_price = price
            self.high = max(self.high, price)
            if price < self.low:
                self.low = price
        if pre_close > 0:
            self.pre_close = pre_close
        if ask1 > 0:
            self.ask1 = ask1
        if bid1 > 0:
            self.bid1 = bid1
        self.ask_volume1 = ask_vol1
        self.bid_volume1 = bid_vol1
        self.last_tick_time = raw_time
        self._update_market_time_raw(raw_time)

    def update_deal_scalar(self, price: float, volume: float, raw_time: str) -> None:
        """Incremental deal update from scalar values."""
        self.deal_count += 1
        self.cum_amount += price * volume
        self.cum_volume += int(volume)
        self.last_deal_time = raw_time
        self._update_market_time_raw(raw_time)

    def update_order_scalar(self, side: int, volume: float, order_type: int,
                            raw_time: str) -> None:
        """Incremental order update from scalar values."""
        self.order_count += 1
        if side == 0:
            self.buy_order_count += 1
            self.buy_order_volume += int(volume)
        elif side == 1:
            self.sell_order_count += 1
            self.sell_order_volume += int(volume)
        if order_type == 5:
            self.cancel_count += 1
        self.last_order_time = raw_time
        self._update_market_time_raw(raw_time)

    @property
    def last_market_time(self) -> str:
        """所有数据类型中最新的行情时间。"""
        return getattr(self, '_last_market_time_raw', '')

    def update_tick(self, df: pd.DataFrame) -> None:
        """从新的 tick chunk 更新状态。"""
        if df.empty:
            return
        self.tick_count += len(df)

        if 'CurrentPrice' in df.columns:
            prices = df['CurrentPrice']
            nonzero = prices[prices > 0]
            if not nonzero.empty:
                if self.open == 0.0:
                    self.open = float(nonzero.iloc[0])
                self.latest_price = float(nonzero.iloc[-1])
                self.high = max(self.high, float(nonzero.max()))
                low_candidate = float(nonzero.min())
                if low_candidate < self.low:
                    self.low = low_candidate

        if 'PreClosePrice' in df.columns:
            pc = df['PreClosePrice'].iloc[-1]
            if pc > 0:
                self.pre_close = float(pc)

        if 'AskPrice1' in df.columns:
            a1 = df['AskPrice1'].iloc[-1]
            if a1 > 0:
                self.ask1 = float(a1)
        if 'BidPrice1' in df.columns:
            b1 = df['BidPrice1'].iloc[-1]
            if b1 > 0:
                self.bid1 = float(b1)
        if 'AskVolume1' in df.columns:
            v = df['AskVolume1'].iloc[-1]
            if pd.notna(v):
                self.ask_volume1 = int(v)
        if 'BidVolume1' in df.columns:
            v = df['BidVolume1'].iloc[-1]
            if pd.notna(v):
                self.bid_volume1 = int(v)

        if 'Time' in df.columns:
            t = df['Time'].iloc[-1]
            self.last_tick_time = str(t)
            self._update_market_time(t)

    def update_deal(self, df: pd.DataFrame) -> None:
        """从新的 deal chunk 更新状态。"""
        if df.empty:
            return
        self.deal_count += len(df)

        if 'Price' in df.columns and 'Volume' in df.columns:
            self.cum_amount += float((df['Price'] * df['Volume']).sum())
            self.cum_volume += int(df['Volume'].sum())

        if 'Time' in df.columns:
            t = df['Time'].iloc[-1]
            self.last_deal_time = str(t)
            self._update_market_time(t)

    def update_order(self, df: pd.DataFrame) -> None:
        """从新的 order chunk 更新状态。"""
        if df.empty:
            return
        self.order_count += len(df)

        # 买卖方向统计 (Side: 0=买, 1=卖)
        if 'Side' in df.columns:
            buy_mask = df['Side'] == 0
            sell_mask = df['Side'] == 1
            self.buy_order_count += int(buy_mask.sum())
            self.sell_order_count += int(sell_mask.sum())

            # 买卖委托量
            if 'Volume' in df.columns:
                self.buy_order_volume += int(df.loc[buy_mask, 'Volume'].sum())
                self.sell_order_volume += int(df.loc[sell_mask, 'Volume'].sum())

        # 撤单统计 (OrderType: SH=5, SZ暂无撤单类型)
        if 'OrderType' in df.columns:
            self.cancel_count += int((df['OrderType'] == 5).sum())

        if 'Time' in df.columns:
            t = df['Time'].iloc[-1]
            self.last_order_time = str(t)
            self._update_market_time(t)

    @property
    def vwap(self) -> float:
        """加权平均成交价。"""
        if self.cum_volume > 0:
            return round(self.cum_amount / self.cum_volume, 4)
        return float('nan')

    @property
    def spread(self) -> float:
        """买卖一档价差。"""
        if self.ask1 > 0 and self.bid1 > 0:
            return round(self.ask1 - self.bid1, 4)
        return float('nan')

    @property
    def change_pct(self) -> float:
        """涨跌幅。"""
        if self.pre_close > 0 and self.latest_price > 0:
            return round((self.latest_price - self.pre_close) / self.pre_close * 100, 4)
        return float('nan')

    @property
    def order_imbalance(self) -> float:
        """委托不平衡度：买方笔数占比（0~1），>0.5 偏买方。"""
        total = self.buy_order_count + self.sell_order_count
        if total > 0:
            return round(self.buy_order_count / total, 4)
        return float('nan')

    @property
    def order_buy_vol_ratio(self) -> float:
        """买方委托量占比。"""
        total = self.buy_order_volume + self.sell_order_volume
        if total > 0:
            return round(self.buy_order_volume / total, 4)
        return float('nan')

    @property
    def cancel_ratio(self) -> float:
        """撤单率。"""
        if self.order_count > 0:
            return round(self.cancel_count / self.order_count, 4)
        return float('nan')

    @classmethod
    def from_stock_data(cls, data: 'StockData') -> 'StockState':
        """从 StockData（回测全量数据）提取聚合状态，用于回测兼容。"""
        state = cls(code=data.code)
        if not data.l1_tick.empty:
            state.update_tick(data.l1_tick)
        if not data.l2_deal.empty:
            state.update_deal(data.l2_deal)
        if not data.l2_order.empty:
            state.update_order(data.l2_order)
        return state


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
