# -*- coding: utf-8 -*-
"""
实盘动量因子示例

基于 StockState 实时聚合指标计算：
  - vwap:          加权平均成交价
  - change_pct:    涨跌幅（基于昨收价）
  - total_vol:     累计成交量
  - deal_count:    成交笔数
  - spread:        买卖一档价差
  - vol_ratio:     成交量与 tick 数的比率（衡量单笔成交大小）
  - price_pos:     当前价在当日高低区间的位置（0=最低，1=最高）
  - buy_pressure:  买一量 / (买一量 + 卖一量)，衡量买卖压力

交易员可以复制此文件作为模板，修改 factor_calculation 即可。
"""


def factor_calculation(state, code, date, end_time):
    """
    实盘因子计算函数。

    Args:
        state:    StockState，由 StreamingEngine 维护的聚合状态
        code:     股票代码
        date:     交易日 YYYYMMDD
        end_time: 截面时刻 HHMMSS

    Returns:
        dict: 因子结果
    """
    # 成交量比率：平均每笔成交量
    vol_ratio = state.cum_volume / state.deal_count if state.deal_count > 0 else 0

    # 价格位置：当前价在当日高低区间的位置
    price_range = state.high - state.low if state.high > state.low else 0
    price_pos = (state.latest_price - state.low) / price_range if price_range > 0 else 0.5

    # 买卖压力
    total_lv = state.bid_volume1 + state.ask_volume1
    buy_pressure = state.bid_volume1 / total_lv if total_lv > 0 else 0.5

    return {
        "code": code,
        "date": date,
        "end_time": end_time,
        "vwap": state.vwap,
        "change_pct": state.change_pct,
        "total_vol": state.cum_volume,
        "deal_count": state.deal_count,
        "spread": state.spread,
        "latest_price": state.latest_price,
        "high": state.high,
        "low": state.low if state.low != float('inf') else 0.0,
        "vol_ratio": round(vol_ratio, 2),
        "price_pos": round(price_pos, 4),
        "buy_pressure": round(buy_pressure, 4),
    }


def outfun(date, end_time, result_df):
    """
    输出回调：打印摘要。
    """
    if result_df.empty:
        return
    valid = result_df.dropna(subset=["vwap"])
    print(f"[{date} {end_time}] 计算完成: {len(valid)}/{len(result_df)} 只股票")
    if not valid.empty:
        print(f"  VWAP 均值: {valid['vwap'].mean():.4f}")
        print(f"  总成交量: {valid['total_vol'].sum():,.0f}")
        print(f"  涨跌幅均值: {valid['change_pct'].mean():.4f}%")
    if "data_latency_ms" in result_df.columns:
        lat = result_df["data_latency_ms"].dropna()
        if not lat.empty:
            print(f"  数据延迟(行情→计算): avg={lat.mean():.0f}ms max={lat.max():.0f}ms")
