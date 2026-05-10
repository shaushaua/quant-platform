# -*- coding: utf-8 -*-
"""
实盘因子示例：实时 VWAP + 成交量统计

使用 StockState（聚合状态）计算：
  - vwap:        加权平均成交价
  - total_vol:   累计成交量
  - deal_count:  成交笔数
  - spread:      买卖一档价差
  - change_pct:  涨跌幅

实盘和回测通用（回测时引擎自动从 StockData 提取 StockState）。
"""


def factor_calculation(state, code, date, end_time):
    """
    实盘因子计算函数。

    Args:
        state:    StockState，由引擎维护的聚合状态
        code:     股票代码
        date:     交易日 YYYYMMDD
        end_time: 截面时刻 HHMMSS

    Returns:
        dict: 因子结果
    """
    return {
        "code": code,
        "date": date,
        "end_time": end_time,
        "vwap": state.vwap,
        "total_vol": state.cum_volume,
        "deal_count": state.deal_count,
        "spread": state.spread,
        "latest_price": state.latest_price,
        "change_pct": state.change_pct,
        "high": state.high,
        "low": state.low if state.low != float('inf') else 0.0,
    }


def outfun(date, end_time, result_df):
    """
    输出回调：结果会自动上传 OSS，这里只打印摘要。
    """
    if result_df.empty:
        return
    valid = result_df.dropna(subset=["vwap"])
    print(f"[{date} {end_time}] 计算完成: {len(valid)}/{len(result_df)} 只股票")
    if not valid.empty:
        print(f"  VWAP 均值: {valid['vwap'].mean():.4f}")
        print(f"  总成交量: {valid['total_vol'].sum():,.0f}")
