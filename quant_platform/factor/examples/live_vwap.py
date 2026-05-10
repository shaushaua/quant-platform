# -*- coding: utf-8 -*-
"""
实盘因子示例：实时 VWAP + 成交量统计 + 延迟统计

使用 StockState（聚合状态）计算：
  - vwap:        加权平均成交价
  - total_vol:   累计成交量
  - deal_count:  成交笔数
  - spread:      买卖一档价差
  - change_pct:  涨跌幅

延迟字段（由 StreamingEngine 自动注入）：
  - _chunk_ts:       数据写入 ShmStore 的时间（来自 chunk 文件名）
  - _consume_ts:     StreamingEngine 消费 chunk 的时间
  - _compute_ts:     因子计算开始的时间
  - e2e_latency_ms:  端到端延迟 = _compute_ts - _chunk_ts

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
        "last_tick_time": state.last_tick_time,
        "last_deal_time": state.last_deal_time,
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
    # 打印延迟摘要
    if "e2e_latency_ms" in result_df.columns:
        lat = result_df["e2e_latency_ms"].dropna()
        if not lat.empty:
            print(f"  延迟: avg={lat.mean():.0f}ms max={lat.max():.0f}ms")
