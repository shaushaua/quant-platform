# -*- coding: utf-8 -*-
"""
实盘动量因子示例

综合使用 tick / deal / order 三种数据：

Tick (快照):
  - vwap:        加权平均成交价（依赖 deal 累计）
  - change_pct:  涨跌幅
  - spread:      买卖一档价差
  - price_pos:   当前价在当日高低区间位置
  - buy_pressure: 买一量/(买一量+卖一量)

Deal (成交):
  - total_vol:   累计成交量
  - deal_count:  成交笔数
  - vol_ratio:   平均每笔成交量

Order (委托):
  - order_imbalance:    买方委托笔数占比
  - order_buy_vol_ratio: 买方委托量占比
  - cancel_ratio:       撤单率
"""


def factor_calculation(data, code, date, end_time):
    """
    实盘因子计算函数。

    Args:
        data:     StockData（回测兼容，实盘模式下 data.state 有聚合状态）
        code:     股票代码
        date:     交易日 YYYYMMDD
        end_time: 截面时刻 HHMMSS

    Returns:
        dict: 因子结果
    """
    # 实盘模式通过 data.state 获取聚合状态，回测模式从 DataFrame 计算
    state = data.state

    if state is not None:
        # --- 实盘模式：用 StockState 聚合值 ---
        deal_count = state.deal_count
        cum_volume = state.cum_volume
        latest_price = state.latest_price
        high = state.high
        low = state.low
        pre_close = state.pre_close
        change_pct = state.change_pct
        spread = state.spread
        bid_volume1 = state.bid_volume1
        ask_volume1 = state.ask_volume1
        vwap = state.vwap
        order_imbalance = state.order_imbalance
        order_buy_vol_ratio = state.order_buy_vol_ratio
        cancel_ratio = state.cancel_ratio
        order_count = state.order_count
    else:
        # --- 回测模式：从 DataFrame 计算 ---
        deal = data.l2_deal
        tick = data.l1_tick

        deal_count = len(deal)
        cum_volume = int(deal["Volume"].sum()) if not deal.empty else 0
        if not tick.empty:
            latest_price = float(tick["CurrentPrice"].iloc[-1])
            high = float(tick["HighPrice"].iloc[-1]) if "HighPrice" in tick.columns else 0
            low = float(tick["LowPrice"].iloc[-1]) if "LowPrice" in tick.columns else 0
            pre_close = float(tick["PreCloPrice"].iloc[-1]) if "PreCloPrice" in tick.columns else 0
            change_pct = ((latest_price - pre_close) / pre_close * 100) if pre_close > 0 else 0
            spread = 0.0
            bid_volume1 = int(tick["BidVolume1"].iloc[-1]) if "BidVolume1" in tick.columns else 0
            ask_volume1 = int(tick["AskVolume1"].iloc[-1]) if "AskVolume1" in tick.columns else 0
        else:
            latest_price = high = low = pre_close = change_pct = spread = 0
            bid_volume1 = ask_volume1 = 0
        vwap = 0
        if cum_volume > 0 and not deal.empty:
            total_amount = (deal["Price"] * deal["Volume"]).sum()
            vwap = total_amount / cum_volume
        order_imbalance = 0
        order_buy_vol_ratio = 0
        cancel_ratio = 0
        order_count = len(data.l2_order)

    # --- Tick 衍生 ---
    vol_ratio = cum_volume / deal_count if deal_count > 0 else 0

    price_range = high - low if high > low else 0
    price_pos = (latest_price - low) / price_range if price_range > 0 else 0.5

    total_lv = bid_volume1 + ask_volume1
    buy_pressure = bid_volume1 / total_lv if total_lv > 0 else 0.5

    return {
        "code": code,
        "date": date,
        "end_time": end_time,
        # tick 衍生
        "latest_price": latest_price,
        "high": high,
        "low": low if low != float('inf') else 0.0,
        "change_pct": change_pct,
        "spread": spread,
        "price_pos": round(price_pos, 4),
        "buy_pressure": round(buy_pressure, 4),
        # deal 衍生
        "vwap": vwap,
        "total_vol": cum_volume,
        "deal_count": deal_count,
        "vol_ratio": round(vol_ratio, 2),
        # order 衍生
        "order_imbalance": order_imbalance,
        "order_buy_vol_ratio": order_buy_vol_ratio,
        "cancel_ratio": cancel_ratio,
        "order_count": order_count,
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
    imb = valid["order_imbalance"].dropna()
    if not imb.empty:
        print(f"  委托不平衡度均值: {imb.mean():.4f}")
    if "data_latency_ms" in result_df.columns:
        lat = result_df["data_latency_ms"].dropna()
        if not lat.empty:
            print(f"  数据延迟(行情→计算): avg={lat.mean():.0f}ms max={lat.max():.0f}ms")
