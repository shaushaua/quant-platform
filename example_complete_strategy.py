#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整示例策略：展示三种数据获取方式

1. 基础数据（daily_basic）- 获取股票基本信息
2. 历史窗口（lookback_days）- 获取前几天的 tick/deal 数据
3. OSS 因子结果（load_factor_result）- 读取之前计算的因子结果

因子定义：
    - 动量因子：过去 N 天的收盘价涨跌幅
    - 成交量变化率：当天成交量 vs 历史均值
    - 因子组合：结合本次计算和历史因子结果
"""

import pandas as pd

# ============================================================
# 配置：数据需求
# ============================================================
factor_info = {
    # 基础数据
    "market_count": 21,      # 需要 21 天日线数据（用于计算 20 日动量）
    "need_l1_tick": False,   # 不需要 tick（本例不使用）
    "need_l2_deal": True,    # 需要 deal（计算成交量）
    "need_l2_order": False,  # 不需要 order

    # 历史窗口：获取前 3 天的数据（用于成交量比较）
    "lookback_days": [0, 1, 2, 3],  # 当天 + 1天前 + 2天前 + 3天前

    # 自定义参数：OSS 上历史因子结果的路径
    "prev_factor_path": "factor_results/momentum_20d/",
}

securities = ["000001.SZ", "000002.SZ"]

end_times = [""]


# ============================================================
# 辅助函数：从 OSS 读取历史因子结果
# ============================================================
def load_prev_factor_result(code, date, oss_path):
    """
    从 OSS 读取之前计算的因子结果

    Args:
        code: 股票代码
        date: 当前日期
        oss_path: OSS 路径前缀

    Returns:
        dict: 历史因子值，如果没有则返回 None
    """
    try:
        from quant_platform.data.api import DataAPI
        api = DataAPI(mode="backtest")

        # 列出该路径下的文件
        files = api.list_factor_results(oss_path)
        if not files:
            return None

        # 读取最近的文件（假设文件名包含日期）
        # 例如：momentum_20d_20250106.parquet
        import os
        parquet_files = [f for f in files if f.endswith(".parquet")]
        if not parquet_files:
            return None

        # 读取第一个文件（实际场景中应该根据日期筛选）
        df = api.load_factor_result(os.path.join(oss_path, parquet_files[-1]))

        if df.empty:
            return None

        # 筛选当前股票的数据
        row = df[df["code"] == code]
        if row.empty:
            return None

        # 返回最近一条记录
        return row.iloc[-1].to_dict()

    except Exception as e:
        print(f"[WARN] 读取历史因子失败 {code} {date}: {e}")
        return None


# ============================================================
# 因子计算函数
# ============================================================
def factor_calculation(stock_data, code, date, end_time):
    """
    计算因子值

    使用三种数据来源：
    1. stock_data.market - 基础日线数据（21天）
    2. stock_data.l2_deal_hist - 历史成交数据（4天：今天+前3天）
    3. OSS 历史因子结果 - 之前计算的因子值
    """
    result = {
        "code": code,
        "date": date,
    }

    # --------------------------------------------------------
    # 方式1：使用基础数据（market/daily_basic）
    # --------------------------------------------------------
    market = stock_data.market
    if not market.empty and "close" in market.columns:
        # 按 _date 排序（如果有）
        if "_date" in market.columns:
            market = market.sort_values("_date")

        closes = market["close"].dropna().values

        # 计算 20 日动量（需要至少 21 天数据）
        if len(closes) >= 21:
            momentum_20d = float(closes[-1] / closes[-21] - 1)
            result["momentum_20d"] = momentum_20d
        else:
            result["momentum_20d"] = float("nan")

        # 计算收盘价
        result["close"] = float(closes[-1])

        # 计算其他基础指标
        if "volume" in market.columns:
            volumes = market["volume"].dropna().values
            if len(volumes) >= 5:
                # 5 日平均成交量
                result["avg_volume_5d"] = float(volumes[-5:].mean())
                # 当天成交量
                result["today_volume_from_daily"] = float(volumes[-1])

    # --------------------------------------------------------
    # 方式2：使用历史窗口（lookback_days）
    # --------------------------------------------------------
    if stock_data.l2_deal_hist:
        hist_volumes = []
        vwap_values = []

        for i, deal_df in enumerate(stock_data.l2_deal_hist):
            if not deal_df.empty and "Volume" in deal_df.columns:
                daily_volume = float(deal_df["Volume"].sum())
                hist_volumes.append(daily_volume)

                # 计算 VWAP
                if "Price" in deal_df.columns and daily_volume > 0:
                    vwap = float((deal_df["Price"] * deal_df["Volume"]).sum() / daily_volume)
                    vwap_values.append(vwap)

                print(f"  [{i}] 成交量: {daily_volume:,.0f}, VWAP: {vwap_values[-1]:.2f}" if vwap_values else f"  [{i}] 成交量: {daily_volume:,.0f}")

        # 当天（index=0）vs 历史
        if len(hist_volumes) >= 2:
            today_vol = hist_volumes[0]
            prev_avg_vol = sum(hist_volumes[1:]) / (len(hist_volumes) - 1)

            result["deal_volume_today"] = today_vol
            result["deal_volume_prev_avg"] = prev_avg_vol
            result["volume_ratio"] = today_vol / prev_avg_vol if prev_avg_vol > 0 else float("nan")

        # VWAP 变化
        if len(vwap_values) >= 2:
            result["vwap_today"] = vwap_values[0]
            result["vwap_prev1"] = vwap_values[1]
            result["vwap_change"] = vwap_values[0] - vwap_values[1]

    # --------------------------------------------------------
    # 方式3：从 OSS 读取历史因子结果
    # --------------------------------------------------------
    oss_path = factor_info.get("prev_factor_path", "")
    if oss_path:
        prev_factor = load_prev_factor_result(code, date, oss_path)
        if prev_factor:
            # 读取历史因子值，用于计算因子变化或组合
            result["prev_momentum_20d"] = prev_factor.get("momentum_20d", float("nan"))
            result["prev_date"] = prev_factor.get("date", "")

            # 计算因子变化
            if "momentum_20d" in result and not pd.isna(result["momentum_20d"]):
                if not pd.isna(result["prev_momentum_20d"]):
                    result["momentum_change"] = result["momentum_20d"] - result["prev_momentum_20d"]

            print(f"  [历史因子] 上次计算日期: {result['prev_date']}, 动量: {result.get('prev_momentum_20d', 'N/A')}")

    # --------------------------------------------------------
    # 组合因子（综合三种数据来源）
    # --------------------------------------------------------
    if all(k in result for k in ["momentum_20d", "volume_ratio"]):
        # 简单的因子组合示例
        if not pd.isna(result["momentum_20d"]) and not pd.isna(result["volume_ratio"]):
            # 动量 > 0 且成交量放大 > 1.2 倍
            result["signal"] = 1 if (result["momentum_20d"] > 0 and result["volume_ratio"] > 1.2) else 0

    print(f"[{code} {date}] 计算完成，共 {len(result)} 个字段")
    return result


# ============================================================
# 结果处理函数（可选）
# ============================================================
def outfun(date, end_time, test_df):
    """
    每个日期×时间切片的批次完成后调用

    可以在这里保存结果到 OSS 或其他存储
    """
    import os

    output_dir = "factor_results/momentum_20d/"
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{output_dir}momentum_20d_{date}.parquet"
    test_df.to_parquet(output_file, index=False)

    print(f"✅ 已保存结果: {output_file}")
    print(f"   股票数: {len(test_df)}, 字段数: {len(test_df.columns)}")


# ============================================================
# 独立运行脚本
# ============================================================
if __name__ == "__main__":
    from quant_platform.factor.engine import calc_factors_by_date_range

    print("=" * 60)
    print("完整示例策略：三种数据获取方式")
    print("=" * 60)
    print()
    print("数据来源：")
    print("  1. market (daily_basic) - 基础日线数据")
    print("  2. l2_deal_hist - 历史成交数据（4天）")
    print("  3. OSS 历史因子 - 之前计算的因子值")
    print()
    print("因子定义：")
    print("  - momentum_20d: 20日收盘价动量")
    print("  - volume_ratio: 当天成交量 vs 历史3天平均")
    print("  - signal: 组合信号（动量>0 且放量>1.2倍）")
    print("=" * 60)
    print()

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date="20250106",
        end_date="20250106",
        end_times=end_times,
        securities=securities,
        factor_data_handler=factor_calculation,
        outfun=outfun,
    )

    print()
    print("=" * 60)
    print("计算完成！结果已保存到 factor_results/momentum_20d/")
    print("=" * 60)
