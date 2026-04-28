#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
示例：从 OSS 读取因子计算结果

展示如何使用 DataAPI.load_factor_result() 读取之前计算的因子结果。
"""

import pandas as pd
from quant_platform.data.api import DataAPI


def main():
    # 初始化 API
    api = DataAPI(mode="backtest")

    print("=" * 60)
    print("因子结果读取示例")
    print("=" * 60)

    # 1. 列出可用的因子结果文件
    print("\n📂 列出因子结果文件:")
    files = api.list_factor_results("factor_results/")
    print(f"   找到 {len(files)} 个文件")
    for f in files[:10]:  # 只显示前 10 个
        print(f"   - {f}")

    if not files:
        print("   ⚠️  没有找到因子结果文件")
        print("   提示：先运行因子计算生成结果文件")
        return

    # 2. 读取单个因子结果文件
    sample_file = files[0]
    print(f"\n📖 读取因子结果: {sample_file}")

    df = api.load_factor_result(sample_file)

    print(f"   数据维度: {df.shape[0]} 行 × {df.shape[1]} 列")
    print(f"   列名: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
    print(f"   日期范围: {df['date'].min()} ~ {df['date'].max()}" if "date" in df.columns else "")

    # 3. 显示样本数据
    print(f"\n📋 样本数据（前 3 行）:")
    print(df.head(3).to_string())

    # 4. 按股票筛选
    if "code" in df.columns:
        print(f"\n🔍 按股票筛选:")
        codes = df["code"].unique()[:3]
        print(f"   前 3 只股票: {codes}")

        for code in codes:
            df_code = df[df["code"] == code]
            print(f"   {code}: {len(df_code)} 条记录")

    # 5. 按日期筛选
    if "date" in df.columns:
        print(f"\n📅 按日期筛选:")
        dates = sorted(df["date"].unique())[:3]
        print(f"   前 3 个日期: {dates}")

        for date in dates:
            df_date = df[df["date"] == date]
            print(f"   {date}: {len(df_date)} 只股票")

    # 6. 计算因子统计
    if "code" in df.columns and "date" in df.columns:
        print(f"\n📊 因子统计:")
        print(f"   总股票数: {df['code'].nunique()}")
        print(f"   总日期数: {df['date'].nunique()}")
        print(f"   总记录数: {len(df)}")

        # 检查是否有 NaN
        numeric_cols = df.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            nan_counts = df[numeric_cols].isna().sum()
            nan_cols = nan_counts[nan_counts > 0]
            if len(nan_cols) > 0:
                print(f"   ⚠️  有 NaN 值的列:")
                for col, count in nan_cols.head(5).items():
                    print(f"      {col}: {count} ({count/len(df)*100:.1f}%)")
            else:
                print(f"   ✓ 所有数值列无缺失值")


if __name__ == "__main__":
    # 确保环境变量已设置
    import os
    if not os.getenv("OSS_ACCESS_KEY_ID"):
        print("❌ 请设置环境变量:")
        print("   export OSS_ACCESS_KEY_ID='your_key'")
        print("   export OSS_ACCESS_KEY_SECRET='your_secret'")
    else:
        main()
