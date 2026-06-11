#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地策略测试工具

用途：在提交到 Kubernetes 集群之前，先在本地测试策略代码
优势：
  - 快速验证策略逻辑是否正确
  - 检查数据加载是否成功
  - 避免浪费集群资源
  - 缩短开发迭代周期

用法：
  python local_test.py strategy.py --date 20250106 --codes "000001.SZ,000002.SZ"
  python local_test.py strategy.py --date 20250106-20250110 --codes "000001.SZ"
  python local_test.py strategy.py --date 20250106 --all  # 测试全市场（仅前10只股票）
"""

import argparse
import importlib.util
import json
import os
import sys

import pandas as pd
from datetime import datetime
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from quant_platform.factor.engine import calc_factors_by_date_range
from quant_platform.factor.time_slices import generate_intraday_end_times
from quant_platform.data.api import DataAPI


def load_strategy(strategy_path: str):
    """动态加载策略文件"""
    spec = importlib.util.spec_from_file_location("strategy", strategy_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_date_range(date_arg: str):
    """解析日期参数"""
    if '-' in date_arg and len(date_arg) > 8:
        # 日期范围: 20250106-20250110
        start, end = date_arg.split('-')
        return start, end
    else:
        # 单日: 20250106
        return date_arg, date_arg


class BatchSignatureAdapter:
    """Force local tests to use the production batch strategy signature.

    The factor engine's serial path invokes a scalar handler. This adapter keeps
    local `--processes=1` useful by converting that scalar engine call into the
    batch-style call expected in live/backtest worker paths:
      factor_calculation({code: StockData}, [code], date, [end_time])
    """

    def __init__(self, factor_calculation):
        self.factor_calculation = factor_calculation

    def __call__(self, stock_data, code, date, end_time):
        raw = self.factor_calculation({code: stock_data}, [code], date, [end_time])
        return self._unwrap(code, end_time, raw)

    @staticmethod
    def _unwrap(code, end_time, raw):
        if not isinstance(raw, dict):
            return raw

        code_value = raw.get(code)
        if code_value is None:
            return raw
        if isinstance(code_value, dict) and end_time in code_value:
            return code_value.get(end_time)
        return code_value


def main():
    parser = argparse.ArgumentParser(description='本地测试因子策略')
    parser.add_argument('strategy', help='策略文件路径 (如 strategy.py)')
    parser.add_argument('--date', required=True, help='日期或日期范围 (如 20250106 或 20250106-20250110)')
    parser.add_argument('--codes', help='股票代码列表，逗号分隔 (如 "000001.SZ,000002.SZ")')
    parser.add_argument('--all', action='store_true', help='测试全市场模式（仅前10只股票）')
    parser.add_argument('--output', default='./local_test_result', help='输出结果文件路径（不含扩展名）')
    parser.add_argument('--format', choices=['csv', 'parquet', 'json'], default='csv', help='输出格式（默认 csv）')
    parser.add_argument('--processes', type=int, default=1, help='并行进程数；设为 4 可复现线上 worker pool 调用路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志')

    args = parser.parse_args()

    # 设置日志级别
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    # 检查策略文件
    if not os.path.exists(args.strategy):
        print(f"❌ 错误: 策略文件不存在: {args.strategy}")
        sys.exit(1)

    # 检查环境变量
    required_env = ['OSS_ACCESS_KEY_ID', 'OSS_ACCESS_KEY_SECRET']
    missing_env = [e for e in required_env if not os.getenv(e)]
    if missing_env:
        print(f"❌ 错误: 缺少环境变量: {', '.join(missing_env)}")
        print("\n请设置以下环境变量:")
        print("  export OSS_ACCESS_KEY_ID='your_access_key'")
        print("  export OSS_ACCESS_KEY_SECRET='your_secret_key'")
        print("\n可选环境变量:")
        print("  export OSS_ENDPOINT='https://oss-cn-hangzhou.aliyuncs.com'  # 公网地址")
        print("  export OSS_DATA_BUCKET='quant-mdl-data'  # 默认为 quant-mdl-data")
        sys.exit(1)

    # 自动选择 endpoint（优先使用环境变量，否则使用公网）
    if not os.getenv('OSS_ENDPOINT'):
        # 尝试检测是否在集群内网环境
        import socket
        try:
            # 尝试解析内网地址
            socket.gethostbyname('oss-cn-hangzhou-internal.aliyuncs.com')
            default_endpoint = 'https://oss-cn-hangzhou-internal.aliyuncs.com'
            print("ℹ️  检测到内网环境，使用内网 endpoint")
        except socket.gaierror:
            # 内网不通，使用公网
            default_endpoint = 'https://oss-cn-hangzhou.aliyuncs.com'
            print("ℹ️  检测到公网环境，使用公网 endpoint（会产生流量费用）")

        os.environ['OSS_ENDPOINT'] = default_endpoint

    print("=" * 60)
    print("🧪 本地策略测试")
    print("=" * 60)

    # 加载策略
    print(f"\n📝 加载策略: {args.strategy}")
    try:
        strategy = load_strategy(args.strategy)

        # 检查必要的函数和变量
        if not hasattr(strategy, 'factor_calculation'):
            print("❌ 错误: 策略缺少 factor_calculation 函数")
            sys.exit(1)

        factor_info = getattr(strategy, 'factor_info', {})
        securities = getattr(strategy, 'securities', [])
        end_times = getattr(strategy, 'end_times', None)
        if not end_times:
            interval = int(factor_info.get("compute_interval", 0))
            end_times = generate_intraday_end_times(interval) if interval > 0 else [""]

        print(f"   ✓ factor_info: {factor_info}")
        print(f"   ✓ securities: {len(securities)} 只股票")
        print(f"   ✓ end_times: count={len(end_times)}, first={end_times[:3]}, last={end_times[-3:]}")
        print("   ✓ call_signature: batch ({code: StockData}, [code], date, [end_time])")

    except Exception as e:
        print(f"❌ 加载策略失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 解析日期范围
    start_date, end_date = parse_date_range(args.date)
    print(f"\n📅 测试日期: {start_date} ~ {end_date}")

    # 确定股票列表
    if args.all:
        # 全市场模式：从策略中的 securities 获取，如果为空则只测试前10只
        if not securities:
            print("   全市场模式：测试前 10 只股票（实际运行时会处理全部）")
            test_codes = []  # 空列表会触发引擎的全市场模式
        else:
            test_codes = securities[:10] if len(securities) > 10 else securities
            print(f"   测试 {len(test_codes)} 只股票（策略中指定）")
    elif args.codes:
        test_codes = [c.strip() for c in args.codes.split(',')]
        print(f"   测试 {len(test_codes)} 只股票: {test_codes}")
    elif securities:
        test_codes = securities[:10] if len(securities) > 10 else securities
        print(f"   使用策略中的股票（前 {len(test_codes)} 只）: {test_codes}")
    else:
        print("❌ 错误: 请指定 --codes 或 --all 或在策略中定义 securities")
        sys.exit(1)

    # 初始化数据 API
    print(f"\n🔌 连接 OSS: {os.getenv('OSS_ENDPOINT')}")
    print(f"   Bucket: {os.getenv('OSS_DATA_BUCKET', 'quant-mdl-data')}")

    try:
        api = DataAPI()
        print("   ✓ OSS 连接成功")
    except Exception as e:
        print(f"❌ OSS 连接失败: {e}")
        sys.exit(1)

    # 执行计算
    print(f"\n⚙️  开始计算因子...")
    print(f"   (这可能需要一些时间，取决于数据量)")
    print()

    results = []

    def collect_result(date, end_time, data):
        """收集结果的回调函数"""
        if data is not None and not data.empty:
            records = data.to_dict('records')
            results.extend(records)
            print(f"   ✓ {date} {end_time}: {len(records)} 条记录")
        else:
            print(f"   - {date} {end_time}: 无数据")

    try:
        factor_handler = (
            strategy.factor_calculation
            if args.processes > 1
            else BatchSignatureAdapter(strategy.factor_calculation)
        )
        call_signature = (
            "batch direct ({code: StockData}, [code...], date, [end_time...])"
            if args.processes > 1
            else "batch adapter ({code: StockData}, [code], date, [end_time])"
        )
        print(f"   call_path: {call_signature}")
        if args.processes > 1:
            os.environ.setdefault("FACTOR_FORCE_STREAMING", "1")

        calc_factors_by_date_range(
            factor_info=factor_info,
            start_date=start_date,
            end_date=end_date,
            end_times=end_times,
            securities=test_codes,
            processes=args.processes,
            factor_data_handler=factor_handler,
            outfun=collect_result,
            oss_base_path=None,
        )

        print(f"\n✅ 计算完成")

    except Exception as e:
        print(f"\n❌ 计算失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 保存结果
    if results:
        df = pd.DataFrame(results)
        # float64 → float32，减少产物存储体积
        float_cols = df.select_dtypes(include=["float64"]).columns
        if len(float_cols) > 0:
            df[float_cols] = df[float_cols].astype("float32")
        fmt = args.format

        # 根据格式确定输出路径
        if args.output.endswith(f'.{fmt}'):
            output_path = args.output
        else:
            output_path = f"{args.output}.{fmt}"

        if fmt == 'csv':
            df.to_csv(output_path, index=False)
        elif fmt == 'parquet':
            df.to_parquet(output_path, index=False)
        else:
            df.to_json(output_path, orient='records', indent=2, force_ascii=False)

        print(f"\n📊 结果统计:")
        print(f"   总记录数: {len(results)}")

        # 分析结果
        if len(results) > 0:
            sample = results[0]
            print(f"   字段数: {len(sample)}")
            print(f"   字段名: {list(sample.keys())[:10]}{'...' if len(sample) > 10 else ''}")

            # 检查 NaN 值
            import math
            nan_fields = {}
            for record in results[:min(10, len(results))]:  # 只检查前10条
                for key, value in record.items():
                    if isinstance(value, float) and math.isnan(value):
                        nan_fields[key] = nan_fields.get(key, 0) + 1

            if nan_fields:
                print(f"\n⚠️  警告: 发现 NaN 值的字段:")
                for field, count in sorted(nan_fields.items(), key=lambda x: -x[1])[:5]:
                    print(f"      {field}: {count} 条记录")
            else:
                print(f"\n   ✓ 所有字段都有有效值（前10条检查）")

            # 显示样本
            print(f"\n📋 样本数据（第1条）:")
            for key, value in list(sample.items())[:15]:
                if isinstance(value, float):
                    print(f"      {key}: {value:.4f}")
                else:
                    print(f"      {key}: {value}")
            if len(sample) > 15:
                print(f"      ... (还有 {len(sample) - 15} 个字段)")

        print(f"\n💾 结果已保存到: {output_path} ({fmt})")

    else:
        print(f"\n⚠️  警告: 没有生成任何结果")
        print(f"   可能原因:")
        print(f"   1. 选择的日期没有数据")
        print(f"   2. 选择的股票代码不存在")
        print(f"   3. factor_calculation 函数返回了 None")

    print("\n" + "=" * 60)
    print("✅ 本地测试完成")
    print("=" * 60)


if __name__ == '__main__':
    main()
