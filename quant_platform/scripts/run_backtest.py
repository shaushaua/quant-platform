#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
回测启动脚本
运行在抢占式实例上
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from quant_platform.backtest import run_backtest
from quant_platform.core.config import reload_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="运行策略回测")

    parser.add_argument(
        "-s", "--strategy",
        type=str,
        required=True,
        help="策略文件路径"
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="开始日期 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="结束日期 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=100_000_000.0,
        help="初始资金"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./backtest_results",
        help="结果输出目录"
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=None,
        help="OSS数据路径 (默认从配置读取)"
    )

    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()

    logger.info("=" * 60)
    logger.info("回测引擎启动")
    logger.info("=" * 60)
    logger.info(f"策略文件: {args.strategy}")
    logger.info(f"日期范围: {args.start} ~ {args.end}")
    logger.info(f"初始资金: {args.capital:,.2f}")

    # 检查策略文件
    strategy_path = Path(args.strategy)
    if not strategy_path.exists():
        logger.error(f"策略文件不存在: {strategy_path}")
        sys.exit(1)

    # 运行回测
    try:
        oss_path = args.data_path
        if oss_path:
            os.environ["OSS_DATA_PATH"] = oss_path
            reload_config()
        result = run_backtest(
            strategy_file=str(strategy_path),
            start_date=args.start,
            end_date=args.end,
            initial_capital=args.capital,
            oss_base_path=oss_path
        )

        # 打印结果
        logger.info("=" * 60)
        logger.info("回测完成")
        logger.info("=" * 60)
        if result.get("success"):
            results_df = result.get("results")
            n_rows = len(results_df) if results_df is not None and not results_df.empty else 0
            logger.info(f"交易日数: {result.get('trading_days', 0)}")
            logger.info(f"股票数: {result.get('total_codes', 0)}")
            logger.info(f"因子记录数: {n_rows}")
            logger.info(f"错误数: {len(result.get('errors', []))}")
        else:
            logger.warning(f"回测失败: {result.get('error', '未知错误')}")

        # 保存结果
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)

        import json
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')

        # 因子 DataFrame 单独保存为 parquet
        results_df = result.pop("results", None)
        if results_df is not None and not results_df.empty:
            factor_file = output_dir / f"factors_{ts}.parquet"
            results_df.to_parquet(factor_file, index=False)
            logger.info(f"因子数据已保存到: {factor_file}")
            result["factor_file"] = str(factor_file)

        result_file = output_dir / f"result_{ts}.json"
        with open(result_file, "w") as f:
            json.dump(result, f, indent=2, default=str)

        logger.info(f"结果已保存到: {result_file}")

    except Exception as e:
        logger.error(f"回测失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
