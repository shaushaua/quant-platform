#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
实盘交易启动脚本
完整流程：数据采集 -> 策略计算 -> 信号生成 -> 订单执行
"""

import os
import sys
import time
import logging
import signal
import argparse
from datetime import datetime
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="实盘交易启动")
    parser.add_argument("--strategy", type=str, default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-collector", action="store_true")
    return parser.parse_args()


def main():
    from quant_platform.realtime.collector import TonglanceCollector
    from quant_platform.realtime.live_engine import LiveEngine
    from quant_platform.core.config import get_config

    args = parse_args()
    config = get_config()
    logger.info("=" * 60)
    logger.info("实盘交易系统启动")
    logger.info("=" * 60)

    # 检查配置
    if not config.tonglance_token:
        logger.warning("通联Token未配置，将使用模拟数据模式")

    collector = None
    if not args.no_collector:
        collector = TonglanceCollector(
            codes=None,
            enable_sh=True,
            enable_sz=True
        )

    engine = LiveEngine(simulation_mode=args.no_collector)
    if args.strategy:
        engine.load_strategy(args.strategy)
    if args.once:
        engine.run_once()
        return

    # 信号处理
    def signal_handler(signum, frame):
        logger.info(f"收到信号 {signum}, 正在停止...")
        engine.stop()
        if collector:
            collector.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if collector:
        logger.info("启动数据采集器...")
        if not collector.start():
            logger.warning("采集器启动失败，使用模拟模式")
        else:
            logger.info("采集器启动成功!")

    # 4. 启动交易引擎
    logger.info("启动交易引擎...")
    if not engine.start():
        logger.error("交易引擎启动失败!")
        if collector:
            collector.stop()
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("实盘交易系统运行中")
    logger.info("按 Ctrl+C 停止")
    logger.info("=" * 60)

    # 状态报告
    def status_report():
        while engine.running:
            time.sleep(60)
            if collector:
                collector_status = collector.get_status()
                logger.info(f"采集状态: {collector_status}")
            engine_status = engine.get_status()
            logger.info(f"交易状态: {engine_status}")

    import threading
    report_thread = threading.Thread(target=status_report, daemon=True)
    report_thread.start()

    # 主循环
    try:
        while engine.running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到中断信号")
        engine.stop()
        if collector:
            collector.stop()


if __name__ == "__main__":
    main()
