# -*- coding: utf-8 -*-
"""
实盘引擎
完整的实盘交易服务，包括：
1. 数据采集（通联实时数据 -> 内存）
2. 策略执行（计算Alpha因子）
3. 信号输出（生成交易信号）
"""

import logging
import threading
import time
import signal
from datetime import datetime
from typing import Dict, List, Optional, Callable
from pathlib import Path

import pandas as pd

from ..data.memory_store import MemoryStore
from ..data.api import DataAPI
from ..data.oss_loader import OSSDataLoader
from ..core.config import get_config

logger = logging.getLogger(__name__)

# 尝试导入采集器，如果失败则使用模拟模式
try:
    from .collector import TonglanceCollector
    HAS_COLLECTOR = True
except ImportError:
    HAS_COLLECTOR = False
    logger.warning("TonglanceCollector 不可用，将使用模拟模式")


class SignalGenerator:
    """信号生成器"""

    def __init__(self):
        self.signals: List[dict] = []
        self._callbacks: List[Callable] = []

    def add_signal(self, code: str, signal_type: str, confidence: float = 1.0, **kwargs):
        """添加交易信号"""
        signal = {
            "time": datetime.now(),
            "code": code,
            "type": signal_type,
            "confidence": confidence,
            **kwargs
        }
        self.signals.append(signal)

        # 触发回调
        for callback in self._callbacks:
            try:
                callback(signal)
            except Exception as e:
                logger.error(f"信号回调失败: {e}")

        logger.info(f"信号: {code} {signal_type} (置信度: {confidence:.2f})")

    def on_signal(self, callback: Callable):
        """注册信号回调"""
        self._callbacks.append(callback)

    def get_latest_signals(self, n: int = 10) -> List[dict]:
        """获取最近的信号"""
        return self.signals[-n:]


class LiveEngine:
    """
    实盘引擎

    完整的实盘交易服务:
    1. 启动数据采集
    2. 加载历史数据
    3. 运行策略计算
    4. 生成交易信号

    使用方式:
        engine = LiveEngine()
        engine.load_strategy("my_strategy.py")
        engine.start()
    """

    def __init__(
        self,
        strategy_func: Optional[Callable] = None,
        strategy_file: Optional[str] = None,
        simulation_mode: bool = False
    ):
        """
        初始化实盘引擎

        Args:
            strategy_func: 策略函数
            strategy_file: 策略文件路径
            simulation_mode: 是否强制使用模拟模式
        """
        self.config = get_config()
        self.strategy_func = strategy_func
        self.strategy_file = strategy_file
        self.simulation_mode = simulation_mode or not HAS_COLLECTOR

        # 数据存储
        self.store = MemoryStore.get_instance()
        self.data_api = DataAPI(mode="realtime")

        # 数据采集器（延迟初始化）
        self.collector = None
        if HAS_COLLECTOR and not self.simulation_mode:
            try:
                self.collector = TonglanceCollector()
            except Exception as e:
                logger.warning(f"采集器初始化失败: {e}，将使用模拟模式")
                self.simulation_mode = True

        # 信号生成器
        self.signal_generator = SignalGenerator()

        # 状态
        self.running = False
        self._strategy_thread: Optional[threading.Thread] = None

        # 策略执行间隔（秒）
        self.strategy_interval = 60  # 默认1分钟执行一次

        mode_str = "模拟模式" if self.simulation_mode else "实盘模式"
        logger.info(f"实盘引擎初始化完成 ({mode_str})")

    def load_strategy(self, strategy_file: str):
        """加载策略文件"""
        import importlib.util
        import sys

        strategy_path = Path(strategy_file)
        if not strategy_path.exists():
            raise FileNotFoundError(f"策略文件不存在: {strategy_file}")

        # 动态加载
        module_name = strategy_path.stem
        spec = importlib.util.spec_from_file_location(module_name, strategy_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # 查找策略函数
        if hasattr(module, "run_strategy"):
            self.strategy_func = module.run_strategy
        elif hasattr(module, "strategy"):
            self.strategy_func = module.strategy
        elif hasattr(module, "calculate_alpha"):
            self.strategy_func = module.calculate_alpha
        else:
            raise ValueError("策略文件必须定义 run_strategy(data, engine) 函数")

        logger.info(f"已加载策略: {strategy_file}")

    def on_signal(self, callback: Callable):
        """注册信号回调"""
        self.signal_generator.on_signal(callback)

    def run_once(self):
        self._load_daily_basic()
        self._execute_strategy()

    def start(self) -> bool:
        """启动实盘引擎"""
        if self.running:
            logger.warning("引擎已在运行中")
            return False

        logger.info("=" * 50)
        logger.info("启动实盘引擎")
        logger.info("=" * 50)

        # 1. 加载日频基础数据
        self._load_daily_basic()

        # 2. 启动数据采集（如果不是模拟模式）
        if self.collector and not self.simulation_mode:
            if not self.collector.start():
                logger.warning("采集器启动失败，切换到模拟模式")
                self.simulation_mode = True
            else:
                logger.info("数据采集器启动成功")
        else:
            logger.info("使用模拟模式运行")

        # 3. 启动策略执行线程
        self.running = True
        self._strategy_thread = threading.Thread(target=self._run_strategy_loop, daemon=True)
        self._strategy_thread.start()

        logger.info("实盘引擎启动成功")
        return True

    def stop(self):
        """停止实盘引擎"""
        logger.info("正在停止实盘引擎...")
        self.running = False

        # 停止采集器
        if self.collector:
            self.collector.stop()

        # 等待策略线程
        if self._strategy_thread:
            self._strategy_thread.join(timeout=5)

        logger.info("实盘引擎已停止")

    def _load_daily_basic(self):
        """加载日频基础数据"""
        try:
            today = datetime.now().strftime("%Y%m%d")
            oss = OSSDataLoader(base_path=self.config.oss_data_path)
            df = oss.load(today, "daily_basic")
            if not df.empty:
                self.store.update_daily_basic(df)
                logger.info(f"已加载日频基础数据: {len(df)} 条")
            else:
                logger.warning("未找到今日日频基础数据")
        except Exception as e:
            logger.error(f"加载日频基础数据失败: {e}")

    def _run_strategy_loop(self):
        """策略执行循环"""
        logger.info(f"策略执行线程启动，间隔: {self.strategy_interval}秒")

        while self.running:
            try:
                # 执行策略
                self._execute_strategy()

            except Exception as e:
                logger.error(f"策略执行失败: {e}")

            # 等待
            time.sleep(self.strategy_interval)

    def _execute_strategy(self):
        """执行策略计算"""
        if self.strategy_func is None:
            return

        try:
            # 获取当前数据
            df_1min = self.data_api.get_all_stocks_1min()
            df_5min = self.data_api.get_all_stocks_5min()
            quotes = self.data_api.get_all_quotes()

            if df_1min.empty:
                logger.debug("无1分钟K线数据")
                return

            # 调用策略
            result = self.strategy_func(self.data_api, self)

            # 处理结果
            if result is not None:
                self._process_strategy_result(result)

        except Exception as e:
            logger.error(f"策略执行异常: {e}")

    def _process_strategy_result(self, result):
        """处理策略结果"""
        if isinstance(result, pd.DataFrame):
            # DataFrame格式: Code, Signal, Confidence
            for _, row in result.iterrows():
                code = row.get("Code")
                signal = row.get("Signal", "hold")
                confidence = row.get("Confidence", 1.0)
                self.signal_generator.add_signal(code, signal, confidence)

        elif isinstance(result, dict):
            # 字典格式
            code = result.get("code")
            signal = result.get("signal", "hold")
            confidence = result.get("confidence", 1.0)
            if code:
                self.signal_generator.add_signal(code, signal, confidence)

        elif isinstance(result, list):
            # 列表格式
            for item in result:
                if isinstance(item, dict):
                    code = item.get("code")
                    signal = item.get("signal", "hold")
                    confidence = item.get("confidence", 1.0)
                    if code:
                        self.signal_generator.add_signal(code, signal, confidence)

    def get_status(self) -> dict:
        """获取引擎状态"""
        collector_status = {}
        if self.collector:
            collector_status = self.collector.get_status()
        elif self.simulation_mode:
            collector_status = {"mode": "simulation"}

        return {
            "running": self.running,
            "simulation_mode": self.simulation_mode,
            "collector": collector_status,
            "signals_count": len(self.signal_generator.signals),
            "latest_signals": self.signal_generator.get_latest_signals(5),
            "memory_usage": self.store.get_memory_usage(),
        }

    # ==================== 便捷方法供策略使用 ====================

    def emit_signal(self, code: str, signal_type: str, confidence: float = 1.0, **kwargs):
        """发送交易信号（供策略调用）"""
        self.signal_generator.add_signal(code, signal_type, confidence, **kwargs)

    def buy(self, code: str, confidence: float = 1.0, **kwargs):
        """发送买入信号"""
        self.emit_signal(code, "buy", confidence, **kwargs)

    def sell(self, code: str, confidence: float = 1.0, **kwargs):
        """发送卖出信号"""
        self.emit_signal(code, "sell", confidence, **kwargs)


def create_live_engine(strategy_file: str = None, simulation_mode: bool = False) -> LiveEngine:
    """创建实盘引擎"""
    return LiveEngine(strategy_file=strategy_file, simulation_mode=simulation_mode)
