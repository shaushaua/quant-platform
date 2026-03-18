# -*- coding: utf-8 -*-
"""
回测引擎
支持交易员上传策略脚本进行历史数据回测
"""

import logging
import importlib
import fnmatch
import sys
import traceback
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable, Union

import pandas as pd
import numpy as np

from ..data.api import DataAPI
from ..core.config import get_config

logger = logging.getLogger(__name__)


class BacktestContext:
    """
    回测上下文
    在回测过程中提供给策略的环境信息
    """

    def __init__(
        self,
        start_date: str,
        end_date: str,
        initial_capital: float = 100_000_000.0
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = start_date
        self.current_code: Optional[str] = None
        self.initial_capital = initial_capital
        self.capital = initial_capital

        # 持仓
        self.positions: Dict[str, float] = {}  # code -> volume

        # 订单记录
        self.orders: List[dict] = []

        # 交易记录
        self.trades: List[dict] = []

        # 净值曲线
        self.nav_curve: List[dict] = []

        # 数据API
        self.data: Optional[DataAPI] = None

    def set_date(self, date: str):
        """设置当前日期"""
        self.current_date = date

    def set_code(self, code: Optional[str]):
        """设置当前股票（per-code 模式由引擎注入，交易员不需要调用）"""
        self.current_code = code

    def buy(self, code: str, volume: float, price: Optional[float] = None) -> bool:
        """
        买入

        Args:
            code: 股票代码
            volume: 买入数量
            price: 买入价格，None表示以收盘价买入

        Returns:
            是否成功
        """
        if price is None:
            # 获取当日收盘价
            daily = self.data.get_daily_basic(self.current_date)
            code_data = daily[daily["Code"] == code]
            if code_data.empty:
                logger.warning(f"无法获取 {code} 的收盘价")
                return False
            price = code_data.iloc[0].get("Close", 0)

        if price <= 0:
            return False

        # 计算金额
        amount = volume * price
        if amount > self.capital:
            logger.warning(f"资金不足: 需要 {amount}, 可用 {self.capital}")
            return False

        # 扣除资金
        self.capital -= amount

        # 更新持仓
        self.positions[code] = self.positions.get(code, 0) + volume

        # 记录订单
        order = {
            "date": self.current_date,
            "code": code,
            "side": "buy",
            "volume": volume,
            "price": price,
            "amount": amount,
        }
        self.orders.append(order)
        self.trades.append(order)

        logger.debug(f"买入: {code} {volume}@{price}")
        return True

    def sell(self, code: str, volume: float, price: Optional[float] = None) -> bool:
        """
        卖出

        Args:
            code: 股票代码
            volume: 卖出数量
            price: 卖出价格

        Returns:
            是否成功
        """
        if code not in self.positions or self.positions[code] < volume:
            logger.warning(f"持仓不足: {code}")
            return False

        if price is None:
            daily = self.data.get_daily_basic(self.current_date)
            code_data = daily[daily["Code"] == code]
            if code_data.empty:
                return False
            price = code_data.iloc[0].get("Close", 0)

        if price <= 0:
            return False

        # 计算金额
        amount = volume * price

        # 增加资金
        self.capital += amount

        # 更新持仓
        self.positions[code] -= volume
        if self.positions[code] <= 0:
            del self.positions[code]

        # 记录订单
        order = {
            "date": self.current_date,
            "code": code,
            "side": "sell",
            "volume": volume,
            "price": price,
            "amount": amount,
        }
        self.orders.append(order)
        self.trades.append(order)

        logger.debug(f"卖出: {code} {volume}@{price}")
        return True

    def get_position(self, code: str) -> float:
        """获取持仓"""
        return self.positions.get(code, 0)

    def get_total_value(self) -> float:
        """获取总资产"""
        total = self.capital
        for code, volume in self.positions.items():
            daily = self.data.get_daily_basic(self.current_date)
            code_data = daily[daily["Code"] == code]
            if not code_data.empty:
                price = code_data.iloc[0].get("Close", 0)
                total += volume * price
        return total


class BacktestEngine:
    """
    回测引擎
    支持用户上传策略脚本进行回测

    使用方式:
        engine = BacktestEngine()
        result = engine.run(
            strategy_file="my_strategy.py",
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
    """

    def __init__(self, initial_capital: float = 100_000_000.0, oss_base_path: Optional[str] = None):
        self.config = get_config()
        self.initial_capital = initial_capital
        self.oss_base_path = oss_base_path

    def run(
        self,
        strategy_func: Callable,
        start_date: str,
        end_date: str,
        data_type: str = "daily_basic"
    ) -> dict:
        """
        运行回测

        Args:
            strategy_func: 策略函数，签名为 func(data: DataAPI, ctx: BacktestContext)
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型

        Returns:
            回测结果字典
        """
        logger.info(f"开始回测: {start_date} ~ {end_date}")

        # 创建数据API（回测模式）
        data_api = DataAPI(mode="backtest", oss_base_path=self.oss_base_path)

        # 创建上下文
        ctx = BacktestContext(
            start_date=start_date,
            end_date=end_date,
            initial_capital=self.initial_capital
        )
        ctx.data = data_api

        # 获取交易日列表
        trading_days = data_api._oss.get_trading_days(start_date, end_date)
        if not trading_days:
            logger.error("无可用交易日")
            return {"success": False, "error": "无可用交易日"}

        logger.info(f"共 {len(trading_days)} 个交易日")

        # 遍历每个交易日
        for trading_day in trading_days:
            try:
                ctx.set_date(trading_day)
                data_api._set_context(trading_day, None)

                # 调用策略
                strategy_func(data_api, ctx)

                # 记录净值
                nav = ctx.get_total_value()
                ctx.nav_curve.append({
                    "date": trading_day,
                    "nav": nav,
                    "capital": ctx.capital,
                    "positions": ctx.positions.copy(),
                })

            except Exception as e:
                logger.error(f"回测 {trading_day} 出错: {e}")
                traceback.print_exc()

        # 计算绩效指标
        result = self._calculate_performance(ctx, trading_days)

        if result.get("success", True) and "total_return" in result:
            logger.info(f"回测完成: 总收益率 {result['total_return']:.2%}")
        else:
            logger.warning(f"回测完成但无有效结果: {result.get('error', '未知错误')}")
        return result

    def run_from_file(
        self,
        strategy_file: str,
        start_date: str,
        end_date: str
    ) -> dict:
        """
        从策略文件运行回测

        Args:
            strategy_file: 策略文件路径
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            回测结果
        """
        # 加载策略模块
        strategy_path = Path(strategy_file)
        if not strategy_path.exists():
            logger.error(f"策略文件不存在: {strategy_file}")
            return {"success": False, "error": "策略文件不存在"}

        # 动态导入
        module_name = strategy_path.stem
        spec = importlib.util.spec_from_file_location(module_name, strategy_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # 查找策略函数
        if hasattr(module, "run_strategy"):
            strategy_func = module.run_strategy
        elif hasattr(module, "strategy"):
            strategy_func = module.strategy
        else:
            logger.error("策略文件必须定义 run_strategy(data, ctx) 函数")
            return {"success": False, "error": "未找到策略函数"}

        # 把模块级变量附加到函数对象，供 run_per_code 读取
        strategy_func.PRELOAD_DATA_TYPES = getattr(module, "PRELOAD_DATA_TYPES", None)

        # 策略文件可通过模块级变量 RUN_MODE = "default" 切换回单日迭代模式
        run_mode = getattr(module, "RUN_MODE", "per_code")
        if run_mode == "default":
            return self.run(strategy_func, start_date, end_date)
        return self.run_per_code(strategy_func, start_date, end_date)

    def run_per_code(
        self,
        strategy_func: Callable,
        start_date: str,
        end_date: str,
        universe: Optional[List[str]] = None,
        router: Optional["CodeRouter"] = None,
    ) -> dict:
        """
        按 (日期, 股票) 双层迭代执行策略，交易员完全无感知。

        引擎自动：
          1. 按交易日迭代
          2. 每日从 daily_basic 获取股票池（或使用指定 universe）
          3. 按股票迭代，注入 ctx.current_code 和 DataAPI 上下文
          4. 调用策略函数，收集返回的 DataFrame 或 dict
          5. 汇总所有 (date, code) 的结果

        策略函数签名:
            def run_strategy(data_api, ctx):
                df = data_api.get_current_data("deal")  # 自动=当天+当前code
                return pd.DataFrame({"signal": [1.0]})  # 或 dict

        Args:
            strategy_func: 策略函数
            start_date: 开始日期
            end_date: 结束日期
            universe: 固定股票池，None 表示每日从 daily_basic 动态获取
            router: CodeRouter 实例，支持按 code 分发不同策略；None 则所有 code 用同一策略

        Returns:
            {
                "success": True,
                "results": pd.DataFrame,  # 所有 (date, code) 结果合并
                "errors": [{"date", "code", "error"}],
                "trading_days": int,
                "total_codes": int,
            }
        """
        data_api = DataAPI(mode="backtest", oss_base_path=self.oss_base_path)
        ctx = BacktestContext(start_date, end_date, self.initial_capital)
        ctx.data = data_api

        trading_days = data_api._oss.get_trading_days(start_date, end_date)
        if not trading_days:
            return {"success": False, "error": "日期范围内无交易日"}

        all_results: List[pd.DataFrame] = []
        errors: List[dict] = []
        total_codes = 0

        for trading_day in trading_days:
            ctx.set_date(trading_day)
            data_api._set_context(trading_day, None)

            # 确定当日股票池
            if universe is not None:
                codes = universe
                id_map = {}  # 外部传入 universe 时无映射
            else:
                daily = data_api._oss.load(trading_day, "daily_basic")
                if daily.empty:
                    logger.warning(f"{trading_day} daily_basic 为空，跳过")
                    continue
                # ID_QI: 可读股票代码（如 '000001'），SECURITY_ID: 数字ID（deal/tick/order.Code）
                # 过滤掉 SECURITY_ID 为 NaN 的行（无数字ID则无法关联 deal/tick/order）
                daily_valid = daily.dropna(subset=["SECURITY_ID"])
                codes = daily_valid["ID_QI"].astype(str).tolist()
                id_map = dict(zip(daily_valid["ID_QI"].astype(str), daily_valid["SECURITY_ID"].astype(int)))

            # 每日开始前一次性读所有数据进内存并按 code 预分组，O(1) 取数
            # 只预加载策略声明的数据类型，避免 tick/order 等大文件撑爆内存
            preload_types = getattr(strategy_func, 'PRELOAD_DATA_TYPES', None)
            if preload_types is None:
                # 策略未声明，默认只加载 daily_basic 和 deal
                preload_types = [t for t in data_api._oss.DATA_TYPES.keys()
                                 if t not in ('tick', 'order')]
            data_api._oss.pregroup_day(trading_day, preload_types)

            for code in codes:
                total_codes += 1
                ctx.set_code(code)
                numeric_id = id_map.get(str(code))
                data_api._set_context(trading_day, code, numeric_id=numeric_id)

                # 路由：有 router 则按 code 分发，否则用默认策略
                func = strategy_func
                if router is not None:
                    resolved = router.resolve(code)
                    if resolved is not None:
                        func = resolved

                try:
                    ret = func(data_api, ctx)
                    if ret is not None:
                        if isinstance(ret, pd.DataFrame):
                            df = ret.copy()
                        else:
                            df = pd.DataFrame([ret])
                        df["_date"] = trading_day
                        df["_code"] = code
                        all_results.append(df)
                except Exception as e:
                    errors.append({"date": trading_day, "code": code, "error": str(e)})
                    logger.error(f"per-code 策略出错 {trading_day}/{code}: {e}")

            # 每日结束后清除 code 上下文和预分组缓存
            ctx.set_code(None)
            data_api._set_context(trading_day, None)
            data_api._oss.clear_grouped_cache(trading_day)

        results_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

        logger.info(
            f"run_per_code 完成: {len(trading_days)} 交易日, "
            f"{total_codes} 次调用, {len(errors)} 个错误"
        )
        return {
            "success": True,
            "results": results_df,
            "errors": errors,
            "trading_days": len(trading_days),
            "total_codes": total_codes,
        }

    def _calculate_performance(self, ctx: BacktestContext, trading_days: List[str]) -> dict:
        """计算绩效指标"""
        if not ctx.nav_curve:
            return {
                "success": False,
                "error": "无交易记录",
            }

        nav_df = pd.DataFrame(ctx.nav_curve)
        nav_df["date"] = pd.to_datetime(nav_df["date"])
        nav_df = nav_df.set_index("date")

        # 计算收益率
        nav_df["return"] = nav_df["nav"].pct_change()

        # 总收益率
        total_return = (nav_df["nav"].iloc[-1] / ctx.initial_capital - 1) if len(nav_df) > 0 else 0

        # 年化收益率
        days = len(nav_df)
        annual_return = (1 + total_return) ** (252 / days) - 1 if days > 0 else 0

        # 最大回撤
        cummax = nav_df["nav"].cummax()
        drawdown = (nav_df["nav"] - cummax) / cummax
        max_drawdown = drawdown.min()

        # 夏普比率 (简化计算)
        if nav_df["return"].std() > 0:
            sharpe = nav_df["return"].mean() / nav_df["return"].std() * np.sqrt(252)
        else:
            sharpe = 0

        # 胜率
        win_trades = sum(1 for t in ctx.trades if t.get("side") == "sell")
        total_trades = len([t for t in ctx.trades if t.get("side") == "sell"])

        return {
            "success": True,
            "total_return": total_return,
            "annual_return": annual_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe,
            "total_trades": len(ctx.trades),
            "buy_trades": len([t for t in ctx.trades if t["side"] == "buy"]),
            "sell_trades": len([t for t in ctx.trades if t["side"] == "sell"]),
            "final_nav": ctx.get_total_value(),
            "trading_days": days,
            "nav_curve": nav_df.to_dict(),
            "trades": ctx.trades,
        }


class CodeRouter:
    """
    按股票代码路由到不同策略函数，支持精确匹配和 fnmatch 通配符。

    用法:
        router = CodeRouter()
        router.register("000001.XSHE", strategy_bank)   # 精确匹配
        router.register("60*.XSHG", strategy_sh)        # 通配符

        engine.run_per_code(
            strategy_func=default_strategy,  # 未匹配到时的默认策略
            start_date="2025-01-01",
            end_date="2025-01-31",
            router=router,
        )
    """

    def __init__(self) -> None:
        # 有序列表：[(pattern, func), ...]，精确匹配优先于通配符
        self._exact: Dict[str, Callable] = {}
        self._patterns: List[tuple] = []  # [(pattern, func)]

    def register(self, pattern: str, strategy_func: Callable) -> None:
        """
        注册策略路由规则。

        Args:
            pattern: 股票代码或 fnmatch 通配符，如 '000001.XSHE' 或 '60*.XSHG'
            strategy_func: 对应的策略函数，签名同 run_strategy(data_api, ctx)
        """
        if "*" in pattern or "?" in pattern or "[" in pattern:
            self._patterns.append((pattern, strategy_func))
        else:
            self._exact[pattern] = strategy_func

    def resolve(self, code: str) -> Optional[Callable]:
        """
        解析 code 对应的策略函数。

        Returns:
            匹配到的策略函数，未匹配返回 None（引擎使用默认策略）
        """
        if code in self._exact:
            return self._exact[code]
        for pattern, func in self._patterns:
            if fnmatch.fnmatch(code, pattern):
                return func
        return None


def run_backtest(
    strategy_file: str,
    start_date: str,
    end_date: str,
    initial_capital: float = 100_000_000.0,
    oss_base_path: Optional[str] = None
) -> dict:
    """
    便捷函数：运行回测

    Args:
        strategy_file: 策略文件路径
        start_date: 开始日期
        end_date: 结束日期
        initial_capital: 初始资金

    Returns:
        回测结果
    """
    engine = BacktestEngine(initial_capital=initial_capital, oss_base_path=oss_base_path)
    return engine.run_from_file(strategy_file, start_date, end_date)
