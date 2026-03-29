# -*- coding: utf-8 -*-
"""
因子计算引擎

核心接口：calc_factors_by_date_range

使用方式：
    from quant_platform.factor.engine import calc_factors_by_date_range

    factor_info = {
        "market_count": 21,      # 需要几日 daily_basic 历史
        "need_l2_order": True,   # 是否需要 L2 逐笔委托
        "need_l2_deal": True,    # 是否需要 L2 逐笔成交
        "need_l1_tick": False,   # 是否需要 L1 tick
    }

    def factor_calculation(data, code, date, end_time):
        ...  # 返回 dict，如 {"code": code, "fac1": 0.1}

    def outfun(date, end_time, test):
        ...  # test 是 pd.DataFrame，包含当日所有股票的因子结果

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date="20240101",
        end_date="20241231",
        end_times=["093000", "150000"],
        securities=["000001.SZ", "600000.SH"],
        processes=4,
        factor_data_handler=factor_calculation,
        outfun=outfun,
    )
"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

import pandas as pd

from .base import StockData
from ..data.api import DataAPI

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 公开接口
# ---------------------------------------------------------------------------

def calc_factors_by_date_range(
    factor_info: Dict,
    start_date: str,
    end_date: str,
    end_times: List[str],
    securities: List[str],
    processes: int = 1,
    factor_data_handler: Optional[Callable] = None,
    outfun: Optional[Callable] = None,
    oss_base_path: Optional[str] = None,
) -> None:
    """
    按日期范围批量计算因子。

    引擎内部执行顺序：
        for date in trading_days:
            for end_time in end_times:
                for code in securities:
                    data  = _build_stock_data(date, end_time, code, factor_info)
                    res   = factor_data_handler(data, code, date, end_time)
                test = merge(all_res)          # → pd.DataFrame
                outfun(date, end_time, test)

    Args:
        factor_info:          数据需求配置，见模块说明。
        start_date:           开始日期，格式 YYYYMMDD。
        end_date:             结束日期，格式 YYYYMMDD。
        end_times:            时间切片列表，如 ["093000", "150000"]；
                              传空列表时用 [""] 占位（全天一次）。
        securities:           股票代码列表。
        processes:            并行进程数（暂保留参数，当前单进程实现）。
        factor_data_handler:  单票因子计算函数，签名见上。
        outfun:               批次结果处理函数，签名 outfun(date, end_time, test)。
        oss_base_path:        OSS 数据根路径，None 时读环境变量。
    """
    api = DataAPI(mode="backtest", oss_base_path=oss_base_path)
    _calc_fn = factor_data_handler
    _out_fn = outfun

    # 交易日列表
    try:
        trading_days = api.get_trading_days(start_date, end_date)
    except Exception:
        trading_days = _fallback_trading_days(start_date, end_date)

    if not trading_days:
        logger.warning("交易日列表为空，start=%s end=%s", start_date, end_date)
        return

    _end_times = end_times if end_times else [""]

    logger.info(
        "开始因子计算：%d 个交易日 × %d 个时间切片 × %d 只股票",
        len(trading_days), len(_end_times), len(securities),
    )

    for date in trading_days:
        # 每日加载一次原始数据（四分数据），跨 code/end_time 复用
        bundle = _load_day_bundle(date, factor_info, api)

        for end_time in _end_times:
            all_res: list = []

            for code in securities:
                try:
                    stock_data = _build_stock_data(bundle, code, date, end_time)
                    if _calc_fn is not None:
                        res = _calc_fn(stock_data, code, date, end_time)
                        if res is not None:
                            all_res.append(res)
                except Exception as e:
                    logger.warning(
                        "因子计算异常 date=%s end_time=%s code=%s: %s",
                        date, end_time, code, e,
                    )

            test = _merge_results(all_res)

            if _out_fn is not None:
                try:
                    _out_fn(date, end_time, test)
                except Exception as e:
                    logger.error(
                        "outfun 异常 date=%s end_time=%s: %s", date, end_time, e
                    )

    logger.info("因子计算完成")


# ---------------------------------------------------------------------------
# 内部辅助
# ---------------------------------------------------------------------------

class _DayBundle:
    """
    单日全市场原始数据包。
    每个字段是该日全市场的 DataFrame，按 code 过滤后注入给单只股票。
    """
    def __init__(
        self,
        date: str,
        l2_order: pd.DataFrame,
        l2_deal: pd.DataFrame,
        l1_tick: pd.DataFrame,
        market: pd.DataFrame,
    ):
        self.date = date
        self.l2_order = l2_order
        self.l2_deal = l2_deal
        self.l1_tick = l1_tick
        self.market = market


def _load_day_bundle(date: str, factor_info: Dict, api: DataAPI) -> _DayBundle:
    """
    按 factor_info 加载当日全市场四分数据。
    只加载 factor_info 中声明需要的数据类型，减少不必要 IO。
    """
    def _safe_load(data_type: str) -> pd.DataFrame:
        try:
            return api.get_daily_data(date, data_type)
        except Exception as e:
            logger.warning("加载 %s %s 失败: %s", date, data_type, e)
            return pd.DataFrame()

    l2_order = _safe_load("order") if factor_info.get("need_l2_order") else pd.DataFrame()
    l2_deal  = _safe_load("deal")  if factor_info.get("need_l2_deal")  else pd.DataFrame()
    l1_tick  = _safe_load("tick")  if factor_info.get("need_l1_tick")  else pd.DataFrame()
    market   = _safe_load("daily_basic")

    # 若需要多日 market 历史（market_count > 1），尝试追加历史
    market_count = int(factor_info.get("market_count", 1))
    if market_count > 1:
        try:
            hist = api.get_history_days(market_count, "daily_basic", from_date=date)
            if not hist.empty:
                market = hist
        except Exception as e:
            logger.warning("加载 daily_basic 历史 %d 日失败: %s", market_count, e)

    return _DayBundle(
        date=date,
        l2_order=l2_order,
        l2_deal=l2_deal,
        l1_tick=l1_tick,
        market=market,
    )


def _build_stock_data(
    bundle: _DayBundle,
    code: str,
    date: str,
    end_time: str,
) -> StockData:
    """
    从全市场数据包中过滤出单只股票的数据，组装成 StockData。
    过滤列优先尝试 "Code"，其次 "stock_code"。
    """
    def _filter(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        for col in ("Code", "stock_code", "code"):
            if col in df.columns:
                return df[df[col] == code].reset_index(drop=True)
        return df  # 无 code 列时原样返回（如已是单股数据）

    return StockData(
        code=code,
        date=date,
        end_time=end_time,
        l2_order=_filter(bundle.l2_order),
        l2_deal=_filter(bundle.l2_deal),
        l1_tick=_filter(bundle.l1_tick),
        market=_filter(bundle.market),
        daily_basic=_filter(bundle.market),  # 别名，与 market 相同
    )


def _merge_results(all_res: list) -> pd.DataFrame:
    """
    将所有股票的结果 dict 合并成 DataFrame。
    若列表为空，返回空 DataFrame。
    """
    if not all_res:
        return pd.DataFrame()
    return pd.DataFrame(all_res)


def _fallback_trading_days(start_date: str, end_date: str) -> List[str]:
    """无法从 OSS 获取交易日时的降级实现（剔除周末）。"""
    fmt = "%Y%m%d"
    cur = datetime.strptime(start_date, fmt)
    end = datetime.strptime(end_date, fmt)
    dates = []
    while cur <= end:
        if cur.weekday() < 5:
            dates.append(cur.strftime(fmt))
        cur += timedelta(days=1)
    return dates
