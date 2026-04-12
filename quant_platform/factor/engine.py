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
        logger.warning("从 OSS 获取交易日列表失败，使用回退逻辑（剔除周末）")
        trading_days = _fallback_trading_days(start_date, end_date)

    _end_times = end_times if end_times else [""]

    # 判断是否为全市场模式
    is_explicit_list = securities and len(securities) > 0

    if is_explicit_list:
        # 明确指定了股票列表，使用传统预加载模式
        _securities = list(securities)
        use_streaming = len(_securities) > 100
        logger.info("指定股票模式：%d 只股票", len(_securities))
    else:
        # 全市场模式（securities 为 None 或 []）：从 daily_basic 获取股票列表
        # 使用流式加载模式，避免一次性加载全市场数据导致 OOM
        use_streaming = True
        logger.info("全市场模式：从 daily_basic 获取股票列表")

    logger.info(
        "开始因子计算：%d 个交易日 × %d 个时间切片 × %s",
        len(trading_days), len(_end_times),
        f"{len(_securities)} 只股票" if is_explicit_list else "全市场流式模式",
    )

    for date in trading_days:
        # 全市场模式：从 daily_basic 获取股票列表
        if not is_explicit_list:
            daily_basic = api.get_daily_data(date, "daily_basic")
            logger.info("daily_basic 加载结果 date=%s shape=%s columns=%s", date, daily_basic.shape, list(daily_basic.columns))
            if daily_basic.empty:
                logger.warning("daily_basic 为空，无法获取股票列表，跳过 date=%s", date)
                continue
            # 支持多种列名格式
            if "ID_QI" in daily_basic.columns:
                _securities = daily_basic["ID_QI"].tolist()
            elif "ts_code" in daily_basic.columns:
                _securities = daily_basic["ts_code"].tolist()
            elif "Code" in daily_basic.columns:
                _securities = daily_basic["Code"].tolist()
            else:
                logger.warning("无法从 daily_basic 获取股票列表，columns=%s 跳过 date=%s", list(daily_basic.columns), date)
                continue
            logger.info("从 daily_basic 获取到 %d 只股票", len(_securities))

        # 如果 securities 数量较大，使用流式加载模式（按股票逐个加载）
        # 避免一次性加载全市场数据导致 OOM
        use_streaming = len(_securities) > 100  # 超过100只股票启用流式模式

        if use_streaming:
            # 全量预加载模式：每天只下载一次大文件，传入全部 codes 做一次性过滤
            # 避免每只股票单独触发完整文件下载（全市场 5000 只 × 4GB = 20TB）
            logger.info("预加载模式：一次性下载并过滤全部 %d 只股票数据", len(_securities))
            bundle = _load_day_bundle(date, factor_info, api, _securities)
            if not is_explicit_list:
                # 全市场模式已加载 daily_basic，注入到 bundle
                bundle.market = daily_basic

            for end_time in _end_times:
                all_res: list = []

                for code in _securities:
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
                        logger.error("outfun 异常 date=%s end_time=%s: %s", date, end_time, e)
        else:
            # 传统模式：预加载全市场数据（适用于小规模股票列表）
            bundle = _load_day_bundle(date, factor_info, api, _securities)

            for end_time in _end_times:
                all_res: list = []

                for code in _securities:
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


def _load_day_bundle(date: str, factor_info: Dict, api: DataAPI, securities: List[str] = None) -> _DayBundle:
    """
    按 factor_info 加载当日全市场四分数据。
    只加载 factor_info 中声明需要的数据类型，减少不必要 IO。
    大文件类型（tick/order/deal）传 codes 过滤，避免 OOM。
    """
    def _safe_load(data_type: str, codes=None) -> pd.DataFrame:
        try:
            return api.get_daily_data(date, data_type, codes=codes)
        except Exception as e:
            logger.warning("加载 %s %s 失败: %s", date, data_type, e)
            return pd.DataFrame()

    codes = securities if securities else None
    l2_order = _safe_load("order", codes=codes) if factor_info.get("need_l2_order") else pd.DataFrame()
    l2_deal  = _safe_load("deal",  codes=codes) if factor_info.get("need_l2_deal")  else pd.DataFrame()
    l1_tick  = _safe_load("tick",  codes=codes) if factor_info.get("need_l1_tick")  else pd.DataFrame()
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
    大文件（tick/order/deal）的 Code 列是 SECURITY_ID 整数，
    需从 market(daily_basic) 的 ID_QI/SECURITY_ID 映射转换。
    """
    # 从 market 数据推导 security_id（整数）
    id_qi = code.split(".")[0].zfill(6)  # "000001.SZ" -> "000001"
    security_id: Optional[int] = None
    if not bundle.market.empty and "ID_QI" in bundle.market.columns and "SECURITY_ID" in bundle.market.columns:
        rows = bundle.market[bundle.market["ID_QI"].astype(str) == id_qi]
        if not rows.empty:
            security_id = int(rows.iloc[0]["SECURITY_ID"])

    def _filter(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # 1) 大文件列: Code (整数 SECURITY_ID)
        for col in ("Code", "stock_code", "code"):
            if col in df.columns:
                if pd.api.types.is_integer_dtype(df[col]) and security_id is not None:
                    return df[df[col] == security_id].reset_index(drop=True)
                return df[df[col] == code].reset_index(drop=True)
        # 2) daily_basic 列: SECURITY_ID (整数) 或 ID_QI (6位字符串)
        if "SECURITY_ID" in df.columns and security_id is not None:
            return df[df["SECURITY_ID"] == security_id].reset_index(drop=True)
        if "ID_QI" in df.columns:
            return df[df["ID_QI"].astype(str) == id_qi].reset_index(drop=True)
        return df  # 无可识别的 code 列时原样返回

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
