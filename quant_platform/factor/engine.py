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
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

import numpy as np
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
            # 分批加载模式：将全市场股票分批，每批下载一次文件并过滤
            # 避免每只股票单独下载（O(N×4GB)），也避免全量加载 OOM
            # 默认每批 200 只，内存占用约 600MB~1GB/批（deal+tick 合计）
            BATCH_SIZE = int(os.environ.get("FACTOR_BATCH_SIZE", "200"))
            batches = [_securities[i:i+BATCH_SIZE] for i in range(0, len(_securities), BATCH_SIZE)]
            logger.info("分批加载模式：%d 只股票分 %d 批处理（每批 %d 只）",
                        len(_securities), len(batches), BATCH_SIZE)

            for end_time in _end_times:
                all_res: list = []

                for batch_idx, batch_codes in enumerate(batches):
                    logger.info("处理批次 %d/%d：%d 只股票", batch_idx+1, len(batches), len(batch_codes))
                    import time as _time
                    _t_load = _time.time()
                    bundle = _load_day_bundle(date, factor_info, api, batch_codes)
                    _load_elapsed = _time.time() - _t_load
                    logger.info("[Perf] batch %d/%d _load_day_bundle=%.2fs", batch_idx+1, len(batches), _load_elapsed)
                    if not is_explicit_list:
                        bundle.market = daily_basic

                    for code in batch_codes:
                        try:
                            # 检查是否需要历史窗口
                            hist_bundles = None
                            lookback_days = factor_info.get("lookback_days")
                            if lookback_days:
                                # lookback_days 支持两种格式：
                                # - 整数：如 5，自动转换为 [0, 1, 2, 3, 4]（当天 + 前4天）
                                # - 列表：如 [0, 1, 5]，精确指定哪些天
                                day_offsets = None
                                if isinstance(lookback_days, int):
                                    day_offsets = list(range(0, lookback_days))
                                elif isinstance(lookback_days, list):
                                    day_offsets = lookback_days

                                if day_offsets:
                                    # 先从当天的 market 获取 security_id
                                    id_qi = code.split(".")[0].zfill(6)
                                    security_id = None
                                    if not bundle.market.empty:
                                        rows = bundle.market[bundle.market["ID_QI"].astype(str) == id_qi]
                                        if not rows.empty and "SECURITY_ID" in bundle.market.columns:
                                            security_id = int(rows.iloc[0]["SECURITY_ID"])
                                    hist_bundles = _load_history_bundle_by_offsets(date, day_offsets, factor_info, api, code, security_id)

                            stock_data = _build_stock_data(bundle, code, date, end_time, factor_info, api, hist_bundles)
                            if _calc_fn is not None:
                                import time as _time
                                _t_calc = _time.time()
                                res = _calc_fn(stock_data, code, date, end_time)
                                _calc_elapsed = _time.time() - _t_calc
                                if _calc_elapsed > 1.0:
                                    logger.info("[Perf] %s factor_calculation=%.2fs", code, _calc_elapsed)
                                if res is not None:
                                    all_res.append(res)
                        except Exception as e:
                            logger.warning(
                                "因子计算异常 date=%s end_time=%s code=%s: %s",
                                date, end_time, code, e,
                            )

                    # 批次完成后释放大 DataFrame，避免内存积累
                    del bundle

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
                        # 检查是否需要历史窗口
                        hist_bundles = None
                        lookback_days = factor_info.get("lookback_days")
                        if lookback_days:
                            # lookback_days 支持两种格式：
                            # - 整数：如 5，自动转换为 [0, 1, 2, 3, 4]（当天 + 前4天）
                            # - 列表：如 [0, 1, 5]，精确指定哪些天
                            day_offsets = None
                            if isinstance(lookback_days, int):
                                day_offsets = list(range(0, lookback_days))
                            elif isinstance(lookback_days, list):
                                day_offsets = lookback_days

                            if day_offsets:
                                # 先从当天的 market 获取 security_id
                                id_qi = code.split(".")[0].zfill(6)
                                security_id = None
                                if not bundle.market.empty:
                                    rows = bundle.market[bundle.market["ID_QI"].astype(str) == id_qi]
                                    if not rows.empty and "SECURITY_ID" in bundle.market.columns:
                                        security_id = int(rows.iloc[0]["SECURITY_ID"])
                                hist_bundles = _load_history_bundle_by_offsets(date, day_offsets, factor_info, api, code, security_id)

                        stock_data = _build_stock_data(bundle, code, date, end_time, factor_info, api, hist_bundles)
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

    logger.info(
        "[数据加载] date=%s codes=%d order=%d行 deal=%d行 tick=%d行 market=%d行",
        date, len(codes or []), len(l2_order), len(l2_deal), len(l1_tick), len(market),
    )

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


def _restore_oss_precision(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """
    将 OSS 历史压缩数据还原为原始精度。

    Go data-converter (deeptrade) 对数据做了压缩：
      - Code 列存为 SECURITY_ID 整数 (int32)
      - 价格列 ×100 存为 int32
      - 成交量列 ÷100 存为 int32

    通过检测列类型（int32 vs float64/string）自动判断是否需要还原，
    不影响实时 collector 数据（float64 价格 + string code）。
    """
    if df.empty:
        return df

    # 检测 Code 列是否为整数类型（OSS 历史数据的标志）
    code_col = None
    for col in ("Code", "code", "stock_code"):
        if col in df.columns:
            code_col = col
            break
    if code_col is None:
        return df

    if not pd.api.types.is_integer_dtype(df[code_col]):
        return df  # 实时数据，无需还原

    # --- 需要还原 ---
    restored_prices = []
    restored_volumes = []

    # 1. Code → 股票代码字符串
    df[code_col] = code

    # 2. 价格列 ÷100（只处理 int32 列）
    _PRICE_SUFFIXES = ("Price", "IOPV")
    for col in df.columns:
        if col == code_col:
            continue
        if pd.api.types.is_integer_dtype(df[col]):
            # 价格类：列名含 Price 或 IOPV → ÷100
            is_price = any(s in col for s in _PRICE_SUFFIXES)
            # 成交量类：列名含 Volume → ×100
            is_volume = "Volume" in col
            # 其他 int32 列（SeqNum, OrderID, Channel, TradeNum, Num 等）不处理

            if is_price:
                restored_prices.append(col)
                df[col] = df[col].astype("float64") / 100.0
            elif is_volume:
                restored_volumes.append(col)
                df[col] = df[col].astype("float64") * 100.0

    if restored_prices or restored_volumes:
        sample_price = df[restored_prices[0]].iloc[0] if restored_prices and len(df) > 0 else None
        logger.debug(
            "[精度还原] %s: Code int→str, 价格÷100[%s], 成交量×100[%s], 样本 %s=%.4f",
            code, ",".join(restored_prices[:3]), ",".join(restored_volumes[:3]),
            restored_prices[0] if restored_prices else "", sample_price,
        )

    # 3. 还原 Time 列：int64 UnixMicro → datetime64[ns]
    if "Time" in df.columns and pd.api.types.is_integer_dtype(df["Time"]):
        # Unix 微秒时间戳转换为 datetime，默认是 UTC
        # A 股数据是北京时间，需要转换为 Asia/Shanghai (UTC+8)
        df["Time"] = pd.to_datetime(df["Time"], unit="us", utc=True).dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    # 还原 UpdateTime 列：int32 微秒偏移 → datetime64[ns]（基于已还原的 Time）
    if "UpdateTime" in df.columns and pd.api.types.is_integer_dtype(df["UpdateTime"]) and "Time" in df.columns:
        df["UpdateTime"] = df["Time"] + pd.to_timedelta(df["UpdateTime"], unit="us")

    return df


def _load_history_bundle_by_offsets(
    target_date: str,
    day_offsets: List[int],
    factor_info: Dict,
    api: DataAPI,
    code: str,
    security_id: Optional[int],
) -> List[_DayBundle]:
    """
    根据日期偏移列表加载历史数据包。

    Args:
        target_date: 目标日期（如 "20250110"）
        day_offsets: 日期偏移列表，[0]=当天, [1]=1天前, [2]=2天前
                     例如 [0, 1, 2] 表示加载当天、1天前、2天前
        factor_info: 数据需求配置
        api: DataAPI 实例
        code: 股票代码（如 "000001.SZ"）
        security_id: SECURITY_ID 整数

    Returns:
        List[_DayBundle]，按 day_offsets 顺序排列
        例如 day_offsets=[0,1], target_date="20250110"，
        返回 [bundle_20250110, bundle_20250109]
    """
    if not day_offsets:
        return []

    bundles = []
    # 计算需要加载的最大偏移量，用于获取交易日列表
    max_offset = max(day_offsets) if day_offsets else 0

    try:
        # 获取足够大的交易日范围
        start_date = _shift_date_str(target_date, -max_offset - 10)
        all_trading_days = api.get_trading_days(start_date, target_date)
    except Exception:
        # 回退到简单的日期递推
        all_trading_days = []
        d = _shift_date_str(target_date, -max_offset - 10)
        while d <= target_date:
            all_trading_days.append(d)
            d = _shift_date_str(d, 1)

    # 找到目标日期在交易日列表中的索引
    try:
        target_idx = all_trading_days.index(target_date)
    except ValueError:
        target_idx = len(all_trading_days) - 1

    # 根据 day_offsets 加载对应日期的数据
    for offset in day_offsets:
        if offset < 0:
            continue
        hist_idx = target_idx - offset
        if 0 <= hist_idx < len(all_trading_days):
            hist_date = all_trading_days[hist_idx]

            def _safe_load(data_type: str) -> pd.DataFrame:
                try:
                    df = api.get_daily_data(hist_date, data_type, codes=[code])
                    return df
                except Exception as e:
                    logger.debug("加载历史 %s %s %s 失败: %s", hist_date, code, data_type, e)
                    return pd.DataFrame()

            l2_order = _safe_load("order") if factor_info.get("need_l2_order") else pd.DataFrame()
            l2_deal = _safe_load("deal") if factor_info.get("need_l2_deal") else pd.DataFrame()
            l1_tick = _safe_load("tick") if factor_info.get("need_l1_tick") else pd.DataFrame()
            market = _safe_load("daily_basic")

            bundles.append(_DayBundle(
                date=hist_date,
                l2_order=l2_order,
                l2_deal=l2_deal,
                l1_tick=l1_tick,
                market=market,
            ))

    return bundles


def _build_stock_data(
    bundle: _DayBundle,
    code: str,
    date: str,
    end_time: str,
    factor_info: Optional[Dict] = None,
    api: Optional[DataAPI] = None,
    hist_bundles: Optional[List[_DayBundle]] = None,
) -> StockData:
    """
    从全市场数据包中过滤出单只股票的数据，组装成 StockData。
    过滤列优先尝试 "Code"，其次 "stock_code"。
    大文件（tick/order/deal）的 Code 列是 SECURITY_ID 整数，
    需从 market(daily_basic) 的 ID_QI/SECURITY_ID 映射转换。

    Args:
        bundle: 当日数据包
        code: 股票代码
        date: 日期
        end_time: 时间切片
        factor_info: 数据需求配置（用于历史窗口）
        api: DataAPI 实例（用于历史窗口）
        hist_bundles: 历史数据包列表（用于历史窗口）
    """
    # 从 market 数据推导 security_id（整数）
    id_qi = code.split(".")[0].zfill(6)  # "000001.SZ" -> "000001"
    security_id: Optional[int] = None
    if not bundle.market.empty and "ID_QI" in bundle.market.columns and "SECURITY_ID" in bundle.market.columns:
        rows = bundle.market[bundle.market["ID_QI"].astype(str) == id_qi]
        if not rows.empty:
            security_id = int(rows.iloc[0]["SECURITY_ID"])
            logger.debug("[ID映射] %s -> SECURITY_ID=%d", code, security_id)
        else:
            logger.warning("[ID映射] %s 未在 daily_basic 中找到 SECURITY_ID", code)

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

    l2_order_data = _restore_oss_precision(_filter(bundle.l2_order), code)
    l2_deal_data = _restore_oss_precision(_filter(bundle.l2_deal), code)
    l1_tick_data = _restore_oss_precision(_filter(bundle.l1_tick), code)
    market_data = _filter(bundle.market)

    if any(len(d) > 0 for d in [l2_order_data, l2_deal_data, l1_tick_data, market_data]):
        price_col = "Price"
        price_sample = None
        if not l2_deal_data.empty and price_col in l2_deal_data.columns:
            price_sample = l2_deal_data[price_col].iloc[0]
        elif not l2_order_data.empty and price_col in l2_order_data.columns:
            price_sample = l2_order_data[price_col].iloc[0]
        logger.info(
            "[StockData] %s date=%s order=%d行 deal=%d行 tick=%d行 market=%d行 Price样本=%s",
            code, date, len(l2_order_data), len(l2_deal_data),
            len(l1_tick_data), len(market_data),
            f"{price_sample:.2f}" if price_sample is not None else "N/A",
        )

    # 构建历史列表（如果启用 lookback_days）
    l2_order_hist = []
    l2_deal_hist = []
    l1_tick_hist = []

    if factor_info and factor_info.get("lookback_days", 0) > 0 and hist_bundles:
        # hist_bundles 已是按日期升序排列的历史数据包
        for hist_bundle in hist_bundles:
            l2_order_hist.append(_restore_oss_precision(_filter(hist_bundle.l2_order), code))
            l2_deal_hist.append(_restore_oss_precision(_filter(hist_bundle.l2_deal), code))
            l1_tick_hist.append(_restore_oss_precision(_filter(hist_bundle.l1_tick), code))

        logger.debug(
            "[历史窗口] %s date=%s offsets=%s 历史列表长度: order=%d deal=%d tick=%d",
            code, date, factor_info.get("lookback_days"),
            len(l2_order_hist), len(l2_deal_hist), len(l1_tick_hist),
        )

    return StockData(
        code=code,
        date=date,
        end_time=end_time,
        l2_order=l2_order_data,
        l2_deal=l2_deal_data,
        l1_tick=l1_tick_data,
        market=market_data,
        daily_basic=market_data,  # 别名，与 market 相同
        l2_order_hist=l2_order_hist,
        l2_deal_hist=l2_deal_hist,
        l1_tick_hist=l1_tick_hist,
    )


def _merge_results(all_res: list) -> pd.DataFrame:
    """
    将所有股票的结果 dict 合并成 DataFrame。
    若列表为空，返回空 DataFrame。
    """
    if not all_res:
        return pd.DataFrame()
    return pd.DataFrame(all_res)


def _shift_date_str(date_str: str, days: int) -> str:
    """日期字符串偏移，days 可为负数。"""
    fmt = "%Y%m%d"
    d = datetime.strptime(date_str, fmt)
    return (d + timedelta(days=days)).strftime(fmt)


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
