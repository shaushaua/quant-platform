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
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

import pandas as pd

from .base import StockData
from ..data.api import DataAPI
from ..inference.interface import (
    call_inference,
    compute_index_composition,
    compute_trading_universe,
)

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
    inference_handler: Optional[Callable] = None,
    oss_base_path: Optional[str] = None,
) -> None:
    """
    按日期范围批量计算因子。

    引擎内部执行顺序（每只股票只 filter 一次，多个 end_time 复用）：
        for date in trading_days:
            for code in securities:
                filtered = filter(bundle, code)      # 只做一次
                for end_time in end_times:
                    data = StockData(filtered, end_time)  # 轻量 copy
                    res  = factor_data_handler(data, code, date, end_time)
            for end_time in end_times:
                test = merge(end_time_results[end_time])  # → pd.DataFrame
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
        inference_handler:    推理函数，签名 inference(date, end_time, prev_day_factors,
                              result_df, daily_basic_df, trading_universe_df,
                              index_composition_df[, portfolio_context]) -> DataFrame。
        oss_base_path:        OSS 数据根路径，None 时读环境变量。
    """
    api = DataAPI(mode="backtest", oss_base_path=oss_base_path)
    _calc_fn = factor_data_handler
    _out_fn = outfun
    _infer_fn = inference_handler

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

    _prev_day_result: Optional[pd.DataFrame] = None

    for date in trading_days:
        prev_day_for_inference = _prev_day_result
        last_result_for_date: Optional[pd.DataFrame] = None
        daily_basic_for_inference: Optional[pd.DataFrame] = None

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
            #
            # 循环顺序：batch → code → end_time
            # 每只股票只 filter 一次，多个 end_time 复用已过滤数据
            BATCH_SIZE = int(os.environ.get("FACTOR_BATCH_SIZE", "200"))
            batches = [_securities[i:i+BATCH_SIZE] for i in range(0, len(_securities), BATCH_SIZE)]
            logger.info("分批加载模式：%d 只股票分 %d 批处理（每批 %d 只）",
                        len(_securities), len(batches), BATCH_SIZE)

            # per-end_time result collectors
            end_time_results: dict = {et: [] for et in _end_times}

            for batch_idx, batch_codes in enumerate(batches):
                import time as _time
                _t_load = _time.time()
                market_override = (
                    daily_basic if not is_explicit_list and _uses_current_day_market(factor_info)
                    else None
                )
                bundle = _load_day_bundle(date, factor_info, api, batch_codes,
                                          market_override=market_override)
                _load_elapsed = _time.time() - _t_load
                logger.info("[Perf] batch %d/%d _load_day_bundle=%.2fs", batch_idx+1, len(batches), _load_elapsed)
                if is_explicit_list and daily_basic_for_inference is None and _uses_current_day_market(factor_info):
                    daily_basic_for_inference = bundle.market

                # 预解析 security_id（构建映射字典，O(N) 而非 O(N²)）
                _id_map = _build_security_id_map(bundle.market)
                security_id_map = {code: _id_map.get(_code_to_id_qi(code)) for code in batch_codes}

                # 批量加载历史数据（同一批次共享下载）
                day_offsets = _parse_day_offsets(factor_info)
                hist_offsets = _history_load_offsets(day_offsets)
                hist_by_code: Dict[str, list] = {}
                if hist_offsets:
                    hist_by_code = _load_history_bundles_batch(
                        date, hist_offsets, factor_info, api, batch_codes, security_id_map)

                for code in batch_codes:
                    try:
                        security_id = security_id_map[code]

                        # filter + restore 一次，多个 end_time 复用
                        fc = _filter_code_from_bundle(bundle, code, security_id, factor_info)
                        if day_offsets:
                            hist_bundles = _combine_history_bundles(
                                date, day_offsets, fc, hist_by_code.get(code, []))
                            _attach_history_to_filtered(fc, hist_bundles)

                        for end_time in _end_times:
                            stock_data = _stock_data_from_filtered(fc, date, end_time)
                            if _calc_fn is not None:
                                _t_calc = _time.time()
                                res = _calc_fn(stock_data, code, date, end_time)
                                _calc_elapsed = _time.time() - _t_calc
                                if _calc_elapsed > 1.0:
                                    logger.info("[Perf] %s factor_calculation=%.2fs", code, _calc_elapsed)
                                if res is not None:
                                    end_time_results[end_time].append(res)
                    except Exception as e:
                        logger.warning("因子计算异常 date=%s code=%s: %s", date, code, e)

                # 批次完成后释放大 DataFrame，避免内存积累
                del bundle

            # 按 end_time 汇总结果、运行推理、调用 outfun
            for end_time in _end_times:
                test = _merge_results(end_time_results[end_time])
                if _infer_fn is not None and test is not None and not test.empty:
                    try:
                        _daily = _daily_basic_for_inference(
                            api, date, is_explicit_list,
                            daily_basic if not is_explicit_list else None,
                            daily_basic_for_inference)
                        if is_explicit_list and daily_basic_for_inference is None:
                            daily_basic_for_inference = _daily
                        universe_extra = {
                            "date": date,
                            "end_time": end_time,
                            "codes": _securities,
                            "factor_result": test,
                        }
                        trading_universe_df = compute_trading_universe(_daily, universe_extra)
                        index_composition_df = compute_index_composition(_daily, universe_extra)
                        positions = call_inference(
                            _infer_fn,
                            date,
                            end_time,
                            prev_day_for_inference,
                            test,
                            _daily,
                            trading_universe_df,
                            index_composition_df,
                            None,
                        )
                        if positions is not None and not positions.empty:
                            logger.info("[inference] date=%s end_time=%s positions=%d", date, end_time, len(positions))
                    except Exception as e:
                        logger.error("inference 异常 date=%s end_time=%s: %s", date, end_time, e)
                if _out_fn is not None:
                    try:
                        _out_fn(date, end_time, test)
                    except Exception as e:
                        logger.error("outfun 异常 date=%s end_time=%s: %s", date, end_time, e)
                if test is not None and not test.empty:
                    last_result_for_date = test
        else:
            # 传统模式：预加载全市场数据（适用于小规模股票列表）
            # 循环顺序：code → end_time，每只股票只 filter 一次
            market_override = (
                daily_basic if not is_explicit_list and _uses_current_day_market(factor_info)
                else None
            )
            bundle = _load_day_bundle(date, factor_info, api, _securities,
                                      market_override=market_override)
            if is_explicit_list and _uses_current_day_market(factor_info):
                daily_basic_for_inference = bundle.market

            end_time_results: dict = {et: [] for et in _end_times}

            # 预解析 security_id + 批量加载历史
            _id_map = _build_security_id_map(bundle.market)
            security_id_map = {code: _id_map.get(_code_to_id_qi(code)) for code in _securities}
            day_offsets = _parse_day_offsets(factor_info)
            hist_offsets = _history_load_offsets(day_offsets)
            hist_by_code: Dict[str, list] = {}
            if hist_offsets:
                hist_by_code = _load_history_bundles_batch(
                    date, hist_offsets, factor_info, api, _securities, security_id_map)

            for code in _securities:
                try:
                    security_id = security_id_map[code]

                    # filter + restore 一次，多个 end_time 复用
                    fc = _filter_code_from_bundle(bundle, code, security_id, factor_info)
                    if day_offsets:
                        hist_bundles = _combine_history_bundles(
                            date, day_offsets, fc, hist_by_code.get(code, []))
                        _attach_history_to_filtered(fc, hist_bundles)

                    for end_time in _end_times:
                        stock_data = _stock_data_from_filtered(fc, date, end_time)
                        if _calc_fn is not None:
                            res = _calc_fn(stock_data, code, date, end_time)
                            if res is not None:
                                end_time_results[end_time].append(res)
                except Exception as e:
                    logger.warning("因子计算异常 date=%s code=%s: %s", date, code, e)

            # 按 end_time 汇总结果、运行推理、调用 outfun
            for end_time in _end_times:
                test = _merge_results(end_time_results[end_time])

                if _infer_fn is not None and test is not None and not test.empty:
                    try:
                        _daily = _daily_basic_for_inference(
                            api, date, is_explicit_list,
                            daily_basic if not is_explicit_list else None,
                            daily_basic_for_inference)
                        if is_explicit_list and daily_basic_for_inference is None:
                            daily_basic_for_inference = _daily
                        universe_extra = {
                            "date": date,
                            "end_time": end_time,
                            "codes": _securities,
                            "factor_result": test,
                        }
                        trading_universe_df = compute_trading_universe(_daily, universe_extra)
                        index_composition_df = compute_index_composition(_daily, universe_extra)
                        positions = call_inference(
                            _infer_fn,
                            date,
                            end_time,
                            prev_day_for_inference,
                            test,
                            _daily,
                            trading_universe_df,
                            index_composition_df,
                            None,
                        )
                        if positions is not None and not positions.empty:
                            logger.info("[inference] date=%s end_time=%s positions=%d", date, end_time, len(positions))
                    except Exception as e:
                        logger.error("inference 异常 date=%s end_time=%s: %s", date, end_time, e)

                if _out_fn is not None:
                    try:
                        _out_fn(date, end_time, test)
                    except Exception as e:
                        logger.error("outfun 异常 date=%s end_time=%s: %s", date, end_time, e)

                if test is not None and not test.empty:
                    last_result_for_date = test

        if last_result_for_date is not None and not last_result_for_date.empty:
            _prev_day_result = last_result_for_date

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


def _load_day_bundle(date: str, factor_info: Dict, api: DataAPI, securities: List[str] = None,
                     market_override: Optional[pd.DataFrame] = None) -> _DayBundle:
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
    market = market_override if market_override is not None else _safe_load("daily_basic")

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


def _resolve_hist_dates(target_date: str, day_offsets: List[int], api: DataAPI) -> List[Optional[str]]:
    """根据 day_offsets 计算历史交易日列表。"""
    max_offset = max(day_offsets) if day_offsets else 0

    try:
        start_date = _shift_date_str(target_date, -max_offset - 10)
        all_trading_days = api.get_trading_days(start_date, target_date)
    except Exception:
        all_trading_days = []
        d = _shift_date_str(target_date, -max_offset - 10)
        while d <= target_date:
            all_trading_days.append(d)
            d = _shift_date_str(d, 1)

    try:
        target_idx = all_trading_days.index(target_date)
    except ValueError:
        target_idx = len(all_trading_days) - 1

    hist_dates = []
    for offset in day_offsets:
        if offset < 0:
            hist_dates.append(None)
            continue
        hist_idx = target_idx - offset
        hist_dates.append(all_trading_days[hist_idx] if 0 <= hist_idx < len(all_trading_days) else None)
    return hist_dates


def _load_history_bundles_batch(
    target_date: str,
    day_offsets: List[int],
    factor_info: Dict,
    api: DataAPI,
    batch_codes: List[str],
    security_id_map: Dict[str, Optional[int]],
) -> Dict[str, List[_DayBundle]]:
    """批量加载历史数据：同一批次所有 code 共享下载，按 code 过滤分发。

    对每个历史日期只下载一次（传 batch_codes），然后按 code 分发。
    避免 N 个 code × M 个历史日 = N×M 次下载。

    Returns:
        dict: {code: List[_DayBundle]} 按 day_offsets 顺序排列
    """
    hist_dates = _resolve_hist_dates(target_date, day_offsets, api)

    result: Dict[str, List[_DayBundle]] = {code: [] for code in batch_codes}
    filtered_cache: Dict[str, Dict[str, _DayBundle]] = {}

    for hist_date in hist_dates:
        if hist_date is None:
            for code in batch_codes:
                result[code].append(_DayBundle(
                    date="", l2_order=pd.DataFrame(), l2_deal=pd.DataFrame(),
                    l1_tick=pd.DataFrame(), market=pd.DataFrame(),
                ))
            continue

        filtered_for_date = filtered_cache.get(hist_date)
        if filtered_for_date is None:
            def _safe_load(data_type: str) -> pd.DataFrame:
                try:
                    return api.get_daily_data(hist_date, data_type, codes=batch_codes)
                except Exception as e:
                    logger.debug("加载历史 %s %s 失败: %s", hist_date, data_type, e)
                    return pd.DataFrame()

            full = _DayBundle(
                date=hist_date,
                l2_order=_safe_load("order") if factor_info.get("need_l2_order") else pd.DataFrame(),
                l2_deal=_safe_load("deal") if factor_info.get("need_l2_deal") else pd.DataFrame(),
                l1_tick=_safe_load("tick") if factor_info.get("need_l1_tick") else pd.DataFrame(),
                market=pd.DataFrame(),
            )

            filtered_for_date = {}
            for code in batch_codes:
                security_id = security_id_map.get(code)
                fc = _filter_code_from_bundle(full, code, security_id, factor_info)
                filtered_for_date[code] = _DayBundle(
                    date=hist_date,
                    l2_order=fc.l2_order,
                    l2_deal=fc.l2_deal,
                    l1_tick=fc.l1_tick,
                    market=fc.market,
                )
            filtered_cache[hist_date] = filtered_for_date
            del full

        for code in batch_codes:
            result[code].append(filtered_for_date[code])

    return result


class _FilteredCode:
    """单只股票的已过滤数据（全天），可按 end_time 重复创建 StockData。"""
    __slots__ = ("code", "security_id", "l2_order", "l2_deal", "l1_tick",
                 "market", "l2_order_hist", "l2_deal_hist", "l1_tick_hist")

    def __init__(self, code: str, security_id: Optional[int],
                 l2_order: pd.DataFrame, l2_deal: pd.DataFrame,
                 l1_tick: pd.DataFrame, market: pd.DataFrame,
                 l2_order_hist: list, l2_deal_hist: list, l1_tick_hist: list):
        self.code = code
        self.security_id = security_id
        self.l2_order = l2_order
        self.l2_deal = l2_deal
        self.l1_tick = l1_tick
        self.market = market
        self.l2_order_hist = l2_order_hist
        self.l2_deal_hist = l2_deal_hist
        self.l1_tick_hist = l1_tick_hist


def _resolve_security_id(market_df: pd.DataFrame, code: str,
                        _id_map: Optional[Dict[str, int]] = None) -> Optional[int]:
    """从 daily_basic(market) 中查找 code 对应的 SECURITY_ID 整数。"""
    if market_df.empty or "ID_QI" not in market_df.columns or "SECURITY_ID" not in market_df.columns:
        return None
    id_qi = _code_to_id_qi(code)
    if _id_map is not None:
        return _id_map.get(id_qi)
    rows = market_df[market_df["ID_QI"].astype(str).str.zfill(6) == id_qi]
    if not rows.empty:
        return int(rows.iloc[0]["SECURITY_ID"])
    return None


def _build_security_id_map(market_df: pd.DataFrame) -> Dict[str, int]:
    """一次性构建 ID_QI → SECURITY_ID 映射，避免逐股全表扫描。"""
    if market_df.empty or "ID_QI" not in market_df.columns or "SECURITY_ID" not in market_df.columns:
        return {}
    id_qi = market_df["ID_QI"].astype(str).str.zfill(6)
    return dict(zip(id_qi, market_df["SECURITY_ID"].astype(int)))


def _filter_code_from_bundle(
    bundle: _DayBundle,
    code: str,
    security_id: Optional[int] = None,
    factor_info: Optional[Dict] = None,
    hist_bundles: Optional[List[_DayBundle]] = None,
) -> _FilteredCode:
    """从全市场数据包中过滤出单只股票的全天数据（只做一次）。

    包含 _restore_oss_precision + 历史 lookback，结果可在多个 end_time 间复用。
    security_id 由调用方通过 _resolve_security_id 预先查找，避免重复。
    """

    def _filter(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        for col in ("Code", "stock_code", "code"):
            if col in df.columns:
                if pd.api.types.is_integer_dtype(df[col]) and security_id is not None:
                    return df[df[col] == security_id].reset_index(drop=True)
                exact = df[df[col] == code]
                if not exact.empty:
                    return exact.reset_index(drop=True)
                id_qi = _code_to_id_qi(code)
                normalized = df[col].astype(str).str.split(".").str[0].str.zfill(6)
                return df[normalized == id_qi].reset_index(drop=True)
        if "SECURITY_ID" in df.columns and security_id is not None:
            return df[df["SECURITY_ID"] == security_id].reset_index(drop=True)
        if "ID_QI" in df.columns:
            id_qi = _code_to_id_qi(code)
            return df[df["ID_QI"].astype(str).str.zfill(6) == id_qi].reset_index(drop=True)
        return df

    l2_order = _restore_oss_precision(_filter(bundle.l2_order), code)
    l2_deal = _restore_oss_precision(_filter(bundle.l2_deal), code)
    l1_tick = _restore_oss_precision(_filter(bundle.l1_tick), code)
    market = _filter(bundle.market)

    l2_order_hist: list = []
    l2_deal_hist: list = []
    l1_tick_hist: list = []

    if hist_bundles:
        for hist_bundle in hist_bundles:
            l2_order_hist.append(hist_bundle.l2_order)
            l2_deal_hist.append(hist_bundle.l2_deal)
            l1_tick_hist.append(hist_bundle.l1_tick)

    return _FilteredCode(code, security_id, l2_order, l2_deal, l1_tick, market,
                         l2_order_hist, l2_deal_hist, l1_tick_hist)


def _stock_data_from_filtered(fc: _FilteredCode, date: str, end_time: str) -> StockData:
    """从已过滤数据创建 StockData（轻量，每个 end_time 调用一次）。"""
    if any(len(d) > 0 for d in [fc.l2_order, fc.l2_deal, fc.l1_tick, fc.market]):
        price_sample = None
        if not fc.l2_deal.empty and "Price" in fc.l2_deal.columns:
            price_sample = fc.l2_deal["Price"].iloc[0]
        elif not fc.l2_order.empty and "Price" in fc.l2_order.columns:
            price_sample = fc.l2_order["Price"].iloc[0]
        logger.info(
            "[StockData] %s date=%s end_time=%s order=%d行 deal=%d行 tick=%d行 "
            "market=%d行 Price样本=%s",
            fc.code, date, end_time, len(fc.l2_order), len(fc.l2_deal),
            len(fc.l1_tick), len(fc.market),
            f"{price_sample:.2f}" if price_sample is not None else "N/A",
        )
    return StockData(
        code=fc.code,
        date=date,
        end_time=end_time,
        l2_order=fc.l2_order.copy(),
        l2_deal=fc.l2_deal.copy(),
        l1_tick=fc.l1_tick.copy(),
        market=fc.market.copy(),
        daily_basic=fc.market.copy(),
        l2_order_hist=[df.copy() for df in fc.l2_order_hist],
        l2_deal_hist=[df.copy() for df in fc.l2_deal_hist],
        l1_tick_hist=[df.copy() for df in fc.l1_tick_hist],
    )


def _history_load_offsets(day_offsets: Optional[List[int]]) -> List[int]:
    """Return offsets that require an extra historical load.

    Offset 0 is the target day and is already available in the current bundle.
    """
    if not day_offsets:
        return []
    return [offset for offset in day_offsets if offset != 0]


def _combine_history_bundles(
    date: str,
    day_offsets: List[int],
    fc: _FilteredCode,
    loaded_bundles: List[_DayBundle],
) -> List[_DayBundle]:
    """Combine current-day filtered data with separately loaded history.

    The returned list preserves the factor_info["lookback_days"] offset order:
    [0, 1, 2] means [today, previous trading day, two trading days ago].
    """
    loaded_iter = iter(loaded_bundles)
    combined: List[_DayBundle] = []
    for offset in day_offsets:
        if offset == 0:
            combined.append(_DayBundle(
                date=date,
                l2_order=fc.l2_order,
                l2_deal=fc.l2_deal,
                l1_tick=fc.l1_tick,
                market=fc.market,
            ))
        else:
            combined.append(next(loaded_iter, _DayBundle(
                date="",
                l2_order=pd.DataFrame(),
                l2_deal=pd.DataFrame(),
                l1_tick=pd.DataFrame(),
                market=pd.DataFrame(),
            )))
    return combined


def _attach_history_to_filtered(fc: _FilteredCode, hist_bundles: List[_DayBundle]) -> None:
    """Attach lookback lists to a filtered code object without re-filtering."""
    fc.l2_order_hist = [bundle.l2_order for bundle in hist_bundles]
    fc.l2_deal_hist = [bundle.l2_deal for bundle in hist_bundles]
    fc.l1_tick_hist = [bundle.l1_tick for bundle in hist_bundles]


def _merge_results(all_res: list) -> pd.DataFrame:
    """
    将所有股票的结果 dict 合并成 DataFrame。
    若列表为空，返回空 DataFrame。
    """
    if not all_res:
        return pd.DataFrame()
    return pd.DataFrame(all_res)


def _daily_basic_for_inference(
    api: DataAPI,
    date: str,
    is_explicit_list: bool,
    daily_basic: Optional[pd.DataFrame],
    cached_daily_basic: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Return current-day daily_basic for inference without per-end_time reloads."""
    if not is_explicit_list:
        return daily_basic if daily_basic is not None else pd.DataFrame()
    if cached_daily_basic is not None:
        return cached_daily_basic
    return api.get_daily_data(date, "daily_basic")


def _uses_current_day_market(factor_info: Dict) -> bool:
    """Return True when bundle.market is current-day daily_basic, not history."""
    return int(factor_info.get("market_count", 1)) <= 1


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


def _parse_day_offsets(factor_info: Dict) -> Optional[List[int]]:
    """从 factor_info 解析 lookback_days，返回 day_offsets 列表或 None。"""
    lookback_days = factor_info.get("lookback_days")
    if not lookback_days:
        return None
    if isinstance(lookback_days, int):
        return list(range(0, lookback_days))
    if isinstance(lookback_days, list):
        return lookback_days
    return None


def _code_to_id_qi(code) -> str:
    """Normalize code-like values to six-digit ID_QI."""
    return str(code).split(".")[0].zfill(6)
