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
import multiprocessing
import os
import pickle
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
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
# Fork-shared globals: set before pool creation, inherited by workers via COW
# ---------------------------------------------------------------------------
_shared_bundle = None           # type: Optional[_DayBundle]
_shared_security_id_map = {}    # type: Dict[str, Optional[int]]
_shared_factor_info = {}        # type: Dict
_shared_calc_fn = None          # type: Optional[Callable]


# ---------------------------------------------------------------------------
# 公开接口
# ---------------------------------------------------------------------------

def _init_idx_cons_cache_for_backtest():
    """初始化回测用的成分股缓存(可选)。

    通过环境变量 IDX_CONS_CODES(优先) 或 IDX_CONS_IDS(向后兼容) 控制是否启用。
    未配置任何变量时返回 None,回测里跳过成分股相关逻辑(保持旧行为)。

    数据源:OSS composition.parquet 优先,fallback MySQL 区间查询。
    """
    import os
    codes_str = os.environ.get("IDX_CONS_CODES", "")
    if not codes_str:
        # 向后兼容:把 SECURITY_ID 映射回 TICKER
        legacy = os.environ.get("IDX_CONS_IDS", "")
        if legacy:
            mapping = {"1782": "000300", "2103": "000905", "33736": "000852",
                       "3800": "000985", "1200245": "932000"}
            codes_str = ",".join(
                mapping[s.strip()] for s in legacy.split(",")
                if s.strip() and s.strip() in mapping
            )

    if not codes_str:
        logger.info("[backtest] IDX_CONS_CODES 未配置,跳过成分股缓存初始化")
        return None

    codes = [s.strip() for s in codes_str.split(",") if s.strip()]

    # 懒加载 OSSDataLoader(可选,失败不影响 MySQL fallback)
    oss_loader = None
    if os.environ.get("OSS_ACCESS_KEY_ID") and os.environ.get("OSS_ACCESS_KEY_SECRET"):
        try:
            from ..data.oss_loader import OSSDataLoader
            oss_loader = OSSDataLoader()
        except Exception as exc:
            logger.warning("[backtest] OSSDataLoader init failed: %s", exc)

    try:
        from ..data.mysql_loader import IdxConsCache
        cache = IdxConsCache(index_codes=codes, oss_loader=oss_loader)
        logger.info("[backtest] idx_cons 缓存已初始化: codes=%s", codes)
        return cache
    except Exception as exc:
        logger.warning("[backtest] idx_cons 缓存初始化失败: %s", exc)
        return None




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

    # 初始化成分股缓存(可选,由 IDX_CONS_CODES / IDX_CONS_IDS 环境变量控制)
    # 数据源优先 OSS composition.parquet,fallback MySQL 区间查询
    # 用法:每个交易日按 date 查 cache,得到当日真实成分股(避免幸存者偏差)
    _idx_cons_cache = _init_idx_cons_cache_for_backtest()

    # 交易日列表
    try:
        trading_days = api.get_trading_days(start_date, end_date)
    except Exception:
        trading_days = _fallback_trading_days(start_date, end_date)

    if not trading_days:
        logger.warning("从 OSS 获取交易日列表失败，使用回退逻辑（剔除周末）")
        trading_days = _fallback_trading_days(start_date, end_date)

    _end_times = end_times if end_times else [""]
    zero_copy_stock_data = os.environ.get("FACTOR_ZERO_COPY", "1").lower() not in ("0", "false", "no")
    multi_slice = len(_end_times) > 1  # 多时间切片策略（分钟/10分钟/20分钟等）
    force_streaming = os.environ.get("FACTOR_FORCE_STREAMING", "0").lower() in ("1", "true", "yes")

    # 判断是否为全市场模式
    is_explicit_list = securities and len(securities) > 0

    if is_explicit_list:
        # 明确指定了股票列表，使用传统预加载模式
        _securities = list(securities)
        use_streaming = force_streaming or len(_securities) > 100
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
        use_streaming = force_streaming or len(_securities) > 100  # 超过100只股票启用流式模式

        if use_streaming:
            # 分批加载模式：将全市场股票分批，每批下载一次文件并过滤
            # 避免每只股票单独下载（O(N×4GB)），也避免全量加载 OOM
            #
            # 两种 batch size。默认按 4C8G 分布式 worker 设置，优先控制内存峰值。
            # - DATA_BATCH_SIZE: DuckDB 查询每批的股票数（默认 200）
            # - FACTOR_BATCH_SIZE: 内存中同时持有的过滤后数据量（默认 100）
            #
            # 循环顺序：data_batch → compute_sub_batch → code → end_time
            # 每只股票只 filter 一次，多个 end_time 复用已过滤数据
            DATA_BATCH_SIZE = int(os.environ.get("DATA_BATCH_SIZE", "200"))
            COMPUTE_BATCH_SIZE = int(os.environ.get("FACTOR_BATCH_SIZE", "100"))
            TASK_CODE_BATCH_SIZE = int(os.environ.get("FACTOR_TASK_CODE_BATCH_SIZE", str(COMPUTE_BATCH_SIZE)))
            TASK_CODE_BATCH_SIZE = max(1, min(TASK_CODE_BATCH_SIZE, COMPUTE_BATCH_SIZE))
            if multi_slice:
                logger.info(
                    "多时间切片策略：%d 个 end_time，数据加载 batch=%d，计算 batch=%d，task_codes=%d",
                    len(_end_times), DATA_BATCH_SIZE, COMPUTE_BATCH_SIZE, TASK_CODE_BATCH_SIZE,
                )

            data_batches = [_securities[i:i+DATA_BATCH_SIZE]
                            for i in range(0, len(_securities), DATA_BATCH_SIZE)]
            logger.info("分批加载模式：%d 只股票分 %d 个数据批次（每批 %d 只）",
                        len(_securities), len(data_batches), DATA_BATCH_SIZE)

            # per-end_time result collectors
            end_time_results: dict = {et: [] for et in _end_times}
            import time as _time
            flush_each_data_batch = (
                os.environ.get("FACTOR_FLUSH_EACH_DATA_BATCH", "1").lower() in ("1", "true", "yes")
                and _out_fn is not None
                and _infer_fn is None
            )

            # --- Pool setup ---
            FORK_SHARED = (
                os.environ.get("FACTOR_FORK_SHARED_BUNDLE", "1").lower() in ("1", "true", "yes")
                and processes > 1
                and _calc_fn is not None
            )
            pool = None  # ProcessPoolExecutor (fallback path)
            if not FORK_SHARED and processes > 1 and _calc_fn is not None:
                enable_process_pool = os.environ.get("FACTOR_ENABLE_PROCESS_POOL", "1").lower() in ("1", "true", "yes")
                if enable_process_pool:
                    try:
                        pickle.dumps(_calc_fn)
                        pool = ProcessPoolExecutor(max_workers=processes)
                    except Exception as e:
                        logger.warning(
                            "factor_data_handler 不支持进程池序列化，降级为串行计算: %s",
                            e,
                        )
                else:
                    logger.info("FACTOR_ENABLE_PROCESS_POOL=0，进程池已关闭，使用串行计算")
            elif FORK_SHARED:
                logger.info("使用 fork-shared bundle 模式（per-data_batch pool）")

            day_offsets = _parse_day_offsets(factor_info)
            hist_offsets = _history_load_offsets(day_offsets)
            try:
                for data_batch_idx, data_batch_codes in enumerate(data_batches):
                    _t_load = _time.time()
                    market_override = (
                        daily_basic if not is_explicit_list and _uses_current_day_market(factor_info)
                        else None
                    )
                    bundle = _load_day_bundle(date, factor_info, api, data_batch_codes,
                                              market_override=market_override)
                    _load_elapsed = _time.time() - _t_load
                    logger.info("[Perf] data_batch %d/%d _load_day_bundle=%.2fs (%d stocks)",
                                data_batch_idx+1, len(data_batches), _load_elapsed, len(data_batch_codes))
                    if is_explicit_list and daily_basic_for_inference is None and _uses_current_day_market(factor_info):
                        daily_basic_for_inference = bundle.market

                    # 预解析 security_id
                    _id_map = _build_security_id_map(bundle.market)
                    security_id_map = {code: _id_map.get(_code_to_id_qi(code)) for code in data_batch_codes}

                    # Split data batch into compute sub-batches to control memory
                    compute_batches = [data_batch_codes[i:i+COMPUTE_BATCH_SIZE]
                                       for i in range(0, len(data_batch_codes), COMPUTE_BATCH_SIZE)]

                    # Fork-shared path: create pool per data_batch AFTER bundle loaded
                    fork_pool = None
                    shared_state_set = False
                    if FORK_SHARED:
                        global _shared_bundle, _shared_security_id_map
                        global _shared_factor_info, _shared_calc_fn
                        _shared_bundle = bundle
                        _shared_security_id_map = security_id_map
                        _shared_factor_info = factor_info
                        _shared_calc_fn = _calc_fn
                        shared_state_set = True
                        try:
                            fork_ctx = multiprocessing.get_context("fork")
                        except ValueError:
                            logger.warning(
                                "当前平台不支持 fork start method，降级为串行计算；"
                                "fork-shared bundle 仅支持 Linux/Unix fork"
                            )
                            FORK_SHARED = False
                        else:
                            fork_pool = fork_ctx.Pool(
                                processes=processes, maxtasksperchild=None,
                            )
                            logger.info("[Perf] data_batch %d: fork pool created (%d workers, bundle COW)",
                                        data_batch_idx + 1, processes)

                    try:
                        for comp_batch in compute_batches:
                            # Batch-load history for this compute batch
                            hist_by_code: Dict[str, list] = {}
                            if hist_offsets:
                                hist_by_code = _load_history_bundles_batch(
                                    date, hist_offsets, factor_info, api, comp_batch, security_id_map)

                            if fork_pool is not None:
                                # --- FORK-SHARED PARALLEL: workers inherit bundle via COW ---
                                _t_par = _time.time()
                                task_codes = [comp_batch[i:i+TASK_CODE_BATCH_SIZE]
                                              for i in range(0, len(comp_batch), TASK_CODE_BATCH_SIZE)]
                                tasks = [
                                    (batch, date, _end_times, day_offsets,
                                     {c: hist_by_code.get(c, []) for c in batch})
                                    for batch in task_codes
                                ]
                                n_codes = 0
                                for results in fork_pool.imap_unordered(
                                    _compute_codes_from_shared_bundle, tasks, chunksize=1,
                                ):
                                    for code, code_results in results.items():
                                        for et, res in code_results.items():
                                            if res is not None:
                                                end_time_results[et].append(res)
                                    n_codes += len(results)
                                _par_elapsed = _time.time() - _t_par
                                logger.info("[Perf] fork_parallel %d codes in %.2fs (%d processes)",
                                            n_codes, _par_elapsed, processes)

                            elif pool is not None:
                                # --- PICKLE PARALLEL: reuse ProcessPoolExecutor (fallback) ---
                                _t_par = _time.time()
                                n_codes = 0
                                max_inflight = processes * 2
                                futures = {}
                                submitted_at = {}
                                task_codes = [comp_batch[i:i+TASK_CODE_BATCH_SIZE]
                                              for i in range(0, len(comp_batch), TASK_CODE_BATCH_SIZE)]

                                for task_code_batch in task_codes:
                                    while len(futures) >= max_inflight:
                                        done = _wait_one_future(
                                            futures, submitted_at, end_time_results,
                                            date, data_batch_idx, comp_batch,
                                        )
                                        if done is None:
                                            continue

                                    try:
                                        _t_filter = _time.time()
                                        filtered_codes = _filter_codes_from_bundle(
                                            bundle, task_code_batch, security_id_map, factor_info,
                                        )
                                        filter_elapsed = _time.time() - _t_filter
                                        fc_dicts = []
                                        for fc in filtered_codes:
                                            if day_offsets:
                                                hist_bundles = _combine_history_bundles(
                                                    date, day_offsets, fc, hist_by_code.get(fc.code, []))
                                                _attach_history_to_filtered(fc, hist_bundles)
                                            fc_dicts.append(_filtered_code_to_dict(fc))
                                        _t_ser = _time.time()
                                        future = pool.submit(
                                            _compute_codes_all_endtimes,
                                            fc_dicts, date, _end_times, _calc_fn, factor_info,
                                        )
                                        _t_submit_elapsed = _time.time() - _t_ser
                                        futures[future] = list(task_code_batch)
                                        submitted_at[future] = _time.time()
                                        n_codes += len(task_code_batch)
                                        if n_codes <= 20 or filter_elapsed > 1.0 or _t_submit_elapsed > 1.0:
                                            logger.info(
                                                "[Perf] codes=%d filter=%.3fs submit=%.3fs",
                                                len(task_code_batch), filter_elapsed, _t_submit_elapsed)
                                    except Exception as e:
                                        logger.warning("Filter failed codes=%s: %s", task_code_batch[:5], e)

                                while futures:
                                    done = _wait_one_future(
                                        futures, submitted_at, end_time_results,
                                        date, data_batch_idx, comp_batch,
                                    )
                                    if done is None:
                                        continue

                                _par_elapsed = _time.time() - _t_par
                                logger.info("[Perf] pickle_parallel %d codes in %.2fs (%d processes)",
                                            n_codes, _par_elapsed, processes)
                            else:
                                # --- SERIAL ---
                                for code in comp_batch:
                                    try:
                                        security_id = security_id_map[code]
                                        fc = _filter_code_from_bundle(bundle, code, security_id, factor_info)
                                        if day_offsets:
                                            hist_bundles = _combine_history_bundles(
                                                date, day_offsets, fc, hist_by_code.get(code, []))
                                            _attach_history_to_filtered(fc, hist_bundles)

                                        for end_time in _end_times:
                                            stock_data = _stock_data_from_filtered(
                                                fc, date, end_time, factor_info=factor_info, zero_copy=zero_copy_stock_data
                                            )
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
                    finally:
                        if fork_pool is not None:
                            fork_pool.close()
                            fork_pool.join()
                        if shared_state_set:
                            _shared_bundle = None
                            _shared_security_id_map = {}
                            _shared_factor_info = {}
                            _shared_calc_fn = None

                    # Data batch completed, release bundle
                    del bundle

                    if flush_each_data_batch:
                        for end_time in _end_times:
                            test = _merge_results(end_time_results[end_time])
                            if test is not None and not test.empty:
                                test.attrs["part_index"] = data_batch_idx
                                test.attrs["part_count"] = len(data_batches)
                                test.attrs["partial"] = True
                                try:
                                    _out_fn(date, end_time, test)
                                except Exception as e:
                                    logger.error(
                                        "outfun 异常 date=%s end_time=%s part=%s: %s",
                                        date, end_time, data_batch_idx, e,
                                    )
                                last_result_for_date = test
                            end_time_results[end_time] = []
            finally:
                if pool is not None:
                    pool.shutdown(wait=True)

            # 按 end_time 汇总结果、运行推理、调用 outfun
            if not flush_each_data_batch:
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
                            if _idx_cons_cache is not None:
                                universe_extra["idx_cons_df"] = _idx_cons_cache.get_by_date(date)
                            trading_universe_df = compute_trading_universe(_daily, universe_extra)
                            index_composition_df = compute_index_composition(
                                _daily, universe_extra,
                                trading_day=date if _idx_cons_cache else None,
                                idx_cons_cache=_idx_cons_cache,
                            )
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
                        stock_data = _stock_data_from_filtered(
                            fc, date, end_time, factor_info=factor_info, zero_copy=zero_copy_stock_data
                        )
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
                        if _idx_cons_cache is not None:
                            universe_extra["idx_cons_df"] = _idx_cons_cache.get_by_date(date)
                        trading_universe_df = compute_trading_universe(_daily, universe_extra)
                        index_composition_df = compute_index_composition(
                            _daily, universe_extra,
                            trading_day=date if _idx_cons_cache else None,
                            idx_cons_cache=_idx_cons_cache,
                        )
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


def _restore_time_column(epoch_us: pd.Series, code: str) -> pd.Series:
    """
    将 int64 UnixMicro 时间戳还原为 Beijing datetime64[ns]。

    两种编码兼容：
      - 真实 UTC epoch（旧 live collector 写的）：UTC→北京 +8
      - 北京 wallclock 当作 UTC 的 epoch（新 collector archive bug）：
        原本就是北京 wallclock，不能再 +8，否则会得到 17:15-23:00 这种错位时段

    兜底策略：采样后比较两种解读在 A 股交易时段 [08:30, 15:30] 内的行数，
    取落在交易时段多的那种。这样无需关心 parquet 是哪个 collector 写的。
    """
    # 都先按 UTC 解
    utc_dt = pd.to_datetime(epoch_us, unit="us", utc=True)
    # A: 标准 UTC→北京
    bj_via_utc = utc_dt.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    # B: 直接当北京 wallclock（archive 双重编码兜底）
    bj_direct = utc_dt.dt.tz_localize(None)

    # 采样最多 200 行做判断，避免大表全量计算
    n = len(epoch_us)
    if n > 200:
        step = max(1, n // 200)
        sample_via = bj_via_utc.iloc[::step]
        sample_direct = bj_direct.iloc[::step]
    else:
        sample_via = bj_via_utc
        sample_direct = bj_direct

    # A 股交易时段：08:30-15:30 北京时间（含集合竞价 9:15-9:25 + 盘后）
    _TRADING_START = 8 * 60 + 30   # 08:30
    _TRADING_END = 15 * 60 + 30    # 15:30
    mins_via = sample_via.dt.hour * 60 + sample_via.dt.minute
    mins_direct = sample_direct.dt.hour * 60 + sample_direct.dt.minute
    n_via = int(((mins_via >= _TRADING_START) & (mins_via <= _TRADING_END)).sum())
    n_direct = int(((mins_direct >= _TRADING_START) & (mins_direct <= _TRADING_END)).sum())

    if n_direct > n_via:
        # archive 把北京 wallclock 当 UTC 写了 epoch，直接按 wallclock 用
        logger.warning(
            "[精度还原] %s Time 检测到时区双重编码（UTC解读 in-trading=%d, "
            "wallclock解读 in-trading=%d），按北京 wallclock 直解",
            code, n_via, n_direct,
        )
        return bj_direct
    return bj_via_utc


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
        df["Time"] = _restore_time_column(df["Time"], code)
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


def _filtered_code_to_dict(fc: _FilteredCode) -> dict:
    """Convert _FilteredCode to plain dict for inter-process serialization."""
    return {
        "code": fc.code,
        "security_id": fc.security_id,
        "l2_order": fc.l2_order,
        "l2_deal": fc.l2_deal,
        "l1_tick": fc.l1_tick,
        "market": fc.market,
        "l2_order_hist": fc.l2_order_hist,
        "l2_deal_hist": fc.l2_deal_hist,
        "l1_tick_hist": fc.l1_tick_hist,
    }


def _compute_code_all_endtimes(
    fc_data: dict,
    date: str,
    end_times: list,
    calc_fn: Callable,
    factor_info: Optional[Dict] = None,
) -> dict:
    """Worker entry: compute factor_calculation for one code batch.

    The public strategy signature stays unchanged, but batch calls always pass:
      data={code: StockData}, code=[code], end_time=end_times.
    Returns the engine's internal {end_time: result_or_None} shape.
    """
    import time as _wtime
    _t0 = _wtime.time()
    fc = _FilteredCode(
        code=fc_data["code"],
        security_id=fc_data["security_id"],
        l2_order=fc_data["l2_order"],
        l2_deal=fc_data["l2_deal"],
        l1_tick=fc_data["l1_tick"],
        market=fc_data["market"],
        l2_order_hist=fc_data["l2_order_hist"],
        l2_deal_hist=fc_data["l2_deal_hist"],
        l1_tick_hist=fc_data["l1_tick_hist"],
    )
    _t_deser = _wtime.time()

    stock_data = _stock_data_from_filtered(fc, date, "", factor_info=factor_info, zero_copy=True)
    try:
        batch_results = calc_fn({fc.code: stock_data}, [fc.code], date, end_times)
        _t_total = _wtime.time() - _t0
        normalized = _normalize_batch_results(fc.code, end_times, batch_results)
        if normalized is not None:
            if _t_total > 1.0:
                logger.info("[Worker] code=%s batch total=%.2fs deser=%.3fs results=%d",
                            fc.code, _t_total, _t_deser - _t0, len(normalized))
            return normalized
        logger.debug("[Worker] code=%s batch returned %s, fallback to per-et",
                     fc.code, type(batch_results).__name__)
    except Exception as e:
        logger.warning("[Worker] code=%s batch failed, fallback to per-et: %s", fc.code, e)

    # --- Fallback: per-end_time calls with correct end_time ---
    results = {}
    _slow_count = 0
    for et in end_times:
        try:
            _t1 = _wtime.time()
            et_stock_data = _stock_data_from_filtered(fc, date, et, factor_info=factor_info, zero_copy=True)
            res = calc_fn(et_stock_data, fc.code, date, et)
            _t_calc = _wtime.time() - _t1
            if _t_calc > 0.1:
                _slow_count += 1
            results[et] = res
        except Exception as e:
            logger.warning("Worker: factor_calculation failed code=%s end_time=%s: %s",
                          fc.code, et, e)
            results[et] = None
    _t_total = _wtime.time() - _t0
    if _t_total > 2.0:
        logger.info("[Worker] code=%s fallback total=%.2fs deser=%.3fs calc=%d×%d slow_et=%d",
                    fc.code, _t_total, _t_deser - _t0, len(end_times), len(results),
                    _slow_count)
    return results


def _compute_codes_all_endtimes(
    fc_data_list: list,
    date: str,
    end_times: list,
    calc_fn: Callable,
    factor_info: Optional[Dict] = None,
) -> dict:
    """Worker entry: compute a batch of codes in one strategy call.

    Strategy receives the unified batch signature:
      data={code: StockData}, code=[...], end_time=end_times.

    Returns {code: {end_time: result_or_None}}.
    """
    import time as _wtime
    _t0 = _wtime.time()
    data_map = {}
    codes = []

    for fc_data in fc_data_list:
        fc = _FilteredCode(
            code=fc_data["code"],
            security_id=fc_data["security_id"],
            l2_order=fc_data["l2_order"],
            l2_deal=fc_data["l2_deal"],
            l1_tick=fc_data["l1_tick"],
            market=fc_data["market"],
            l2_order_hist=fc_data["l2_order_hist"],
            l2_deal_hist=fc_data["l2_deal_hist"],
            l1_tick_hist=fc_data["l1_tick_hist"],
        )
        stock_data = _stock_data_from_filtered(
            fc,
            date,
            end_times[0] if len(end_times) == 1 else end_times,
            factor_info=factor_info,
            zero_copy=True,
        )
        data_map[fc.code] = stock_data
        codes.append(fc.code)

    _t_build = _wtime.time()
    raw = calc_fn(data_map, codes, date, end_times) if data_map else {}
    normalized = _normalize_multi_code_batch_results(codes, end_times, raw)
    if normalized is None:
        raise ValueError(
            f"batch result protocol mismatch: got {type(raw).__name__}, "
            "expected {code: result}, {code: {end_time: result}}, or rows with code/ID_QI"
        )
    _t_total = _wtime.time() - _t0
    if _t_total > 1.0:
        logger.info(
            "[Worker] codes=%d batch total=%.2fs build=%.3fs results=%d",
            len(codes), _t_total, _t_build - _t0, sum(len(v) for v in normalized.values()),
        )
    return normalized


def _compute_codes_from_shared_bundle(args):
    """Fork worker: read shared bundle via COW, filter codes, batch strategy call.

    Instead of receiving pickled DataFrames, this worker reads the bundle from
    module globals (inherited via fork COW) and performs its own filtering.
    Only the code list and small history data are passed via the task argument.

    Returns {code: {end_time: result_or_None}}.
    """
    import time as _wtime
    _t0 = _wtime.time()

    try:
        codes, date, end_times, day_offsets, hist_by_code = args

        bundle = _shared_bundle
        security_id_map = _shared_security_id_map
        factor_info = _shared_factor_info
        calc_fn = _shared_calc_fn
        if bundle is None or calc_fn is None:
            raise RuntimeError("fork shared state is not initialized")

        # Worker-side filtering: groupby produces new DataFrames (no COW mutation)
        filtered_codes = _filter_codes_from_bundle(
            bundle, codes, security_id_map, factor_info,
        )

        data_map = {}
        for fc in filtered_codes:
            try:
                if day_offsets:
                    hist_bundles = _combine_history_bundles(
                        date, day_offsets, fc, hist_by_code.get(fc.code, []),
                    )
                    _attach_history_to_filtered(fc, hist_bundles)
                stock_data = _stock_data_from_filtered(
                    fc, date,
                    end_times[0] if len(end_times) == 1 else end_times,
                    factor_info=factor_info,
                    zero_copy=True,
                )
                data_map[fc.code] = stock_data
            except Exception as e:
                logger.warning("[ForkWorker] filter+build failed code=%s: %s", fc.code, e)

        _t_build = _wtime.time()
        raw = calc_fn(data_map, list(data_map.keys()), date, end_times) if data_map else {}
        normalized = _normalize_multi_code_batch_results(list(data_map.keys()), end_times, raw)
        if normalized is None:
            raise ValueError(f"batch result protocol mismatch: {type(raw).__name__}")

        _t_total = _wtime.time() - _t0
        if _t_total > 1.0:
            logger.info(
                "[ForkWorker] codes=%d total=%.2fs build=%.3fs results=%d",
                len(data_map), _t_total, _t_build - _t0,
                sum(len(v) for v in normalized.values()),
            )
        return normalized
    except Exception as e:
        code_sample = args[0][:5] if args and isinstance(args[0], list) else []
        logger.warning("[ForkWorker] task failed codes=%s: %s", code_sample, e)
        codes = args[0] if args and isinstance(args[0], list) else []
        end_times = args[2] if len(args) > 2 and isinstance(args[2], list) else [""]
        return {code: {et: None for et in end_times} for code in codes}


def _normalize_multi_code_batch_results(codes: list, end_times: list, value) -> Optional[dict]:
    """Normalize batch output to {code: {end_time: result_or_None}}."""
    if not isinstance(value, dict):
        rows = _flatten_factor_result(value)
        return _group_flat_rows_by_code(codes, end_times, rows)

    normalized = {}
    code_keyed = True
    for code in codes:
        code_value = value.get(code)
        if code_value is None:
            normalized[code] = {et: None for et in end_times}
            continue
        if not isinstance(code_value, dict):
            code_keyed = False
            break
        if len(end_times) > 1:
            if not all(et in code_value for et in end_times):
                code_keyed = False
                break
            normalized[code] = {et: code_value.get(et) for et in end_times}
        else:
            normalized[code] = {end_times[0]: code_value}
    if code_keyed:
        return normalized

    rows = _flatten_factor_result(value)
    return _group_flat_rows_by_code(codes, end_times, rows)


def _normalize_batch_results(code: str, end_times: list, value) -> Optional[dict]:
    """Normalize strategy batch output to {end_time: result_or_None}.

    Strict protocol:
      1. Single end_time  → strategy returns {code: result_dict}
         → we return {end_time: result_dict}
      2. Multiple end_times → strategy returns {code: {et: result_dict}}
         → we return {et: result_dict}
    Anything else returns None, triggering per-et fallback.
    """
    if not isinstance(value, dict):
        rows = _flatten_factor_result(value)
        return {end_times[0]: rows} if len(end_times) == 1 and rows else None

    # Must be keyed by code
    code_value = value.get(code)
    if code_value is None:
        rows = _flatten_factor_result(value)
        return {end_times[0]: rows} if len(end_times) == 1 and rows else None

    if not isinstance(code_value, dict):
        return None

    # Multi end_time: code_value must be {et: result_dict}
    if len(end_times) > 1:
        if all(et in code_value for et in end_times):
            return {et: code_value.get(et) for et in end_times}
        return None

    # Single end_time: code_value is the result dict directly
    return {end_times[0]: code_value}


def _group_flat_rows_by_code(codes: list, end_times: list, rows: list) -> Optional[dict]:
    """Group flat row output from a batched strategy into worker result shape."""
    if len(end_times) != 1 or not rows:
        return None

    wanted = {_code_to_id_qi(code): code for code in codes}
    grouped = {code: [] for code in codes}
    for row in rows:
        if not isinstance(row, dict):
            return None
        row_code = _row_code_key(row)
        code = wanted.get(row_code)
        if code is None:
            return None
        grouped[code].append(row)

    if any(not items for items in grouped.values()):
        return None
    return {code: {end_times[0]: items} for code, items in grouped.items()}


def _row_code_key(row: dict) -> str:
    for key in ("code", "Code", "ID_QI", "stock_code", "ts_code"):
        if key in row and row[key] is not None:
            return _code_to_id_qi(row[key])
    return ""


def _harvest_future(future, futures_map: dict, end_time_results: dict) -> None:
    """Collect result from a completed future, remove from map."""
    codes = futures_map.pop(future, None)
    if codes is None:
        return
    try:
        results = future.result()
        if isinstance(codes, list):
            for code in codes:
                code_results = results.get(code, {}) if isinstance(results, dict) else {}
                for et, res in code_results.items():
                    if res is not None:
                        end_time_results[et].append(res)
        else:
            for et, res in results.items():
                if res is not None:
                    end_time_results[et].append(res)
    except Exception as e:
        logger.warning("Worker exception codes=%s: %s", codes, e)


def _wait_one_future(
    futures_map: dict,
    submitted_at: dict,
    end_time_results: dict,
    date: str,
    data_batch_idx: int,
    comp_batch: list,
):
    """Wait for one process-pool task, logging progress instead of blocking silently."""
    done, _ = wait(list(futures_map.keys()), timeout=30, return_when=FIRST_COMPLETED)
    if not done:
        now = datetime.now().timestamp()
        slow = []
        for future, code in list(futures_map.items()):
            elapsed = now - submitted_at.get(future, now)
            slow.append((elapsed, code))
        slow.sort(reverse=True)
        sample = ", ".join(f"{code}:{elapsed:.0f}s" for elapsed, code in slow[:5])
        logger.info(
            "[Perf] comp_batch waiting date=%s data_batch=%d pending=%d codes=%d slowest=[%s]",
            date, data_batch_idx + 1, len(futures_map), len(comp_batch), sample,
        )
        return None

    future = next(iter(done))
    submitted_at.pop(future, None)
    _harvest_future(future, futures_map, end_time_results)
    return future


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
    _perf_log: bool = False,
) -> _FilteredCode:
    """从全市场数据包中过滤出单只股票的全天数据（只做一次）。

    包含 _restore_oss_precision + 历史 lookback，结果可在多个 end_time 间复用。
    security_id 由调用方通过 _resolve_security_id 预先查找，避免重复。
    """
    import time as _ptime

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

    _t0 = _ptime.time()
    order_filtered = _filter(bundle.l2_order)
    _t1 = _ptime.time()
    order_restored = _restore_oss_precision(order_filtered, code)
    _t2 = _ptime.time()
    deal_filtered = _filter(bundle.l2_deal)
    _t3 = _ptime.time()
    deal_restored = _restore_oss_precision(deal_filtered, code)
    _t4 = _ptime.time()
    tick_filtered = _filter(bundle.l1_tick)
    _t5 = _ptime.time()
    tick_restored = _restore_oss_precision(tick_filtered, code)
    _t6 = _ptime.time()
    market = _filter(bundle.market)
    _t7 = _ptime.time()

    if _perf_log and (_t7 - _t0) > 0.5:
        logger.info(
            "[Perf] filter+restore code=%s order_f=%.3fs order_r=%.3fs(%drows) "
            "deal_f=%.3fs deal_r=%.3fs(%drows) tick_f=%.3fs tick_r=%.3fs(%drows) "
            "market_f=%.3fs total=%.3fs",
            code,
            _t1 - _t0, _t2 - _t1, len(order_restored),
            _t3 - _t2, _t4 - _t3, len(deal_restored),
            _t5 - _t4, _t6 - _t5, len(tick_restored),
            _t7 - _t6, _t7 - _t0,
        )

    l2_order = order_restored
    l2_deal = deal_restored
    l1_tick = tick_restored

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


def _filter_codes_from_bundle(
    bundle: _DayBundle,
    codes: List[str],
    security_id_map: Dict[str, Optional[int]],
    factor_info: Optional[Dict] = None,
) -> List[_FilteredCode]:
    """Filter a bundle into per-code frames in one pass per table.

    This avoids scanning the same large tick/deal DataFrame once per code.
    It still materializes per-code DataFrames because StockData is per-code,
    but the split is done with one groupby over the requested task batch.
    """
    code_by_security_id = {
        int(sec_id): code
        for code in codes
        for sec_id in [security_id_map.get(code)]
        if sec_id is not None
    }
    id_qi_by_code = {_code_to_id_qi(code): code for code in codes}
    code_set = set(codes)

    def _empty_by_code() -> Dict[str, pd.DataFrame]:
        return {code: pd.DataFrame() for code in codes}

    def _split(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if df.empty:
            return _empty_by_code()

        col = next((c for c in ("Code", "stock_code", "code") if c in df.columns), None)
        if col is None and "SECURITY_ID" in df.columns:
            col = "SECURITY_ID"
        if col is None and "ID_QI" in df.columns:
            col = "ID_QI"
        if col is None:
            return {code: df.reset_index(drop=True) for code in codes}

        if pd.api.types.is_integer_dtype(df[col]) and code_by_security_id:
            wanted_ids = set(code_by_security_id.keys())
            subset = df[df[col].isin(wanted_ids)]
            groups = {
                code_by_security_id[int(key)]: part.reset_index(drop=True)
                for key, part in subset.groupby(col, sort=False)
                if int(key) in code_by_security_id
            }
            return {code: groups.get(code, pd.DataFrame()) for code in codes}

        if col == "ID_QI":
            normalized = df[col].astype(str).str.zfill(6)
        else:
            normalized = df[col].astype(str).str.split(".").str[0].str.zfill(6)

        subset = df[normalized.isin(id_qi_by_code.keys())].copy()
        if subset.empty:
            return _empty_by_code()
        subset["_qp_split_code"] = normalized[subset.index].map(id_qi_by_code)
        groups = {
            key: part.drop(columns=["_qp_split_code"]).reset_index(drop=True)
            for key, part in subset.groupby("_qp_split_code", sort=False)
            if key in code_set
        }
        return {code: groups.get(code, pd.DataFrame()) for code in codes}

    order_by_code = _split(bundle.l2_order)
    deal_by_code = _split(bundle.l2_deal)
    tick_by_code = _split(bundle.l1_tick)
    market_by_code = _split(bundle.market)

    filtered: List[_FilteredCode] = []
    for code in codes:
        order = _restore_oss_precision(order_by_code.get(code, pd.DataFrame()), code)
        deal = _restore_oss_precision(deal_by_code.get(code, pd.DataFrame()), code)
        tick = _restore_oss_precision(tick_by_code.get(code, pd.DataFrame()), code)
        market = market_by_code.get(code, pd.DataFrame())
        filtered.append(_FilteredCode(
            code,
            security_id_map.get(code),
            order,
            deal,
            tick,
            market,
            [],
            [],
            [],
        ))
    return filtered


def _stock_data_from_filtered(fc: _FilteredCode, date: str, end_time: str,
                             factor_info: Optional[Dict] = None,
                             zero_copy: bool = False) -> StockData:
    """从已过滤数据创建 StockData（统一走 build_stock_data）。"""
    from .stock_data_builder import build_stock_data

    if any(len(d) > 0 for d in [fc.l2_order, fc.l2_deal, fc.l1_tick, fc.market]):
        price_sample = None
        if not fc.l2_deal.empty and "Price" in fc.l2_deal.columns:
            price_sample = fc.l2_deal["Price"].iloc[0]
        elif not fc.l2_order.empty and "Price" in fc.l2_order.columns:
            price_sample = fc.l2_order["Price"].iloc[0]
        logger.debug(
            "[StockData] %s date=%s end_time=%s order=%d行 deal=%d行 tick=%d行 "
            "market=%d行 Price样本=%s",
            fc.code, date, end_time, len(fc.l2_order), len(fc.l2_deal),
            len(fc.l1_tick), len(fc.market),
            f"{price_sample:.2f}" if price_sample is not None else "N/A",
        )
    return build_stock_data(
        code=fc.code, date=date, end_time=end_time,
        tick_df=fc.l1_tick if zero_copy else fc.l1_tick.copy(),
        deal_df=fc.l2_deal if zero_copy else fc.l2_deal.copy(),
        order_df=fc.l2_order if zero_copy else fc.l2_order.copy(),
        market_df=fc.market if zero_copy else fc.market.copy(),
        daily_basic_df=fc.market if zero_copy else fc.market.copy(),
        factor_info=factor_info,
        tick_hist=fc.l1_tick_hist if zero_copy else [d.copy() for d in fc.l1_tick_hist],
        deal_hist=fc.l2_deal_hist if zero_copy else [d.copy() for d in fc.l2_deal_hist],
        order_hist=fc.l2_order_hist if zero_copy else [d.copy() for d in fc.l2_order_hist],
        validate=True,
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

    rows = []
    for res in all_res:
        rows.extend(_flatten_factor_result(res))
    return pd.DataFrame(rows)


def _flatten_factor_result(res) -> list:
    """Normalize strategy output to a list of row dicts.

    Some intraday strategies keep the public day-level call signature and
    return 245 minute rows as {0: row, 1: row, ...}. Pandas would otherwise
    treat that as one record with integer columns, corrupting the output.
    """
    if res is None:
        return []

    if isinstance(res, pd.DataFrame):
        return res.to_dict("records")

    if isinstance(res, list):
        rows = []
        for item in res:
            rows.extend(_flatten_factor_result(item))
        return rows

    if not isinstance(res, dict):
        return []

    if _is_row_dict(res):
        return [res]

    values = list(res.values())
    if values and all(isinstance(item, dict) for item in values):
        return values

    return [res]


def _is_row_dict(res: dict) -> bool:
    """Return True when a dict already represents one factor row."""
    return any(key in res for key in ("code", "Code", "ID_QI", "datetime", "date", "end_time"))


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
