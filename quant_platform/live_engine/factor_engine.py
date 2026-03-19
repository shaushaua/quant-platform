# -*- coding: utf-8 -*-
"""
实盘因子计算引擎

数据源：ShmStore（/dev/shm/quant-store），由 collector 实时写入。
factor_calculation / outfun 函数签名与回测引擎完全兼容，可直接复用。

CronJob 调用示例：
    python -m quant_platform.live_engine.factor_engine --phase pre-open
    python -m quant_platform.live_engine.factor_engine --phase pre-close

环境变量：
    SHM_STORE_PATH     共享内存目录（默认 /dev/shm/quant-store）
    UNIVERSE           逗号分隔的股票代码，如 000001.SZ,600000.SH
                       不设置则计算 ShmStore 中全部有数据的股票
    FACTOR_MODULE      用户因子模块的 import 路径
                       模块须暴露 FACTOR_INFO / factor_calculation / outfun
    FACTOR_OUTPUT_PATH 因子结果输出目录（可选），写 CSV 到此目录
    LOG_LEVEL          日志级别（默认 INFO）
"""

import argparse
import importlib
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd

from ..data.shm_store import ShmStore
from ..factor.base import StockData

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# StockData 构建
# ---------------------------------------------------------------------------

def _build_stock_data(
    store: ShmStore,
    code: str,
    date: str,
    end_time: str,
    factor_info: Dict,
) -> StockData:
    """从 ShmStore 读取单只股票数据，构建 StockData（与回测引擎接口兼容）。"""
    l2_order = store.get_order(code) if factor_info.get("need_l2_order") else pd.DataFrame()
    l2_deal = store.get_deal(code) if factor_info.get("need_l2_deal") else pd.DataFrame()
    l1_tick = store.get_tick(code) if factor_info.get("need_l1_tick") else pd.DataFrame()
    daily_basic = store.get_daily_basic()

    # 按 end_time 过滤（截取截面时刻之前的数据）
    if end_time:
        for df_ref, attr in [
            (l2_order, "l2_order"),
            (l2_deal, "l2_deal"),
            (l1_tick, "l1_tick"),
        ]:
            if df_ref.empty:
                continue
            time_col = next(
                (c for c in df_ref.columns if c.lower() in ("time", "transacttime", "sendingtime")),
                None,
            )
            if time_col:
                try:
                    mask = df_ref[time_col].astype(str) <= end_time
                    locals()[attr] = df_ref[mask].reset_index(drop=True)
                except Exception:
                    pass

    return StockData(
        code=code,
        date=date,
        end_time=end_time,
        l2_order=l2_order,
        l2_deal=l2_deal,
        l1_tick=l1_tick,
        market=daily_basic,
        daily_basic=daily_basic,
    )


# ---------------------------------------------------------------------------
# 单股计算包装（支持多进程）
# ---------------------------------------------------------------------------

def _calc_one(
    code: str,
    date: str,
    end_time: str,
    factor_info: Dict,
    factor_module_path: str,
) -> Optional[Dict]:
    """
    在子进程中导入因子模块并计算单只股票的因子值。
    返回 dict 或 None（若计算失败）。
    """
    try:
        mod = importlib.import_module(factor_module_path)
        factor_calculation: Callable = mod.factor_calculation
        factor_info_mod: Dict = getattr(mod, "FACTOR_INFO", factor_info)

        store = ShmStore()
        data = _build_stock_data(store, code, date, end_time, factor_info_mod)
        result = factor_calculation(data, code, date, end_time)
        return result
    except Exception as exc:
        logging.getLogger(__name__).warning("[%s] 计算失败: %s", code, exc)
        return None


# ---------------------------------------------------------------------------
# 主引擎
# ---------------------------------------------------------------------------

def run(
    phase: str,
    end_time: str,
    securities: List[str],
    factor_module_path: str,
    processes: int = 4,
    output_path: Optional[Path] = None,
) -> None:
    """
    运行一次实盘因子计算。

    Args:
        phase:              "pre-open" 或 "pre-close"，用于日志和输出文件命名。
        end_time:           截面时刻，格式 HHMMSS（如 "092500"）。
        securities:         股票代码列表。
        factor_module_path: 因子模块 import 路径。
        processes:          并行进程数。
        output_path:        结果输出目录，None 则不写文件。
    """
    store = ShmStore()
    trading_day = store.get_trading_day() or datetime.now().strftime("%Y%m%d")
    date = trading_day

    logger.info("[live-engine] 开始计算 phase=%s date=%s end_time=%s stocks=%d",
                phase, date, end_time, len(securities))

    mod = importlib.import_module(factor_module_path)
    factor_info: Dict = getattr(mod, "FACTOR_INFO", {})
    outfun: Optional[Callable] = getattr(mod, "outfun", None)

    all_results = []
    if processes > 1:
        with ProcessPoolExecutor(max_workers=processes) as pool:
            futures = {
                pool.submit(_calc_one, code, date, end_time, factor_info, factor_module_path): code
                for code in securities
            }
            for fut in as_completed(futures):
                res = fut.result()
                if res is not None:
                    all_results.append(res)
    else:
        for code in securities:
            res = _calc_one(code, date, end_time, factor_info, factor_module_path)
            if res is not None:
                all_results.append(res)

    result_df = pd.DataFrame(all_results) if all_results else pd.DataFrame()
    logger.info("[live-engine] 计算完成，结果行数=%d", len(result_df))

    # 写文件
    if output_path and not result_df.empty:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        out_file = output_path / f"{date}_{phase}.csv"
        result_df.to_csv(out_file, index=False)
        logger.info("[live-engine] 结果已写入 %s", out_file)

    # 调用用户 outfun
    if outfun is not None:
        try:
            outfun(date, end_time, result_df)
        except Exception as exc:
            logger.error("[live-engine] outfun 执行失败: %s", exc)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def _get_securities(store: ShmStore) -> List[str]:
    """从 UNIVERSE 环境变量读取，或自动发现 ShmStore 中有 tick 数据的股票。"""
    universe_env = os.environ.get("UNIVERSE", "").strip()
    if universe_env:
        return [s.strip() for s in universe_env.split(",") if s.strip()]
    # 自动发现
    tick_dir = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/quant-store")) / "tick"
    if tick_dir.exists():
        codes = [f.stem for f in tick_dir.glob("*.arrow")]
        if codes:
            logger.info("[live-engine] 自动发现 %d 只股票", len(codes))
            return codes
    logger.warning("[live-engine] UNIVERSE 未配置且 ShmStore 无数据，退出")
    return []


PHASE_END_TIME = {
    "pre-open":  "092500",   # 开盘集合竞价结束前
    "pre-close": "145500",   # 收盘集合竞价结束前
}


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="实盘因子计算引擎")
    parser.add_argument(
        "--phase",
        choices=["pre-open", "pre-close"],
        required=True,
        help="触发阶段：pre-open（开盘前）或 pre-close（收盘前）",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="截面时刻 HHMMSS，不传则按 phase 自动推断",
    )
    args = parser.parse_args()

    factor_module = os.environ.get("FACTOR_MODULE")
    if not factor_module:
        logger.error("[live-engine] 环境变量 FACTOR_MODULE 未设置，退出")
        sys.exit(1)

    end_time = args.end_time or PHASE_END_TIME[args.phase]
    output_path_str = os.environ.get("FACTOR_OUTPUT_PATH", "")
    output_path = Path(output_path_str) if output_path_str else None
    processes = int(os.environ.get("FACTOR_PROCESSES", "4"))

    store = ShmStore()
    securities = _get_securities(store)
    if not securities:
        sys.exit(0)

    run(
        phase=args.phase,
        end_time=end_time,
        securities=securities,
        factor_module_path=factor_module,
        processes=processes,
        output_path=output_path,
    )


if __name__ == "__main__":
    main()
