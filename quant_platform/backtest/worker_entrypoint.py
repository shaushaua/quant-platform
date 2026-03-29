# -*- coding: utf-8 -*-
"""
Worker Entrypoint - 基础镜像提供，策略代码无需关心 OSS 写入

职责：
  1. 从环境变量读取任务参数（START_DATE / END_DATE / TASK_ID / SHARD_INDEX 等）
  2. 动态 import /app/strategy/strategy.py，获取 factor_info / securities /
     factor_calculation / outfun（后两者可选）
  3. 用内置 outfun 替换或包装用户 outfun，将每个 date×end_time 批次结果
     累积在内存中
  4. 全部计算完成后，将汇总结果序列化为 JSON 写入
     OSS: {RESULT_BUCKET}/{TASK_ID}/{SHARD_INDEX}.json
     格式与 aggregator.py 期望的 shard 格式一致

策略代码只需提供：
  - factor_info: dict
  - securities:  list[str]  （可选，默认读全市场）
  - factor_calculation(data, code, date, end_time) -> dict
  - outfun(date, end_time, test):  可选，不提供则基础镜像默认实现
"""

import importlib.util
import json
import os
import sys
from typing import Optional

import oss2
import pandas as pd

from quant_platform.factor.engine import calc_factors_by_date_range


# ---------------------------------------------------------------------------
# 环境变量读取
# ---------------------------------------------------------------------------

def _require_env(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        print(f"[worker] ERROR: 环境变量 {key} 未设置", file=sys.stderr)
        sys.exit(1)
    return val


START_DATE   = _require_env("START_DATE").replace("-", "")   # YYYYMMDD
END_DATE     = _require_env("END_DATE").replace("-", "")     # YYYYMMDD
TASK_ID      = _require_env("TASK_ID")
SHARD_INDEX  = int(os.environ.get("SHARD_INDEX", "0"))
DATA_PATH    = os.environ.get("DATA_PATH", "/data")
RESULT_BUCKET = os.environ.get("RESULT_BUCKET", "stock-mdl-data-result")
PROCESSES    = int(os.environ.get("WORKERS", "1"))

STRATEGY_PATH = "/app/strategy/strategy.py"


# ---------------------------------------------------------------------------
# OSS 工具
# ---------------------------------------------------------------------------

def _get_bucket() -> oss2.Bucket:
    auth = oss2.Auth(
        _require_env("OSS_ACCESS_KEY_ID"),
        _require_env("OSS_ACCESS_KEY_SECRET"),
    )
    return oss2.Bucket(
        auth,
        _require_env("OSS_ENDPOINT"),
        RESULT_BUCKET,
    )


def _write_shard_result(result: dict) -> None:
    key = f"{TASK_ID}/{SHARD_INDEX}.json"
    bucket = _get_bucket()
    bucket.put_object(key, json.dumps(result, ensure_ascii=False, default=str).encode("utf-8"))
    print(f"[worker] 结果已写入 oss://{RESULT_BUCKET}/{key}")


# ---------------------------------------------------------------------------
# 动态加载策略模块
# ---------------------------------------------------------------------------

def _load_strategy():
    if not os.path.exists(STRATEGY_PATH):
        print(f"[worker] ERROR: 策略文件不存在: {STRATEGY_PATH}", file=sys.stderr)
        sys.exit(1)

    spec = importlib.util.spec_from_file_location("strategy", STRATEGY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# 默认 outfun：收集每批次结果到全局列表
# ---------------------------------------------------------------------------

_all_results: list[dict] = []


def _make_collecting_outfun(user_outfun=None):
    """
    返回一个 outfun，将每批次 DataFrame 收集到 _all_results。
    如果策略提供了自己的 outfun，先调用它（允许策略做额外处理），
    再把结果收集起来。
    """
    def _outfun(date: str, end_time: str, test: pd.DataFrame) -> None:
        if user_outfun is not None:
            try:
                user_outfun(date, end_time, test)
            except Exception as e:
                print(f"[worker] WARNING: 用户 outfun 异常: {e}")

        if test.empty:
            return

        for _, row in test.iterrows():
            record = row.to_dict()
            record["_date"] = date
            record["_end_time"] = end_time
            _all_results.append(record)

    return _outfun


# ---------------------------------------------------------------------------
# 汇总所有批次结果为 shard JSON
# ---------------------------------------------------------------------------

def _build_shard_result(all_results: list[dict]) -> dict:
    """
    将多个 date×end_time 批次的结果合并为 shard 级别的汇总。
    数值列取均值，保留元信息。
    """
    if not all_results:
        return {
            "shard_index": SHARD_INDEX,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "task_id": TASK_ID,
            "total_records": 0,
        }

    df = pd.DataFrame(all_results)
    meta_cols = {"_date", "_end_time", "code"}
    numeric_cols = [c for c in df.columns if c not in meta_cols and pd.api.types.is_numeric_dtype(df[c])]

    summary = {
        "shard_index": SHARD_INDEX,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "task_id": TASK_ID,
        "total_records": len(df),
        "trading_days": int(df["_date"].nunique()) if "_date" in df.columns else 0,
    }

    # 数值因子取均值（跨日期、股票）
    for col in numeric_cols:
        summary[f"avg_{col}"] = float(df[col].mean())

    return summary


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    print(f"[worker] task={TASK_ID} shard={SHARD_INDEX} {START_DATE}~{END_DATE}")

    strategy = _load_strategy()

    # 必须提供 factor_info 和 factor_calculation
    if not hasattr(strategy, "factor_info"):
        print("[worker] ERROR: strategy.py 必须定义 factor_info = {...}", file=sys.stderr)
        sys.exit(1)
    if not hasattr(strategy, "factor_calculation"):
        print("[worker] ERROR: strategy.py 必须定义 factor_calculation(data, code, date, end_time)", file=sys.stderr)
        sys.exit(1)

    factor_info = strategy.factor_info
    securities = getattr(strategy, "securities", None)  # 可选
    end_times = getattr(strategy, "end_times", [""])     # 默认每日收盘后一次
    user_outfun = getattr(strategy, "outfun", None)

    collecting_outfun = _make_collecting_outfun(user_outfun)

    calc_factors_by_date_range(
        factor_info=factor_info,
        start_date=START_DATE,
        end_date=END_DATE,
        end_times=end_times,
        securities=securities or [],
        processes=PROCESSES,
        factor_data_handler=strategy.factor_calculation,
        outfun=collecting_outfun,
        oss_base_path=DATA_PATH,
    )

    shard_result = _build_shard_result(_all_results)
    print(f"[worker] 计算完成，共 {shard_result['total_records']} 条记录")

    _write_shard_result(shard_result)
    print("[worker] done")


if __name__ == "__main__":
    main()
