# -*- coding: utf-8 -*-
"""
Worker Entrypoint - 基础镜像提供，策略代码无需关心 OSS 写入

职责：
  1. 从环境变量读取任务参数（START_DATE / END_DATE / TASK_ID / SHARD_INDEX 等）
  2. 动态 import /app/strategy/strategy.py，获取 factor_info / securities /
     factor_calculation / outfun（后两者可选）
  3. 每日计算完成后立即将该日所有股票结果写入 OSS
     路径: {RESULT_BUCKET}/{TASK_ID}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json
     每个文件是当日所有股票的因子结果列表（不做跨日聚合）

策略代码只需提供：
  - factor_info: dict
  - securities:  list[str]  （可选，默认读全市场）
  - factor_calculation(data, code, date, end_time) -> dict
  - outfun(date, end_time, test):  可选，不提供则基础镜像默认实现
"""

import importlib.util
import json
import logging
import os
import sys
import time
from typing import Optional

import oss2
import pandas as pd

# 让 engine/loader 的 logging 输出到 stdout（kubectl logs 可见）
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

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

SLS_ENDPOINT  = os.environ.get("SLS_ENDPOINT", "")
SLS_PROJECT   = os.environ.get("SLS_PROJECT", "")
SLS_LOGSTORE  = os.environ.get("SLS_LOGSTORE", "")
SLS_AK_ID     = os.environ.get("SLS_AK_ID", "")
SLS_AK_SECRET = os.environ.get("SLS_AK_SECRET", "")


# ---------------------------------------------------------------------------
# SLS 日志工具
# ---------------------------------------------------------------------------

class _SlsLogger:
    """
    向阿里云 SLS 写结构化日志。
    若 SLS 配置缺失则自动降级为 stdout print，不影响正常运行。
    """

    def __init__(self):
        self._client = None
        if all([SLS_ENDPOINT, SLS_PROJECT, SLS_LOGSTORE, SLS_AK_ID, SLS_AK_SECRET]):
            try:
                from aliyun.log import LogClient
                self._client = LogClient(SLS_ENDPOINT, SLS_AK_ID, SLS_AK_SECRET)
                print(f"[worker] SLS 日志已启用: {SLS_PROJECT}/{SLS_LOGSTORE}")
            except Exception as e:
                print(f"[worker] WARNING: SLS 初始化失败，降级为 stdout: {e}")
        else:
            print("[worker] SLS 环境变量未配置，日志仅输出到 stdout")

    def info(self, message: str, **kwargs):
        self._emit("INFO", message, **kwargs)

    def error(self, message: str, **kwargs):
        self._emit("ERROR", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._emit("WARNING", message, **kwargs)

    def _emit(self, level: str, message: str, **kwargs):
        contents = {
            "level": level,
            "message": message,
            "task_id": TASK_ID,
            "shard_index": str(SHARD_INDEX),
            **{k: str(v) for k, v in kwargs.items()},
        }
        # 始终打印到 stdout（kubectl logs 可见）
        extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
        print(f"[worker][{level}] {message}" + (f" | {extra}" if extra else ""))

        if self._client is None:
            return
        try:
            from aliyun.log import LogItem, PutLogsRequest
            log_item = LogItem(contents=list(contents.items()))
            req = PutLogsRequest(SLS_PROJECT, SLS_LOGSTORE, "", "", [log_item])
            self._client.put_logs(req)
        except Exception as e:
            print(f"[worker] WARNING: SLS 写入失败: {e}")


_logger = _SlsLogger()


# ---------------------------------------------------------------------------
# OSS 工具
# ---------------------------------------------------------------------------

_oss_bucket: Optional[oss2.Bucket] = None


def _get_bucket() -> oss2.Bucket:
    global _oss_bucket
    if _oss_bucket is None:
        auth = oss2.Auth(
            _require_env("OSS_ACCESS_KEY_ID"),
            _require_env("OSS_ACCESS_KEY_SECRET"),
        )
        _oss_bucket = oss2.Bucket(
            auth,
            _require_env("OSS_ENDPOINT"),
            RESULT_BUCKET,
        )
    return _oss_bucket


def _write_daily_result(date: str, records: list[dict]) -> None:
    """
    按日期写入结果到 OSS，路径格式:
    {TASK_ID}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json
    """
    year = date[:4]
    month = date[4:6]
    key = f"{TASK_ID}/{year}/{year}{month}/{date}.json"
    bucket = _get_bucket()
    payload = json.dumps(records, ensure_ascii=False, default=str).encode("utf-8")
    bucket.put_object(key, payload)
    _logger.info("日结果已写入 OSS", date=date, records=len(records),
                 path=f"oss://{RESULT_BUCKET}/{key}")


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
# outfun：每日计算完成后直接写入 OSS
# ---------------------------------------------------------------------------


def _make_daily_outfun(user_outfun=None):
    """
    返回一个 outfun，每个 date 计算完成后立即将该日所有股票结果写入 OSS。
    路径: {TASK_ID}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json
    """
    stats = {"total_records": 0, "days_written": 0}

    def _outfun(date: str, end_time: str, test: pd.DataFrame) -> None:
        if user_outfun is not None:
            try:
                user_outfun(date, end_time, test)
            except Exception as e:
                _logger.warning("用户 outfun 异常", date=date, error=str(e))

        if test.empty:
            _logger.info("日期计算完成", date=date, end_time=end_time, records=0)
            return

        records = test.to_dict(orient="records")
        _logger.info("日期计算完成", date=date, end_time=end_time, records=len(records))

        try:
            _write_daily_result(date, records)
            stats["total_records"] += len(records)
            stats["days_written"] += 1
        except Exception as e:
            _logger.error("写入每日结果失败", date=date, error=str(e))

    return _outfun, stats


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    _logger.info("worker started",
                 start_date=START_DATE, end_date=END_DATE)

    strategy = _load_strategy()

    # 必须提供 factor_info 和 factor_calculation
    if not hasattr(strategy, "factor_info"):
        _logger.error("strategy.py 缺少 factor_info")
        sys.exit(1)
    if not hasattr(strategy, "factor_calculation"):
        _logger.error("strategy.py 缺少 factor_calculation")
        sys.exit(1)

    factor_info = strategy.factor_info
    securities = getattr(strategy, "securities", None)  # 可选
    end_times = getattr(strategy, "end_times", [""])     # 默认每日收盘后一次
    user_outfun = getattr(strategy, "outfun", None)

    _logger.info("strategy loaded",
                 securities_count=len(securities) if securities else "all",
                 market_count=factor_info.get("market_count", 1),
                 need_tick=factor_info.get("need_l1_tick", False),
                 need_order=factor_info.get("need_l2_order", False),
                 need_deal=factor_info.get("need_l2_deal", False))

    daily_outfun, stats = _make_daily_outfun(user_outfun)

    t0 = time.time()
    try:
        calc_factors_by_date_range(
            factor_info=factor_info,
            start_date=START_DATE,
            end_date=END_DATE,
            end_times=end_times,
            securities=securities or [],
            processes=PROCESSES,
            factor_data_handler=strategy.factor_calculation,
            outfun=daily_outfun,
            oss_base_path=DATA_PATH,
        )
    except Exception as e:
        _logger.error("calc_factors_by_date_range 异常", error=str(e))
        raise

    elapsed = round(time.time() - t0, 1)
    _logger.info("计算完成",
                 total_records=stats["total_records"],
                 days_written=stats["days_written"],
                 elapsed_seconds=elapsed)

    _logger.info("worker done", elapsed_seconds=elapsed)


if __name__ == "__main__":
    main()
