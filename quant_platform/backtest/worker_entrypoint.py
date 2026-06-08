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
import math
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
from quant_platform.factor.time_slices import generate_intraday_end_times


def _generate_end_times(interval_seconds: int) -> list:
    """根据 compute_interval 生成全天分钟级 end_times 列表。

    A 股交易时段: 09:25-11:30, 13:00-15:00（右开，不含 11:30 / 15:00）
    返回: ["092500", "092600", "092700", ..., "145700", "145800", "145900"]
    """
    return generate_intraday_end_times(interval_seconds)


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
STRATEGY_NAME = os.environ.get("STRATEGY_NAME", TASK_ID)  # OSS路径用策略名
SHARD_INDEX  = int(os.environ.get("SHARD_INDEX", "0"))
STOCK_SHARDS = int(os.environ.get("STOCK_SHARDS", "1"))
STOCK_SHARD_INDEX = int(os.environ.get("STOCK_SHARD_INDEX", "0"))
DATA_PATH    = os.environ.get("DATA_PATH", "/data")
RESULT_BUCKET = os.environ.get("RESULT_BUCKET", "stock-mdl-data-result")


def _effective_cpu_limit() -> Optional[int]:
    """Return cgroup CPU quota as whole cores when available."""
    try:
        with open("/sys/fs/cgroup/cpu.max", "r", encoding="utf-8") as fh:
            quota, period = fh.read().strip().split()[:2]
        if quota == "max":
            return None
        cores = int(quota) / int(period)
        return max(1, int(math.floor(cores)))
    except Exception:
        return None


_requested_processes = int(os.environ.get("WORKERS", "1"))
_cpu_limit = _effective_cpu_limit()
PROCESSES = min(_requested_processes, _cpu_limit) if _cpu_limit else _requested_processes

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


def _write_result(
    date: str,
    end_time: str,
    records: list[dict],
    part_index: Optional[int] = None,
    part_count: Optional[int] = None,
) -> None:
    """
    写入结果到 OSS。

    路径格式:
    - 有 end_time: {STRATEGY_NAME}/{YYYY}/{YYYYMM}/{date}/{date}_{end_time}_s{shard}.json
    - 无 end_time: {STRATEGY_NAME}/{YYYY}/{YYYYMM}/{date}/{date}_s{shard}.json
    """
    year = date[:4]
    month = date[4:6]
    shard_suffix = f"_s{STOCK_SHARD_INDEX}" if STOCK_SHARDS > 1 else ""
    part_suffix = f"_p{part_index}" if part_index is not None else ""
    if end_time:
        filename = f"{date}_{end_time}{shard_suffix}{part_suffix}.json"
    else:
        filename = f"{date}{shard_suffix}{part_suffix}.json"
    key = f"{STRATEGY_NAME}/{year}/{year}{month}/{date}/{filename}"
    bucket = _get_bucket()
    payload = json.dumps(records, ensure_ascii=False, default=str).encode("utf-8")
    bucket.put_object(key, payload)
    _logger.info("结果已写入 OSS", date=date, end_time=end_time, records=len(records),
                 part_index=part_index if part_index is not None else "",
                 part_count=part_count if part_count is not None else "",
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
    返回一个 outfun，每个 date + end_time 计算完成后写入 OSS。

    OSS key 规则：
    - 有 end_time（分钟级策略）: {date}_{end_time}_s0.json
    - 无 end_time（日频策略）:   {date}_s0.json
    """
    stats = {"total_records": 0, "writes": 0}

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
        part_index = test.attrs.get("part_index")
        part_count = test.attrs.get("part_count")
        _logger.info("日期计算完成", date=date, end_time=end_time, records=len(records))

        try:
            _write_result(date, end_time, records, part_index=part_index, part_count=part_count)
            stats["total_records"] += len(records)
            stats["writes"] += 1
        except Exception as e:
            _logger.error("写入结果失败", date=date, end_time=end_time, error=str(e))

    return _outfun, stats


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    _logger.info("worker started",
                 start_date=START_DATE, end_date=END_DATE,
                 requested_workers=_requested_processes,
                 effective_workers=PROCESSES,
                 cpu_limit=_cpu_limit if _cpu_limit is not None else "")

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
    end_times = getattr(strategy, "end_times", None)    # 策略自定义 end_times
    user_outfun = getattr(strategy, "outfun", None)

    # 策略未定义 end_times 时，从 factor_info.compute_interval 自动生成
    if not end_times:
        interval = int(factor_info.get("compute_interval", 0))
        if interval > 0:
            end_times = _generate_end_times(interval)
            _logger.info("end_times from compute_interval", interval=interval, count=len(end_times))
        else:
            end_times = [""]  # 日频/全天一次

    _logger.info("strategy loaded",
                 securities_count=len(securities) if securities else "all",
                 market_count=factor_info.get("market_count", 1),
                 need_tick=factor_info.get("need_l1_tick", False),
                 need_order=factor_info.get("need_l2_order", False),
                 need_deal=factor_info.get("need_l2_deal", False))

    # ---- 股票分片：按 STOCK_SHARDS / STOCK_SHARD_INDEX 过滤 ----
    if STOCK_SHARDS > 1:
        if securities:
            # 策略指定了股票列表，直接按排序取模分片
            all_stocks = sorted(securities)
        else:
            # 全市场模式：从 daily_basic 获取全量股票列表
            from quant_platform.data.api import DataAPI
            _api = DataAPI(mode="backtest", oss_base_path=DATA_PATH)
            _db = _api.get_daily_data(START_DATE, "daily_basic")
            if not _db.empty and "ID_QI" in _db.columns:
                all_stocks = sorted(_db["ID_QI"].tolist())
            else:
                _logger.info("当日无交易数据，跳过（非交易日）", date=START_DATE)
                _upload_log()
                sys.exit(0)  # 正常退出，非交易日不算失败
            _logger.info("全市场股票列表获取完成", total=len(all_stocks))

        my_stocks = [s for i, s in enumerate(all_stocks) if i % STOCK_SHARDS == STOCK_SHARD_INDEX]
        securities = my_stocks
        _logger.info("股票分片过滤",
                     stock_shards=STOCK_SHARDS,
                     stock_shard_index=STOCK_SHARD_INDEX,
                     total_stocks=len(all_stocks),
                     my_stocks=len(my_stocks),
                     sample=my_stocks[:5])

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
                 writes=stats["writes"],
                 elapsed_seconds=elapsed)

    _logger.info("worker done", elapsed_seconds=elapsed)

    # 上传日志到 OSS
    _upload_log()


def _upload_log():
    """将 worker 日志上传到 OSS，路径: {TASK_ID}/logs/shard_{SHARD_INDEX}.log"""
    log_path = os.environ.get("WORKER_LOG_FILE")
    if not log_path or not os.path.exists(log_path):
        return
    try:
        bucket = _get_bucket()
        key = f"{STRATEGY_NAME}/logs/shard_{SHARD_INDEX}.log"
        bucket.put_object_from_file(key, log_path)
        print(f"[worker] log uploaded to oss://{RESULT_BUCKET}/{key}")
    except Exception as e:
        print(f"[worker] WARNING: log upload failed: {e}")


if __name__ == "__main__":
    # 同时写 stdout 和日志文件，以便上传到 OSS
    import io

    _log_file = f"/tmp/worker_shard_{SHARD_INDEX}.log"
    os.environ["WORKER_LOG_FILE"] = _log_file

    class _Tee:
        """同时写入多个流"""
        def __init__(self, *streams):
            self._streams = streams
        def write(self, data):
            for s in self._streams:
                s.write(data)
                s.flush()
        def flush(self):
            for s in self._streams:
                s.flush()

    _fh = open(_log_file, "w", encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, _fh)
    sys.stderr = _Tee(sys.__stderr__, _fh)

    main()
