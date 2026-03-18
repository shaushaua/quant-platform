# -*- coding: utf-8 -*-
"""
因子计算引擎

支持两种模式：
- 历史模式：按日期范围批量计算，从 OSS 加载历史数据，结果写 OSS parquet
- 实盘模式：单日计算，从 MemoryStore 读取当日实时数据
"""

import logging
import os
from typing import Optional
from datetime import datetime, timedelta

import pandas as pd

from .base import BaseFactor, FactorData
from ..data.api import DataAPI
from ..core.config import get_config

logger = logging.getLogger(__name__)


def _trading_dates(start_date: str, end_date: str) -> list[str]:
    """生成 [start_date, end_date] 之间的自然日列表（粗略，不过滤非交易日）。
    生产环境建议替换为从 OSS 或接口获取真实交易日历。
    """
    fmt = "%Y-%m-%d"
    cur = datetime.strptime(start_date, fmt)
    end = datetime.strptime(end_date, fmt)
    dates = []
    while cur <= end:
        if cur.weekday() < 5:  # 剔除周末
            dates.append(cur.strftime(fmt))
        cur += timedelta(days=1)
    return dates


class FactorEngine:
    """
    因子计算引擎。

    历史模式用法:
        engine = FactorEngine(data_api, oss_result_prefix="factors/my-task")
        engine.run_history(factor, start_date, end_date, task_id, shard_index)

    实盘模式用法:
        engine = FactorEngine(data_api)
        result = engine.run_live(factor, date)
    """

    def __init__(self, data_api: DataAPI, oss_result_prefix: str = ""):
        self.data_api = data_api
        self.oss_result_prefix = oss_result_prefix

    # ── 历史批量计算 ────────────────────────────────────────────────────────

    def run_history(
        self,
        factor: BaseFactor,
        start_date: str,
        end_date: str,
        task_id: str,
        shard_index: int,
    ) -> str:
        """
        批量计算 [start_date, end_date] 内每个交易日的因子值，
        结果写入 OSS：{oss_result_prefix}/{task_id}/shard-{shard_index}.parquet

        Returns:
            OSS 结果路径
        """
        factor.on_init()
        dates = _trading_dates(start_date, end_date)
        logger.info("shard %d: 计算 %d 个交易日 [%s, %s]", shard_index, len(dates), start_date, end_date)

        frames = []
        for date in dates:
            try:
                data = self._load_history_data(date, factor.required_data)
                series = factor.compute(date, data)
                if not isinstance(series, pd.Series):
                    raise TypeError(f"compute() 必须返回 pd.Series，实际返回 {type(series)}")
                series.name = date
                frames.append(series)
            except Exception:
                logger.exception("日期 %s 计算失败，跳过", date)

        if not frames:
            raise RuntimeError(f"shard {shard_index} 无有效计算结果")

        # 结果矩阵：行=stock_code，列=date
        result_df = pd.concat(frames, axis=1).sort_index(axis=1)
        result_path = self._save_shard(result_df, task_id, shard_index)
        logger.info("shard %d 完成，结果写入 %s", shard_index, result_path)
        return result_path

    # ── 实盘单日计算 ────────────────────────────────────────────────────────

    def run_live(self, factor: BaseFactor, date: str) -> pd.Series:
        """
        实盘模式：从 MemoryStore 读取当日数据，返回因子值 Series。
        不写 OSS，结果由调用方处理。
        """
        factor.on_init()
        data = self._load_live_data(date, factor.required_data)
        result = factor.compute(date, data)
        if not isinstance(result, pd.Series):
            raise TypeError(f"compute() 必须返回 pd.Series，实际返回 {type(result)}")
        return result

    # ── 内部方法 ────────────────────────────────────────────────────────────

    def _load_history_data(self, date: str, required: tuple) -> FactorData:
        """从 OSS 加载指定日期的历史数据。"""
        kwargs = dict(date=date)

        def _safe_load(data_type: str) -> pd.DataFrame:
            if data_type not in required:
                return pd.DataFrame()
            try:
                return self.data_api.get_history(date, date, data_type)
            except Exception:
                logger.warning("%s %s 数据加载失败，返回空 DataFrame", date, data_type)
                return pd.DataFrame()

        return FactorData(
            date=date,
            daily_basic=_safe_load("daily_basic"),
            tick=_safe_load("tick"),
            order=_safe_load("order"),
            deal=_safe_load("deal"),
        )

    def _load_live_data(self, date: str, required: tuple) -> FactorData:
        """从 MemoryStore 加载当日实时数据。"""
        def _safe(fn):
            try:
                return fn()
            except Exception:
                return pd.DataFrame()

        return FactorData(
            date=date,
            daily_basic=_safe(self.data_api.get_all_stocks_daily) if "daily_basic" in required else pd.DataFrame(),
            tick=_safe(lambda: self.data_api.get_tick("all")) if "tick" in required else pd.DataFrame(),
            order=pd.DataFrame(),
            deal=pd.DataFrame(),
        )

    def _save_shard(self, df: pd.DataFrame, task_id: str, shard_index: int) -> str:
        """将结果 DataFrame 写入 OSS，返回 OSS key。"""
        cfg = get_config()
        key = f"{self.oss_result_prefix}/{task_id}/shard-{shard_index}.parquet".lstrip("/")

        import io
        import oss2

        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="snappy")
        buf.seek(0)

        auth = oss2.Auth(cfg.oss_access_key, cfg.oss_secret_key)
        bucket = oss2.Bucket(auth, cfg.oss_endpoint, cfg.oss_bucket)
        bucket.put_object(key, buf.read())
        return f"oss://{cfg.oss_bucket}/{key}"
