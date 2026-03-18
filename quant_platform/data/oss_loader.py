# -*- coding: utf-8 -*-
"""
OSS数据加载器
从OSS挂载路径加载历史数据
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict
import os

import pandas as pd
import numpy as np

try:
    import duckdb as _duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _duckdb = None
    _DUCKDB_AVAILABLE = False

logger = logging.getLogger(__name__)


class OSSDataLoader:
    """
    OSS数据加载器
    从OSS挂载路径加载历史数据

    路径格式: /2025/202501/20250102/20250102_tick.parquet

    使用方式:
        loader = OSSDataLoader(base_path="/2025")
        df = loader.load("2025-01-02", "tick")
        df_range = loader.load_range("2025-01-01", "2025-01-31", "deal")
    """

    # 数据类型映射
    DATA_TYPES = {
        "tick": "tick",
        "order": "order",
        "deal": "deal",
        "daily_basic": "daily_basic_data",
        "kline_1min": "kline_1min",
        "kline_5min": "kline_5min",
        "kline_10min": "kline_10min",
        "kline_30min": "kline_30min",
        "kline_60min": "kline_60min",
    }

    def __init__(
        self,
        base_path: str = "/2025",
        cache_enabled: bool = True,
        cache_size: int = None
    ):
        """
        初始化OSS数据加载器

        Args:
            base_path: OSS挂载的基础路径
            cache_enabled: 是否启用缓存
            cache_size: 缓存最大天数，None表示不限制
        """
        self.base_path = Path(base_path)
        self.cache_enabled = cache_enabled
        self.cache_size = cache_size if cache_size is not None else int(os.getenv("CACHE_SIZE", "10"))
        self._cache: Dict[str, pd.DataFrame] = {}
        # 预分组缓存: key=f"{date_str}_{data_type}", value=dict[code->DataFrame]
        self._grouped_cache: Dict[str, Dict] = {}

        # 验证路径
        if not self.base_path.exists():
            logger.warning(f"OSS数据路径不存在: {base_path}")
        else:
            logger.info(f"OSS数据加载器初始化完成: {base_path}")

    def load(
        self,
        date: str,
        data_type: str,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        加载单日数据

        Args:
            date: 日期 (YYYY-MM-DD 或 YYYYMMDD)
            data_type: 数据类型 (tick/order/deal/daily_basic)
            codes: 股票代码列表，None表示全部

        Returns:
            数据DataFrame
        """
        # 标准化日期格式
        date_str = date.replace("-", "")
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        # 检查缓存
        cache_key = f"{date_str}_{data_type}"
        if self.cache_enabled and cache_key in self._cache:
            df = self._cache[cache_key]
            if codes:
                code_col = self._CODE_COL.get(data_type, self._DEFAULT_CODE_COL)
                return df[df[code_col].isin(codes)].copy()
            return df

        # 构建文件路径
        file_suffix = self.DATA_TYPES.get(data_type, data_type)
        file_path = self.base_path / f"{year}/{year}{month}/{year}{month}{day}/{year}{month}{day}_{file_suffix}.parquet"

        if not file_path.exists():
            minute_pattern = f"{date_str}_*_{file_suffix}.parquet"
            minute_files = sorted(file_path.parent.glob(minute_pattern))
            if not minute_files:
                logger.warning(f"数据文件不存在: {file_path}")
                return pd.DataFrame()

            dfs = []
            for minute_file in minute_files:
                try:
                    dfs.append(pd.read_parquet(minute_file))
                except Exception as e:
                    logger.error(f"读取数据失败 {minute_file}: {e}")
            if not dfs:
                return pd.DataFrame()
            df = pd.concat(dfs, ignore_index=True)
            logger.info(f"加载分钟数据: {len(df)} 条记录, {len(minute_files)} 文件")

            if self.cache_enabled:
                self._manage_cache()
                self._cache[cache_key] = df

            if codes:
                df = df[df["Code"].isin(codes)]
            return df

        try:
            # 读取parquet文件
            df = pd.read_parquet(file_path)
            logger.info(f"加载数据: {file_path}, {len(df)} 条记录")

            # 缓存
            if self.cache_enabled:
                self._manage_cache()
                self._cache[cache_key] = df

            # 过滤股票代码
            if codes:
                df = df[df["Code"].isin(codes)]

            return df

        except Exception as e:
            logger.error(f"读取数据失败 {file_path}: {e}")
            return pd.DataFrame()

    # daily_basic 用 ID_QI（字符串股票代码）过滤；其他数据类型用数字 ID（Code列）过滤
    _CODE_COL = {
        "daily_basic": "ID_QI",
    }
    _DEFAULT_CODE_COL = "Code"  # deal/tick/order 等

    def query_code(
        self,
        date: str,
        data_type: str,
        code,  # str(ID_QI) for daily_basic, int/str numeric ID for others
        numeric_id: int = None
    ) -> pd.DataFrame:
        """
        用 DuckDB predicate pushdown 查询单只股票的单日数据。

        Args:
            date: 日期 (YYYY-MM-DD 或 YYYYMMDD)
            data_type: 数据类型 (tick/order/deal/daily_basic 等)
            code: 股票代码 ID_QI，如 '000001'
            numeric_id: 数字ID（deal/tick/order 用），来自 daily_basic.ID

        Returns:
            该股票当日数据 DataFrame，文件不存在时返回空 DataFrame
        """
        if not _DUCKDB_AVAILABLE:
            raise ImportError(
                "query_code() 需要 duckdb，请执行: pip install 'duckdb>=0.10.0'"
            )

        date_str = date.replace("-", "")
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]
        file_suffix = self.DATA_TYPES.get(data_type, data_type)
        file_path = (
            self.base_path
            / f"{year}/{year}{month}/{year}{month}{day}/{year}{month}{day}_{file_suffix}.parquet"
        )

        if not file_path.exists():
            logger.debug(f"文件不存在，跳过: {file_path}")
            return pd.DataFrame()

        # OSS FUSE 挂载下 DuckDB 随机 seek 极慢，改用 load() 缓存全量读取后过滤
        try:
            df = self.load(date, data_type)
            if df.empty:
                return pd.DataFrame()
            if data_type == "daily_basic":
                result = df[df["ID_QI"].astype(str) == str(code)]
            else:
                nid = numeric_id if numeric_id is not None else code
                result = df[df["Code"] == int(nid)]
            logger.debug(f"query_code {code} {date} {data_type}: {len(result)} 条")
            return result.copy()
        except Exception as e:
            logger.error(f"query_code 失败 code={code} {date} {data_type}: {e}")
            return pd.DataFrame()

    def pregroup_day(self, date: str, data_types: List[str]) -> None:
        """
        预分组指定日期的多种数据类型，按 Code 建立 dict 索引。
        在每日股票循环开始前调用，将 O(N) pandas 过滤降为 O(1) dict 查找。
        """
        date_str = date.replace("-", "")
        for data_type in data_types:
            group_key = f"{date_str}_{data_type}"
            if group_key in self._grouped_cache:
                continue
            df = self.load(date, data_type)
            if df.empty:
                self._grouped_cache[group_key] = {}
                continue
            if data_type == "daily_basic":
                grouped = {str(k): v for k, v in df.groupby("ID_QI")}
            else:
                grouped = {k: v for k, v in df.groupby("Code")}
            self._grouped_cache[group_key] = grouped
            # 原始DataFrame已分组，从_cache释放，避免同一份数据占双倍内存
            cache_key = f"{date_str}_{data_type}"
            if cache_key in self._cache:
                del self._cache[cache_key]
            logger.info(f"pregroup_day {date} {data_type}: {len(grouped)} 支股票")

    def query_code_fast(self, date: str, data_type: str, code, numeric_id: int = None) -> pd.DataFrame:
        """
        从预分组缓存中 O(1) 查询单只股票数据。
        必须先调用 pregroup_day() 才能使用。
        """
        date_str = date.replace("-", "")
        group_key = f"{date_str}_{data_type}"
        grouped = self._grouped_cache.get(group_key)
        if grouped is None:
            # 未预分组：直接返回空，避免全表扫描大文件（tick/order 可达亿级条数）
            return pd.DataFrame()
        if data_type == "daily_basic":
            result = grouped.get(str(code))
        else:
            nid = numeric_id if numeric_id is not None else code
            try:
                result = grouped.get(int(nid))
            except (TypeError, ValueError):
                result = grouped.get(nid)
        if result is None:
            return pd.DataFrame()
        return result.copy()

    def clear_grouped_cache(self, date: str = None) -> None:
        """
        清理预分组缓存。

        Args:
            date: 指定日期则只清理该日期，None 则清理全部
        """
        if date is None:
            self._grouped_cache.clear()
        else:
            date_str = date.replace("-", "")
            keys_to_del = [k for k in self._grouped_cache if k.startswith(date_str)]
            for k in keys_to_del:
                del self._grouped_cache[k]

    def load_range(
        self,
        start_date: str,
        end_date: str,
        data_type: str,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        加载日期范围内的数据

        Args:
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型
            codes: 股票代码列表

        Returns:
            合并后的DataFrame
        """
        trading_days = self.get_trading_days(start_date, end_date)

        if not trading_days:
            logger.warning(f"日期范围内无交易日: {start_date} ~ {end_date}")
            return pd.DataFrame()

        dfs = []
        total_rows = 0
        for day in trading_days:
            df = self.load(day, data_type, codes)
            if not df.empty:
                dfs.append(df)
                total_rows += len(df)

        if dfs:
            result = pd.concat(dfs, ignore_index=True)
            # 释放中间引用，让 GC 尽快回收各日 DataFrame
            dfs.clear()
            logger.info(f"加载范围数据: {total_rows} 条记录, {len(trading_days)} 个交易日")
            return result

        return pd.DataFrame()

    def load_time_range(
        self,
        start_time: str,
        end_time: str,
        data_type: str,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt

        current = start_dt.normalize()
        end_day = end_dt.normalize()
        dfs = []
        while current <= end_day:
            day_start = max(current, start_dt)
            day_end = min(current + timedelta(days=1) - timedelta(seconds=1), end_dt)
            df = self._load_day_time_range(current.strftime("%Y%m%d"), day_start, day_end, data_type, codes)
            if not df.empty:
                dfs.append(df)
            current += timedelta(days=1)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()

    def _load_day_time_range(
        self,
        date_str: str,
        start_dt: datetime,
        end_dt: datetime,
        data_type: str,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]
        dir_path = self.base_path / f"{year}/{year}{month}/{year}{month}{day}"
        file_suffix = self.DATA_TYPES.get(data_type, data_type)

        minute_pattern = f"{date_str}_*_{file_suffix}.parquet"
        minute_files = sorted(dir_path.glob(minute_pattern))
        if minute_files:
            start_key = start_dt.strftime("%H%M")
            end_key = end_dt.strftime("%H%M")
            dfs = []
            for minute_file in minute_files:
                name_parts = minute_file.stem.split("_")
                if len(name_parts) < 3:
                    continue
                minute_key = name_parts[1]
                if start_key <= minute_key <= end_key:
                    try:
                        dfs.append(pd.read_parquet(minute_file))
                    except Exception as e:
                        logger.error(f"读取数据失败 {minute_file}: {e}")
            if not dfs:
                return pd.DataFrame()
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = self.load(date_str, data_type, codes)

        if df.empty:
            return df
        if "Time" in df.columns:
            try:
                ts = pd.to_datetime(df["Time"])
                df = df[(ts >= start_dt) & (ts <= end_dt)]
            except Exception:
                pass
        if codes:
            df = df[df["Code"].isin(codes)]
        return df

    def load_prev_days(
        self,
        n_days: int,
        data_type: str,
        end_date: Optional[str] = None,
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        加载前N个交易日的数据

        Args:
            n_days: 交易日数量
            data_type: 数据类型
            end_date: 结束日期，默认今天
            codes: 股票代码列表

        Returns:
            合并后的DataFrame
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")

        # 获取前N个交易日，搜索窗口 = n_days * 2 以容纳节假日，至少30天
        window = max(n_days * 2, 30)
        all_days = self._get_recent_dates(window)
        end_idx = next(
            (i for i, d in enumerate(all_days) if d <= end_date),
            len(all_days)
        )
        trading_days = all_days[end_idx:min(end_idx + n_days, len(all_days))]

        if not trading_days:
            return pd.DataFrame()

        return self.load_range(
            trading_days[-1] if trading_days else end_date,
            trading_days[0] if trading_days else end_date,
            data_type,
            codes
        )

    def get_trading_days(
        self,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        获取日期范围内的交易日列表

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表 (YYYYMMDD格式)
        """
        start_dt = datetime.strptime(start_date.replace("-", ""), "%Y%m%d")
        end_dt = datetime.strptime(end_date.replace("-", ""), "%Y%m%d")

        trading_days = []
        current = start_dt

        while current <= end_dt:
            # 排除周末
            if current.weekday() < 5:
                date_str = current.strftime("%Y%m%d")
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]

                # 检查数据目录是否存在
                dir_path = self.base_path / f"{year}/{year}{month}/{year}{month}{day}"
                if dir_path.exists():
                    trading_days.append(date_str)

            current += timedelta(days=1)

        return trading_days

    def list_available_dates(
        self,
        year: Optional[str] = None,
        month: Optional[str] = None
    ) -> List[str]:
        """
        列出可用的交易日

        Args:
            year: 年份 (YYYY)
            month: 月份 (MM)

        Returns:
            可用日期列表
        """
        dates = []

        if year and month:
            # 指定年月
            search_path = self.base_path / f"{year}/{year}{month}"
        elif year:
            # 指定年份
            search_path = self.base_path / year
        else:
            # 全部
            search_path = self.base_path

        if not search_path.exists():
            return dates

        # 递归查找所有日期目录
        for path in search_path.rglob("*"):
            if path.is_dir() and path.name.isdigit() and len(path.name) == 8:
                dates.append(path.name)

        return sorted(dates, reverse=True)

    def get_stock_codes(self, date: str) -> List[str]:
        """
        获取某日所有股票代码

        Args:
            date: 日期

        Returns:
            股票代码列表
        """
        # 尝试从daily_basic获取（只读ID_QI列，避免全量加载）
        date_str = date.replace("-", "")
        year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
        file_suffix = self.DATA_TYPES.get("daily_basic", "daily_basic_data")
        file_path = self.base_path / f"{year}/{year}{month}/{year}{month}{day}/{year}{month}{day}_{file_suffix}.parquet"
        if file_path.exists():
            try:
                import pyarrow.parquet as pq
                codes = pq.read_table(str(file_path), columns=["ID_QI"])["ID_QI"].to_pylist()
                return list(dict.fromkeys(codes))  # 去重保序
            except Exception:
                pass

        # 尝试从deal获取（比tick小得多，只读Code列）
        file_suffix2 = self.DATA_TYPES.get("deal", "deal")
        file_path2 = self.base_path / f"{year}/{year}{month}/{year}{month}{day}/{year}{month}{day}_{file_suffix2}.parquet"
        if file_path2.exists():
            try:
                import pyarrow.parquet as pq
                codes = pq.read_table(str(file_path2), columns=["Code"])["Code"].to_pylist()
                return list(dict.fromkeys(codes))
            except Exception:
                pass

        return []

    def get_prev_trading_day(self, date: str, n: int = 1) -> Optional[str]:
        """
        获取前N个交易日

        Args:
            date: 当前日期
            n: 前第几个交易日

        Returns:
            交易日字符串
        """
        date_str = date.replace("-", "")
        recent_dates = self._get_recent_dates(30)

        try:
            idx = recent_dates.index(date_str)
            if idx + n < len(recent_dates):
                return recent_dates[idx + n]
        except ValueError:
            pass

        return None

    def _get_recent_dates(self, days: int = 30) -> List[str]:
        """获取最近N天的日期列表（降序）"""
        dates = []
        current = datetime.now()

        for _ in range(days * 2):  # 多查一些以确保足够
            if current.weekday() < 5:
                dates.append(current.strftime("%Y%m%d"))
            current -= timedelta(days=1)
            if len(dates) >= days:
                break

        return dates

    def _manage_cache(self):
        """管理缓存大小"""
        if len(self._cache) >= self.cache_size:
            # 删除最早的缓存
            keys = list(self._cache.keys())
            for key in keys[:len(keys) - self.cache_size + 1]:
                del self._cache[key]

    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        logger.info("OSS数据缓存已清空")

    def get_cache_info(self) -> dict:
        """获取缓存信息"""
        total_size = sum(df.memory_usage(deep=True).sum() for df in self._cache.values())
        return {
            "cache_count": len(self._cache),
            "cache_size_mb": total_size / 1024 / 1024,
            "cache_keys": list(self._cache.keys()),
        }


# 便捷函数
def create_loader(base_path: str = "/2025") -> OSSDataLoader:
    """创建OSS数据加载器"""
    return OSSDataLoader(base_path)
