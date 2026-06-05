# -*- coding: utf-8 -*-
"""
统一数据API
交易员使用的数据接口 - 本地直接调用，无网络开销

使用方式:
    from quant_platform import DataAPI

    data = DataAPI()

    # 实时数据（从 native SHM）
    df_1min = data.get_all_stocks_1min()
    df_5min = data.get_all_stocks_5min()
    df_tick = data.get_tick("000001.XSHE")

    # 历史数据（从磁盘）
    df_history = data.get_history("2024-01-01", "2024-12-31", "tick")
"""

import logging
import os
from typing import List, Optional, Dict
from datetime import datetime

import pandas as pd

from .shm_store import ShmStore
from .oss_loader import OSSDataLoader
from ..core.config import get_config

logger = logging.getLogger(__name__)

# 高危数据类型：单日数据量可达数GB，多天累积极易OOM
_HEAVY_DATA_TYPES = {"tick", "order", "deal"}

# 允许 get_history 加载的最大天数（高危类型），超过则抛异常
# 通过环境变量 HEAVY_DATA_MAX_DAYS 覆盖，设为 0 表示完全禁止范围加载
_HEAVY_DATA_MAX_DAYS = int(os.getenv("HEAVY_DATA_MAX_DAYS", "1"))


def _check_heavy_data_range(data_type: str, n_days: int, caller: str = "get_history") -> None:
    """检查高危数据类型的日期范围，超限则抛 ValueError。"""
    if data_type not in _HEAVY_DATA_TYPES:
        return
    if _HEAVY_DATA_MAX_DAYS == 0 or n_days > _HEAVY_DATA_MAX_DAYS:
        raise ValueError(
            f"[DataAPI] 拒绝加载 {n_days} 天的 '{data_type}' 数据（上限 {_HEAVY_DATA_MAX_DAYS} 天）。"
            f" 请改用 data_api.get_daily_data(date, '{data_type}') 在每日循环中逐天加载，"
            f" 或设置环境变量 HEAVY_DATA_MAX_DAYS=<N> 提高上限（需确认内存充足）。"
        )


class DataAPI:
    """
    统一数据API
    交易员直接使用，无网络调用，毫秒级访问

    实时数据 -> ShmStore/native mmap
    历史数据 -> OSSLoader (本地磁盘/OSS挂载)
    """

    def __init__(
        self,
        mode: str = "realtime",
        oss_base_path: Optional[str] = None
    ):
        """
        初始化数据API

        Args:
            mode: 运行模式
                - "realtime": 实盘模式，连接内存实时数据
                - "backtest": 回测模式，仅使用历史数据
            oss_base_path: OSS数据路径，默认从配置读取
        """
        self.mode = mode
        self.config = get_config()

        # 实盘实时存储：统一走 C++ collector 写出的 native mmap。
        self._shm: Optional[ShmStore] = None
        if mode == "realtime":
            self._shm = ShmStore()

        # OSS数据加载器
        # 回测模式下 cache_size=1：pregroup_day() 分组后会立即删除原始缓存，
        # _cache 只需在同一天内多次 load() 间共享，无需跨天保留，避免多天数据累积OOM
        oss_path = oss_base_path or self.config.oss_data_path
        oss_cache_size = 1 if mode == "backtest" else int(os.getenv("CACHE_SIZE", "3"))
        self._oss = OSSDataLoader(base_path=oss_path, cache_size=oss_cache_size)

        # 执行上下文（由引擎注入，交易员不直接操作）
        self._current_date: Optional[str] = None
        self._current_code: Optional[str] = None

        logger.info(f"DataAPI 初始化完成, mode={mode}")

    # ==================== 实时数据（内存读取）====================

    def _today_str(self) -> str:
        return datetime.now().strftime("%Y%m%d")

    def _get_realtime_store(self):
        """返回当前生效的实时存储。"""
        return self._shm

    def get_all_stocks_1min(self) -> pd.DataFrame:
        """
        获取所有股票1分钟K线

        Returns:
            DataFrame with columns: Code, Time, Open, High, Low, Close, Volume, Amount
        """
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            logger.warning("非实盘模式，无法获取实时1分钟数据")
            return pd.DataFrame()
        df = store.get_kline("1min")
        if df.empty:
            return self._oss.load(self._today_str(), "kline_1min")
        return df

    def get_all_stocks_5min(self) -> pd.DataFrame:
        """获取所有股票5分钟K线"""
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            logger.warning("非实盘模式，无法获取实时5分钟数据")
            return pd.DataFrame()
        df = store.get_kline("5min")
        if df.empty:
            return self._oss.load(self._today_str(), "kline_5min")
        return df

    def get_all_stocks_10min(self) -> pd.DataFrame:
        """获取所有股票10分钟K线"""
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            logger.warning("非实盘模式，无法获取实时10分钟数据")
            return pd.DataFrame()
        df = store.get_kline("10min")
        if df.empty:
            return self._oss.load(self._today_str(), "kline_10min")
        return df

    def get_all_stocks_30min(self) -> pd.DataFrame:
        """获取所有股票30分钟K线"""
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            return pd.DataFrame()
        df = store.get_kline("30min")
        if df.empty:
            return self._oss.load(self._today_str(), "kline_30min")
        return df

    def get_all_stocks_60min(self) -> pd.DataFrame:
        """获取所有股票60分钟K线"""
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            return pd.DataFrame()
        df = store.get_kline("60min")
        if df.empty:
            return self._oss.load(self._today_str(), "kline_60min")
        return df

    def get_tick(self, code: Optional[str] = None) -> pd.DataFrame:
        """
        获取tick快照数据

        Args:
            code: 股票代码 (如 000001.XSHE)，None表示所有股票

        Returns:
            tick数据DataFrame
        """
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            logger.warning("非实盘模式，无法获取实时tick数据")
            return pd.DataFrame()
        df = store.get_tick(code)
        if df.empty:
            codes = [code] if code else None
            return self._oss.load(self._today_str(), "tick", codes)
        return df

    def get_order(self, code: Optional[str] = None) -> pd.DataFrame:
        """
        获取逐笔委托数据

        Args:
            code: 股票代码，None表示所有股票
        """
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            return pd.DataFrame()
        df = store.get_order(code)
        if df.empty:
            codes = [code] if code else None
            return self._oss.load(self._today_str(), "order", codes)
        return df

    def get_deal(self, code: Optional[str] = None) -> pd.DataFrame:
        """
        获取逐笔成交数据

        Args:
            code: 股票代码，None表示所有股票
        """
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            return pd.DataFrame()
        df = store.get_deal(code)
        if df.empty:
            codes = [code] if code else None
            return self._oss.load(self._today_str(), "deal", codes)
        return df

    def get_quote(self, code: str) -> dict:
        """
        获取单只股票最新行情

        Args:
            code: 股票代码

        Returns:
            行情字典
        """
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            return {}
        return store.get_quote(code)

    def get_all_quotes(self) -> Dict[str, dict]:
        """获取所有股票最新行情"""
        store = self._get_realtime_store()
        if self.mode != "realtime" or store is None:
            return {}
        return store.get_all_quotes()

    def get_daily_basic(self, date: Optional[str] = None) -> pd.DataFrame:
        """
        获取日频基础数据

        Args:
            date: 日期 (YYYYMMDD)，None表示获取当天（仅实时模式有效）

        Returns:
            日频基础数据DataFrame
        """
        # 实时模式且未指定日期，从内存获取
        if date is None:
            store = self._get_realtime_store()
            if self.mode != "realtime" or store is None:
                return pd.DataFrame()
            df = store.get_daily_basic()
            if df.empty:
                return self._oss.load(self._today_str(), "daily_basic")
            return df

        # 指定日期，从OSS加载
        return self._oss.load(date, "daily_basic")

    # ==================== 历史数据（磁盘读取）====================

    def _get_realtime_df(self, data_type: str, codes: Optional[List[str]] = None) -> pd.DataFrame:
        store = self._get_realtime_store()
        if store is None:
            return pd.DataFrame()
        if data_type in ("tick", "order", "deal"):
            if data_type == "tick":
                df = store.get_tick(None)
            elif data_type == "order":
                df = store.get_order(None)
            else:
                df = store.get_deal(None)
        elif data_type.startswith("kline_"):
            freq = data_type.split("_", 1)[1]
            df = store.get_kline(freq)
        else:
            return pd.DataFrame()

        if df.empty:
            return df
        if codes and "Code" in df.columns:
            df = df[df["Code"].isin(codes)]
        return df

    def _filter_time_range(self, df: pd.DataFrame, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        if df.empty or "Time" not in df.columns:
            return df
        try:
            ts = pd.to_datetime(df["Time"], errors="coerce")
            return df[(ts >= start_dt) & (ts <= end_dt)]
        except Exception:
            return df

    def get_time_range(
        self,
        start_time: str,
        end_time: str,
        data_type: str = "tick",
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        if end_dt < start_dt:
            start_dt, end_dt = end_dt, start_dt

        if self.mode == "realtime" and self._get_realtime_store() is not None:
            today = datetime.now().date()
            if start_dt.date() == today and end_dt.date() == today:
                df = self._get_realtime_df(data_type, codes)
                df = self._filter_time_range(df, start_dt, end_dt)
                if not df.empty:
                    return df

        return self._oss.load_time_range(start_time, end_time, data_type, codes)

    def _set_context(self, date: str, code: Optional[str] = None, numeric_id: int = None) -> None:
        """由引擎在每次迭代前调用，注入当前日期和股票上下文。交易员不需要调用此方法。"""
        self._current_date = date
        self._current_code = code
        self._current_numeric_id = numeric_id

    def get_current_data(
        self,
        data_type: str = "daily_basic"
    ) -> pd.DataFrame:
        """
        获取当前上下文（日期 + 股票）的数据。

        在 per-code 执行模式下，引擎会在调用策略前自动注入 ctx.current_date
        和 ctx.current_code，此方法根据上下文自动返回对应数据，交易员无需
        关心 DuckDB、路径、code 过滤等细节。

        用法（在策略函数中）:
            deal_df = data_api.get_current_data("deal")
            tick_df = data_api.get_current_data("tick")
            daily  = data_api.get_current_data("daily_basic")

        Args:
            data_type: 数据类型 (tick/order/deal/daily_basic/kline_1min 等)

        Returns:
            当前股票当日数据 DataFrame

        Raises:
            RuntimeError: 在引擎上下文外直接调用时抛出
        """
        if self._current_date is None:
            raise RuntimeError(
                "get_current_data() 只能在引擎驱动的策略函数内使用。"
                " 请确认策略通过 BacktestEngine.run_per_code() 执行。"
            )
        if self._current_code is not None:
            # per-code 模式：优先查预分组缓存（O(1)），否则回退到 query_code
            return self._oss.query_code_fast(
                self._current_date, data_type, self._current_code,
                numeric_id=getattr(self, "_current_numeric_id", None)
            )
        else:
            # 日级模式（未设置 code）：回退到全市场加载
            return self._oss.load(self._current_date, data_type)

    def get_daily_data(
        self,
        date: str,
        data_type: str = "tick",
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取单日历史数据（推荐用于 tick/order/deal 等大数据类型）

        相比 get_history，此方法只加载单天数据，避免多天累积导致 OOM。
        在每日循环中处理 tick/order/deal 时请优先使用此方法。

        Args:
            date: 日期 (YYYY-MM-DD 或 YYYYMMDD)
            data_type: 数据类型 (tick/order/deal/daily_basic)
            codes: 股票代码列表，None 表示全部

        Returns:
            单日数据 DataFrame
        """
        return self._oss.load(date, data_type, codes)

    def load_stock_data(
        self,
        code: str,
        date: str,
        data_type: str = "deal",
    ) -> pd.DataFrame:
        """
        加载单只股票的单日数据，自动还原 Code 和精度。

        交易员在 Jupyter 里直接用：
            from quant_platform.data.api import DataAPI
            api = DataAPI(mode="backtest")

            deal = api.load_stock_data("000001.SZ", "20250106", "deal")
            print(deal[["Code", "Price", "Volume"]].head())

        OSS 历史数据由 Go data-converter 压缩存储（Code 为整数、价格×100、
        成交量÷100），此方法自动还原为可读格式。实时数据不受影响。

        Args:
            code: 股票代码，如 "000001.SZ"
            date: 日期 (YYYYMMDD 或 YYYY-MM-DD)
            data_type: "order" / "deal" / "tick"

        Returns:
            还原后的 DataFrame（Code 为字符串，价格为 float64 原始精度）
        """
        from ..factor.engine import _restore_oss_precision

        df = self._oss.load(date, data_type, codes=[code])
        return _restore_oss_precision(df, code)

    def get_history(
        self,
        start_date: str,
        end_date: str,
        data_type: str = "tick",
        codes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        获取历史数据

        Args:
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: 结束日期
            data_type: 数据类型 (tick/order/deal/daily_basic)
            codes: 股票代码列表，None表示全部

        Returns:
            历史数据DataFrame

        Raises:
            ValueError: tick/order/deal 类型加载天数超过 HEAVY_DATA_MAX_DAYS 时抛出，
                        防止策略代码意外加载大量数据导致 OOM。
                        请改用 get_daily_data() 在每日循环中逐天加载。
        """
        trading_days = self._oss.get_trading_days(start_date, end_date)
        _check_heavy_data_range(data_type, len(trading_days))
        return self._oss.load_range(start_date, end_date, data_type, codes)

    def get_history_days(
        self,
        n_days: int,
        data_type: str = "tick",
        codes: Optional[List[str]] = None,
        from_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取前N个交易日的历史数据

        Args:
            n_days: 交易日数量
            data_type: 数据类型
            codes: 股票代码列表
            from_date: 基准日期 YYYYMMDD，默认今天

        Returns:
            历史数据DataFrame

        Raises:
            ValueError: tick/order/deal 类型天数超过 HEAVY_DATA_MAX_DAYS 时抛出。
                        请改用 get_daily_data() 在每日循环中逐天加载。
        """
        _check_heavy_data_range(data_type, n_days)
        if from_date is None:
            from_date = datetime.now().strftime("%Y%m%d")
        return self._oss.load_prev_days(from_date, n_days, data_type, codes=codes)

    def get_daily_basic_history(
        self,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取历史日频基础数据

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            日频基础数据DataFrame
        """
        return self._oss.load_range(start_date, end_date, "daily_basic")

    def get_trading_days(
        self,
        start_date: str,
        end_date: str
    ) -> List[str]:
        """
        获取交易日列表

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表
        """
        return self._oss.get_trading_days(start_date, end_date)

    def get_prev_trading_day(self, n: int = 1) -> Optional[str]:
        """
        获取前N个交易日

        Args:
            n: 前第几个交易日

        Returns:
            交易日字符串
        """
        today = datetime.now().strftime("%Y%m%d")
        return self._oss.get_prev_trading_day(today, n)

    # ==================== 便捷方法 ====================

    def get_stock_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        freq: str = "1min"
    ) -> pd.DataFrame:
        """
        获取单只股票的历史K线数据

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 频率 (1min/5min/10min/daily)

        Returns:
            K线数据DataFrame
        """
        if freq == "daily":
            return self.get_daily_basic_history(start_date, end_date)

        # 对于分钟数据，需要从tick聚合（这里简化处理）
        # 实际应用中可能需要预计算好的分钟数据
        tick_data = self.get_history(start_date, end_date, "tick", [code])

        if tick_data.empty:
            return pd.DataFrame()

        # 按频率聚合
        freq_minutes = int(freq.replace("min", ""))
        return self._aggregate_kline(tick_data, freq_minutes)

    def _aggregate_kline(self, tick_df: pd.DataFrame, freq_minutes: int) -> pd.DataFrame:
        """将tick数据聚合为K线"""
        if tick_df.empty:
            return pd.DataFrame()

        # 设置时间索引
        tick_df = tick_df.copy()
        tick_df["Time"] = pd.to_datetime(tick_df["Time"])
        tick_df = tick_df.set_index("Time")

        # 按频率重采样
        agg_dict = {
            "CurrentPrice": ["first", "max", "min", "last"],
            "TotalVolume": "last",
        }

        resampled = tick_df.resample(f"{freq_minutes}min").agg(agg_dict)
        resampled.columns = ["Open", "High", "Low", "Close", "Volume"]

        return resampled.dropna()

    # ==================== 状态信息 ====================

    def get_stats(self) -> dict:
        """获取数据统计信息"""
        stats = {
            "mode": self.mode,
            "oss_path": str(self._oss.base_path),
        }

        if self._memory:
            stats["memory"] = self._memory.get_stats()
            stats["memory_usage"] = self._memory.get_memory_usage()

        stats["oss_cache"] = self._oss.get_cache_info()

        return stats

    def is_realtime_available(self) -> bool:
        """检查实时数据是否可用"""
        return self.mode == "realtime" and self._memory is not None

    def load_factor_result(
        self,
        oss_path: str,
        format: str = "parquet",
    ) -> pd.DataFrame:
        """
        从 OSS 路径加载因子计算结果。

        Args:
            oss_path: OSS 文件路径，如：
                     - "factor_results/alpha101/20250106.parquet"
                     - "factor_results/momentum_5d/20250106_20250110.parquet"
            format: 文件格式，支持 "parquet"（默认）、"csv"

        Returns:
            pd.DataFrame: 因子结果数据

        Example:
            >>> api = DataAPI()
            >>> # 加载单日因子结果
            >>> df = api.load_factor_result("factor_results/alpha101/20250106.parquet")
            >>> # 加载多日因子结果
            >>> df = api.load_factor_result("output/momentum_5d.parquet")
            >>> # 按 code 和 date 筛选
            >>> df_filtered = df[(df["code"] == "000001.SZ") & (df["date"] >= "20250106")]
        """
        import os

        # 解析文件扩展名自动检测格式
        if oss_path.endswith(".csv"):
            format = "csv"
        elif oss_path.endswith(".parquet") or oss_path.endswith(".pq"):
            format = "parquet"

        # 使用 OSS loader 读取文件
        try:
            bucket = os.getenv("OSS_RESULT_BUCKET", "stock-mdl-data-result")
            local_cache_path = self._oss._get_cache_path(f"{bucket}/{oss_path}")

            # 先检查本地缓存
            if os.path.exists(local_cache_path):
                logger.info(f"从本地缓存加载因子结果: {local_cache_path}")
                if format == "csv":
                    return pd.read_csv(local_cache_path)
                else:
                    return pd.read_parquet(local_cache_path)

            # 从 OSS 下载到本地缓存
            logger.info(f"从 OSS 下载因子结果: {oss_path}")
            import oss2
            auth = oss2.Auth(
                os.getenv("OSS_ACCESS_KEY_ID"),
                os.getenv("OSS_ACCESS_KEY_SECRET")
            )
            endpoint = os.getenv("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
            bucket_obj = oss2.Bucket(auth, endpoint, bucket)

            # 确保缓存目录存在
            os.makedirs(os.path.dirname(local_cache_path), exist_ok=True)

            # 下载文件
            bucket_obj.get_object_to_file(oss_path, local_cache_path)

            # 读取数据
            if format == "csv":
                df = pd.read_csv(local_cache_path)
            else:
                df = pd.read_parquet(local_cache_path)

            logger.info(f"因子结果加载成功: {len(df)} 行 × {len(df.columns)} 列")
            return df

        except Exception as e:
            logger.error(f"加载因子结果失败 {oss_path}: {e}")
            return pd.DataFrame()

    def list_factor_results(
        self,
        oss_prefix: str = "factor_results/",
    ) -> List[str]:
        """
        列出 OSS 上的因子结果文件。

        Args:
            oss_prefix: OSS 前缀路径，如 "factor_results/alpha101/"

        Returns:
            List[str]: 文件路径列表

        Example:
            >>> api = DataAPI()
            >>> files = api.list_factor_results("factor_results/alpha101/")
            >>> for f in files:
            ...     print(f)
        """
        import os
        import oss2

        try:
            bucket = os.getenv("OSS_RESULT_BUCKET", "stock-mdl-data-result")
            auth = oss2.Auth(
                os.getenv("OSS_ACCESS_KEY_ID"),
                os.getenv("OSS_ACCESS_KEY_SECRET")
            )
            endpoint = os.getenv("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
            bucket_obj = oss2.Bucket(auth, endpoint, bucket)

            files = []
            for obj in oss2.ObjectIterator(bucket_obj, prefix=oss_prefix):
                if not obj.key.endswith("/"):  # 跳过目录
                    files.append(obj.key)

            return sorted(files)

        except Exception as e:
            logger.error(f"列出因子结果失败 {oss_prefix}: {e}")
            return []


# 便捷创建函数
def create_realtime_api(oss_base_path: Optional[str] = None) -> DataAPI:
    """创建实盘模式DataAPI"""
    return DataAPI(mode="realtime", oss_base_path=oss_base_path)


def create_backtest_api(oss_base_path: Optional[str] = None) -> DataAPI:
    """创建回测模式DataAPI"""
    return DataAPI(mode="backtest", oss_base_path=oss_base_path)
