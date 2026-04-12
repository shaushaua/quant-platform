# -*- coding: utf-8 -*-
"""
OSS数据加载器
直接通过 oss2 SDK（小文件）和 DuckDB S3（大文件）读取 OSS parquet 数据，无需本地挂载。

路径格式: {base_path}/{year}{month}/{year}{month}{day}/{year}{month}{day}_{type}.parquet
示例:     2025/202501/20250102/20250102_daily_basic_data.parquet
"""

import io
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import oss2
import pandas as pd

logger = logging.getLogger(__name__)

# 大文件类型：用 DuckDB S3 谓词下推，必须带 codes 过滤
_LARGE_DATA_TYPES = {"tick", "order", "deal", "kline_1min", "kline_5min",
                     "kline_10min", "kline_30min", "kline_60min"}

try:
    import duckdb as _duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _duckdb = None
    _DUCKDB_AVAILABLE = False


class OSSDataLoader:
    """
    OSS数据加载器

    - daily_basic（小文件，~1MB）: oss2 直接下载到内存
    - tick/order/deal/kline（大文件，1-4GB）: DuckDB S3 谓词下推，
      文件按 Code 排序，row group 统计支持跳过，只下载相关行
    """

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

    _CODE_COL = {
        "tick": "Code",
        "order": "Code",
        "deal": "Code",
        "daily_basic": "ts_code",
        "kline_1min": "Code",
        "kline_5min": "Code",
        "kline_10min": "Code",
        "kline_30min": "Code",
        "kline_60min": "Code",
    }
    _DEFAULT_CODE_COL = "Code"

    # 类级别 DuckDB 连接，复用避免重复 INSTALL httpfs
    _duckdb_con = None

    def __init__(
        self,
        base_path: str = "2025",
        cache_enabled: bool = True,
        cache_size: int = None,
    ):
        self.base_path = base_path.strip("/")
        self.cache_enabled = cache_enabled
        self.cache_size = cache_size if cache_size is not None else int(os.getenv("CACHE_SIZE", "10"))
        self._cache: Dict[str, pd.DataFrame] = {}
        self._grouped_cache: Dict[str, Dict] = {}

        self._ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
        self._sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
        self._endpoint = os.environ.get("OSS_ENDPOINT", "")
        self._data_bucket = os.environ.get("OSS_DATA_BUCKET", "stock-mdl-data")
        self._region = os.environ.get("OSS_REGION", "oss-cn-hangzhou")

        self._oss_bucket = self._init_oss_bucket()
        if _DUCKDB_AVAILABLE:
            self._init_duckdb()
        else:
            logger.warning("duckdb 未安装，大文件（tick/order/deal）将不可用")

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------

    def _init_oss_bucket(self) -> Optional[oss2.Bucket]:
        if not all([self._ak, self._sk, self._endpoint]):
            logger.warning("OSS 凭证未完整配置")
            return None
        auth = oss2.Auth(self._ak, self._sk)
        bucket = oss2.Bucket(auth, self._endpoint, self._data_bucket)
        logger.info(f"OSSDataLoader 初始化: bucket={self._data_bucket} prefix={self.base_path}")
        return bucket

    def _init_duckdb(self):
        if OSSDataLoader._duckdb_con is not None:
            return
        try:
            con = _duckdb.connect()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            # 配置阿里云 OSS 认证 header
            con.execute(f"SET http_server_encoding='utf-8';")
            OSSDataLoader._duckdb_con = con
            logger.info("DuckDB HTTP 连接初始化完成")
        except Exception as e:
            logger.error(f"DuckDB 初始化失败: {e}")

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _object_key(self, date_str: str, data_type: str) -> str:
        """构建 OSS object key。date_str 格式 YYYYMMDD。"""
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]
        file_suffix = self.DATA_TYPES.get(data_type, data_type)
        return f"{self.base_path}/{year}{month}/{year}{month}{day}/{year}{month}{day}_{file_suffix}.parquet"

    def _s3_url(self, key: str) -> str:
        # 使用 HTTPS URL 而不是 s3:// 协议（DuckDB s3 协议与阿里云 OSS 不兼容）
        # 格式: https://{ak}:{sk}@{endpoint}/{bucket}/{key}
        from urllib.parse import quote
        endpoint = self._endpoint.replace("https://", "").replace("http://", "").rstrip("/")
        encoded_ak = quote(self._ak, safe='')
        encoded_sk = quote(self._sk, safe='')
        return f"https://{encoded_ak}:{encoded_sk}@{endpoint}/{self._data_bucket}/{key}"

    def _read_small_file(self, key: str) -> pd.DataFrame:
        """用 oss2 下载小文件到内存，读取 parquet。"""
        if self._oss_bucket is None:
            return pd.DataFrame()
        try:
            result = self._oss_bucket.get_object(key)
            data = result.read()
            return pd.read_parquet(io.BytesIO(data))
        except oss2.exceptions.NoSuchKey:
            logger.warning(f"OSS 文件不存在: {key}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"读取 OSS 小文件失败 {key}: {e}")
            return pd.DataFrame()

    def _resolve_security_ids(self, date_str: str, codes: List[str]) -> List[int]:
        """将 000001.SZ 格式的 codes 转成 SECURITY_ID 整数列表。"""
        key = self._object_key(date_str, "daily_basic")
        df = self._read_small_file(key)
        if df.empty or "ID_QI" not in df.columns or "SECURITY_ID" not in df.columns:
            return []
        # ID_QI 是6位纯数字字符串，codes 可能带交易所后缀如 000001.SZ
        id_map = dict(zip(df["ID_QI"].astype(str), df["SECURITY_ID"].astype(int)))
        result = []
        for code in codes:
            id_qi = code.split(".")[0].zfill(6)  # 000001.SZ -> 000001
            if id_qi in id_map:
                result.append(id_map[id_qi])
        return result

    def _read_large_file_by_codes(self, key: str, data_type: str, codes: List[str]) -> pd.DataFrame:
        """用 DuckDB S3 谓词下推读取大文件中指定股票的数据。"""
        if not _DUCKDB_AVAILABLE or OSSDataLoader._duckdb_con is None:
            logger.error("DuckDB 不可用，无法读取大文件")
            return pd.DataFrame()
        # 从 key 中提取日期 YYYYMMDD
        import re
        m = re.search(r'(\d{8})', key)
        date_str = m.group(1) if m else ""
        security_ids = self._resolve_security_ids(date_str, codes) if date_str else []
        if not security_ids:
            logger.warning(f"无法解析 codes={codes} 对应的 SECURITY_ID，跳过 {key}")
            return pd.DataFrame()
        ids_str = ", ".join(str(i) for i in security_ids)
        url = self._s3_url(key)
        try:
            sql = f"SELECT * FROM read_parquet('{url}') WHERE Code IN ({ids_str})"
            df = OSSDataLoader._duckdb_con.execute(sql).df()
            logger.info(f"DuckDB 读取 {key}: {len(df)} 条记录 security_ids={security_ids}")
            return df
        except Exception as e:
            logger.error(f"DuckDB 读取失败 {key}: {e}")
            return pd.DataFrame()

    def _list_keys_with_prefix(self, prefix: str) -> List[str]:
        if self._oss_bucket is None:
            return []
        keys = []
        try:
            for obj in oss2.ObjectIterator(self._oss_bucket, prefix=prefix):
                keys.append(obj.key)
        except Exception as e:
            logger.error(f"列举 OSS 对象失败 prefix={prefix}: {e}")
        return keys

    def _manage_cache(self):
        if len(self._cache) >= self.cache_size:
            keys = list(self._cache.keys())
            for key in keys[:len(keys) - self.cache_size + 1]:
                del self._cache[key]

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    def load(
        self,
        date: str,
        data_type: str,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        加载单日数据。

        大文件（tick/order/deal）必须提供 codes，否则返回空 DataFrame 防止 OOM。
        """
        date_str = date.replace("-", "")
        is_large = data_type in _LARGE_DATA_TYPES

        if is_large and not codes:
            logger.warning(f"大文件类型 '{data_type}' 必须提供 codes 过滤，已跳过")
            return pd.DataFrame()

        cache_key = f"{date_str}_{data_type}" if not is_large else f"{date_str}_{data_type}_{'_'.join(sorted(codes or []))}"
        if self.cache_enabled and cache_key in self._cache:
            return self._cache[cache_key]

        key = self._object_key(date_str, data_type)

        if is_large:
            df = self._read_large_file_by_codes(key, data_type, codes)
        else:
            df = self._read_small_file(key)
            # 小文件按 codes 过滤
            if codes and not df.empty:
                col = self._CODE_COL.get(data_type, self._DEFAULT_CODE_COL)
                if col in df.columns:
                    df = df[df[col].isin(codes)].copy()

        if not df.empty and self.cache_enabled:
            self._manage_cache()
            self._cache[cache_key] = df

        return df

    def load_range(
        self,
        start_date: str,
        end_date: str,
        data_type: str,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        trading_days = self.get_trading_days(start_date, end_date)
        if not trading_days:
            logger.warning(f"日期范围内无交易日: {start_date} ~ {end_date}")
            return pd.DataFrame()

        dfs = []
        for day in trading_days:
            df = self.load(day, data_type, codes)
            if not df.empty:
                dfs.append(df)

        if dfs:
            result = pd.concat(dfs, ignore_index=True)
            dfs.clear()
            logger.info(f"加载范围数据: {len(result)} 条记录, {len(trading_days)} 个交易日")
            return result
        return pd.DataFrame()

    def load_for_code(
        self,
        code: str,
        date: str,
        data_type: str,
        market_count: int = 1,
    ) -> pd.DataFrame:
        """
        加载单只股票最近 market_count 个交易日的历史数据。
        由因子引擎注入 StockData.market 时调用。
        """
        date_str = date.replace("-", "")
        cache_key = f"for_code_{code}_{date_str}_{data_type}_{market_count}"
        if self.cache_enabled and cache_key in self._grouped_cache:
            return self._grouped_cache[cache_key]

        recent_days = self._get_prev_trading_days(date_str, market_count)
        if not recent_days:
            return pd.DataFrame()

        dfs = []
        for day in recent_days:
            df = self.load(day, data_type, codes=[code])
            if not df.empty:
                df = df.copy()
                df["_date"] = day
                dfs.append(df)

        result = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        if not result.empty and self.cache_enabled:
            self._grouped_cache[cache_key] = result

        return result

    def get_trading_days(self, start_date: str, end_date: str) -> List[str]:
        """
        通过列举 OSS 对象获取实际有数据的交易日列表。
        以存在 daily_basic_data.parquet 为准。
        """
        start_str = start_date.replace("-", "")
        end_str = end_date.replace("-", "")

        start_dt = datetime.strptime(start_str, "%Y%m%d")
        end_dt = datetime.strptime(end_str, "%Y%m%d")

        trading_days = set()
        current = start_dt.replace(day=1)
        while current <= end_dt:
            year_month = current.strftime("%Y%m")
            prefix = f"{self.base_path}/{year_month}/"
            keys = self._list_keys_with_prefix(prefix)
            for key in keys:
                if "daily_basic_data" not in key:
                    continue
                parts = key.split("/")
                if len(parts) >= 3:
                    day_str = parts[-2]  # e.g. "20250102"
                    if len(day_str) == 8 and start_str <= day_str <= end_str:
                        trading_days.add(day_str)
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        result = sorted(trading_days)
        logger.info(f"交易日列表: {start_str}~{end_str} 共 {len(result)} 天")
        return result

    def get_prev_trading_day(self, date: str, n: int = 1) -> Optional[str]:
        days = self._get_prev_trading_days(date.replace("-", ""), n + 1)
        return days[n] if len(days) > n else None

    def _get_prev_trading_days(self, date_str: str, n: int) -> List[str]:
        """向前找 n 个交易日（含当天），从 OSS 列举，失败则降级剔除周末。"""
        end_dt = datetime.strptime(date_str, "%Y%m%d")
        start_dt = end_dt - timedelta(days=max(n * 2, 60))
        days = self.get_trading_days(start_dt.strftime("%Y%m%d"), date_str)
        if days:
            return list(reversed(days[-n:]))
        # 降级
        result, cur = [], end_dt
        while len(result) < n:
            if cur.weekday() < 5:
                result.append(cur.strftime("%Y%m%d"))
            cur -= timedelta(days=1)
        return result

    def load_prev_days(
        self,
        from_date: str,
        n_days: int,
        data_type: str,
        codes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """加载 from_date 之前（含当天）n_days 个交易日的数据，结果带 _date 列。"""
        days = self._get_prev_trading_days(from_date.replace("-", ""), n_days)
        if not days:
            return pd.DataFrame()
        dfs = []
        for day in days:
            df = self.load(day, data_type, codes)
            if not df.empty:
                df = df.copy()
                df["_date"] = day
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def clear_cache(self):
        self._cache.clear()
        self._grouped_cache.clear()
        logger.info("OSS数据缓存已清空")

    def get_cache_info(self) -> dict:
        total_size = sum(df.memory_usage(deep=True).sum() for df in self._cache.values())
        return {
            "cache_count": len(self._cache),
            "cache_size_mb": round(total_size / 1024 / 1024, 2),
            "cache_keys": list(self._cache.keys()),
        }


# 便捷函数
def create_loader(base_path: str = "2025") -> OSSDataLoader:
    return OSSDataLoader(base_path)
