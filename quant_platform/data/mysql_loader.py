# -*- coding: utf-8 -*-
"""
MySQL数据加载器
从MySQL加载涨跌停价格等基础数据

参考 deeptrade/dataconv/repository.go 的实现
表结构:
- mkt_limit: 涨跌停价格 (code, trade_date, high_limit, low_limit, pre_close)
"""

import os
import logging
import threading
from datetime import datetime, date
from typing import Dict, List, Optional
from dataclasses import dataclass

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class LimitPrice:
    """涨跌停价格"""
    code: str
    trade_date: date
    high_limit: float
    low_limit: float
    pre_close: float


class MySQLLoader:
    """
    MySQL数据加载器

    使用方式:
        loader = MySQLLoader()
        loader.connect()

        # 获取涨跌停价格
        prices = loader.get_limit_prices("2025-01-02")
        limit_price = prices.get("000001.XSHE")
        print(limit_price.high_limit, limit_price.low_limit)

        loader.close()
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None,
    ):
        """
        初始化MySQL加载器

        优先使用传入参数，其次环境变量，最后默认值
        """
        self.host = host or os.getenv("MYSQL_HOST", "rm-bp15t3z3kt5174n47so.mysql.rds.aliyuncs.com")
        self.port = port or int(os.getenv("MYSQL_PORT", "3306"))
        self.user = user or os.getenv("MYSQL_USER", "hermes_trade")
        self.password = password or os.getenv("MYSQL_PASSWORD", "1tuweN6688")
        self.database = database or os.getenv("MYSQL_DATABASE", "hermes")

        self._conn = None
        self._lock = threading.Lock()

    def connect(self) -> bool:
        """建立数据库连接"""
        try:
            import pymysql
            self._conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                connect_timeout=10,
            )
            logger.info(f"MySQL连接成功: {self.host}:{self.port}/{self.database}")
            return True
        except ImportError:
            logger.error("pymysql未安装，请运行: pip install pymysql")
            return False
        except Exception as e:
            logger.error(f"MySQL连接失败: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("MySQL连接已关闭")

    def _ensure_connection(self):
        """确保连接有效"""
        if self._conn is None:
            if not self.connect():
                raise ConnectionError("MySQL连接失败")
        # 检查连接是否有效
        try:
            self._conn.ping(reconnect=True)
        except:
            self.connect()

    # ==================== 涨跌停价格 ====================

    def get_limit_prices(self, trade_date: str) -> Dict[str, LimitPrice]:
        """
        获取指定日期的涨跌停价格

        Args:
            trade_date: 交易日期 (YYYY-MM-DD 或 YYYYMMDD)

        Returns:
            {code: LimitPrice} 字典
        """
        # 标准化日期格式
        if len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        self._ensure_connection()

        try:
            with self._conn.cursor() as cursor:
                sql = """
                    SELECT code, trade_date, high_limit, low_limit, pre_close
                    FROM mkt_limit
                    WHERE trade_date = %s
                """
                cursor.execute(sql, (trade_date,))
                rows = cursor.fetchall()

                result = {}
                for row in rows:
                    code = row[0]
                    # 转换代码格式 (000001 -> 000001.XSHE)
                    code = self._format_code(code)
                    result[code] = LimitPrice(
                        code=code,
                        trade_date=row[1],
                        high_limit=float(row[2]) if row[2] else 0.0,
                        low_limit=float(row[3]) if row[3] else 0.0,
                        pre_close=float(row[4]) if row[4] else 0.0,
                    )

                logger.info(f"获取涨跌停价格: {trade_date}, {len(result)} 条")
                return result

        except Exception as e:
            logger.error(f"查询涨跌停价格失败: {e}")
            return {}

    def get_limit_price(self, code: str, trade_date: str) -> Optional[LimitPrice]:
        """
        获取单只股票的涨跌停价格

        Args:
            code: 股票代码 (000001.XSHE 或 000001)
            trade_date: 交易日期

        Returns:
            LimitPrice 或 None
        """
        # 标准化日期格式
        if len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        # 标准化代码格式
        raw_code = code.split('.')[0]

        self._ensure_connection()

        try:
            with self._conn.cursor() as cursor:
                sql = """
                    SELECT code, trade_date, high_limit, low_limit, pre_close
                    FROM mkt_limit
                    WHERE code = %s AND trade_date = %s
                """
                cursor.execute(sql, (raw_code, trade_date))
                row = cursor.fetchone()

                if row:
                    return LimitPrice(
                        code=self._format_code(row[0]),
                        trade_date=row[1],
                        high_limit=float(row[2]) if row[2] else 0.0,
                        low_limit=float(row[3]) if row[3] else 0.0,
                        pre_close=float(row[4]) if row[4] else 0.0,
                    )
                return None

        except Exception as e:
            logger.error(f"查询涨跌停价格失败 {code}: {e}")
            return None

    # ==================== 股票列表 ====================

    def get_all_securities(self, trade_date: str = None) -> List[str]:
        """
        获取指定日期的所有股票代码

        Args:
            trade_date: 交易日期，默认今天

        Returns:
            股票代码列表
        """
        if trade_date is None:
            trade_date = datetime.now().strftime("%Y-%m-%d")
        elif len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        self._ensure_connection()

        try:
            with self._conn.cursor() as cursor:
                sql = """
                    SELECT DISTINCT code
                    FROM mkt_limit
                    WHERE trade_date = %s
                """
                cursor.execute(sql, (trade_date,))
                rows = cursor.fetchall()

                codes = [self._format_code(row[0]) for row in rows]
                logger.info(f"获取股票列表: {trade_date}, {len(codes)} 只")
                return codes

        except Exception as e:
            logger.error(f"查询股票列表失败: {e}")
            return []

    # ==================== 日线基础数据 (daily_basic) ====================

    def get_daily_basic(self, trade_date: str, market_count: int = 1) -> pd.DataFrame:
        """
        从 MySQL 加载日线基础数据（daily_basic），与 Go data-converter 的
        GenerateDailyBasicData 保持一致。

        查询 mkt_equd + mkt_equd_adj_af + md_security 表，输出列:
            _date, ID_QI, SECURITY_ID, SEC_SHORT_NAME, SEC_FULL_NAME,
            open, high, low, close,
            adj_open, adj_close, adj_high, adj_low, adj_pre_close,
            deal_amount, volume, amount, mkt_cap, float_mkt_cap,
            turnover_rate, pe_ttm, pb

        Args:
            trade_date:   交易日期 (YYYY-MM-DD 或 YYYYMMDD)
            market_count: 需要的历史天数，默认 1（仅当天）

        Returns:
            pd.DataFrame
        """
        if len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        self._ensure_connection()

        sql = """
            SELECT
                t1.TRADE_DATE        AS _date,
                t1.TICKER_SYMBOL     AS ID_QI,
                t1.SECURITY_ID,
                s.SEC_SHORT_NAME      AS SEC_SHORT_NAME,
                s.SEC_FULL_NAME       AS SEC_FULL_NAME,
                t2.OPEN_PRICE_2      AS open,
                t2.HIGHEST_PRICE     AS high,
                t2.LOWEST_PRICE      AS low,
                t2.CLOSE_PRICE       AS close,
                t2.OPEN_PRICE_2      AS adj_open,
                t2.CLOSE_PRICE_2     AS adj_close,
                t2.HIGHEST_PRICE_2   AS adj_high,
                t2.LOWEST_PRICE_2    AS adj_low,
                t2.PRE_CLOSE_PRICE_2 AS adj_pre_close,
                t1.DEAL_AMOUNT       AS deal_amount,
                t1.TURNOVER_VOL      AS volume,
                t1.TURNOVER_VALUE    AS amount,
                t1.MARKET_VALUE      AS mkt_cap,
                t1.NEG_MARKET_VALUE  AS float_mkt_cap,
                t1.TURNOVER_RATE     AS turnover_rate,
                t1.PE                AS pe_ttm,
                t1.PB                AS pb
            FROM mkt_equd t1
            JOIN mkt_equd_adj_af t2
                ON  t1.SECURITY_ID = t2.SECURITY_ID
                AND t1.TRADE_DATE  = t2.TRADE_DATE
            LEFT JOIN md_security s
                ON t1.SECURITY_ID = s.SECURITY_ID
            WHERE t1.TRADE_DATE = %s
              AND t1.EXCHANGE_CD IN ('XSHG', 'XSHE')
            ORDER BY t1.TICKER_SYMBOL
        """

        dfs = []
        current_date = trade_date

        try:
            with self._conn.cursor() as cursor:
                for _ in range(market_count):
                    cursor.execute(sql, (current_date,))
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    if rows:
                        dfs.append(pd.DataFrame(rows, columns=columns))

                    if len(dfs) >= market_count:
                        break

                    # 获取前一个交易日
                    cursor.execute(
                        "SELECT MAX(TRADE_DATE) FROM mkt_equd WHERE TRADE_DATE < %s",
                        (current_date,),
                    )
                    prev = cursor.fetchone()
                    if prev and prev[0]:
                        current_date = prev[0].strftime("%Y-%m-%d") if hasattr(prev[0], "strftime") else str(prev[0])
                    else:
                        break

            if not dfs:
                return pd.DataFrame()

            result = pd.concat(dfs, ignore_index=True)

            # 格式化 _date 为 YYYYMMDD 字符串
            result["_date"] = pd.to_datetime(result["_date"]).dt.strftime("%Y%m%d")

            # 数值类型转换
            numeric_cols = [
                "open", "high", "low", "close",
                "adj_open", "adj_close", "adj_high", "adj_low", "adj_pre_close",
                "deal_amount", "volume", "amount", "mkt_cap", "float_mkt_cap",
                "turnover_rate", "pe_ttm", "pb",
            ]
            for col in numeric_cols:
                if col in result.columns:
                    result[col] = pd.to_numeric(result[col], errors="coerce")

            logger.info(f"获取 daily_basic: {trade_date}, market_count={market_count}, 共 {len(result)} 条")
            return result

        except Exception as e:
            logger.error(f"查询 daily_basic 失败: {e}")
            return pd.DataFrame()

    # ==================== 辅助方法 ====================

    def _format_code(self, raw_code: str) -> str:
        """
        格式化股票代码为标准格式

        Args:
            raw_code: 原始代码 (如 000001, 600000)

        Returns:
            标准格式 (如 000001.XSHE, 600000.XSHG)
        """
        code = str(raw_code).zfill(6)
        if code.startswith(('6', '9')):
            return f"{code}.XSHG"
        else:
            return f"{code}.XSHE"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ==================== 指数成分股 (idx_cons) ====================

    def get_idx_cons(self, index_ids: List[str]) -> pd.DataFrame:
        """
        从 MySQL 加载指数成分股数据 (idx_cons)。

        idx_cons 表结构:
            SECURITY_ID = 指数内部ID, CONS_ID = 成分股内部ID,
            INTO_DATE, OUT_DATE, IS_NEW

        通过 JOIN mkt_idxd_csi 获取指数交易代码，
        通过 JOIN md_security 获取成分股代码和名称。

        Args:
            index_ids: 指数 SECURITY_ID 列表，如 ["1782","2103","33736","3800","1200245"]

        Returns:
            pd.DataFrame with columns:
                INDEX_ID, INDEX_CODE, STOCK_ID, ID_QI, SEC_SHORT_NAME,
                INTO_DATE, OUT_DATE, IS_NEW
        """
        self._ensure_connection()

        try:
            with self._conn.cursor() as cursor:
                placeholders = ",".join(["%s"] * len(index_ids))
                sql = f"""
                    SELECT
                        ic.SECURITY_ID  AS INDEX_ID,
                        idx.TICKER_SYMBOL AS INDEX_CODE,
                        ic.CONS_ID      AS STOCK_ID,
                        stock.TICKER_SYMBOL AS ID_QI,
                        stock.SEC_SHORT_NAME AS SEC_SHORT_NAME,
                        ic.INTO_DATE,
                        ic.OUT_DATE,
                        ic.IS_NEW
                    FROM idx_cons ic
                    INNER JOIN (
                        SELECT DISTINCT INDEX_ID, TICKER_SYMBOL
                        FROM mkt_idxd_csi
                        WHERE INDEX_ID IN ({placeholders})
                    ) idx ON ic.SECURITY_ID = idx.INDEX_ID
                    INNER JOIN md_security stock
                        ON ic.CONS_ID = stock.SECURITY_ID
                    WHERE ic.IS_NEW = 1
                      AND stock.ASSET_CLASS = 'E'
                      AND stock.LIST_STATUS_CD = 'L'
                    ORDER BY idx.TICKER_SYMBOL, stock.TICKER_SYMBOL
                """
                cursor.execute(sql, tuple(index_ids))
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                if not rows:
                    logger.warning(f"idx_cons: no rows for index_ids={index_ids}")
                    return pd.DataFrame()

                result = pd.DataFrame(rows, columns=columns)

                # Normalize date columns to YYYYMMDD strings
                for col in ("INTO_DATE", "OUT_DATE"):
                    if col in result.columns:
                        result[col] = pd.to_datetime(result[col], errors="coerce").dt.strftime("%Y%m%d")

                # Pad ID_QI to 6 digits
                if "ID_QI" in result.columns:
                    result["ID_QI"] = result["ID_QI"].astype(str).str.zfill(6)

                logger.info(f"获取 idx_cons: index_ids={index_ids}, {len(result)} 条")
                return result

        except Exception as e:
            logger.error(f"查询 idx_cons 失败: {e}")
            return pd.DataFrame()


class PriceCache:
    """
    价格缓存

    使用方式:
        cache = PriceCache()
        cache.load(trade_date="20250102")

        high, low = cache.get_limit("000001.XSHE")
    """

    def __init__(self, loader: MySQLLoader = None):
        self.loader = loader or MySQLLoader()
        self._prices: Dict[str, LimitPrice] = {}
        self._date: str = ""
        self._lock = threading.RLock()

    def load(self, trade_date: str) -> bool:
        """
        加载指定日期的价格数据

        Args:
            trade_date: 交易日期

        Returns:
            是否成功
        """
        try:
            if not self.loader._conn:
                self.loader.connect()

            prices = self.loader.get_limit_prices(trade_date)

            with self._lock:
                self._prices = prices
                self._date = trade_date

            logger.info(f"价格缓存加载完成: {trade_date}, {len(prices)} 条")
            return True

        except Exception as e:
            logger.error(f"加载价格缓存失败: {e}")
            return False

    def get_limit(self, code: str) -> tuple:
        """
        获取涨跌停价格

        Args:
            code: 股票代码

        Returns:
            (high_limit, low_limit) 元组
        """
        with self._lock:
            price = self._prices.get(code)
            if price and price.high_limit > 0:
                return price.high_limit, price.low_limit

            # 如果没有涨跌停价，根据前收盘价计算 (±10%)
            if price and price.pre_close > 0:
                high = round(price.pre_close * 1.10, 2)
                low = round(price.pre_close * 0.90, 2)
                return high, low

            return 0.0, 0.0

    def get_pre_close(self, code: str) -> float:
        """获取前收盘价"""
        with self._lock:
            price = self._prices.get(code)
            return price.pre_close if price else 0.0

    def get_all_codes(self) -> List[str]:
        """获取所有股票代码"""
        with self._lock:
            return list(self._prices.keys())

    def is_loaded(self) -> bool:
        """是否已加载数据"""
        return len(self._prices) > 0


class DailyBasicCache:
    """
    日线基础数据缓存

    使用方式:
        cache = DailyBasicCache(market_count=5)
        cache.load(trade_date="20250102")

        df = cache.get_daily_basic()
    """

    def __init__(self, loader: MySQLLoader = None, market_count: int = 1):
        self.loader = loader or MySQLLoader()
        self.market_count = market_count
        self._df: Optional[pd.DataFrame] = None
        self._date: str = ""
        self._lock = threading.RLock()

    def load(self, trade_date: str) -> bool:
        """
        加载指定日期的 daily_basic 数据

        Args:
            trade_date: 交易日期

        Returns:
            是否成功
        """
        try:
            if not self.loader._conn:
                self.loader.connect()

            df = self.loader.get_daily_basic(trade_date, self.market_count)

            with self._lock:
                self._df = df
                self._date = trade_date

            logger.info(f"daily_basic 缓存加载完成: {trade_date}, market_count={self.market_count}, {len(df)} 条")
            return True

        except Exception as e:
            logger.error(f"加载 daily_basic 缓存失败: {e}")
            return False

    def get_daily_basic(self) -> pd.DataFrame:
        """获取 daily_basic DataFrame"""
        with self._lock:
            return self._df if self._df is not None else pd.DataFrame()

    def is_loaded(self) -> bool:
        """是否已加载数据"""
        return self._df is not None and not self._df.empty


# ==================== 便捷函数 ====================

def create_loader() -> MySQLLoader:
    """创建MySQL加载器"""
    return MySQLLoader()


class IdxConsCache:
    """
    指数成分股缓存

    使用方式:
        cache = IdxConsCache()
        cache.load()

        df = cache.get_idx_cons()
    """

    def __init__(self, loader: MySQLLoader = None, index_ids: List[str] = None):
        self.loader = loader or MySQLLoader()
        self.index_ids = index_ids or []
        self._df: Optional[pd.DataFrame] = None
        self._lock = threading.RLock()

    def load(self) -> bool:
        """加载指数成分股数据."""
        try:
            if not self.loader._conn:
                self.loader.connect()

            df = self.loader.get_idx_cons(self.index_ids)

            with self._lock:
                self._df = df

            logger.info(f"idx_cons 缓存加载完成: index_ids={self.index_ids}, {len(df)} 条")
            return True
        except Exception as e:
            logger.error(f"加载 idx_cons 缓存失败: {e}")
            return False

    def get_idx_cons(self) -> pd.DataFrame:
        """获取 idx_cons DataFrame."""
        with self._lock:
            return self._df if self._df is not None else pd.DataFrame()

    def get_index_members(self, index_id: str) -> List[str]:
        """获取指定指数的成分股 ID_QI 列表."""
        with self._lock:
            if self._df is None or self._df.empty:
                return []
            mask = self._df["INDEX_ID"].astype(str) == str(index_id)
            return self._df.loc[mask, "ID_QI"].dropna().astype(str).tolist()

    def is_loaded(self) -> bool:
        """是否已加载数据."""
        return self._df is not None and not self._df.empty


def create_price_cache(trade_date: str = None) -> PriceCache:
    """
    创建价格缓存并加载数据

    Args:
        trade_date: 交易日期，默认今天

    Returns:
        PriceCache 实例
    """
    if trade_date is None:
        trade_date = datetime.now().strftime("%Y%m%d")

    cache = PriceCache()
    cache.load(trade_date)
    return cache
