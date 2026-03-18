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


# ==================== 便捷函数 ====================

def create_loader() -> MySQLLoader:
    """创建MySQL加载器"""
    return MySQLLoader()


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
