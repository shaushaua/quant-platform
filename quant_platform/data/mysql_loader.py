# -*- coding: utf-8 -*-
"""
MySQL数据加载器
从MySQL加载涨跌停价格等基础数据

参考 deeptrade/dataconv/repository.go 的实现
表结构:
- mkt_limit: 涨跌停价格
  (TICKER_SYMBOL, EXCHANGE_CD, TRADE_DATE, LIMIT_UP_PRICE,
   LIMIT_DOWN_PRICE, PRE_CLOSE_PRICE)
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
                    SELECT TICKER_SYMBOL, EXCHANGE_CD, TRADE_DATE,
                           LIMIT_UP_PRICE, LIMIT_DOWN_PRICE, PRE_CLOSE_PRICE
                    FROM mkt_limit
                    WHERE TRADE_DATE = %s
                """
                cursor.execute(sql, (trade_date,))
                rows = cursor.fetchall()

                result = {}
                for row in rows:
                    code = self._format_code(row[0], row[1])
                    result[code] = LimitPrice(
                        code=code,
                        trade_date=row[2],
                        high_limit=float(row[3]) if row[3] else 0.0,
                        low_limit=float(row[4]) if row[4] else 0.0,
                        pre_close=float(row[5]) if row[5] else 0.0,
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
        code_parts = str(code).strip().upper().split('.', 1)
        raw_code = code_parts[0].zfill(6)
        exchange_cd = None
        if len(code_parts) == 2:
            exchange_cd = {
                "SH": "XSHG",
                "SZ": "XSHE",
            }.get(code_parts[1], code_parts[1])

        self._ensure_connection()

        try:
            with self._conn.cursor() as cursor:
                sql = """
                    SELECT TICKER_SYMBOL, EXCHANGE_CD, TRADE_DATE,
                           LIMIT_UP_PRICE, LIMIT_DOWN_PRICE, PRE_CLOSE_PRICE
                    FROM mkt_limit
                    WHERE TICKER_SYMBOL = %s AND TRADE_DATE = %s
                """
                params = [raw_code, trade_date]
                if exchange_cd:
                    sql += " AND EXCHANGE_CD = %s"
                    params.append(exchange_cd)
                cursor.execute(sql, tuple(params))
                row = cursor.fetchone()

                if row:
                    return LimitPrice(
                        code=self._format_code(row[0], row[1]),
                        trade_date=row[2],
                        high_limit=float(row[3]) if row[3] else 0.0,
                        low_limit=float(row[4]) if row[4] else 0.0,
                        pre_close=float(row[5]) if row[5] else 0.0,
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
                    SELECT DISTINCT TICKER_SYMBOL, EXCHANGE_CD
                    FROM mkt_limit
                    WHERE TRADE_DATE = %s
                """
                cursor.execute(sql, (trade_date,))
                rows = cursor.fetchall()

                codes = [self._format_code(row[0], row[1]) for row in rows]
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
                t2.OPEN_PRICE        AS open,
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

    def _format_code(self, raw_code: str, exchange_cd: str = None) -> str:
        """
        格式化股票代码为标准格式

        Args:
            raw_code: 原始代码 (如 000001, 600000)
            exchange_cd: 数据库交易所代码 (如 XSHE, XSHG)

        Returns:
            标准格式 (如 000001.XSHE, 600000.XSHG)
        """
        code = str(raw_code).zfill(6)
        exchange = str(exchange_cd or "").strip().upper()
        exchange = {"SH": "XSHG", "SZ": "XSHE"}.get(exchange, exchange)
        if exchange in {"XSHG", "XSHE"}:
            return f"{code}.{exchange}"
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

    def get_idx_cons(
        self,
        index_ids: List[str] = None,
        index_codes: List[str] = None,
        trading_day: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        从 MySQL 加载指数成分股数据 (idx_cons)。

        idx_cons 表结构:
            SECURITY_ID = 指数内部ID, CONS_ID = 成分股内部ID,
            INTO_DATE, OUT_DATE, IS_NEW

        通过 JOIN mkt_idxd_csi 获取指数交易代码，
        通过 JOIN md_security 获取成分股代码和名称。

        过滤模式:
            - trading_day=None: 走 IS_NEW=1 (兼容旧逻辑,只返回当前最新成分股)
            - trading_day="20220103" 或 "2022-01-03": 走区间过滤
              INTO_DATE <= T AND (OUT_DATE IS NULL OR OUT_DATE > T)
              用于历史回测,避免幸存者偏差

        指数识别(优先级):
            1. index_codes=["000300","000905",...]  (推荐,TICKER 代码)
            2. index_ids=["1782","2103",...]        (向后兼容,SECURITY_ID)

        Args:
            index_ids:    指数 SECURITY_ID 列表 (向后兼容,如 ["1782","2103"])
            index_codes:  指数 TICKER_SYMBOL 列表 (推荐,如 ["000300","000905"])
            trading_day:  交易日 (YYYYMMDD 或 YYYY-MM-DD),None=最新

        Returns:
            pd.DataFrame with columns:
                INDEX_ID, INDEX_CODE, STOCK_ID, ID_QI, SEC_SHORT_NAME,
                INTO_DATE, OUT_DATE, IS_NEW
        """
        self._ensure_connection()

        # 优先使用 TICKER 代码(支持 "TICKER.EXCHANGE" 消歧),fallback 到 SECURITY_ID
        if index_codes:
            code_filter_sql, filter_params = self._build_index_filter(index_codes)
            filter_desc = f"codes={index_codes}"
        elif index_ids:
            code_filter_sql = f"idx.INDEX_ID IN ({','.join(['%s']*len(index_ids))})"
            filter_params = tuple(index_ids)
            filter_desc = f"ids={index_ids}"
        else:
            logger.warning("get_idx_cons: 需要传入 index_codes 或 index_ids")
            return pd.DataFrame()

        # 时间过滤模式(与 deeptrade/dataconv/composition.go 保持完全一致)
        if trading_day:
            td = trading_day.replace("-", "")[:8]
            td_dash = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
            time_filter_sql = (
                "ic.INTO_DATE <= %s "
                "AND (ic.OUT_DATE IS NULL OR ic.OUT_DATE > %s)"
            )
            time_params = (td_dash, td_dash)
            time_desc = f"trading_day={td}"
        else:
            time_filter_sql = "ic.IS_NEW = 1"
            time_params = ()
            time_desc = "IS_NEW=1 (latest)"

        try:
            with self._conn.cursor() as cursor:
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
                        SELECT DISTINCT INDEX_ID, TICKER_SYMBOL, EXCHANGE_CD
                        FROM mkt_idxd_csi
                        WHERE {code_filter_sql}
                    ) idx ON ic.SECURITY_ID = idx.INDEX_ID
                    INNER JOIN md_security stock
                        ON ic.CONS_ID = stock.SECURITY_ID
                    WHERE {time_filter_sql}
                      AND stock.ASSET_CLASS = 'E'
                      AND stock.LIST_STATUS_CD = 'L'
                    ORDER BY idx.TICKER_SYMBOL, stock.TICKER_SYMBOL
                """
                cursor.execute(sql, filter_params + time_params)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                if not rows:
                    logger.warning(f"idx_cons: no rows for {filter_desc}, {time_desc}")
                    return pd.DataFrame()

                result = pd.DataFrame(rows, columns=columns)

                # Normalize date columns to YYYYMMDD strings
                for col in ("INTO_DATE", "OUT_DATE"):
                    if col in result.columns:
                        result[col] = pd.to_datetime(result[col], errors="coerce").dt.strftime("%Y%m%d")

                # Pad ID_QI to 6 digits
                if "ID_QI" in result.columns:
                    result["ID_QI"] = result["ID_QI"].astype(str).str.zfill(6)

                logger.info(f"获取 idx_cons: {filter_desc}, {time_desc}, {len(result)} 条")
                return result

        except Exception as e:
            logger.error(f"查询 idx_cons 失败: {e}")
            return pd.DataFrame()

    def get_index_weight(
        self,
        index_codes: List[str],
        trading_day: str,
    ) -> pd.DataFrame:
        """
        计算指定交易日的成分股权重(自由流通市值加权)。

        权重 = (free_float_shares_{T-1} × close_{T-1}) /
               Σ(free_float_shares_{T-1} × close_{T-1}) per INDEX_CODE

        时间口径(避免指数调整日滞后):
          - members CTE 用 T 日成分股(INTO_DATE <= T AND (OUT_DATE IS NULL OR OUT_DATE > T))
          - prices/shares CTE 用 T-1 交易日(从前一交易日的 mkt_equd 取 close、
            equ_free_shares 取 <= T-1 的最新一条)

        INDEX_CODE 消歧:
          - 输入支持 "TICKER.EXCHANGE" 格式(推荐),如 "000300.XSHG"、"932000.CSI"
          - 裸 TICKER(无点号)兼容,但有跨交易所重复风险

        与 deeptrade/dataconv/composition.go 的 GenerateCompositionData 保持一致,
        确保 Python 直查和 Go 生成的 parquet 结果完全相同。

        Args:
            index_codes: 指数标识列表 ["000300.XSHG","000905.XSHG",...] 或裸 TICKER
            trading_day: 交易日 (YYYYMMDD 或 YYYY-MM-DD)

        Returns:
            pd.DataFrame with columns:
                INDEX_CODE, INDEX_ID, ID_QI, SECURITY_ID, SEC_SHORT_NAME, weight
            每个 INDEX_CODE 内的 weight 之和为 1.0
        """
        self._ensure_connection()

        td = trading_day.replace("-", "")[:8]
        td_dash = f"{td[:4]}-{td[4:6]}-{td[6:8]}"

        # 解析 "TICKER.EXCHANGE" / "TICKER" 两种格式,构造过滤条件
        index_filter_sql, filter_params = self._build_index_filter(index_codes)

        try:
            with self._conn.cursor() as cursor:
                # 先取前一交易日 T-1(用 mkt_equd,确保是有行情的真实交易日)
                cursor.execute(
                    "SELECT MAX(TRADE_DATE) FROM mkt_equd WHERE TRADE_DATE < %s",
                    (td_dash,),
                )
                prev_row = cursor.fetchone()
                if prev_row and prev_row[0]:
                    prev_val = prev_row[0]
                    if hasattr(prev_val, "strftime"):
                        prev_td_dash = prev_val.strftime("%Y-%m-%d")
                    else:
                        prev_td_dash = str(prev_val)
                else:
                    logger.warning(
                        f"index_weight: mkt_equd 无早于 {td_dash} 的交易日,回退到 T 日"
                    )
                    prev_td_dash = td_dash

                if prev_td_dash != td_dash:
                    logger.info(
                        f"index_weight: 成分股用 T={td_dash}, 价格/股本用 T-1={prev_td_dash}"
                    )

                # 与 Go buildCompositionSQL 一致:CTE 三段(members/prices/shares)
                sql = f"""
                    WITH members AS (
                        SELECT
                            idx.TICKER_SYMBOL AS INDEX_CODE,
                            idx.EXCHANGE_CD   AS INDEX_EXCHANGE,
                            ic.SECURITY_ID    AS INDEX_ID,
                            stock.TICKER_SYMBOL AS ID_QI,
                            stock.SECURITY_ID   AS SECURITY_ID,
                            stock.SEC_SHORT_NAME AS SEC_SHORT_NAME
                        FROM idx_cons ic
                        INNER JOIN (
                            SELECT DISTINCT INDEX_ID, TICKER_SYMBOL, EXCHANGE_CD
                            FROM mkt_idxd_csi
                            WHERE {index_filter_sql}
                        ) idx ON ic.SECURITY_ID = idx.INDEX_ID
                        INNER JOIN md_security stock
                            ON ic.CONS_ID = stock.SECURITY_ID
                        WHERE ic.INTO_DATE <= %s
                          AND (ic.OUT_DATE IS NULL OR ic.OUT_DATE > %s)
                          AND stock.ASSET_CLASS = 'E'
                          AND stock.LIST_STATUS_CD = 'L'
                    ),
                    prices AS (
                        SELECT SECURITY_ID, CLOSE_PRICE AS close
                        FROM mkt_equd
                        WHERE TRADE_DATE = %s
                          AND EXCHANGE_CD IN ('XSHG', 'XSHE')
                    ),
                    shares AS (
                        SELECT ms.SECURITY_ID, efs.FREE_SHARES AS ff_shares
                        FROM (
                            SELECT PARTY_ID, FREE_SHARES, CHANGE_DATE,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY PARTY_ID ORDER BY CHANGE_DATE DESC
                                   ) AS rn
                            FROM equ_free_shares
                            WHERE CHANGE_DATE <= %s
                        ) efs
                        INNER JOIN md_security ms ON efs.PARTY_ID = ms.PARTY_ID
                        WHERE efs.rn = 1
                    )
                    SELECT
                        m.INDEX_CODE,
                        m.INDEX_ID,
                        m.ID_QI,
                        m.SECURITY_ID,
                        m.SEC_SHORT_NAME,
                        (s.ff_shares * p.close) /
                            SUM(s.ff_shares * p.close) OVER (
                                PARTITION BY m.INDEX_CODE, m.INDEX_EXCHANGE
                            ) AS weight
                    FROM members m
                    JOIN prices p ON m.SECURITY_ID = p.SECURITY_ID
                    JOIN shares s ON m.SECURITY_ID = s.SECURITY_ID
                    WHERE p.close IS NOT NULL
                      AND s.ff_shares IS NOT NULL
                      AND p.close > 0
                      AND s.ff_shares > 0
                    ORDER BY m.INDEX_CODE, m.ID_QI
                """
                params = (
                    filter_params
                    + (td_dash, td_dash, prev_td_dash, prev_td_dash)
                )
                cursor.execute(sql, params)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                if not rows:
                    logger.warning(
                        f"index_weight: no rows for codes={index_codes}, trading_day={td}"
                    )
                    return pd.DataFrame()

                df = pd.DataFrame(rows, columns=columns)
                if "ID_QI" in df.columns:
                    df["ID_QI"] = df["ID_QI"].astype(str).str.zfill(6)
                logger.info(
                    f"获取 index_weight: codes={index_codes}, trading_day={td}, {len(df)} 条"
                )
                return df

        except Exception as e:
            logger.error(f"查询 index_weight 失败: {e}")
            return pd.DataFrame()

    @staticmethod
    def _build_index_filter(index_codes: List[str]):
        """
        把 ["000300.XSHG","932000.CSI"] / ["000300"] 解析为 SQL 过滤条件。
        - "TICKER.EXCHANGE" → (TICKER_SYMBOL=%s AND EXCHANGE_CD=%s)
        - "TICKER"          → TICKER_SYMBOL=%s (有重复风险,日志 warning)
        返回 (sql_fragment, params_tuple)
        """
        parts = []
        params = []
        for raw in index_codes:
            raw = (raw or "").strip()
            if not raw:
                continue
            if "." in raw:
                ticker, exch = raw.split(".", 1)
                parts.append("(TICKER_SYMBOL=%s AND EXCHANGE_CD=%s)")
                params.append(ticker.strip())
                params.append(exch.strip().upper())
            else:
                logger.warning(
                    f"index_code {raw!r} 未指定交易所,若 mkt_idxd_csi 中存在重复 "
                    f"TICKER_SYMBOL 可能误匹配,建议用 {raw}.XSHG 格式"
                )
                parts.append("TICKER_SYMBOL=%s")
                params.append(raw)
        if not parts:
            return "1=0", tuple()
        return " OR ".join(parts), tuple(params)


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
    日线基础数据缓存 (按交易日 LRU 缓存)

    优先从 OSS 读取 daily_basic_data.parquet (由 deeptrade 流水线/Pod 启动刷新生成),
    失败时 fallback 到 MySQL 直查。

    使用方式:
        cache = DailyBasicCache(market_count=5, oss_loader=oss)
        df = cache.get_by_date("20250102")
        # 兼容旧接口:
        cache.load(trade_date="20250102")
        df = cache.get_daily_basic()
    """

    # LRU 缓存上限:最近 N 个交易日
    LRU_MAX = 10

    def __init__(
        self,
        loader: MySQLLoader = None,
        market_count: int = 1,
        oss_loader=None,
    ):
        self.loader = loader or MySQLLoader()
        self.market_count = market_count
        self._oss_loader = oss_loader
        self._cache_by_date: Dict[str, pd.DataFrame] = {}
        # 兼容旧接口
        self._df: Optional[pd.DataFrame] = None
        self._date: str = ""
        self._lock = threading.RLock()

    def get_by_date(
        self, trading_day: str, force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        按交易日获取 daily_basic (优先 OSS, fallback MySQL)。

        trading_day 格式: YYYYMMDD 或 YYYY-MM-DD
        """
        td = trading_day.replace("-", "")[:8]

        with self._lock:
            if not force_refresh and td in self._cache_by_date:
                return self._cache_by_date[td]

        # 1. 优先 OSS
        if self._oss_loader is not None:
            try:
                df = self._oss_loader.read_daily_basic(td, self.market_count)
                if df is not None and not df.empty:
                    logger.info(
                        f"daily_basic OSS hit: trading_day={td}, "
                        f"market_count={self.market_count}, "
                        f"{len(df)} rows, {len(df.columns)} cols"
                    )
                    self._put(td, df)
                    return df
            except Exception as e:
                logger.warning(
                    f"OSS daily_basic read failed (fallback MySQL): {e}"
                )

        # 2. Fallback MySQL
        try:
            if not self.loader._conn:
                self.loader.connect()
            df = self.loader.get_daily_basic(td, self.market_count)
            logger.info(
                f"daily_basic MySQL fallback: trading_day={td}, "
                f"market_count={self.market_count}, {len(df)} rows"
            )
            self._put(td, df)
            return df
        except Exception as e:
            logger.error(f"MySQL daily_basic query failed: {e}")
            return pd.DataFrame()

    def _put(self, trading_day: str, df: pd.DataFrame) -> None:
        """写入 LRU 缓存 + 更新兼容字段。"""
        with self._lock:
            self._cache_by_date[trading_day] = df
            self._df = df
            self._date = trading_day
            while len(self._cache_by_date) > self.LRU_MAX:
                self._cache_by_date.pop(next(iter(self._cache_by_date)))

    def load(self, trade_date: str) -> bool:
        """
        [兼容旧接口] 加载指定日期的 daily_basic 数据,内部走 get_by_date。

        Args:
            trade_date: 交易日期

        Returns:
            是否成功
        """
        try:
            df = self.get_by_date(trade_date)
            return df is not None and not df.empty
        except Exception as e:
            logger.error(f"加载 daily_basic 缓存失败: {e}")
            return False

    def get_daily_basic(self) -> pd.DataFrame:
        """[兼容旧接口] 获取最近一次加载的 daily_basic DataFrame。"""
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
    指数成分股缓存(按交易日 LRU 缓存)

    优先从 OSS 读取 composition.parquet(由 deeptrade 流水线生成),
    失败时 fallback 到 MySQL 直查(按交易日区间过滤)。

    使用方式:
        cache = IdxConsCache(index_codes=["000300","000905"])
        df = cache.get_by_date("20220103")    # 任意历史交易日
        df_latest = cache.get_latest()         # 当前最新(IS_NEW=1)
    """

    # LRU 缓存上限:最近 N 个交易日(避免内存膨胀,每日约 7-8K 行)
    LRU_MAX = 10

    def __init__(
        self,
        loader: MySQLLoader = None,
        index_ids: List[str] = None,
        index_codes: List[str] = None,
        oss_loader=None,
    ):
        self.loader = loader or MySQLLoader()
        # 优先 index_codes,fallback 到 index_ids(向后兼容)
        self.index_codes = index_codes or []
        self.index_ids = index_ids or []
        self._oss_loader = oss_loader
        # 按交易日缓存:_cache_by_date[trading_day] = df
        self._cache_by_date: Dict[str, pd.DataFrame] = {}
        # 兼容旧接口:无 trading_day 调用时返回的最新一次查询结果
        self._df_latest: Optional[pd.DataFrame] = None
        self._lock = threading.RLock()

    def load(self) -> bool:
        """启动时不再预加载(改为懒加载)。保留接口以兼容旧调用。"""
        return True

    def _resolve_codes(self) -> tuple:
        """返回 (filter_kwargs, filter_desc) 用于 MySQLLoader.get_idx_cons。"""
        if self.index_codes:
            return ({"index_codes": self.index_codes}, f"codes={self.index_codes}")
        return ({"index_ids": self.index_ids}, f"ids={self.index_ids}")

    def get_by_date(self, trading_day: str) -> pd.DataFrame:
        """
        按交易日获取成分股(优先 OSS,fallback MySQL)。

        trading_day 格式: YYYYMMDD 或 YYYY-MM-DD
        """
        # 标准化成 YYYYMMDD
        td = trading_day.replace("-", "")[:8]

        with self._lock:
            if td in self._cache_by_date:
                return self._cache_by_date[td]

        # 1. 优先 OSS 读取 composition.parquet(由 deeptrade 生成,含 weight 列)
        if self._oss_loader is not None:
            try:
                df = self._oss_loader.read_composition(td)
                if df is not None and not df.empty:
                    logger.info(
                        f"idx_cons OSS hit: trading_day={td}, {len(df)} rows"
                    )
                    self._put(td, df)
                    return df
            except Exception as e:
                logger.warning(f"OSS composition read failed (fallback MySQL): {e}")

        # 2. Fallback MySQL 直查(按交易日区间过滤)
        try:
            if not self.loader._conn:
                self.loader.connect()
            kwargs, desc = self._resolve_codes()
            df = self.loader.get_idx_cons(trading_day=td, **kwargs)
            logger.info(f"idx_cons MySQL fallback: {desc}, trading_day={td}, {len(df)} rows")
            self._put(td, df)
            return df
        except Exception as e:
            logger.error(f"MySQL idx_cons query failed: {e}")
            return pd.DataFrame()

    def _put(self, trading_day: str, df: pd.DataFrame) -> None:
        """写入 LRU 缓存,超过上限时淘汰最老条目。"""
        with self._lock:
            self._cache_by_date[trading_day] = df
            self._df_latest = df
            while len(self._cache_by_date) > self.LRU_MAX:
                # dict 保持插入顺序,pop(next(iter)) 淘汰最旧
                self._cache_by_date.pop(next(iter(self._cache_by_date)))

    def get_latest(self) -> pd.DataFrame:
        """获取最近一次查询结果(兼容旧接口,等同于无 trading_day 的 IS_NEW=1 查询)。"""
        with self._lock:
            if self._df_latest is not None:
                return self._df_latest
        # 缓存空,直接查最新
        try:
            if not self.loader._conn:
                self.loader.connect()
            kwargs, _ = self._resolve_codes()
            df = self.loader.get_idx_cons(**kwargs)
            with self._lock:
                self._df_latest = df
            return df
        except Exception as e:
            logger.error(f"get_latest MySQL query failed: {e}")
            return pd.DataFrame()

    # ----- 兼容旧接口 -----
    def get_idx_cons(self) -> pd.DataFrame:
        """[已废弃] 兼容旧接口,等同于 get_latest()。"""
        return self.get_latest()

    def get_index_members(self, index_id: str) -> List[str]:
        """获取指定指数的成分股 ID_QI 列表(基于最近一次查询结果)。"""
        df = self.get_latest()
        if df is None or df.empty:
            return []
        mask = df["INDEX_ID"].astype(str) == str(index_id)
        return df.loc[mask, "ID_QI"].dropna().astype(str).tolist()

    def is_loaded(self) -> bool:
        """是否已加载数据(只要有任意一天的缓存就算已加载)。"""
        with self._lock:
            return bool(self._cache_by_date) or self._df_latest is not None


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
