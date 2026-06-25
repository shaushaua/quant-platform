# -*- coding: utf-8 -*-
"""Pod 启动数据刷新：生成 daily_basic + composition parquet 上传 OSS。

与 deeptrade Go 端的 dataconv/daily_basic.go + dataconv/composition.go 对齐。

触发场景:
    Pod 9:30 开盘前启动时调用 refresh_pod_startup(trading_day),
    主动拉取通联 API Barra 因子(T+1, 凌晨 2 点更新),生成 T-1 和 T-0 两日的
    daily_basic_data.parquet (96 列) 和 composition.parquet 上传 OSS。

数据流:
    1. MySQL: mkt_equd + mkt_equd_adj_af + md_security (基础行情)
    2. MySQL: dy1d_exposure_sw21 (Barra 因子,可能滞后)
    3. 通联 API: getRMExposureDaySW21 (覆盖 MySQL 滞后的 Barra 数据)
    4. MySQL: mkt_idxd_csi (指数行情)
    合并 → 96 列宽表 → parquet → OSS

    composition:
        MySQL: idx_cons + md_security + mkt_equd + equ_free_shares (权重 SQL 与 Go 一致)
"""
import logging
import os
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .datayes_client import DatayesClient, ExposureData
from .mysql_loader import MySQLLoader
from .oss_loader import OSSDataLoader

logger = logging.getLogger(__name__)


# ==================== Barra 字段集 (与 Go exposureFieldSet 严格一致) ====================

EXPOSURE_FIELDS: Tuple[str, ...] = (
    # 10 个 style 风格因子
    "BETA", "MOMENTUM", "SIZE", "EARNYILD", "RESVOL",
    "GROWTH", "BTOP", "LEVERAGE", "LIQUIDTY", "SIZENL",
    # 申万 21 行业 (37 列,与 MySQL 表一致)
    "Agriculture", "Automobiles", "Banks", "BuildMater",
    "Chemicals", "Commerce", "Computers", "Conglomerates",
    "ConstrDecor", "Defense", "ElectricalEquip", "Electronics",
    "FoodBeverages", "HealthCare", "HomeAppliances", "Leisure",
    "LightIndustry", "MachineEquip", "Media", "Mining",
    "NonbankFinan", "NonferrousMetals", "RealEstate", "Steel",
    "Telecoms", "TextileGarment", "Transportation", "Utilities",
    "BasicChemicals", "BeautyCare", "Coal", "EnvironProtect",
    "Petroleum", "PowerEquip", "RetailTrade", "SocialServices",
    "TextileApparel",
    # 国家因子
    "COUNTRY",
)
EXPOSURE_FIELD_SET = set(EXPOSURE_FIELDS)

# 默认 cutoff:此日期(含)起 Barra 数据改走通联 API
DATAYES_DEFAULT_CUTOFF = "20260303"

# 默认支持的指数代码 (与 Go 端 defaultIndexCodes 一致)
DEFAULT_INDEX_CODES: Tuple[str, ...] = (
    "000300.XSHG", "000905.XSHG", "000852.XSHG",
    "000985.XSHG", "932000.CSI",
)


# ==================== 工具函数 ====================

def _norm_date(trade_date: str) -> str:
    """YYYY-MM-DD / YYYYMMDD → YYYYMMDD。"""
    return trade_date.replace("-", "")[:8]


def _norm_date_dash(trade_date: str) -> str:
    """YYYY-MM-DD / YYYYMMDD → YYYY-MM-DD。"""
    d = _norm_date(trade_date)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def _get_exposure_columns(mysql: MySQLLoader) -> List[str]:
    """动态获取 dy1d_exposure_sw21 表的非 key 列名 (与 Go 一致)。"""
    try:
        mysql._ensure_connection()
        with mysql._conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM dy1d_exposure_sw21")
            rows = cursor.fetchall()
        skip = {"TRADE_DATE", "TICKER_SYMBOL", "SECURITY_ID"}
        return [r[0] for r in rows if r[0].upper() not in skip]
    except Exception as e:
        logger.warning(f"SHOW COLUMNS dy1d_exposure_sw21 失败,降级用硬编码字段集: {e}")
        return list(EXPOSURE_FIELDS)


def _get_index_columns(mysql: MySQLLoader) -> List[str]:
    """动态获取 mkt_idxd_csi 表的非 key 列名 (与 Go 一致)。"""
    try:
        mysql._ensure_connection()
        with mysql._conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM mkt_idxd_csi")
            rows = cursor.fetchall()
        skip = {"TRADE_DATE", "TICKER_SYMBOL", "SECURITY_ID"}
        return [r[0] for r in rows if r[0].upper() not in skip]
    except Exception as e:
        logger.warning(f"SHOW COLUMNS mkt_idxd_csi 失败,跳过指数列: {e}")
        return []


# ==================== daily_basic 生成 ====================

def _build_daily_basic_sql(
    trade_date_dash: str,
    exposure_cols: List[str],
    index_cols: List[str],
) -> str:
    """构建 96 列 daily_basic SQL (与 Go buildDailyBasicSQL 严格一致)。"""
    exp_select = (
        ",\n".join(f"    e.{c}" for c in exposure_cols)
        if exposure_cols else ""
    )
    if exp_select:
        exp_select = ",\n" + exp_select

    idx_select = ""
    idx_join = ""
    if index_cols:
        idx_select = ",\n" + ",\n".join(
            f"    idx.{c} AS idx_{c}" for c in index_cols
        )
        idx_join = f"""
LEFT JOIN (
    SELECT *
    FROM mkt_idxd_csi
    WHERE TRADE_DATE = '{trade_date_dash}'
      AND TICKER_SYMBOL = '000300'
) idx ON 1=1"""

    return f"""
SELECT
    t1.TRADE_DATE        AS TS,
    t1.TICKER_SYMBOL     AS ID_QI,
    t1.SECURITY_ID       AS SECURITY_ID,
    s.SEC_SHORT_NAME     AS SEC_SHORT_NAME,
    s.SEC_FULL_NAME      AS SEC_FULL_NAME,
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
    t1.PB                AS pb{exp_select}{idx_select}
FROM mkt_equd t1
JOIN mkt_equd_adj_af t2
    ON  t1.SECURITY_ID = t2.SECURITY_ID
    AND t1.TRADE_DATE  = t2.TRADE_DATE
LEFT JOIN md_security s
    ON t1.SECURITY_ID = s.SECURITY_ID
LEFT JOIN dy1d_exposure_sw21 e
    ON  t1.TICKER_SYMBOL = e.TICKER_SYMBOL
    AND t1.TRADE_DATE    = e.TRADE_DATE{idx_join}
WHERE t1.TRADE_DATE   = '{trade_date_dash}'
  AND t1.EXCHANGE_CD IN ('XSHG', 'XSHE')
ORDER BY t1.TICKER_SYMBOL
"""


def _query_daily_basic(
    mysql: MySQLLoader,
    trade_date: str,
) -> pd.DataFrame:
    """执行 96 列 SQL,返回 DataFrame。"""
    td_dash = _norm_date_dash(trade_date)
    exp_cols = _get_exposure_columns(mysql)
    idx_cols = _get_index_columns(mysql)
    sql = _build_daily_basic_sql(td_dash, exp_cols, idx_cols)

    mysql._ensure_connection()
    with mysql._conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

    if not rows:
        logger.warning(f"daily_basic 无数据: trade_date={trade_date}")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=columns)
    # TS 标准化为 YYYYMMDD
    if "TS" in df.columns:
        df["TS"] = pd.to_datetime(df["TS"]).dt.strftime("%Y%m%d")
    # ID_QI 补零
    if "ID_QI" in df.columns:
        df["ID_QI"] = df["ID_QI"].astype(str).str.zfill(6)
    logger.info(
        f"daily_basic MySQL 查询完成: trade_date={trade_date}, "
        f"{len(df)} rows, {len(df.columns)} cols"
    )
    return df


def _apply_exposure_override(
    df: pd.DataFrame,
    exposure_override: Optional[dict],
) -> pd.DataFrame:
    """用通联 API 数据覆盖 Barra 列的 NULL 值 (与 Go writeQueryToParquet 一致)。

    向量化实现:把 API 数据构造为 DataFrame,对每列用 isna 掩码批量赋值。

    exposure_override: {ticker(6位): ExposureData}
    """
    if not exposure_override:
        logger.info("exposure_override 为空,跳过 Barra 覆盖")
        return df

    if "ID_QI" not in df.columns:
        logger.warning("daily_basic 缺少 ID_QI 列,无法覆盖 Barra")
        return df

    # 只处理 SQL 返回的 exposure 列
    exposure_cols_in_df = [c for c in df.columns if c in EXPOSURE_FIELD_SET]
    if not exposure_cols_in_df:
        logger.warning("daily_basic 无 exposure 列,跳过覆盖")
        return df

    # 1. 把 API 数据构造为 DataFrame: 行=ticker, 列=Barra 字段
    api_rows = {}
    for ticker, exp in exposure_override.items():
        if exp and exp.fields:
            api_rows[ticker] = exp.fields
    if not api_rows:
        logger.info("exposure_override 中无有效 fields,跳过")
        return df

    api_df = pd.DataFrame(api_rows).T  # shape: (n_tickers, n_fields)
    # 对齐到 df 的行索引 (按 ID_QI reindex)
    tickers = df["ID_QI"].astype(str).values
    api_aligned = api_df.reindex(tickers)
    api_aligned.index = df.index  # 对齐行索引

    # 2. 对每个 Barra 列, 只在 MySQL 值为 NaN 且 API 有值时覆盖
    override_count = 0
    for col in exposure_cols_in_df:
        if col not in api_aligned.columns:
            continue
        # object/numeric 列统一转 numeric 判断 NaN
        cur = df[col]
        api_col = api_aligned[col]
        # MySQL NULL 判断: object 类型可能是 None, numeric 类型可能是 NaN
        if cur.dtype == object:
            cur_isna = cur.isna() | (cur == "")
            cur_for_compare = pd.to_numeric(cur, errors="coerce")
        else:
            cur_isna = cur.isna()
            cur_for_compare = cur
        api_notna = api_col.notna()
        mask = cur_isna & api_notna
        n = int(mask.sum())
        if n > 0:
            df.loc[mask, col] = api_col.loc[mask].astype(float).values
            override_count += n

    logger.info(
        f"Barra 字段覆盖完成: {override_count} 个 cell 被 API 数据填充 "
        f"(涉及 {len(exposure_cols_in_df)} 列, {len(api_rows)} 只股票)"
    )
    return df


# ==================== composition 生成 ====================

def _query_composition(
    mysql: MySQLLoader,
    trade_date: str,
    index_codes: List[str],
) -> pd.DataFrame:
    """调用 mysql.get_index_weight 拿权重 (SQL 与 Go 一致)。"""
    df = mysql.get_index_weight(index_codes=index_codes, trading_day=trade_date)
    if df.empty:
        logger.warning(f"composition 无数据: trade_date={trade_date}")
        return df

    # 补 TS 列 (与 Go composition schema 对齐)
    if "TS" not in df.columns:
        df.insert(0, "TS", _norm_date(trade_date))

    # 列顺序与 Go composition.parquet schema 对齐
    ordered = ["TS", "INDEX_CODE", "INDEX_ID", "ID_QI", "SECURITY_ID", "SEC_SHORT_NAME", "weight"]
    ordered = [c for c in ordered if c in df.columns]
    df = df[ordered]
    if "ID_QI" in df.columns:
        df["ID_QI"] = df["ID_QI"].astype(str).str.zfill(6)
    return df


# ==================== 刷新主入口 ====================

def refresh_daily_data(
    trade_date: str,
    oss: OSSDataLoader,
    mysql: MySQLLoader,
    datayes: Optional[DatayesClient],
    index_codes: Optional[List[str]] = None,
    force: bool = False,
) -> bool:
    """
    生成并上传指定交易日的 daily_basic + composition parquet。

    Args:
        trade_date: YYYYMMDD 或 YYYY-MM-DD
        oss: OSSDataLoader 实例
        mysql: MySQLLoader 实例
        datayes: DatayesClient 实例,None 则不拉 API
        index_codes: 指数代码列表,默认 DEFAULT_INDEX_CODES
        force: True 则强制重新生成 (即使 OSS 已存在)

    Returns:
        True 表示至少成功上传了一份文件
    """
    td = _norm_date(trade_date)
    if index_codes is None:
        index_codes = list(DEFAULT_INDEX_CODES)

    success = False

    # 注意:不做 OSS 幂等跳过。T-0 当天生成的 parquet Barra 字段为 NULL,
    # 次日 pod 启动时这份文件变成 T-1 必须用新 API 数据覆盖。
    # force 参数仅用于显式控制日志详细度。

    # --- 1. daily_basic 生成 ---
    try:
        df = _query_daily_basic(mysql, td)
        if df.empty:
            logger.error(f"daily_basic 查询为空,跳过上传: trade_date={td}")
        else:
            # 通联 API 覆盖 Barra (仅当日期 >= cutoff);失败不影响上传 MySQL 数据
            if datayes is not None and td >= DATAYES_DEFAULT_CUTOFF:
                try:
                    logger.info(f"调用通联 API 拉 SW21 因子: trade_date={td}")
                    exposure = datayes.get_rm_exposure_day_sw21(td)
                    logger.info(
                        f"通联 API 返回 {len(exposure)} 只股票的因子暴露"
                    )
                    df = _apply_exposure_override(df, exposure)
                except Exception as e:
                    logger.error(
                        f"通联 API 失败,barra 字段保留 MySQL 数据 (可能为空): {e}"
                    )

            # 列数校验 (Go 是 96 列);只要 df 非空就上传,不依赖 datayes 成功
            if len(df.columns) != 96:
                logger.warning(
                    f"daily_basic 列数 {len(df.columns)} != 96,与 Go 不一致"
                    f"(可能是 MySQL 表 schema 变化)"
                )

            if oss.upload_daily_basic(td, df):
                success = True
    except Exception as e:
        logger.error(f"daily_basic 生成失败: {e}", exc_info=True)

    # --- 3. composition 生成 ---
    try:
        comp_df = _query_composition(mysql, td, index_codes)
        if comp_df.empty:
            logger.error(f"composition 查询为空,跳过上传: trade_date={td}")
        else:
            if oss.upload_composition(td, comp_df):
                success = True
    except Exception as e:
        logger.error(f"composition 生成失败: {e}", exc_info=True)

    return success


def _previous_trading_day(mysql: MySQLLoader, trade_date: str) -> str:
    """返回 trade_date 的前一交易日 (YYYYMMDD),无则返回 trade_date 本身。"""
    td_dash = _norm_date_dash(trade_date)
    try:
        mysql._ensure_connection()
        with mysql._conn.cursor() as cursor:
            cursor.execute(
                "SELECT MAX(TRADE_DATE) FROM mkt_equd WHERE TRADE_DATE < %s",
                (td_dash,),
            )
            row = cursor.fetchone()
        if row and row[0]:
            val = row[0]
            if hasattr(val, "strftime"):
                return val.strftime("%Y%m%d")
            return _norm_date(str(val))
    except Exception as e:
        logger.error(f"查询前一交易日失败: {e}")
    return td


def refresh_pod_startup(
    trading_day: str,
    oss: Optional[OSSDataLoader] = None,
    mysql: Optional[MySQLLoader] = None,
    datayes: Optional[DatayesClient] = None,
    index_codes: Optional[List[str]] = None,
    refresh_days: int = 2,
) -> None:
    """
    Pod 启动入口:刷新 T-1 和 T-0 的 daily_basic + composition。

    Args:
        trading_day: 当天交易日 (T-0),YYYYMMDD 或 YYYY-MM-DD
        oss: OSSDataLoader, None 则自动创建
        mysql: MySQLLoader, None 则自动创建
        datayes: DatayesClient, None 则从环境变量读取 token 创建
        index_codes: 指数代码列表
        refresh_days: 刷几天的历史 (默认 2 = T-1 + T-0)
    """
    td = _norm_date(trading_day)
    logger.info(f"[Pod 启动数据刷新] trading_day={td}, refresh_days={refresh_days}")

    # 自动创建依赖
    close_oss = close_mysql = False
    if oss is None:
        oss = OSSDataLoader()
        close_oss = True
    if mysql is None:
        mysql = MySQLLoader()
        mysql.connect()
        close_mysql = True
    if datayes is None:
        token = os.environ.get("DATAYES_TOKEN", "").strip()
        if token:
            try:
                datayes = DatayesClient(token=token)
            except Exception as e:
                logger.warning(f"DatayesClient 创建失败: {e}")
        else:
            logger.warning("DATAYES_TOKEN 未配置,跳过通联 API 路径")

    try:
        # 顺序: T-1, T-0, T-2, T-3, ...
        # refresh_days=2 只刷 T-1 和 T-0(默认);更大的值会继续往前补刷更早日期
        # cursor 始终向过去推进一格;i==1 时改用 td(T-0),cursor 不动
        cursor = td          # 上一个已刷过的日期
        for i in range(refresh_days):
            if i == 1:
                # 插入 T-0(today);不推进 cursor,下一轮从 cursor 继续
                target, label = td, "T-0"
            else:
                prev = _previous_trading_day(mysql, cursor)
                if prev == cursor:
                    logger.warning(
                        f"[Pod 启动数据刷新] 无法解析 {cursor} 的前一交易日,"
                        f"停止回刷 at i={i}"
                    )
                    break
                target = prev
                # i=0 → T-1; i>=2 → T-i
                label = f"T-{i if i >= 2 else 1}"
                cursor = prev
            logger.info(f"[Pod 启动数据刷新] {label}={target}")
            refresh_daily_data(
                target, oss, mysql, datayes, index_codes, force=False
            )
    finally:
        if close_mysql:
            try:
                mysql.close()
            except Exception:
                pass

    logger.info("[Pod 启动数据刷新] 完成")
