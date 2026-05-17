# -*- coding: utf-8 -*-
"""
数据转换器
参考 convert_data.py 的数据格式标准
将通联原始数据转换为标准格式
"""

import logging
import re
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd
import numpy as np

from ..core.constants import (
    ORDER_COLUMNS, ORDER_DTYPE,
    DEAL_COLUMNS, DEAL_DTYPE,
    TICK_COLUMNS, TICK_DTYPE,
    SZ_ORDER_TYPES, SH_ORDER_TYPES,
    SZ_SIDES, SH_SIDES
)

logger = logging.getLogger(__name__)


class TonglanceDataConverter:
    """
    通联数据转换器
    将通联原始数据转换为标准格式
    """

    def __init__(self):
        self.price_precision = 100  # 价格精度（除以100）
        self._sh_pattern = re.compile(r'^[6]\d{5}\.XSHG$')
        self._sz_pattern = re.compile(r'^[03]\d{5}\.XSHE$')

    # 股票代码前缀（只处理这些，过滤掉基金/债券/指数等非股票品种）
    _SH_STOCK_PREFIX = ("6", "9")     # 600xxx, 601xxx, 603xxx, 605xxx, 9xxxxx
    _SZ_STOCK_PREFIX = ("0", "3")     # 000xxx, 001xxx, 002xxx, 003xxx, 300xxx

    def _map_codes(self, df: pd.DataFrame, market: str) -> pd.DataFrame:
        """将 SecurityID（股票代码）格式化为标准格式，过滤非股票品种。"""
        # 如果原始列名是 SecurityID，先改为 Code
        if "SecurityID" in df.columns and "Code" not in df.columns:
            df = df.rename(columns={"SecurityID": "Code"})
        suffix = ".XSHG" if market == "SH" else ".XSHE"
        df["Code"] = df["Code"].astype(str).str.zfill(6) + suffix
        # 只保留股票代码，过滤基金(501xxx/159xxx)、债券(11xxxx)等
        prefixes = self._SH_STOCK_PREFIX if market == "SH" else self._SZ_STOCK_PREFIX
        code_prefix = df["Code"].str[0]
        mask = code_prefix.isin(prefixes)
        n_before = len(df)
        df = df[mask].copy()
        n_filtered = n_before - len(df)
        if n_filtered > 0:
            logger.debug("[_map_codes] %s: 过滤 %d/%d 行非股票品种",
                        market, n_filtered, n_before)
        return df

    # ==================== 上交所合并委托+成交 (mdl_4_24_0) ====================

    def convert_sh_order_deal(
        self, raw_data, trading_day: datetime
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        转换上交所合并委托+成交数据（mdl_4_24_0 格式）。
        通过 Type 字段区分：
            "A" = 普通委托 (OrderType=2)
            "D" = 撤单委托 (OrderType=5)
            "T" = 成交

        Args:
            raw_data: dict 或 DataFrame
            trading_day: 交易日

        Returns:
            (order_df, deal_df) 元组
        """
        try:
            df = raw_data.copy() if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return (
                    pd.DataFrame(columns=ORDER_COLUMNS),
                    pd.DataFrame(columns=DEAL_COLUMNS),
                )

            # 格式化股票代码
            df = self._map_codes(df, "SH")
            df["TradingDay"] = trading_day.date()
            df["Time"] = self._parse_time(df["TickTime"], trading_day)
            df["UpdateTime"] = self._parse_time(df["LocalTime"], trading_day)
            df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0)
            df["Volume"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
            df["Channel"] = pd.to_numeric(df["Channel"], errors="coerce").fillna(0).astype("int64")
            df["SeqNum"] = pd.to_numeric(df["BizIndex"], errors="coerce").fillna(0).astype("int64")

            # Side 映射
            side_map = {"B": 0, "S": 1, "N": 10}
            df["Side"] = df["TickBSFlag"].map(side_map).fillna(10).astype("int16")

            # 按 Type 拆分
            type_col = df["Type"].astype(str).str.strip()
            order_mask = type_col.isin(["A", "D"])
            deal_mask = type_col == "T"

            # ---- 委托部分 ----
            orders = df[order_mask].copy()
            if not orders.empty:
                # OrderID = BuyOrderNO + SellOrderNO（只有一个有值）
                buy_no = pd.to_numeric(orders["BuyOrderNO"], errors="coerce").fillna(0).astype("int64")
                sell_no = pd.to_numeric(orders["SellOrderNO"], errors="coerce").fillna(0).astype("int64")
                orders["OrderID"] = buy_no + sell_no

                # OrderType: A=2(普通), D=5(撤单)
                orders["OrderType"] = type_col[order_mask].map({"A": 2, "D": 5}).astype("int16")

                orders = orders[ORDER_COLUMNS].sort_values("SeqNum").reset_index(drop=True)
                orders = self._convert_dtypes(orders, "order")
            else:
                orders = pd.DataFrame(columns=ORDER_COLUMNS)

            # ---- 成交部分 ----
            deals = df[deal_mask].copy()
            if not deals.empty:
                deals["SaleOrderID"] = pd.to_numeric(deals["SellOrderNO"], errors="coerce").fillna(0).astype("int64")
                deals["BuyOrderID"] = pd.to_numeric(deals["BuyOrderNO"], errors="coerce").fillna(0).astype("int64")
                deals["Money"] = deals["Price"] * deals["Volume"]
                deals = deals[DEAL_COLUMNS].sort_values("SeqNum").reset_index(drop=True)
                deals = self._convert_dtypes(deals, "deal")
            else:
                deals = pd.DataFrame(columns=DEAL_COLUMNS)

            return orders, deals

        except Exception as e:
            logger.error(f"转换上交所合并委托+成交数据失败: {e}", exc_info=True)
            return (
                pd.DataFrame(columns=ORDER_COLUMNS),
                pd.DataFrame(columns=DEAL_COLUMNS),
            )

    # ==================== 委托数据转换 ====================

    def convert_sh_order(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """
        转换上交所逐笔委托

        Args:
            raw_data: dict 或 DataFrame
            trading_day: 交易日
        """
        try:
            df = raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=ORDER_COLUMNS)

            # 字段映射
            rename_map = {
                "SecurityID": "Code",
                "OrderTime": "Time",
                "LocalTime": "UpdateTime",
                "OrderNO": "OrderID",
                "OrderBSFlag": "Side",
                "OrderPrice": "Price",
                "Balance": "Volume",
                "OrderChannel": "Channel",
                "BizIndex": "SeqNum",
            }
            df = df.rename(columns=rename_map)

            # 设置交易日期
            df["TradingDay"] = trading_day.date()

            # 格式化股票代码
            df = self._map_codes(df, "SH")

            # 解析时间
            df["Time"] = self._parse_time(df["Time"], trading_day)
            df["UpdateTime"] = self._parse_time(df["UpdateTime"], trading_day)

            # 转换买卖方向
            df["Side"] = df["Side"].map({"B": 0, "S": 1}).astype("int16")

            # 转换委托类型
            df["OrderType"] = df["OrderType"].map({"A": 2, "D": 5}).astype("int16")

            # 类型转换
            df = self._convert_dtypes(df, "order")

            # 排序
            df = df[ORDER_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"转换上交所委托数据失败: {e}", exc_info=True)
            return pd.DataFrame(columns=ORDER_COLUMNS)

    def convert_sz_order(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """转换深交所逐笔委托"""
        try:
            df = raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=ORDER_COLUMNS)

            # 字段映射
            rename_map = {
                "SecurityID": "Code",
                "TransactTime": "Time",
                "LocalTime": "UpdateTime",
                "ApplSeqNum": "OrderID",
                "OrderQty": "Volume",
                "OrdType": "OrderType",
                "ChannelNo": "Channel",
            }
            df = df.rename(columns=rename_map)

            # 设置交易日期
            df["TradingDay"] = trading_day.date()

            # 格式化股票代码
            df = self._map_codes(df, "SZ")

            # 解析时间
            df["Time"] = self._parse_time(df["Time"], trading_day)
            df["UpdateTime"] = self._parse_time(df["UpdateTime"], trading_day)

            # 转换买卖方向 (深市用数字)
            df["Side"] = df["Side"].map({49: 0, 50: 1})
            df["Side"] = df["Side"].fillna(10).astype("int16")

            # 转换委托类型
            df["OrderType"] = df["OrderType"].map({49: 1, 50: 2, 85: 3})
            df["OrderType"] = df["OrderType"].fillna(0).astype("int16")

            # 数值列填充 NaN
            for col in ["Price", "Volume", "Channel", "OrderID"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # 序列号
            df["SeqNum"] = df["OrderID"]

            # 类型转换
            df = self._convert_dtypes(df, "order")

            # 排序
            df = df[ORDER_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"转换深交所委托数据失败: {e}", exc_info=True)
            return pd.DataFrame(columns=ORDER_COLUMNS)

    # ==================== 成交数据转换 ====================

    def convert_sh_deal(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """转换上交所逐笔成交"""
        try:
            df = raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=DEAL_COLUMNS)

            # 字段映射
            rename_map = {
                "SecurityID": "Code",
                "TradTime": "Time",
                "LocalTime": "UpdateTime",
                "TradeSellNo": "SaleOrderID",
                "TradeBuyNo": "BuyOrderID",
                "TradeBSFlag": "Side",
                "TradPrice": "Price",
                "TradVolume": "Volume",
                "TradeMoney": "Money",
                "TradeChan": "Channel",
                "BizIndex": "SeqNum",
            }
            df = df.rename(columns=rename_map)

            # 设置交易日期
            df["TradingDay"] = trading_day.date()

            # 格式化股票代码
            df = self._map_codes(df, "SH")

            # 解析时间
            df["Time"] = self._parse_time(df["Time"], trading_day)
            df["UpdateTime"] = self._parse_time(df["UpdateTime"], trading_day)

            # 转换买卖方向
            df["Side"] = df["Side"].map({"N": 10, "B": 0, "S": 1}).astype("int16")

            # 类型转换
            df = self._convert_dtypes(df, "deal")

            # 排序
            df = df[DEAL_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"转换上交所成交数据失败: {e}", exc_info=True)
            return pd.DataFrame(columns=DEAL_COLUMNS)

    def convert_sz_deal(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """转换深交所逐笔成交"""
        try:
            df = raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=DEAL_COLUMNS)

            # 字段映射
            rename_map = {
                "SecurityID": "Code",
                "TransactTime": "Time",
                "LocalTime": "UpdateTime",
                "OfferApplSeqNum": "SaleOrderID",
                "BidApplSeqNum": "BuyOrderID",
                "LastPx": "Price",
                "LastQty": "Volume",
                "ChannelNo": "Channel",
                "ApplSeqNum": "SeqNum",
            }
            df = df.rename(columns=rename_map)

            # 设置交易日期
            df["TradingDay"] = trading_day.date()

            # 格式化股票代码
            df = self._map_codes(df, "SZ")

            # 解析时间
            df["Time"] = self._parse_time(df["Time"], trading_day)
            df["UpdateTime"] = self._parse_time(df["UpdateTime"], trading_day)

            # 推断买卖方向
            df["Side"] = np.int16(1)
            df.loc[df["BuyOrderID"] > df["SaleOrderID"], "Side"] = 0
            df.loc[df["ExecType"] == 52, "Side"] = 4

            # 计算成交金额
            df["Money"] = df["Price"] * df["Volume"]

            # 类型转换
            df = self._convert_dtypes(df, "deal")

            # 排序
            df = df[DEAL_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"转换深交所成交数据失败: {e}", exc_info=True)
            return pd.DataFrame(columns=DEAL_COLUMNS)

    # ==================== Tick数据转换 ====================

    def convert_sh_tick(
        self,
        raw_data,
        trading_day: datetime,
        high_limit: float = 0.0,
        low_limit: float = 0.0
    ) -> pd.DataFrame:
        """转换上交所Tick快照"""
        try:
            df = raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=TICK_COLUMNS)

            # 字段映射
            rename_map = {
                "SecurityID": "Code",
                "UpdateTime": "Time",
                "LocalTime": "UpdateTime",
                "LastPrice": "CurrentPrice",
                "TradVolume": "TotalVolume",
                "Turnover": "TotalMoney",
                "PreCloPrice": "PreClosePrice",
                "HighPrice": "HighestPrice",
                "LowPrice": "LowestPrice",
                "TradNumber": "TradeNum",
                "TotalBidVol": "TotalBidVolume",
                "TotalAskVol": "TotalAskVolume",
                "WAvgBidPri": "AvgBidPrice",
                "WAvgAskPri": "AvgAskPrice",
                "SeqNo": "SeqNum",
            }

            # 买卖档位映射
            for i in range(1, 11):
                rename_map[f"NumOrdersB{i}"] = f"BidNum{i}"
                rename_map[f"NumOrdersS{i}"] = f"AskNum{i}"

            df = df.rename(columns=rename_map)

            # 设置交易日期
            df["TradingDay"] = trading_day.date()

            # 格式化股票代码
            df = self._map_codes(df, "SH")

            # 解析时间
            df["Time"] = self._parse_time(df["Time"], trading_day)
            # SH tick 一般有 LocalTime，但防御性处理
            if "UpdateTime" in df.columns:
                df["UpdateTime"] = self._parse_time(df["UpdateTime"], trading_day)
            else:
                df["UpdateTime"] = df["Time"]

            # 设置涨跌停价
            df["HighLimitPrice"] = high_limit
            df["LowLimitPrice"] = low_limit

            # 设置通道和序列号
            df["Channel"] = np.int64(0)
            if "SeqNum" not in df.columns:
                df["SeqNum"] = np.arange(len(df), dtype=np.int64)

            # 填充缺失的档位数据
            for i in range(1, 11):
                for prefix in ["Bid", "Ask"]:
                    for suffix in ["Price", "Volume", "Num"]:
                        col = f"{prefix}{suffix}{i}"
                        if col not in df.columns:
                            df[col] = 0.0

            # IOPV（ETF净值）
            if "IOPV" not in df.columns:
                df["IOPV"] = 0.0

            # 开盘价处理
            if "OpenPrice" not in df.columns:
                df["OpenPrice"] = 0.0

            # 补齐可能缺失的汇总列
            for col, default in [
                ("TotalAskVolume", 0.0),
                ("TotalBidVolume", 0.0),
                ("TradeNum", 0.0),
                ("AvgBidPrice", 0.0),
                ("AvgAskPrice", 0.0),
            ]:
                if col not in df.columns:
                    logger.debug("[SH tick] 列 '%s' 缺失，填充默认值 %s", col, default)
                    df[col] = default

            # 诊断日志：打印原始列名（所有列）
            logger.info("[SH tick] 原始列名(%d个): %s", len(raw_data.columns) if isinstance(raw_data, pd.DataFrame) else 0, list(raw_data.columns) if isinstance(raw_data, pd.DataFrame) else "?")

            # 类型转换
            df = self._convert_dtypes(df, "tick")

            # 排序
            df = df[TICK_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"转换上交所Tick数据失败: {e}", exc_info=True)
            return pd.DataFrame(columns=TICK_COLUMNS)

    def convert_sz_tick(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """转换深交所Tick快照"""
        try:
            df = raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
            if df.empty:
                return pd.DataFrame(columns=TICK_COLUMNS)

            # 字段映射
            rename_map = {
                "SecurityID": "Code",
                "UpdateTime": "Time",
                "LocalTime": "UpdateTime",
                "LastPrice": "CurrentPrice",
                "Volume": "TotalVolume",
                "Turnover": "TotalMoney",
                "PreCloPrice": "PreClosePrice",
                "HighPrice": "HighestPrice",
                "LowPrice": "LowestPrice",
                "TurnNum": "TradeNum",
                "TotalBidQty": "TotalBidVolume",
                "TotalOfferQty": "TotalAskVolume",
                "WeightedAvgBidPx": "AvgBidPrice",
                "WeightedAvgOfferPx": "AvgAskPrice",
                "SeqNo": "SeqNum",
            }

            # 买卖档位映射
            for i in range(1, 11):
                rename_map[f"NumOrdersB{i}"] = f"BidNum{i}"
                rename_map[f"NumOrdersS{i}"] = f"AskNum{i}"

            df = df.rename(columns=rename_map)

            # 设置交易日期
            df["TradingDay"] = trading_day.date()

            # 格式化股票代码
            df = self._map_codes(df, "SZ")

            # 解析时间
            df["Time"] = self._parse_time(df["Time"], trading_day)
            # SZ tick 可能没有 LocalTime 列，此时用 Time 作为 UpdateTime
            if "UpdateTime" in df.columns:
                df["UpdateTime"] = self._parse_time(df["UpdateTime"], trading_day)
            else:
                logger.debug("[SZ tick] 无 LocalTime 列，UpdateTime 复用 Time")
                df["UpdateTime"] = df["Time"]

            # 设置通道和序列号
            df["Channel"] = np.int64(0)
            if "SeqNum" not in df.columns:
                df["SeqNum"] = np.arange(len(df), dtype=np.int64)

            # 填充缺失的档位数据
            for i in range(1, 11):
                for prefix in ["Bid", "Ask"]:
                    for suffix in ["Price", "Volume", "Num"]:
                        col = f"{prefix}{suffix}{i}"
                        if col not in df.columns:
                            df[col] = 0.0

            # IOPV
            if "IOPV" not in df.columns:
                df["IOPV"] = 0.0

            # 开盘价
            if "OpenPrice" not in df.columns:
                df["OpenPrice"] = 0.0

            # 涨跌停价
            if "HighLimitPrice" not in df.columns:
                df["HighLimitPrice"] = 0.0
            if "LowLimitPrice" not in df.columns:
                df["LowLimitPrice"] = 0.0

            # 补齐可能缺失的汇总列
            for col, default in [
                ("TotalAskVolume", 0.0),
                ("TotalBidVolume", 0.0),
                ("TradeNum", 0.0),
                ("AvgBidPrice", 0.0),
                ("AvgAskPrice", 0.0),
            ]:
                if col not in df.columns:
                    logger.debug("[SZ tick] 列 '%s' 缺失，填充默认值 %s", col, default)
                    df[col] = default

            # 诊断日志：打印原始列名（所有列）
            logger.info("[SZ tick] 原始列名(%d个): %s", len(raw_data.columns) if isinstance(raw_data, pd.DataFrame) else 0, list(raw_data.columns) if isinstance(raw_data, pd.DataFrame) else "?")

            # 类型转换
            df = self._convert_dtypes(df, "tick")

            # 排序
            df = df[TICK_COLUMNS].sort_values("SeqNum").reset_index(drop=True)

            return df

        except Exception as e:
            logger.error(f"转换深交所Tick数据失败: {e}", exc_info=True)
            return pd.DataFrame(columns=TICK_COLUMNS)

    # ==================== 通用转换 ====================

    def convert_order(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """自动判断市场并转换委托数据"""
        code = self._get_first_code(raw_data)
        if self._is_sh_stock(str(code)):
            return self.convert_sh_order(raw_data, trading_day)
        else:
            return self.convert_sz_order(raw_data, trading_day)

    def convert_deal(self, raw_data, trading_day: datetime) -> pd.DataFrame:
        """自动判断市场并转换成交数据"""
        code = self._get_first_code(raw_data)
        if self._is_sh_stock(str(code)):
            return self.convert_sh_deal(raw_data, trading_day)
        else:
            return self.convert_sz_deal(raw_data, trading_day)

    def convert_tick(
        self,
        raw_data,
        trading_day: datetime,
        high_limit: float = 0.0,
        low_limit: float = 0.0
    ) -> pd.DataFrame:
        """自动判断市场并转换Tick数据"""
        code = self._get_first_code(raw_data)
        if self._is_sh_stock(str(code)):
            return self.convert_sh_tick(raw_data, trading_day, high_limit, low_limit)
        else:
            return self.convert_sz_tick(raw_data, trading_day)

    # ==================== 辅助方法 ====================

    @staticmethod
    def _normalize_time_str(time_str: str) -> str:
        """归一化时间字符串，处理秒数 >= 60 的情况。
        例如 "00:05:60.780" → "00:06:00.780"
        """
        try:
            parts = time_str.split(":")
            if len(parts) < 3:
                return time_str
            h = int(parts[0])
            m = int(parts[1])
            sec_parts = parts[2].split(".")
            s = int(sec_parts[0])
            ms = int(sec_parts[1].ljust(3, "0")[:3]) if len(sec_parts) > 1 else 0
            # 归一化：毫秒进位到秒，秒进位到分，分进位到时
            extra_s, ms = divmod(ms, 1000)
            s += extra_s
            extra_m, s = divmod(s, 60)
            m += extra_m
            h += m // 60
            m = m % 60
            return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"
        except Exception:
            return time_str

    def _parse_time(self, time_data, trading_day: datetime) -> pd.Series:
        """解析时间字段，支持 HH:MM:SS.mmm 字符串和 HHMMSSmmm 整数格式。
        通联数据秒数可能 >= 60（如 00:05:60.780），会先归一化再解析。
        """
        if time_data.dtype == object:
            time_strs = time_data.astype(str)
            # 归一化：秒数 >= 60 时进位到分钟
            time_strs = time_strs.apply(self._normalize_time_str)
            combined = trading_day.strftime("%Y-%m-%d ") + time_strs
            try:
                return pd.to_datetime(combined, format="mixed", errors="coerce")
            except Exception:
                try:
                    return pd.to_datetime(combined, errors="coerce")
                except Exception:
                    return pd.NaT
        else:
            # 数值格式：通联 SZ 数据可能是 HHMMSSmmm 整数（如 93000000 = 09:30:00.000）
            nums = pd.to_numeric(time_data, errors="coerce")
            if nums.empty:
                return pd.NaT
            max_val = nums.max()
            if pd.notna(max_val) and max_val > 1e12:
                # 大数值：当作毫秒时间戳
                return pd.to_datetime(nums, unit="ms")
            else:
                # 小数值：HHMMSSmmm 格式
                # 93000000 → "09:30:00.000"
                # 148182540 → "14:81:82.540" → 归一化 → "15:22:22.540"
                strs = nums.astype(int).astype(str).str.zfill(9)
                time_strs = strs.str[:2] + ":" + strs.str[2:4] + ":" + strs.str[4:6] + "." + strs.str[6:]
                time_strs = time_strs.apply(self._normalize_time_str)
                return pd.to_datetime(
                    trading_day.strftime("%Y-%m-%d ") + time_strs,
                    format="mixed",
                    errors="coerce",
                )

    def _convert_dtypes(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """转换数据类型"""
        if data_type == "order":
            dtype_map = ORDER_DTYPE
        elif data_type == "deal":
            dtype_map = DEAL_DTYPE
        else:
            dtype_map = TICK_DTYPE

        for col, dtype in dtype_map.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except:
                    pass

        return df

    def _is_sh_stock(self, code: str) -> bool:
        """判断是否为上海股票"""
        code = code.replace(".XSHG", "").replace(".XSHE", "")
        return code.startswith(("6", "9", "68"))

    def _get_first_code(self, raw_data) -> str:
        """从 dict 或 DataFrame 中提取第一个股票代码。"""
        if isinstance(raw_data, pd.DataFrame):
            for col in ("Code", "SecurityID"):
                if col in raw_data.columns and not raw_data.empty:
                    return str(raw_data[col].iloc[0])
            return ""
        return raw_data.get("Code", raw_data.get("SecurityID", ""))

    def validate_data(self, df: pd.DataFrame, data_type: str) -> Tuple[bool, str]:
        """
        验证数据有效性

        Args:
            df: 数据DataFrame
            data_type: 数据类型 (order/deal/tick)

        Returns:
            (是否有效, 错误信息)
        """
        if df.empty:
            return False, "数据为空"

        # 检查必需列
        if data_type == "order":
            required = ORDER_COLUMNS
        elif data_type == "deal":
            required = DEAL_COLUMNS
        else:
            required = TICK_COLUMNS

        missing = set(required) - set(df.columns)
        if missing:
            return False, f"缺少列: {missing}"

        # 检查重复
        if df[["Channel", "SeqNum"]].duplicated().any():
            return False, "存在重复的Channel+SeqNum"

        # 检查代码格式
        code = df["Code"].values[0]
        if not (self._sh_pattern.match(code) or self._sz_pattern.match(code)):
            return False, f"股票代码格式错误: {code}"

        return True, ""
