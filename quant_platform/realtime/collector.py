# -*- coding: utf-8 -*-
"""
通联实时数据采集器
从通联数据源采集实时数据，转换后写入MemoryStore
"""

import logging
import threading
import time
import os
import csv
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict
from pathlib import Path

import pandas as pd
import numpy as np

from ..data.memory_store import MemoryStore
from ..data.shm_store import ShmStore
from ..data.converter import TonglanceDataConverter
from ..data.mysql_loader import PriceCache
from ..core.config import get_config

try:
    from inotify_simple import INotify, flags as inotify_flags
    INOTIFY_AVAILABLE = True
except ImportError:
    INOTIFY_AVAILABLE = False

logger = logging.getLogger(__name__)


class TonglanceCollector:
    """
    通联实时数据采集器

    功能:
    1. 连接通联数据源（L1/L2行情）
    2. 接收实时数据（tick/order/deal）
    3. 转换为标准格式
    4. 写入MemoryStore
    5. 聚合K线

    使用方式:
        collector = TonglanceCollector()
        collector.start()  # 启动采集
        collector.stop()   # 停止采集
    """

    def __init__(
        self,
        codes: Optional[List[str]] = None,
        enable_sh: bool = True,
        enable_sz: bool = True
    ):
        """
        初始化采集器

        Args:
            codes: 订阅的股票代码列表，None表示全市场
            enable_sh: 是否启用上海市场
            enable_sz: 是否启用深圳市场
        """
        self.config = get_config()
        self.codes = codes
        self.enable_sh = enable_sh
        self.enable_sz = enable_sz

        # 数据存储
        self.store = MemoryStore.get_instance()
        self._use_shm = os.getenv("USE_SHM_STORE", "").lower() in ("1", "true", "yes")
        self._shm: Optional[ShmStore] = ShmStore() if self._use_shm else None

        # 数据转换器
        self.converter = TonglanceDataConverter()

        # 价格缓存（涨跌停价格）
        self.price_cache = PriceCache()

        # inotify 模式：通联 CSV 落盘路径（由 tonglian-server 写入 hostPath）
        # 目录结构：{CSV_BASE_PATH}/sh/{date}.csv  {CSV_BASE_PATH}/sz/{date}.csv
        self._csv_base = Path(os.getenv("TONGLIAN_CSV_PATH", "/data/tonglian"))
        # 每个 CSV 文件的已读 offset，用于崩溃重启后续读
        self._csv_offsets: Dict[str, int] = {}
        self._inotify: Optional["INotify"] = None
        self._watch_fds: Dict[int, Path] = {}  # wd -> directory path

        # 状态
        self.running = False
        self.connected = False
        self._thread: Optional[threading.Thread] = None

        # K线聚合状态
        self._kline_state: Dict[str, dict] = defaultdict(dict)

        # 统计
        self._stats = {
            "tick_count": 0,
            "order_count": 0,
            "deal_count": 0,
            "start_time": None,
        }

        self._parquet_base = Path(os.getenv("REALTIME_PARQUET_PATH") or self.config.oss_data_path)
        self._parquet_buffers: Dict[str, List[pd.DataFrame]] = defaultdict(list)
        self._parquet_last_minute: Dict[str, str] = {}

        logger.info("通联采集器初始化完成（inotify CSV 模式）")

    def start(self) -> bool:
        """
        启动数据采集（inotify CSV 模式）

        Returns:
            是否启动成功
        """
        if self.running:
            logger.warning("采集器已在运行中")
            return False

        try:
            # 设置交易日
            trading_day = datetime.now().strftime("%Y%m%d")
            self.store.set_trading_day(trading_day)

            # 加载涨跌停价格
            logger.info("正在加载涨跌停价格...")
            if self.price_cache.load(trading_day):
                logger.info(f"涨跌停价格加载成功: {len(self.price_cache.get_all_codes())} 只股票")
            else:
                logger.warning("涨跌停价格加载失败，将使用默认值0")

            # 初始化 inotify 监听
            if not self._init_client():
                logger.error("初始化 inotify 监听失败")
                return False

            # 注册市场目录
            if self.enable_sh:
                self._connect_sh_market()

            if self.enable_sz:
                self._connect_sz_market()

            self.running = True
            self.connected = True
            self._stats["start_time"] = datetime.now()

            # 启动后台线程
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

            logger.info("通联采集器启动成功（inotify CSV 模式）")
            return True

        except Exception as e:
            logger.error(f"启动采集器失败: {e}")
            return False

    def stop(self):
        """停止数据采集"""
        if not self.running:
            return

        logger.info("正在停止采集器...")
        self.running = False
        self.connected = False

        # 关闭 inotify
        if self._inotify is not None:
            try:
                self._inotify.close()
            except Exception as e:
                logger.error(f"关闭 inotify 失败: {e}")
            self._inotify = None

        # 保存最后的K线
        self._flush_all_klines()

        if self._thread:
            self._thread.join(timeout=5)

        logger.info("采集器已停止")

    def _init_client(self) -> bool:
        """初始化 inotify 监听器，替代 pymdl SDK 客户端"""
        if not INOTIFY_AVAILABLE:
            logger.error("inotify_simple 未安装，请运行: pip install inotify_simple")
            return False

        csv_path = self._csv_base
        if not csv_path.exists():
            logger.error(f"CSV 基础目录不存在: {csv_path}，请确认 tonglian-server 已挂载 hostPath")
            return False

        self._inotify = INotify()
        logger.info("inotify 监听器初始化成功")
        return True

    def _connect_sh_market(self):
        """注册上海市场 CSV 目录的 inotify 监听"""
        sh_dir = self._csv_base / "sh"
        sh_dir.mkdir(parents=True, exist_ok=True)
        wd = self._inotify.add_watch(str(sh_dir), inotify_flags.MODIFY | inotify_flags.CREATE)
        self._watch_fds[wd] = sh_dir
        # 启动时补读当日已存在的 CSV 文件（处理重启续读）
        self._catchup_csv(sh_dir, "SH")
        logger.info(f"上海市场 inotify 监听已注册: {sh_dir}")

    def _connect_sz_market(self):
        """注册深圳市场 CSV 目录的 inotify 监听"""
        sz_dir = self._csv_base / "sz"
        sz_dir.mkdir(parents=True, exist_ok=True)
        wd = self._inotify.add_watch(str(sz_dir), inotify_flags.MODIFY | inotify_flags.CREATE)
        self._watch_fds[wd] = sz_dir
        # 启动时补读当日已存在的 CSV 文件（处理重启续读）
        self._catchup_csv(sz_dir, "SZ")
        logger.info(f"深圳市场 inotify 监听已注册: {sz_dir}")

    def _catchup_csv(self, directory: Path, market: str):
        """启动时补读目录下所有已存在的 CSV 文件（从 offset 续读）"""
        today = datetime.now().strftime("%Y%m%d")
        for csv_file in sorted(directory.glob(f"{today}*.csv")):
            self._read_csv_incremental(csv_file, market)

    def _run_loop(self):
        """inotify 事件主循环，替代原来的 pymdl 回调线程"""
        logger.info("inotify 事件循环启动")
        while self.running:
            try:
                events = self._inotify.read(timeout=1000)  # 1秒超时
                for event in events:
                    watch_dir = self._watch_fds.get(event.wd)
                    if watch_dir is None:
                        continue
                    if not event.name.endswith(".csv"):
                        continue
                    csv_file = watch_dir / event.name
                    market = "SH" if watch_dir.name == "sh" else "SZ"
                    self._read_csv_incremental(csv_file, market)
            except Exception as e:
                if self.running:
                    logger.error(f"inotify 事件处理失败: {e}")
                    time.sleep(1)
        logger.info("inotify 事件循环退出")

    def _read_csv_incremental(self, csv_file: Path, market: str):
        """从 offset 位置增量读取 CSV 文件，解析后写入 MemoryStore"""
        key = str(csv_file)
        offset = self._csv_offsets.get(key, 0)
        try:
            with open(csv_file, "r", newline="") as f:
                f.seek(offset)
                reader = csv.DictReader(f) if offset == 0 else csv.DictReader(
                    f, fieldnames=self._get_csv_headers(csv_file)
                )
                rows_read = 0
                for row in reader:
                    self._dispatch_csv_row(row, market)
                    rows_read += 1
                new_offset = f.tell()
            if rows_read > 0:
                self._csv_offsets[key] = new_offset
                logger.debug(f"读取 {csv_file.name} +{rows_read} 行，offset={new_offset}")
        except Exception as e:
            logger.error(f"读取 CSV 文件失败 {csv_file}: {e}")

    def _get_csv_headers(self, csv_file: Path):
        """读取 CSV 首行获取列名（用于非首次读取时的 DictReader）"""
        try:
            with open(csv_file, "r", newline="") as f:
                return next(csv.reader(f))
        except Exception:
            return None

    def _dispatch_csv_row(self, row: dict, market: str):
        """根据 CSV 行中的 MsgType 字段分发到对应处理方法"""
        msg_type = row.get("MsgType", "")
        if msg_type == "L1Quote":
            self._process_l1_quote(row, market)
        elif msg_type == "L2Order":
            self._process_l2_order(row, market)
        elif msg_type == "L2Deal":
            self._process_l2_deal(row, market)
        elif msg_type == "L2Tick":
            self._process_l2_tick(row, market)

    def _on_sh_message(self, msg):
        """处理上海市场消息"""
        try:
            msg_type = getattr(msg, "MsgType", None)

            if msg_type == "L1Quote":
                self._process_l1_quote(msg, "SH")
            elif msg_type == "L2Order":
                self._process_l2_order(msg, "SH")
            elif msg_type == "L2Deal":
                self._process_l2_deal(msg, "SH")
            elif msg_type == "L2Tick":
                self._process_l2_tick(msg, "SH")

        except Exception as e:
            logger.error(f"处理上海消息失败: {e}")

    def _on_sz_message(self, msg):
        """处理深圳市场消息"""
        try:
            msg_type = getattr(msg, "MsgType", None)

            if msg_type == "L1Quote":
                self._process_l1_quote(msg, "SZ")
            elif msg_type == "L2Order":
                self._process_l2_order(msg, "SZ")
            elif msg_type == "L2Deal":
                self._process_l2_deal(msg, "SZ")
            elif msg_type == "L2Tick":
                self._process_l2_tick(msg, "SZ")

        except Exception as e:
            logger.error(f"处理深圳消息失败: {e}")

    def _process_l1_quote(self, msg, market: str):
        """处理L1行情"""
        try:
            # 提取数据
            raw_data = self._extract_quote_data(msg, market)
            if not raw_data:
                return

            code = raw_data["Code"]
            trading_day = datetime.now()

            # 转换并存储
            quote_dict = {
                "price": raw_data.get("CurrentPrice", 0),
                "volume": raw_data.get("TotalVolume", 0),
                "amount": raw_data.get("TotalMoney", 0),
                "high": raw_data.get("HighestPrice", 0),
                "low": raw_data.get("LowestPrice", 0),
                "open": raw_data.get("OpenPrice", 0),
                "bid": raw_data.get("BidPrice1", 0),
                "ask": raw_data.get("AskPrice1", 0),
            }
            self.store.update_quote(code, quote_dict)

            # 更新K线
            self._update_kline(code, raw_data)

        except Exception as e:
            logger.error(f"处理L1行情失败: {e}")

    def _process_l2_order(self, msg, market: str):
        """处理L2逐笔委托"""
        try:
            raw_data = self._extract_order_data(msg, market)
            if not raw_data:
                return

            code = raw_data["Code"]
            trading_day = datetime.now()

            # 转换数据
            if market == "SH":
                df = self.converter.convert_sh_order(raw_data, trading_day)
            else:
                df = self.converter.convert_sz_order(raw_data, trading_day)

            if not df.empty:
                self.store.update_order(code, df)
                if self._shm:
                    self._shm.update_order(code, df)
                self._append_parquet("order", df, trading_day)
                self._stats["order_count"] += len(df)

        except Exception as e:
            logger.error(f"处理L2委托失败: {e}")

    def _process_l2_deal(self, msg, market: str):
        """处理L2逐笔成交"""
        try:
            raw_data = self._extract_deal_data(msg, market)
            if not raw_data:
                return

            code = raw_data["Code"]
            trading_day = datetime.now()

            # 转换数据
            if market == "SH":
                df = self.converter.convert_sh_deal(raw_data, trading_day)
            else:
                df = self.converter.convert_sz_deal(raw_data, trading_day)

            if not df.empty:
                self.store.update_deal(code, df)
                if self._shm:
                    self._shm.update_deal(code, df)
                self._append_parquet("deal", df, trading_day)
                self._stats["deal_count"] += len(df)

        except Exception as e:
            logger.error(f"处理L2成交失败: {e}")

    def _process_l2_tick(self, msg, market: str):
        """处理L2 Tick快照"""
        try:
            raw_data = self._extract_tick_data(msg, market)
            if not raw_data:
                return

            code = raw_data["Code"]
            trading_day = datetime.now()

            # 从价格缓存获取涨跌停价
            high_limit, low_limit = self.price_cache.get_limit(code)

            # 转换数据
            if market == "SH":
                df = self.converter.convert_sh_tick(raw_data, trading_day, high_limit, low_limit)
            else:
                df = self.converter.convert_sz_tick(raw_data, trading_day)

            if not df.empty:
                self.store.update_tick(code, df)
                if self._shm:
                    self._shm.update_tick(code, df)
                self._append_parquet("tick", df, trading_day)
                self._stats["tick_count"] += len(df)

        except Exception as e:
            logger.error(f"处理L2 Tick失败: {e}")

    def _extract_quote_data(self, msg, market: str) -> Optional[dict]:
        """从消息中提取行情数据"""
        try:
            security_id = str(getattr(msg, "SecurityID", 0)).zfill(6)
            code = f"{security_id}.{market}SHG" if market == "SH" else f"{security_id}.XSHE"

            return {
                "Code": code,
                "CurrentPrice": getattr(msg, "LastPrice", 0),
                "TotalVolume": getattr(msg, "Volume", 0),
                "TotalMoney": getattr(msg, "Turnover", 0),
                "HighestPrice": getattr(msg, "HighPrice", 0),
                "LowestPrice": getattr(msg, "LowPrice", 0),
                "OpenPrice": getattr(msg, "OpenPrice", 0),
                "PreClosePrice": getattr(msg, "PreCloPrice", 0),
                "BidPrice1": getattr(msg, "BidPrice1", 0),
                "AskPrice1": getattr(msg, "OfferPrice1", 0),
            }
        except Exception as e:
            logger.error(f"提取行情数据失败: {e}")
            return None

    def _extract_order_data(self, msg, market: str) -> Optional[dict]:
        """从消息中提取委托数据"""
        try:
            security_id = str(getattr(msg, "SecurityID", 0)).zfill(6)
            code = f"{security_id}.XSHG" if market == "SH" else f"{security_id}.XSHE"

            return {
                "SecurityID": int(security_id),
                "Code": code,
                "OrderTime": getattr(msg, "OrderTime", ""),
                "LocalTime": getattr(msg, "LocalTime", ""),
                "OrderNO": getattr(msg, "OrderNO", 0),
                "OrderBSFlag": getattr(msg, "OrderBSFlag", ""),
                "OrderPrice": getattr(msg, "OrderPrice", 0),
                "Balance": getattr(msg, "Balance", 0),
                "OrderType": getattr(msg, "OrderType", ""),
                "OrderChannel": getattr(msg, "OrderChannel", 0),
                "BizIndex": getattr(msg, "BizIndex", 0),
            }
        except Exception as e:
            logger.error(f"提取委托数据失败: {e}")
            return None

    def _extract_deal_data(self, msg, market: str) -> Optional[dict]:
        """从消息中提取成交数据"""
        try:
            security_id = str(getattr(msg, "SecurityID", 0)).zfill(6)
            code = f"{security_id}.XSHG" if market == "SH" else f"{security_id}.XSHE"

            return {
                "SecurityID": int(security_id),
                "Code": code,
                "TradTime": getattr(msg, "TradTime", ""),
                "LocalTime": getattr(msg, "LocalTime", ""),
                "TradeSellNo": getattr(msg, "TradeSellNo", 0),
                "TradeBuyNo": getattr(msg, "TradeBuyNo", 0),
                "TradeBSFlag": getattr(msg, "TradeBSFlag", ""),
                "TradPrice": getattr(msg, "TradPrice", 0),
                "TradVolume": getattr(msg, "TradVolume", 0),
                "TradeMoney": getattr(msg, "TradeMoney", 0),
                "TradeChan": getattr(msg, "TradeChan", 0),
                "BizIndex": getattr(msg, "BizIndex", 0),
            }
        except Exception as e:
            logger.error(f"提取成交数据失败: {e}")
            return None

    def _extract_tick_data(self, msg, market: str) -> Optional[dict]:
        """从消息中提取Tick数据"""
        try:
            security_id = str(getattr(msg, "SecurityID", 0)).zfill(6)
            code = f"{security_id}.XSHG" if market == "SH" else f"{security_id}.XSHE"

            data = {
                "SecurityID": int(security_id),
                "Code": code,
                "UpdateTime": getattr(msg, "UpdateTime", ""),
                "LocalTime": getattr(msg, "LocalTime", ""),
                "LastPrice": getattr(msg, "LastPrice", 0),
                "Volume": getattr(msg, "Volume", 0),
                "Turnover": getattr(msg, "Turnover", 0),
                "PreCloPrice": getattr(msg, "PreCloPrice", 0),
                "HighPrice": getattr(msg, "HighPrice", 0),
                "LowPrice": getattr(msg, "LowPrice", 0),
                "SeqNo": getattr(msg, "SeqNo", 0),
            }

            # 买卖档位
            for i in range(1, 11):
                data[f"BidPrice{i}"] = getattr(msg, f"BidPrice{i}", 0)
                data[f"AskPrice{i}"] = getattr(msg, f"AskPrice{i}", 0)
                data[f"BidVolume{i}"] = getattr(msg, f"BidVolume{i}", 0)
                data[f"AskVolume{i}"] = getattr(msg, f"AskVolume{i}", 0)
                data[f"NumOrdersB{i}"] = getattr(msg, f"NumOrdersB{i}", 0)
                data[f"NumOrdersS{i}"] = getattr(msg, f"NumOrdersS{i}", 0)

            return data

        except Exception as e:
            logger.error(f"提取Tick数据失败: {e}")
            return None

    def _update_kline(self, code: str, quote_data: dict):
        """更新K线"""
        try:
            current_time = datetime.now()
            current_minute = current_time.replace(second=0, microsecond=0)

            state = self._kline_state.get(code, {})

            # 如果是新分钟，保存旧K线
            if state.get("minute") and state["minute"] != current_minute:
                # 保存1分钟K线
                if "kline" in state:
                    self._save_kline(state["kline"], "1min")

                # 检查5分钟边界
                if current_minute.minute % 5 == 0:
                    self._aggregate_5min_kline(code)

                # 检查10分钟边界
                if current_minute.minute % 10 == 0:
                    self._aggregate_10min_kline(code)

            # 更新或创建K线
            price = quote_data.get("CurrentPrice", 0)
            volume = quote_data.get("TotalVolume", 0)

            if state.get("minute") != current_minute:
                # 新K线
                state = {
                    "minute": current_minute,
                    "kline": {
                        "Code": code,
                        "Time": current_minute,
                        "Open": price,
                        "High": price,
                        "Low": price,
                        "Close": price,
                        "Volume": volume,
                    }
                }
            else:
                # 更新K线
                kline = state["kline"]
                kline["High"] = max(kline["High"], price)
                kline["Low"] = min(kline["Low"], price) if price > 0 and kline["Low"] > 0 else min(kline["Low"], price)
                kline["Close"] = price
                kline["Volume"] = volume

            self._kline_state[code] = state

        except Exception as e:
            logger.error(f"更新K线失败: {e}")

    def _save_kline(self, kline: dict, freq: str):
        df = pd.DataFrame([kline])
        current_df = self.store.get_kline(freq)
        if current_df.empty:
            self.store.update_kline(freq, df)
        else:
            self.store.update_kline(freq, pd.concat([current_df, df], ignore_index=True))
        data_type = f"kline_{freq}"
        time_value = kline.get("Time") or datetime.now()
        if isinstance(time_value, pd.Timestamp):
            time_value = time_value.to_pydatetime()
        if isinstance(time_value, datetime):
            self._append_parquet(data_type, df, time_value)

    def _append_parquet(self, data_type: str, df: pd.DataFrame, dt: datetime):
        minute_key = dt.strftime("%Y%m%d_%H%M")
        last_minute = self._parquet_last_minute.get(data_type)
        if last_minute and last_minute != minute_key:
            self._flush_parquet(data_type, last_minute)
        self._parquet_last_minute[data_type] = minute_key
        self._parquet_buffers[data_type].append(df)

    def _flush_parquet(self, data_type: str, minute_key: str):
        buffers = self._parquet_buffers.get(data_type)
        if not buffers:
            return
        df = pd.concat(buffers, ignore_index=True)
        self._parquet_buffers[data_type] = []
        date_str, minute_str = minute_key.split("_")
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]
        dir_path = self._parquet_base / f"{year}/{year}{month}/{year}{month}{day}"
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{date_str}_{minute_str}_{data_type}.parquet"
        try:
            df.to_parquet(file_path, index=False)
        except Exception as e:
            logger.error(f"保存Parquet失败 {file_path}: {e}")

    def _aggregate_5min_kline(self, code: str):
        """聚合5分钟K线"""
        # 从1分钟K线聚合
        df_1min = self.store.get_kline("1min")
        if df_1min.empty:
            return

        code_df = df_1min[df_1min["Code"] == code].copy()
        if code_df.empty:
            return

        # 简单实现：取最近5根1分钟K线
        recent = code_df.tail(5)
        if len(recent) < 5:
            return

        kline_5min = {
            "Code": code,
            "Time": recent.iloc[0]["Time"],
            "Open": recent.iloc[0]["Open"],
            "High": recent["High"].max(),
            "Low": recent["Low"].min(),
            "Close": recent.iloc[-1]["Close"],
            "Volume": recent["Volume"].sum(),
        }
        self._save_kline(kline_5min, "5min")

    def _aggregate_10min_kline(self, code: str):
        """聚合10分钟K线"""
        df_1min = self.store.get_kline("1min")
        if df_1min.empty:
            return

        code_df = df_1min[df_1min["Code"] == code].copy()
        if code_df.empty:
            return

        recent = code_df.tail(10)
        if len(recent) < 10:
            return

        kline_10min = {
            "Code": code,
            "Time": recent.iloc[0]["Time"],
            "Open": recent.iloc[0]["Open"],
            "High": recent["High"].max(),
            "Low": recent["Low"].min(),
            "Close": recent.iloc[-1]["Close"],
            "Volume": recent["Volume"].sum(),
        }
        self._save_kline(kline_10min, "10min")

    def _flush_all_klines(self):
        for code, state in self._kline_state.items():
            if "kline" in state:
                self._save_kline(state["kline"], "1min")
        for data_type, minute_key in list(self._parquet_last_minute.items()):
            self._flush_parquet(data_type, minute_key)

    def _run_loop(self):
        """后台运行循环"""
        while self.running:
            time.sleep(1)

    def get_status(self) -> dict:
        """获取采集器状态"""
        return {
            "running": self.running,
            "connected": self.connected,
            "codes_count": len(self.codes) if self.codes else "all",
            "stats": self._stats.copy(),
            "memory_stats": self.store.get_stats() if self.store else {},
        }


def create_collector(
    codes: Optional[List[str]] = None,
    enable_sh: bool = True,
    enable_sz: bool = True
) -> TonglanceCollector:
    """创建采集器"""
    return TonglanceCollector(codes=codes, enable_sh=enable_sh, enable_sz=enable_sz)
