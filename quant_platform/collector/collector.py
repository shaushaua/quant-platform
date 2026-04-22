# -*- coding: utf-8 -*-
"""
Collector：监听通联客户端实时写入的 CSV 文件，增量解析后写入 ShmStore

文件命名与存量数据处理（Go data-converter）保持一致：
    /root/mdl/msg_backup/YYYYMMDD/
        YYYYMMDD_mdl_4_24_0.csv   上交所 逐笔委托+成交 合并（Type 字段区分 A/D=委托, T=成交）
        YYYYMMDD_MarketData.csv   上交所 tick 快照
        YYYYMMDD_mdl_6_33_0.csv   深交所 逐笔委托
        YYYYMMDD_mdl_6_36_0.csv   深交所 逐笔成交
        YYYYMMDD_mdl_6_28_0.csv   深交所 tick 快照
        YYYYMMDD_OrderQueue.csv   委托队列（暂不处理）

使用 inotify 实时监听文件变化，替代轮询模式。
"""

import logging
import os
import threading
import time
from collections import deque
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Deque, Dict, Optional, Tuple

import oss2
import pandas as pd

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

from ..data.converter import TonglanceDataConverter
from ..data.mysql_loader import DailyBasicCache
from ..data.shm_store import ShmStore

logger = logging.getLogger(__name__)

# 通联 msg_backup 根目录
MSG_BACKUP_DIR = Path(os.environ.get("MSG_BACKUP_DIR", "/root/mdl/msg_backup"))

# 落盘目录（转换后的 parquet 文件，用于 OSS 上传）
DISK_OUTPUT_DIR = Path(os.environ.get("COLLECTOR_DISK_OUTPUT", "/data/collector_output"))

# 启动时是否跳过历史数据（True=只处理启动后的新增行）
SKIP_HISTORY = os.environ.get("COLLECTOR_SKIP_HISTORY", "true").lower() == "true"

# 是否在处理完成后删除源 CSV 文件（防止磁盘打满）
DELETE_SOURCE_AFTER_DAYS = int(os.environ.get("DELETE_SOURCE_AFTER_DAYS", "1"))

# 文件变化去重窗口（毫秒），防止同一文件多次触发
DEBOUNCE_MS = int(os.environ.get("COLLECTOR_DEBOUNCE_MS", "100"))

Market = str   # "SH" | "SZ"
DataType = str  # "order" | "deal" | "tick" | "order_deal"


def classify_file(filename: str) -> Optional[Tuple[Market, DataType]]:
    """
    根据文件名返回 (market, data_type)。
    同时支持实时文件名（mdl_X_Y_Z.csv）和批量文件名（YYYYMMDD_mdl_X_Y_Z.csv）。
    无法识别返回 None。
    """
    # 去掉 .csv 后缀，按 _ 分隔取最后几段来判断
    # 实时: mdl_4_24_0.csv  批量: 20260407_mdl_4_24_0.csv
    # 统一取 "mdl_" 开始的后缀部分
    idx = filename.find("mdl_")
    if idx < 0 and not filename.endswith("_MarketData.csv"):
        return None

    suffix = filename[idx:] if idx >= 0 else filename

    if suffix == "mdl_4_24_0.csv":
        return ("SH", "order_deal")
    if suffix == "MarketData.csv" or filename.endswith("_MarketData.csv"):
        return ("SH", "tick")
    if suffix == "mdl_6_33_0.csv":
        return ("SZ", "order")
    if suffix == "mdl_6_36_0.csv":
        return ("SZ", "deal")
    if suffix == "mdl_6_28_0.csv":
        return ("SZ", "tick")
    if suffix == "mdl_4_19_0.csv":
        return ("SH", "order")
    return None


class FilePoller:
    """
    单文件增量读取器。
    记录文件字节偏移，每次只读取新增内容。
    处理通联追加写入时文件可能不完整的情况（末尾行不含换行则跳过）。
    """

    # 单次读取最大字节数（~100MB），防止大文件增量导致 OOM
    _MAX_CHUNK_BYTES = 100 * 1024 * 1024

    def __init__(self, path: Path, skip_existing: bool = True):
        self.path = path
        self._offset = path.stat().st_size if skip_existing else 0
        self._header: Optional[list] = None
        self._last_read_time = 0.0

    def read_new_rows(self) -> Optional[pd.DataFrame]:
        """
        读取自上次以来的新增行，返回 DataFrame。
        如果没有新数据或读取失败返回 None。
        单次最多读取 _MAX_CHUNK_BYTES，剩余下次再读。
        """
        try:
            current_size = self.path.stat().st_size
        except FileNotFoundError:
            return None

        if current_size <= self._offset:
            return None

        # 限制单次读取量，避免 OOM
        bytes_to_read = min(current_size - self._offset, self._MAX_CHUNK_BYTES)

        logger.debug("[read] %s size=%d offset=%d 本次读取=%d bytes",
                    self.path.name, current_size, self._offset, bytes_to_read)

        with open(self.path, "rb") as f:
            f.seek(self._offset)
            chunk = f.read(bytes_to_read)

        if not chunk:
            return None

        # 保证只处理完整行：截断到最后一个换行符
        last_newline = chunk.rfind(b"\n")
        if last_newline == -1:
            # 没有完整行，等下次
            return None

        complete_chunk = chunk[: last_newline + 1]
        self._offset += last_newline + 1
        self._last_read_time = time.time()

        # 解码并解析 CSV
        try:
            text = complete_chunk.decode("utf-8", errors="replace")

            if self._header is None:
                # 第一次读取，需要从文件头获取列名
                with open(self.path, "r", encoding="utf-8", errors="replace") as f:
                    header_line = f.readline().strip()
                self._header = header_line.split(",")

            # 如果 offset 从 0 开始，text 本身包含 header 行，用 pd.read_csv
            import io
            if text.startswith(",".join(self._header[:3])):
                # chunk 包含 header 行
                df = pd.read_csv(io.StringIO(text))
            else:
                # chunk 只有数据行，手动指定 header
                df = pd.read_csv(io.StringIO(text), header=None, names=self._header)

            return df if not df.empty else None
        except Exception as e:
            logger.warning("解析文件 %s 失败: %s", self.path.name, e)
            return None


class FileChangeHandler(FileSystemEventHandler):
    """
    watchdog 事件处理器：监听文件创建和修改，触发回调。
    使用防抖机制避免同一文件短时间内多次触发。
    """

    def __init__(self, callback: Callable[[Path], None], debounce_ms: int = 100):
        super().__init__()
        self._callback = callback
        self._debounce_ms = debounce_ms
        self._pending: Dict[str, float] = {}  # path -> last_trigger_time
        self._lock = threading.Lock()

    def _schedule(self, path: Path):
        """防抖处理：同一文件在 debounce_ms 内只触发一次。"""
        path_str = str(path)
        now = time.time()
        with self._lock:
            # 如果已经在等待队列中，更新时间
            self._pending[path_str] = now

        # 延迟执行，让后续事件合并
        def deferred():
            time.sleep(self._debounce_ms / 1000.0)
            with self._lock:
                if self._pending.get(path_str, 0) == now:
                    # 仍然是最新事件，执行回调
                    del self._pending[path_str]
                    self._callback(path)

        threading.Thread(target=deferred, daemon=True).start()

    def on_created(self, event):
        if not event.is_directory:
            self._schedule(Path(event.src_path))

    def on_modified(self, event):
        if not event.is_directory:
            self._schedule(Path(event.src_path))


class DayDirWatcher:
    """
    使用 inotify 实时监听当天日期目录下所有符合命名规范的 CSV 文件。
    自动发现新文件，对每个文件做增量读取。
    """

    def __init__(self, day_dir: Path, skip_history: bool = True):
        self.day_dir = day_dir
        self.skip_history = skip_history
        # path -> (FilePoller, market, data_type)
        self._pollers: Dict[Path, Tuple[FilePoller, Market, DataType]] = {}
        self._results: Deque[Tuple[Path, Market, DataType, pd.DataFrame]] = deque()
        self._results_lock = threading.Lock()
        self._observer = None

        if not HAS_WATCHDOG:
            logger.warning("[watcher] watchdog 库未安装，将使用轮询模式")

    def _register_file(self, path: Path):
        """注册新文件到跟踪列表。"""
        if path in self._pollers:
            return
        classification = classify_file(path.name)
        if classification is None:
            return  # OrderQueue 等暂不处理
        market, data_type = classification
        self._pollers[path] = (FilePoller(path, self.skip_history), market, data_type)
        logger.info("[发现] %s -> %s %s", path.name, market, data_type)

    def _on_file_changed(self, path: Path):
        """文件变化回调：读取新增数据并放入结果队列。"""
        if path not in self._pollers:
            self._register_file(path)

        if path not in self._pollers:
            return  # 未识别的文件类型，跳过

        poller, market, data_type = self._pollers[path]
        df = poller.read_new_rows()
        if df is not None and not df.empty:
            with self._results_lock:
                self._results.append((path, market, data_type, df))
            logger.info("[%s][%s] %s +%d 行", market, data_type, path.name, len(df))

    def start(self):
        """启动监听（使用 inotify 或轮询）。"""
        # 目录不存在则创建（等通联写入文件后自动触发）
        if not self.day_dir.exists():
            self.day_dir.mkdir(parents=True, exist_ok=True)
            logger.info("[watcher] 创建监听目录: %s", self.day_dir)

        # 扫描现有文件
        for f in self.day_dir.glob("*.csv"):
            self._register_file(f)
        logger.info("[watcher] 已注册 %d 个文件", len(self._pollers))

        if HAS_WATCHDOG:
            self._observer = Observer()
            handler = FileChangeHandler(self._on_file_changed, DEBOUNCE_MS)
            self._observer.schedule(handler, str(self.day_dir), recursive=False)
            self._observer.start()
            logger.info("[watcher] inotify 监听已启动: %s", self.day_dir)
        else:
            logger.warning("[watcher] watchdog 未安装，请运行: pip install watchdog")

    def stop(self):
        """停止监听。"""
        if self._observer:
            self._observer.stop()
            self._observer.join()

    def get_new_data(self) -> list:
        """
        获取所有新的数据（非阻塞）。
        返回 list of (path, market, data_type, DataFrame)
        """
        with self._results_lock:
            results = list(self._results)
            self._results.clear()
        return results

    def cleanup_old_files(self, days_to_keep: int = 1):
        """
        清理超过指定天数的旧 CSV 文件，防止磁盘打满。
        只删除已完全读取的文件（在 _pollers 中跟踪过的）。
        """
        cutoff_time = time.time() - (days_to_keep * 86400)
        cleaned = []

        for path, (poller, market, data_type) in list(self._pollers.items()):
            try:
                stat = path.stat()
                # 只删除修改时间超过阈值且已完全读取的文件
                if stat.st_mtime < cutoff_time and poller._offset > 0:
                    path.unlink()
                    del self._pollers[path]
                    cleaned.append(path.name)
                    logger.info("[清理] 删除旧文件: %s", path.name)
            except Exception as e:
                logger.warning("[清理] 删除失败 %s: %s", path.name, e)

        if cleaned:
            logger.info("[清理] 已删除 %d 个旧文件", len(cleaned))


class Collector:
    """
    主 Collector：监听当天通联数据目录，解析 CSV 新增行，写入 ShmStore。
    启动时从 MySQL 加载 daily_basic 到 ShmStore。
    """

    def __init__(self):
        self._converter = TonglanceDataConverter()
        self._store = ShmStore()
        self._stop_event = threading.Event()
        self._trading_day = date.today()
        self._day_dir = MSG_BACKUP_DIR / self._trading_day.strftime("%Y%m%d")
        self._watcher = DayDirWatcher(self._day_dir, SKIP_HISTORY)
        self._daily_basic_cache: Optional[DailyBasicCache] = None
        self._uploaded_today = False

    def _load_daily_basic(self):
        """从 MySQL 加载 daily_basic 写入 ShmStore。"""
        try:
            market_count = int(os.environ.get("DAILY_BASIC_MARKET_COUNT", "1"))
            cache = DailyBasicCache(market_count=market_count)
            trade_date = self._trading_day.strftime("%Y%m%d")
            if cache.load(trade_date):
                df = cache.get_daily_basic()
                if not df.empty:
                    self._store.update_daily_basic(df)
                    self._daily_basic_cache = cache
                    logger.info("[daily_basic] 已加载到 ShmStore: %d 条", len(df))
                else:
                    logger.warning("[daily_basic] MySQL 返回空数据")
            else:
                logger.warning("[daily_basic] 加载失败，继续运行")
        except Exception as e:
            logger.warning("[daily_basic] 加载异常: %s", e)

    def _handle(self, path: Path, market: str, data_type: str, raw_df: pd.DataFrame):
        """将原始 DataFrame 转换后写入 ShmStore（实时）+ 落盘 parquet（全量）。"""
        try:
            trading_day_dt = pd.Timestamp(self._trading_day)
            raw_dict = raw_df.to_dict("list")

            if data_type == "order_deal":
                # 上交所合并委托+成交：按 Type 字段拆分
                order_df, deal_df = self._converter.convert_sh_order_deal(raw_dict, trading_day_dt)
                if not order_df.empty:
                    for code, sub in order_df.groupby("Code"):
                        self._store.update_order(code, sub)
                    self._append_to_disk("order", order_df)
                if not deal_df.empty:
                    for code, sub in deal_df.groupby("Code"):
                        self._store.update_deal(code, sub)
                    self._append_to_disk("deal", deal_df)

            elif data_type == "order":
                if market == "SZ":
                    df = self._converter.convert_sz_order(raw_dict, trading_day_dt)
                else:
                    df = self._converter.convert_sh_order(raw_dict, trading_day_dt)
                for code, sub in df.groupby("Code"):
                    self._store.update_order(code, sub)
                self._append_to_disk("order", df)

            elif data_type == "deal":
                if market == "SZ":
                    df = self._converter.convert_sz_deal(raw_dict, trading_day_dt)
                else:
                    df = self._converter.convert_sh_deal(raw_dict, trading_day_dt)
                for code, sub in df.groupby("Code"):
                    self._store.update_deal(code, sub)
                self._append_to_disk("deal", df)

            elif data_type == "tick":
                if market == "SZ":
                    df = self._converter.convert_sz_tick(raw_dict, trading_day_dt)
                else:
                    df = self._converter.convert_sh_tick(raw_dict, trading_day_dt)
                for code, sub in df.groupby("Code"):
                    self._store.update_tick(code, sub)
                self._append_to_disk("tick", df)

            logger.info("[%s][%s] %s 处理 +%d 行", market, data_type, path.name, len(raw_df))

        except Exception as e:
            logger.warning("处理 %s 失败: %s", path.name, e, exc_info=True)

    def _append_to_disk(self, data_type: str, df: pd.DataFrame):
        """将转换后的数据追加到本地 parquet 文件（用于 OSS 上传）。

        使用分片写入：每个文件最多 _CHUNK_SIZE 行，避免全量读取导致 OOM。
        """
        try:
            date_str = self._trading_day.strftime("%Y%m%d")
            out_dir = DISK_OUTPUT_DIR / date_str / data_type
            out_dir.mkdir(parents=True, exist_ok=True)

            # 分片写入：每次写一个独立的小 parquet 文件
            chunk_idx = len(list(out_dir.glob("*.parquet")))
            chunk_file = out_dir / f"{chunk_idx:06d}.parquet"
            df.to_parquet(chunk_file, index=False)
            logger.info("[落盘] %s: +%d 行 -> %s", data_type, len(df), chunk_file.name)
        except Exception as e:
            logger.warning("[落盘] 写入 %s 失败: %s", data_type, e)

    def _check_day_rollover(self):
        """交易日切换时：更新监听目录和 daily_basic。"""
        today = date.today()
        if today != self._trading_day:
            logger.info("[日切] %s -> %s", self._trading_day, today)
            self._trading_day = today
            self._day_dir = MSG_BACKUP_DIR / today.strftime("%Y%m%d")
            self._uploaded_today = False

            # 停止旧 watcher，启动新 watcher
            self._watcher.stop()
            self._watcher = DayDirWatcher(self._day_dir, skip_history=False)
            self._watcher.start()

            self._load_daily_basic()

            # 日切时清理前一天的所有数据
            self._cleanup_old_data()

    def _cleanup_old_data(self):
        """清理前一天及更早的 CSV 源文件和落盘 parquet，防止磁盘打满。"""
        import shutil
        cutoff = (self._trading_day - __import__("datetime").timedelta(days=DELETE_SOURCE_AFTER_DAYS))

        # 清理旧的 CSV 源文件目录
        try:
            for d in MSG_BACKUP_DIR.iterdir():
                if d.is_dir() and d.name.isdigit() and len(d.name) == 8:
                    dir_date = date(int(d.name[:4]), int(d.name[4:6]), int(d.name[6:8]))
                    if dir_date <= cutoff:
                        shutil.rmtree(d)
                        logger.info("[清理] 删除旧 CSV 目录: %s", d.name)
        except Exception as e:
            logger.warning("[清理] CSV 目录清理失败: %s", e)

        # 清理旧的落盘 parquet 目录
        try:
            for d in DISK_OUTPUT_DIR.iterdir():
                if d.is_dir() and d.name.isdigit() and len(d.name) == 8:
                    dir_date = date(int(d.name[:4]), int(d.name[4:6]), int(d.name[6:8]))
                    if dir_date <= cutoff:
                        shutil.rmtree(d)
                        logger.info("[清理] 删除旧落盘目录: %s", d.name)
        except Exception as e:
            logger.warning("[清理] 落盘目录清理失败: %s", e)

    def _check_upload_time(self):
        """16:00~17:00 自动上传当天全量 CSV 数据到 OSS。"""
        if self._uploaded_today:
            return
        now = datetime.now()
        # 北京时间 16:00~17:00 上传（闭市后，窗口1小时防止重启错过）
        if now.hour == 16:
            self._upload_day_to_oss(self._trading_day)
            self._uploaded_today = True

    def _get_oss_bucket(self) -> oss2.Bucket:
        """获取 OSS bucket 客户端。"""
        auth = oss2.Auth(
            os.environ["OSS_ACCESS_KEY_ID"],
            os.environ["OSS_ACCESS_KEY_SECRET"],
        )
        endpoint = os.environ.get("OSS_ENDPOINT", "")
        if endpoint and not endpoint.startswith("http"):
            endpoint = f"https://{endpoint}"
        bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")
        return oss2.Bucket(auth, endpoint, bucket_name)

    def _upload_day_to_oss(self, trading_date: date) -> None:
        """
        16:00 收盘后，读取落盘 parquet，按 Code+SeqNum 排序后上传 OSS。
        排序规则与存量数据（Go data-converter）一致，支持 DuckDB 谓词下推。
        """
        date_str = trading_date.strftime("%Y%m%d")
        year = date_str[:4]
        month = date_str[4:6]
        prefix = f"{year}/{year}{month}/{date_str}"
        disk_dir = DISK_OUTPUT_DIR / date_str

        try:
            bucket = self._get_oss_bucket()
        except Exception as e:
            logger.error("[OSS] bucket 初始化失败: %s", e)
            return

        if not disk_dir.exists():
            logger.warning("[OSS] 落盘目录不存在: %s", disk_dir)
            return

        # 合并分片、排序、上传 order/deal/tick parquet
        for data_type in ["order", "deal", "tick"]:
            chunk_dir = disk_dir / data_type
            if not chunk_dir.exists():
                logger.info("[OSS] %s 无落盘数据，跳过", data_type)
                continue

            try:
                # 合并所有分片
                chunks = sorted(chunk_dir.glob("*.parquet"))
                dfs = [pd.read_parquet(f) for f in chunks]
                if not dfs:
                    continue
                df = pd.concat(dfs, ignore_index=True)
                df = df.sort_values(["Code", "SeqNum"]).reset_index(drop=True)

                # 写入临时文件后上传
                tmp_file = disk_dir / f"{data_type}.parquet"
                df.to_parquet(tmp_file, index=False)

                key = f"{prefix}/{date_str}_{data_type}.parquet"
                bucket.put_object_from_file(key, str(tmp_file))
                size_mb = tmp_file.stat().st_size / 1024 / 1024
                logger.info("[OSS] 已上传 %s -> %s (%d 行, %.1f MB, %d 分片)",
                           data_type, key, len(df), size_mb, len(chunks))

                # 上传成功后删除临时文件和分片
                tmp_file.unlink(missing_ok=True)
                for f in chunks:
                    f.unlink()
            except Exception as e:
                logger.error("[OSS] 上传 %s 失败: %s", data_type, e)

        # 上传 daily_basic
        daily_basic = self._store.get_daily_basic()
        if not daily_basic.empty:
            try:
                import io
                buffer = io.BytesIO()
                daily_basic.to_parquet(buffer, index=False)
                buffer.seek(0)
                key = f"{prefix}/{date_str}_daily_basic_data.parquet"
                bucket.put_object(key, buffer.read())
                logger.info("[OSS] 已上传 daily_basic -> %s (%d 行)", key, len(daily_basic))
            except Exception as e:
                logger.error("[OSS] 上传 daily_basic 失败: %s", e)

        logger.info("[OSS] %s 全部上传完成", date_str)

    def _run_loop(self):
        last_cleanup = time.time()
        logger.info("[Collector] 启动，监听目录: %s", self._day_dir)
        logger.info("[Collector] 源文件清理: 保留 %d 天", DELETE_SOURCE_AFTER_DAYS)

        self._watcher.start()

        while not self._stop_event.is_set():
            self._check_day_rollover()
            self._check_upload_time()
            for path, market, data_type, df in self._watcher.get_new_data():
                self._handle(path, market, data_type, df)

            # 每小时清理一次旧文件
            now = time.time()
            if now - last_cleanup > 3600:
                self._watcher.cleanup_old_files(DELETE_SOURCE_AFTER_DAYS)
                last_cleanup = now

            # 短暂休眠避免 CPU 空转
            self._stop_event.wait(0.1)

        self._watcher.stop()
        logger.info("[Collector] 已停止")

    def start(self):
        # day_dir 由 hostPath 挂载，可能只读，确保目录存在即可，无需创建
        if not self._day_dir.exists():
            logger.warning("[Collector] 监听目录不存在: %s，等待创建", self._day_dir)
        self._load_daily_basic()
        self._run_loop()

    def stop(self):
        self._stop_event.set()


def main():
    import signal
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    c = Collector()
    signal.signal(signal.SIGTERM, lambda *_: c.stop())
    signal.signal(signal.SIGINT,  lambda *_: c.stop())
    c.start()


if __name__ == "__main__":
    main()
