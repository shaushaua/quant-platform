# -*- coding: utf-8 -*-
"""
流水线延迟日志记录器

在实盘数据流水线的每一步记录耗时和延迟，写入磁盘 JSON Lines 文件。

埋点环节：
  sdk_connected → SDK 已连接通联客户端
  sdk_system_message → SDK 系统消息/订阅回包
  native_shm_write → native collector 写入 mmap
  native_minute_write → native collector 写入 mmap 的行情分钟计数
  chunk_read    → NativeEngine 读取 mmap
  factor_compute → 因子计算
  output        → CSV 写入 + OSS 上传

关键延迟字段：
  market_to_receive_*_ms → 通联行情交易时间到 SDK 回调接收时间
  receive_to_write_*_ms  → SDK 回调接收时间到 mmap 写入时间
  chunk_age_ms           → mmap 写入到 live-engine 读取时间
  data_latency_*_ms      → 行情交易时间到因子计算时间
  compute_ms             → 本轮因子计算总耗时

环境变量：
  PIPELINE_LOG_DIR     日志目录（默认 /data/quant/pipeline_logs）
  PIPELINE_LOG_FALLBACK_DIR 主目录不可写时的兜底目录（默认 /tmp/quant_pipeline_logs）
  PIPELINE_LOG_ENABLED 开关（默认 true）
"""

import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_LOG_DIR = Path(os.environ.get("PIPELINE_LOG_DIR", "/data/quant/pipeline_logs"))
_FALLBACK_LOG_DIR = Path(os.environ.get("PIPELINE_LOG_FALLBACK_DIR", "/tmp/quant_pipeline_logs"))
_ENABLED = os.environ.get("PIPELINE_LOG_ENABLED", "true").lower() == "true"


class PipelineLogger:
    """流水线延迟日志记录器，JSON Lines 格式，每天一个文件。"""

    def __init__(self, source: str = ""):
        """
        Args:
            source: 标识来源进程（"native-collector" / "native-engine"）
        """
        self._source = source
        self._lock = threading.Lock()
        self._current_day: str = ""
        self._file = None
        self._active_dir: Optional[Path] = None
        self._fallback_warned = False

    def _open_log_file(self, log_dir: Path, today: str):
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"pipeline_{today}.log"
        return open(log_path, "a", encoding="utf-8")

    def _ensure_file(self) -> None:
        """确保日志文件已打开，按天切换。"""
        today = datetime.now().strftime("%Y%m%d")
        if today == self._current_day and self._file is not None:
            return
        # 关闭旧文件
        if self._file is not None:
            try:
                self._file.close()
            except Exception:
                pass
        # 打开新文件：优先使用 hostPath，失败时落到容器本地 /tmp，避免静默丢链路日志。
        try:
            self._file = self._open_log_file(_LOG_DIR, today)
            self._active_dir = _LOG_DIR
        except Exception as primary_exc:
            try:
                self._file = self._open_log_file(_FALLBACK_LOG_DIR, today)
                self._active_dir = _FALLBACK_LOG_DIR
                if not self._fallback_warned:
                    logger.warning(
                        "[pipeline_logger] 主日志目录不可写，已切到兜底目录: primary=%s fallback=%s error=%s",
                        _LOG_DIR, _FALLBACK_LOG_DIR, primary_exc,
                    )
                    self._fallback_warned = True
            except Exception as fallback_exc:
                raise RuntimeError(
                    f"pipeline log open failed: primary={_LOG_DIR} error={primary_exc}; "
                    f"fallback={_FALLBACK_LOG_DIR} error={fallback_exc}"
                ) from fallback_exc
        self._current_day = today

    def log(self, stage: str, **kwargs) -> None:
        """
        写一条流水线日志。

        Args:
            stage: 阶段名称（sdk_connected / native_shm_write /
                   chunk_read / factor_compute / output）
            **kwargs: 附加字段（rows, elapsed_ms, latency_ms 等）
        """
        if not _ENABLED:
            return
        line = ""
        try:
            record = {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "stage": stage,
                "source": self._source,
            }
            record.update(kwargs)
            line = json.dumps(record, ensure_ascii=False, default=str)

            with self._lock:
                self._ensure_file()
                self._file.write(line + "\n")
                self._file.flush()
        except Exception as exc:
            logger.warning("[pipeline_logger] 写入失败，改打到容器日志: %s", exc)
            try:
                logger.warning("[pipeline_record] %s", line)
            except Exception:
                pass

    @contextmanager
    def timer(self, stage: str, **extra):
        """
        上下文管理器，自动计算耗时。

        用法:
            with pipe_logger.timer("csv_parse", file="mdl_6_36_0.csv") as t:
                df = parse_csv(...)
                t["rows"] = len(df)
            # 自动写入日志: stage="csv_parse", elapsed_ms=..., rows=...
        """
        t0 = time.time()
        ctx: Dict = dict(extra)
        try:
            yield ctx
        finally:
            elapsed_ms = round((time.time() - t0) * 1000, 1)
            ctx["elapsed_ms"] = elapsed_ms
            self.log(stage, **ctx)

    def close(self) -> None:
        """关闭日志文件。"""
        with self._lock:
            if self._file is not None:
                try:
                    self._file.close()
                except Exception:
                    pass
                self._file = None
                self._current_day = ""
                self._active_dir = None


# 全局单例（按 source 区分）
_collector_logger: Optional[PipelineLogger] = None
_streaming_logger: Optional[PipelineLogger] = None
_logger_lock = threading.Lock()


def get_collector_logger() -> PipelineLogger:
    """获取 Collector 专用的 PipelineLogger 实例。"""
    global _collector_logger
    if _collector_logger is None:
        with _logger_lock:
            if _collector_logger is None:
                _collector_logger = PipelineLogger(source="collector")
    return _collector_logger


def get_streaming_logger() -> PipelineLogger:
    """获取 StreamingEngine 专用的 PipelineLogger 实例。"""
    global _streaming_logger
    if _streaming_logger is None:
        with _logger_lock:
            if _streaming_logger is None:
                _streaming_logger = PipelineLogger(source="streaming")
    return _streaming_logger
