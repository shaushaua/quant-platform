# -*- coding: utf-8 -*-
"""
流水线延迟日志记录器

在实盘数据流水线的每一步记录耗时和延迟，写入磁盘 JSON Lines 文件。

埋点环节：
  csv_detect    → CSV 文件变化检测
  csv_parse     → CSV 行解析
  data_convert  → 通联格式转换
  shm_write     → 写入 ShmStore
  chunk_read    → StreamingEngine 读取 chunk
  factor_compute → 因子计算
  output        → CSV 写入 + OSS 上传

环境变量：
  PIPELINE_LOG_DIR     日志目录（默认 /data/quant/pipeline_logs）
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
_ENABLED = os.environ.get("PIPELINE_LOG_ENABLED", "true").lower() == "true"


class PipelineLogger:
    """流水线延迟日志记录器，JSON Lines 格式，每天一个文件。"""

    def __init__(self, source: str = ""):
        """
        Args:
            source: 标识来源进程（"collector" / "streaming"）
        """
        self._source = source
        self._lock = threading.Lock()
        self._current_day: str = ""
        self._file = None

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
        # 打开新文件
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_path = _LOG_DIR / f"pipeline_{today}.log"
        self._file = open(log_path, "a", encoding="utf-8")
        self._current_day = today

    def log(self, stage: str, **kwargs) -> None:
        """
        写一条流水线日志。

        Args:
            stage: 阶段名称（csv_detect / csv_parse / data_convert / shm_write /
                   chunk_read / factor_compute / output）
            **kwargs: 附加字段（rows, elapsed_ms, latency_ms 等）
        """
        if not _ENABLED:
            return
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
            logger.debug("[pipeline_logger] 写入失败: %s", exc)

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
