# -*- coding: utf-8 -*-
"""Shared runtime helpers for the native live engine."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

_TRADING_START = (9, 15)
_TRADING_END = (15, 5)


def _is_trading_hours() -> bool:
    """Return whether current local time is inside the trading window."""
    now = datetime.now()
    h, m = now.hour, now.minute
    return (h, m) >= _TRADING_START and (h, m) < _TRADING_END


def _time_to_seconds(time_val) -> float:
    """Convert common market time formats to seconds since midnight."""
    if hasattr(time_val, "hour"):
        return (
            time_val.hour * 3600
            + time_val.minute * 60
            + time_val.second
            + time_val.microsecond / 1e6
        )

    s = str(time_val).strip()
    if ":" in s:
        try:
            dt = pd.Timestamp(s)
            return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6
        except Exception:
            pass

    try:
        raw = s.replace(".", "").replace(":", "")
        raw = raw.zfill(9)
        h = int(raw[0:2])
        m = int(raw[2:4])
        sec = int(raw[4:6])
        ms = int(raw[6:9]) if len(raw) >= 9 else 0
        return h * 3600 + m * 60 + sec + ms / 1000.0
    except (ValueError, IndexError):
        return 0.0


def _upload_to_oss(df: pd.DataFrame, date_str: str, end_time: str) -> None:
    """Upload live factor results to OSS as JSON.

    Caller should pass a compacted copy (round + float32) if storage reduction
    is desired; this function serializes df as-is.
    """
    try:
        import oss2

        endpoint = os.environ.get("OSS_ENDPOINT", "")
        ak_id = os.environ.get("OSS_ACCESS_KEY_ID", "")
        ak_secret = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
        bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
        prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")

        if not all([endpoint, ak_id, ak_secret]):
            logger.warning("[OSS] credentials incomplete; skip upload")
            return

        auth = oss2.Auth(ak_id, ak_secret)
        ep_clean = endpoint.replace("https://", "").replace("http://", "")
        bucket = oss2.Bucket(auth, ep_clean, bucket_name)

        year = date_str[:4]
        month = date_str[4:6]
        key = f"{prefix}/{year}/{year}{month}/{date_str}/{end_time}.json"

        records = df.to_dict(orient="records")
        payload = json.dumps(records, ensure_ascii=False, default=str).encode("utf-8")
        bucket.put_object(key, payload)

        logger.info(
            "[OSS] uploaded oss://%s/%s (%d rows, %d bytes)",
            bucket_name,
            key,
            len(records),
            len(payload),
        )
    except Exception as exc:
        logger.error("[OSS] upload failed: %s", exc)
