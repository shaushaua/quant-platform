#!/usr/bin/env python3
"""Merge recomputed factor shards into one daily.parquet for live-factors.

Pipeline:
    1. Download 28 shard parquets from
       oss://{RESULT_BUCKET}/protected-eillen-strategy-v2/{YYYY}/{YYYYMM}/{YYYYMMDD}/
    2. Concat + schema-convert to match live daily.parquet format
       (code -> ID_QI with .XSHE/.XSHG suffix, strip _<endtime> suffix,
        TS_RECORDED_<endtime> -> datetime, drop date)
    3. Upload to
       oss://{RESULT_BUCKET}/{LIVE_PREFIX}/{YYYY}/{YYYYMM}/{YYYYMMDD}/daily-feature/daily.parquet

Env vars (all required):
    OSS_ENDPOINT, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET
    RESULT_BUCKET            (default: stock-mdl-data-result)
    LIVE_PREFIX              (default: live-factors)
    SOURCE_DATE              (YYYYMMDD, required)
    SOURCE_END_TIME          (default: 150000)
    SOURCE_PREFIX            (default: protected-eillen-strategy-v2)
"""
from __future__ import annotations

import io
import os
import sys
import time
from typing import List

import oss2
import pandas as pd


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default).strip()
    return v


def _die(msg: str, code: int = 1) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)


def _bucket() -> oss2.Bucket:
    endpoint = _env("OSS_ENDPOINT")
    ak_id = _env("OSS_ACCESS_KEY_ID")
    ak_secret = _env("OSS_ACCESS_KEY_SECRET")
    if not all([endpoint, ak_id, ak_secret]):
        _die("OSS_ENDPOINT / OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET must be set")
    bucket_name = _env("RESULT_BUCKET", "stock-mdl-data-result")
    auth = oss2.Auth(ak_id, ak_secret)
    ep = endpoint.replace("https://", "").replace("http://", "")
    return oss2.Bucket(auth, ep, bucket_name)


def _to_id_qi(code: str) -> str:
    """6-digit code -> 000001.XSHE / 600000.XSHG (matches existing daily.parquet)."""
    c = str(code).strip().zfill(6)
    suffix = "XSHG" if c.startswith("6") else "XSHE"
    # 北交所 8/4 开头暂按 SH 路由 (.BJ 需 broker 支持, 实盘 universe 通常不含)
    if c.startswith(("4", "8")):
        suffix = "XSHG"
    return f"{c}.{suffix}"


def list_shards(bucket: oss2.Bucket, source_dir: str) -> List[str]:
    keys: List[str] = []
    for obj in oss2.ObjectIterator(bucket, prefix=source_dir):
        if obj.key.endswith(".parquet"):
            keys.append(obj.key)
    keys.sort()
    return keys


def download_shard(bucket: oss2.Bucket, key: str) -> pd.DataFrame:
    buf = bucket.get_object(key).read()
    return pd.read_parquet(io.BytesIO(buf))


def convert_schema(df: pd.DataFrame, end_time: str) -> pd.DataFrame:
    """Keep schema consistent with recomputed historical factors.

    The recomputed shards carry an `_<end_time>` suffix on every F_* column
    and TS_RECORDED, plus `code` (plain 6-digit) and `date`. We preserve this
    format verbatim — only dedup by code (keep last) — so the merged file is
    byte-identical in schema to the source shards.
    """
    # Dedup by `code` (keep last — same as tree_model normalize logic)
    if "code" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)
        if len(df) != before:
            print(f"[step] dedup by code {before} -> {len(df)}")
    return df


def main() -> None:
    source_date = _env("SOURCE_DATE")
    if len(source_date) != 8 or not source_date.isdigit():
        _die("SOURCE_DATE must be YYYYMMDD")
    end_time = _env("SOURCE_END_TIME", "150000")
    source_prefix = _env("SOURCE_PREFIX", "protected-eillen-strategy-v2")
    live_prefix = _env("LIVE_PREFIX", "live-factors")

    year = source_date[:4]
    month = source_date[:6]
    source_dir = f"{source_prefix}/{year}/{month}/{source_date}/"
    target_key = (
        f"{live_prefix}/{year}/{month}/{source_date}/daily-feature/daily.parquet"
    )

    bucket = _bucket()
    print(f"[step] listing {source_dir}")
    shards = list_shards(bucket, source_dir)
    if not shards:
        _die(f"no parquet shards under {source_dir}")
    print(f"[step] found {len(shards)} shards")

    frames: List[pd.DataFrame] = []
    for i, key in enumerate(shards):
        t0 = time.time()
        df = download_shard(bucket, key)
        print(f"  [{i+1}/{len(shards)}] {key} rows={len(df)} ms={int((time.time()-t0)*1000)}")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True, copy=False)
    print(f"[step] concat rows={len(combined)} cols={len(combined.columns)}")

    converted = convert_schema(combined, end_time)

    print(f"[step] final shape={converted.shape}")
    print(f"[step] sample cols: {list(converted.columns)[:3]} ... {list(converted.columns)[-2:]}")
    print(f"[step] code head: {converted['code'].head(3).tolist() if 'code' in converted.columns else 'MISSING'}")
    print(f"[step] F_ cols: {sum(c.startswith('F_') for c in converted.columns)}")
    if "date" in converted.columns:
        print(f"[step] date head: {converted['date'].head(2).tolist()}")

    # Sanity checks
    if len(converted) < 4000:
        _die(f"row count {len(converted)} < 4000, refusing to upload")
    f_cols = [c for c in converted.columns if c.startswith("F_")]
    if len(f_cols) < 100:
        _die(f"F_ col count {len(f_cols)} < 100, schema looks wrong")

    # Upload
    buf = io.BytesIO()
    converted.to_parquet(buf, index=False)
    payload = buf.getvalue()
    print(f"[step] uploading {len(payload)} bytes -> oss://{bucket.bucket_name}/{target_key}")
    bucket.put_object(target_key, payload)
    print(f"[done] uploaded date={source_date} rows={len(converted)} cols={len(converted.columns)}")


if __name__ == "__main__":
    main()
