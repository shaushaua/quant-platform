#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Repair a daily raw archive parquet on OSS by de-duplicating Code+SeqNum."""

import argparse
import logging
import os
from pathlib import Path

import duckdb
import oss2


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("repair-archive")


def _bucket():
    endpoint = os.environ.get("OSS_ENDPOINT", "")
    if endpoint and not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"
    auth = oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"])
    return oss2.Bucket(auth, endpoint, os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data"))


def _key(date: str, kind: str) -> str:
    return f"{date[:4]}/{date[:4]}{date[4:6]}/{date}/{date}_{kind}.parquet"


def repair_kind(date: str, kind: str, work_dir: Path) -> None:
    bucket = _bucket()
    key = _key(date, kind)
    work_dir.mkdir(parents=True, exist_ok=True)
    src = work_dir / f"{date}_{kind}.src.parquet"
    dst = work_dir / f"{date}_{kind}.dedup.parquet"

    log.info("[%s] downloading oss://%s/%s -> %s", kind, bucket.bucket_name, key, src)
    bucket.get_object_to_file(key, str(src))
    src_mb = src.stat().st_size / 1024 / 1024

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET memory_limit='{os.environ.get('ARCHIVE_DUCKDB_MEMORY', '32GB')}'")
        before_rows, before_distinct = con.execute(
            f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT (Code, SeqNum)) AS distinct_keys
            FROM read_parquet('{src}')
            """
        ).fetchone()
        before_rows = int(before_rows or 0)
        before_distinct = int(before_distinct or 0)
        dup_rows = before_rows - before_distinct
        log.info("[%s] before rows=%d distinct=%d dup_extra=%d size=%.1fMB",
                 kind, before_rows, before_distinct, dup_rows, src_mb)
        if dup_rows <= 0:
            log.info("[%s] no duplicates, keep existing object", kind)
            return

        dst.unlink(missing_ok=True)
        con.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (_rn)
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY Code, SeqNum
                            ORDER BY Time, UpdateTime
                        ) AS _rn
                    FROM read_parquet('{src}')
                )
                WHERE _rn = 1
                ORDER BY Code, SeqNum
            ) TO '{dst}' (FORMAT PARQUET, COMPRESSION 'zstd')
            """
        )
        after_rows, after_distinct = con.execute(
            f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT (Code, SeqNum)) AS distinct_keys
            FROM read_parquet('{dst}')
            """
        ).fetchone()
        after_rows = int(after_rows or 0)
        after_distinct = int(after_distinct or 0)
        if after_rows != after_distinct or after_rows != before_distinct:
            raise RuntimeError(
                f"dedupe validation failed: before_distinct={before_distinct} "
                f"after_rows={after_rows} after_distinct={after_distinct}"
            )
        dst_mb = dst.stat().st_size / 1024 / 1024
        log.info("[%s] after rows=%d distinct=%d size=%.1fMB", kind, after_rows, after_distinct, dst_mb)
    finally:
        con.close()

    log.info("[%s] uploading repaired parquet -> oss://%s/%s", kind, bucket.bucket_name, key)
    oss2.resumable_upload(
        bucket,
        key,
        str(dst),
        multipart_threshold=100 * 1024 * 1024,
        part_size=50 * 1024 * 1024,
        num_threads=int(os.environ.get("OSS_UPLOAD_THREADS", "4")),
    )
    meta = bucket.head_object(key)
    log.info("[%s] uploaded ok size=%d", kind, meta.content_length)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--kinds", nargs="+", choices=["order", "deal", "tick"], default=["deal", "tick", "order"])
    ap.add_argument("--work-dir", default="/data/collector_output/repair")
    args = ap.parse_args()

    for kind in args.kinds:
        repair_kind(args.date, kind, Path(args.work_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
