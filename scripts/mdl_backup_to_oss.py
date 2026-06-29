#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 mdl_msg_backup 里当日 CSV 原始数据逐个压缩上传到 OSS。

mdl_msg_backup 是通联 feeder_client 实时备份的扁平 CSV 文件，文件名格式和
ftp.datayes.com 上的一致（如 20260629_mdl_6_50_0.csv）。
本脚本每个 csv 单独 zip 压缩，上传到：
    oss://{bucket}/raw_msgs/{date}/{csv文件名}.zip

这样 OSS 上的命名和 ftp-to-oss 拉来的完全对齐，下游无需区分来源。

环境变量：
  BACKUP_DIR        feeder_client msg_backup 目录（默认 /data/quant/mdl_msg_backup）
  TARGET_DATE       要打包的日期 YYYYMMDD（默认今天 CST）。按 mtime 过滤当日文件
  OSS_PREFIX        OSS 子路径，默认 raw_msgs（和 ftp-to-oss 一致）
  SKIP_EXISTING=1   OSS 上已存在同名且非空的对象则跳过（默认 1）
  DRY_RUN=1         只列文件不压缩不上传
  OSS_ENDPOINT / OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET / OSS_BUCKET_NAME
"""
import os
import sys
import time
import zipfile
import logging
import datetime
import tempfile
from pathlib import Path

import oss2

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("mdl-backup-to-oss")

OSS_MULTIPART_THRESHOLD = 100 * 1024 * 1024
OSS_MULTIPART_SIZE = 20 * 1024 * 1024
OSS_PART_CONCURRENCY = 4

LOCAL_TMP_ROOT = Path(os.environ.get("LOCAL_TMP_ROOT", "/tmp/mdl_backup"))


def cst_today() -> str:
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%Y%m%d")


def collect_day_csvs(root: Path, date_str: str) -> list:
    """扫描 root 下 mtime 落在 date_str 当天（CST）的 .csv 文件。

    mdl_msg_backup 是扁平结构，文件名格式 {date}_{type}.csv。
    """
    day = datetime.datetime.strptime(date_str, "%Y%m%d")
    day_start = day - datetime.timedelta(hours=8)   # CST 00:00 = UTC 前一天 16:00
    day_end = day_start + datetime.timedelta(days=1)
    ts_start = day_start.timestamp()
    ts_end = day_end.timestamp()

    files = []
    for p in sorted(root.glob("*.csv")):
        if not p.is_file():
            continue
        try:
            st = p.stat()
        except OSError:
            continue
        if ts_start <= st.st_mtime < ts_end:
            files.append((p, st.st_size))
    return files


def zip_one(csv_path: Path, zip_path: Path) -> int:
    """单个 csv 压缩成 zip，返回 zip 大小。"""
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=3) as zf:
        zf.write(csv_path, csv_path.name)
    return zip_path.stat().st_size


def oss_bucket() -> oss2.Bucket:
    endpoint = os.environ.get("OSS_ENDPOINT",
                              "https://oss-cn-hangzhou-internal.aliyuncs.com")
    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    bucket_name = os.environ.get("OSS_BUCKET_NAME", "quant-mdl-data")
    if not ak or not sk:
        raise RuntimeError("OSS_ACCESS_KEY_ID/SECRET missing")
    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, endpoint, bucket_name, connect_timeout=30)
    log.info("OSS bucket ready: %s @ %s", bucket_name, endpoint)
    return bucket


def oss_key_exists(bucket: oss2.Bucket, key: str) -> bool:
    """OSS 上同名对象存在且非空即视为已传过。

    zip 大小和原 csv 不同，不能用 csv 大小比对，否则永远 mismatch、跳过失效。
    """
    try:
        meta = bucket.head_object(key)
        return meta.content_length > 0
    except oss2.exceptions.NoSuchKey:
        return False
    except Exception as exc:
        log.warning("head_object %s failed: %s", key, exc)
        return False


def upload_to_oss(bucket: oss2.Bucket, local_path: Path, oss_key: str) -> int:
    size = local_path.stat().st_size
    log.info("uploading %s (%d bytes) -> oss://%s/%s",
             local_path.name, size, bucket.bucket_name, oss_key)

    def _progress(consumed, total):
        if consumed % (500 * 1024 * 1024) < 20 * 1024 * 1024:
            pct = consumed * 100 / total if total else 0
            log.info("  %.1f%% (%d / %d)", pct, consumed, total)

    if size >= OSS_MULTIPART_THRESHOLD:
        oss2.resumable_upload(
            bucket, oss_key, str(local_path),
            store=oss2.ResumableStore(root=str(LOCAL_TMP_ROOT / ".oss_resume")),
            multipart_threshold=OSS_MULTIPART_THRESHOLD,
            part_size=OSS_MULTIPART_SIZE,
            num_threads=OSS_PART_CONCURRENCY,
            progress_callback=_progress,
        )
    else:
        bucket.put_object_from_file(oss_key, str(local_path),
                                    progress_callback=_progress)
    log.info("uploaded %s", oss_key)
    return size


def main():
    date_str = os.environ.get("TARGET_DATE") or cst_today()
    root = Path(os.environ.get("BACKUP_DIR", "/data/quant/mdl_msg_backup"))
    oss_prefix = os.environ.get("OSS_PREFIX", "raw_msgs").strip("/")
    skip_existing = os.environ.get("SKIP_EXISTING", "1") == "1"

    log.info("=== mdl-backup-to-oss for %s (root=%s) ===", date_str, root)
    if not root.exists():
        log.error("BACKUP_DIR %s does not exist", root)
        sys.exit(2)

    files = collect_day_csvs(root, date_str)
    if not files:
        log.warning("no csv files matching %s found under %s", date_str, root)
        sys.exit(0)

    total = sum(s for _, s in files)
    log.info("found %d csv files, total %.2f GB", len(files), total / 1024**3)
    for p, s in files:
        log.info("  %s (%.1f MB)", p.name, s / 1024**2)

    if os.environ.get("DRY_RUN") == "1":
        log.info("DRY_RUN=1, exit without zip/upload")
        return

    LOCAL_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    bucket = oss_bucket()

    done = 0
    failed = []
    t0 = time.time()
    for csv_path, csv_size in files:
        oss_key = f"{oss_prefix}/{date_str}/{csv_path.name}.zip"

        if skip_existing and oss_key_exists(bucket, oss_key):
            log.info("skip (oss has it): %s", oss_key)
            done += 1
            continue

        tmp_zip = LOCAL_TMP_ROOT / csv_path.name.replace(".csv", ".csv.zip")
        try:
            zip_size = zip_one(csv_path, tmp_zip)
            upload_to_oss(bucket, tmp_zip, oss_key)
            done += 1
        except Exception as exc:
            log.error("upload %s failed: %s", csv_path.name, exc, exc_info=True)
            failed.append(csv_path.name)
        finally:
            try:
                tmp_zip.unlink()
            except OSError:
                pass

    elapsed = time.time() - t0
    log.info("=== done %d/%d in %.0fs, %d failed ===",
             done, len(files), elapsed, len(failed))
    if failed:
        log.error("failed: %s", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
