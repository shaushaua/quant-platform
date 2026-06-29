#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 ftp.datayes.com 拉当日 zip 上传到 OSS raw_msgs/{date}/。

环境变量：
  FTP_HOST / FTP_USER / FTP_PASS  （必须，从 quant-secrets 注入，不在代码里写默认）
  OSS_ENDPOINT / OSS_BUCKET_NAME / raw_msgs 路径
  OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET (从 quant-secrets 注入)
  TARGET_DATE     要拉取的日期 YYYYMMDD（默认今天 CST）
  OSS_PREFIX      OSS 子路径，默认 raw_msgs
  FTP_REMOTE_DIR  FTP 上的目录，默认 /
  LIST_ONLY=1     只列文件不下载（探查模式）

用法：
  docker run --rm \
    -e TARGET_DATE=20260629 \
    --env-from-secret quant-secrets \
    172.24.99.176:5000/quant-platform/ftp-to-oss:latest
"""
import os
import re
import sys
import time
import ftplib
import shutil
import hashlib
import logging
import datetime
import tempfile
from pathlib import Path

import oss2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ftp-to-oss")

OSS_MULTIPART_THRESHOLD = 100 * 1024 * 1024   # >100MB 走分片
OSS_MULTIPART_SIZE = 20 * 1024 * 1024          # 每片 20MB
OSS_PART_CONCURRENCY = 4

LOCAL_TMP_ROOT = Path(os.environ.get("LOCAL_TMP_ROOT", "/tmp/ftp_to_oss"))


def cst_today() -> str:
    """FC 默认 UTC，A 股按北京时间。"""
    utc = datetime.datetime.utcnow()
    cst = utc + datetime.timedelta(hours=8)
    return cst.strftime("%Y%m%d")


def ftp_connect() -> ftplib.FTP:
    host = os.environ.get("FTP_HOST")
    user = os.environ.get("FTP_USER")
    pwd = os.environ.get("FTP_PASS")
    if not (host and user and pwd):
        raise RuntimeError("FTP_HOST / FTP_USER / FTP_PASS must be set (via quant-secrets)")
    log.info("connecting FTP %s as %s ...", host, user)
    ftp = ftplib.FTP(host, timeout=30)
    ftp.login(user, pwd)
    ftp.set_pasv(True)
    log.info("FTP login OK; welcome=%s", ftp.welcome[:120])
    return ftp


def list_remote_zips(ftp: ftplib.FTP, date_str: str, remote_dir: str = "/"):
    """列出 FTP 上文件名含 date_str 的 .zip 文件。

    通联 FTP 目录结构：根目录下按日期返回 zip 切片。
    没有目录树就扫根目录；如有子目录则递归一层。
    """
    target = date_str
    matches = []  # [(path, size)]

    def _scan_dir(prefix: str):
        log.info("listing %s", prefix or "/")
        entries = []
        ftp.cwd(prefix or "/")
        ftp.voidcmd("TYPE I")
        try:
            lines = []
            ftp.retrlines("LIST", lines.append)
        except ftplib.error_perm as exc:
            log.warning("LIST %s failed: %s", prefix, exc)
            return
        for line in lines:
            parts = line.split()
            if len(parts) < 9:
                continue
            # -rw-r--r-- 1 ftp ftp 213123123 Jun 29 14:00 file.zip
            size = 0
            try:
                size = int(parts[4])
            except ValueError:
                pass
            name = parts[-1]
            is_dir = line.startswith("d")
            full = f"{prefix.rstrip('/')}/{name}".replace("//", "/")
            if is_dir:
                # 递归一层（仅当目录名含日期，避免全树扫描）
                if target in name:
                    _scan_dir(full)
            else:
                if name.lower().endswith(".zip") and target in name:
                    matches.append((full, size))

    _scan_dir(remote_dir)
    # 去重（多目录扫描可能命中）
    seen = set()
    out = []
    for path, size in matches:
        if path in seen:
            continue
        seen.add(path)
        out.append((path, size))
    return out


def download_file(ftp: ftplib.FTP, remote_path: str, local_path: Path,
                  expected_size: int = 0) -> int:
    """下载单个文件到本地。支持断点续传。"""
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # 已下载完整 → 跳
    if local_path.exists() and local_path.stat().st_size > 0:
        if expected_size and local_path.stat().st_size == expected_size:
            log.info("skip download %s (already %d bytes)", local_path.name,
                     local_path.stat().st_size)
            return local_path.stat().st_size

    tmp_path = local_path.with_suffix(local_path.suffix + ".part")
    log.info("downloading %s -> %s", remote_path, tmp_path)

    # 设置 BINARY 模式
    ftp.voidcmd("TYPE I")

    # 用 REST 支持断点
    rest_offset = tmp_path.stat().st_size if tmp_path.exists() else 0

    with open(tmp_path, "ab") as fp:
        if rest_offset:
            log.info("resume from offset %d", rest_offset)
        cmd = f"RETR {remote_path}"
        if rest_offset:
            cmd = f"REST {rest_offset}\nRETR {remote_path}"
        # ftplib 的 retrbinary 不直接支持 REST，需要发 REST 命令
        if rest_offset:
            ftp.sendcmd(f"REST {rest_offset}")
        ftp.retrbinary(f"RETR {remote_path}", fp.write, blocksize=64 * 1024)
        fp.flush()

    actual = tmp_path.stat().st_size
    if expected_size and actual != expected_size:
        log.warning("size mismatch: expected %d got %d for %s",
                    expected_size, actual, remote_path)
    tmp_path.replace(local_path)
    log.info("downloaded %s (%d bytes)", local_path.name, actual)
    return actual


def oss_bucket() -> oss2.Bucket:
    endpoint = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou-internal.aliyuncs.com")
    ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
    sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    bucket_name = os.environ.get("OSS_BUCKET_NAME", "quant-mdl-data")
    if not ak or not sk:
        raise RuntimeError("OSS_ACCESS_KEY_ID/SECRET missing")
    auth = oss2.Auth(ak, sk)
    bucket = oss2.Bucket(auth, endpoint, bucket_name, connect_timeout=30)
    log.info("OSS bucket ready: %s @ %s", bucket_name, endpoint)
    return bucket


def oss_key_exists(bucket: oss2.Bucket, key: str, expected_size: int = 0) -> bool:
    try:
        meta = bucket.head_object(key)
        if expected_size and meta.content_length == expected_size:
            return True
        if not expected_size and meta.content_length > 0:
            return True
        log.info("OSS object exists but size mismatch: %s (oss=%d, ftp=%d)",
                 key, meta.content_length, expected_size)
        return False
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
            log.info("  upload %s: %.1f%% (%d / %d)",
                     local_path.name, pct, consumed, total)

    if size >= OSS_MULTIPART_THRESHOLD:
        # resumable multipart
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
    log.info("=== ftp-to-oss for %s ===", date_str)

    bucket = oss_bucket()
    oss_prefix = os.environ.get("OSS_PREFIX", "raw_msgs").strip("/")
    remote_dir = os.environ.get("FTP_REMOTE_DIR", "/")

    ftp = ftp_connect()
    try:
        files = list_remote_zips(ftp, date_str, remote_dir)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    if not files:
        log.error("No zip files matching %s found on FTP", date_str)
        sys.exit(2)

    total_size = sum(s for _, s in files)
    log.info("found %d files, total %.2f GB",
             len(files), total_size / 1024**3)
    for path, size in files:
        log.info("  %s (%.1f MB)", path, size / 1024**2)

    if os.environ.get("LIST_ONLY") == "1":
        log.info("LIST_ONLY=1, exit without download")
        return

    LOCAL_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    done = 0
    failed = []
    t0 = time.time()
    for remote_path, size in files:
        name = Path(remote_path).name
        oss_key = f"{oss_prefix}/{date_str}/{name}"
        if oss_key_exists(bucket, oss_key, size):
            log.info("skip (oss has it): %s", oss_key)
            done += 1
            continue

        local_path = LOCAL_TMP_ROOT / date_str / name
        ftp = ftp_connect()
        try:
            download_file(ftp, remote_path, local_path, size)
        except Exception as exc:
            log.error("download %s failed: %s", remote_path, exc)
            failed.append(remote_path)
            try:
                ftp.close()
            except Exception:
                pass
            continue
        try:
            ftp.quit()
        except Exception:
            pass

        try:
            upload_to_oss(bucket, local_path, oss_key)
            done += 1
            # 上传完删本地
            try:
                local_path.unlink()
            except Exception:
                pass
        except Exception as exc:
            log.error("upload %s failed: %s", local_path, exc)
            failed.append(remote_path)

    elapsed = time.time() - t0
    log.info("=== done %d/%d in %.0fs (%.1f GB total), %d failed ===",
             done, len(files), elapsed, total_size / 1024**3, len(failed))
    if failed:
        log.error("failed: %s", failed)
        sys.exit(1)


if __name__ == "__main__":
    main()
