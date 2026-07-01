#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""一次性脚本：手动触发当日归档上传到 OSS。

用途：native_engine.py 的 _check_raw_upload_time 有 bug —— post-close snapshot
在 15:10 跑（早于配置的 15:30 上传时间），导致 archived_close 锁死后 upload
永远不触发。本脚本绕过 engine 进程，直接读本地 chunks → DuckDB merge → 上传 OSS。

复用 native_engine._upload_raw_day_to_oss 的逻辑（同 select_clause、同 OSS 路径）。
不依赖 engine 进程状态，独立运行。

用法（pod 内）：
  python /tmp/manual_archive_upload.py --date 20260630
  python /tmp/manual_archive_upload.py --date 20260630 --kind tick  # 只传 tick
  python /tmp/manual_archive_upload.py --date 20260630 --skip-existing  # OSS 已有则跳过（默认）
"""
import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("manual-upload")

# ── archive select clause (从 native_engine._archive_select_clause 复制) ──
def select_clause(kind: str) -> str:
    if kind == "order":
        exprs = [
            "m.SECURITY_ID::INTEGER AS Code",
            "epoch_us(x.Time) AS Time",
            "epoch_us(x.UpdateTime) AS UpdateTime",
            "x.OrderID::INTEGER AS OrderID",
            "x.Side::TINYINT AS Side",
            "ROUND(x.Price * 100)::INTEGER AS Price",
            "x.Volume::BIGINT AS Volume",
            "x.OrderType::TINYINT AS OrderType",
            "x.SeqNum::INTEGER AS SeqNum",
        ]
    elif kind == "deal":
        exprs = [
            "m.SECURITY_ID::INTEGER AS Code",
            "epoch_us(x.Time) AS Time",
            "epoch_us(x.UpdateTime) AS UpdateTime",
            "x.SaleOrderID::BIGINT AS SaleOrderID",
            "x.BuyOrderID::BIGINT AS BuyOrderID",
            "x.Side::TINYINT AS Side",
            "ROUND(x.Price * 100)::INTEGER AS Price",
            "x.Volume::BIGINT AS Volume",
            "x.SeqNum::INTEGER AS SeqNum",
        ]
    elif kind == "tick":
        # 价格列做范围检查：A 股真实价格 ≤ 10000 元 (×100=1e6)，超过的视为 SDK 脏值
        # (如 001399.XSHE 这类非股票漏过 C++ 过滤，HighLimitPrice=1e9 会溢出 INT32)
        def _safe_price(col):
            return (f"CASE WHEN x.{col} BETWEEN 0 AND 10000 "
                    f"THEN ROUND(x.{col} * 100)::INTEGER ELSE 0 END AS {col}")
        exprs = [
            "m.SECURITY_ID::INTEGER AS Code",
            "epoch_us(x.Time) AS Time",
            "epoch_us(x.UpdateTime) AS UpdateTime",
            f"{_safe_price('CurrentPrice')}",
            "x.TotalVolume::BIGINT AS TotalVolume",
        ]
        exprs += [_safe_price(c) for c in
                  ["PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice",
                   "HighLimitPrice", "LowLimitPrice", "IOPV"]]
        exprs += [
            "COALESCE(x.TradeNum, 0)::INTEGER AS TradeNum",
            "x.TotalBidVolume::BIGINT AS TotalBidVolume",
            "x.TotalAskVolume::BIGINT AS TotalAskVolume",
            "ROUND(x.AvgBidPrice * 100)::INTEGER AS AvgBidPrice",
            "ROUND(x.AvgAskPrice * 100)::INTEGER AS AvgAskPrice",
        ]
        exprs += [_safe_price(f"AskPrice{i}") for i in range(1, 11)]
        exprs += [f"COALESCE(x.AskVolume{i}, 0)::BIGINT AS AskVolume{i}" for i in range(1, 11)]
        exprs += [f"COALESCE(x.AskNum{i}, 0)::INTEGER AS AskNum{i}" for i in range(1, 11)]
        exprs += [_safe_price(f"BidPrice{i}") for i in range(1, 11)]
        exprs += [f"COALESCE(x.BidVolume{i}, 0)::BIGINT AS BidVolume{i}" for i in range(1, 11)]
        exprs += [f"COALESCE(x.BidNum{i}, 0)::INTEGER AS BidNum{i}" for i in range(1, 11)]
        exprs += ["x.SeqNum::INTEGER AS SeqNum"]
    else:
        raise ValueError(f"unsupported kind: {kind}")
    return ",\n                            ".join(exprs)


def get_oss_bucket():
    import oss2
    auth = oss2.Auth(os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"])
    endpoint = os.environ.get("OSS_ENDPOINT", "")
    if endpoint and not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"
    bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")
    return oss2.Bucket(auth, endpoint, bucket_name)


def upload_kind(bucket, date_str: str, kind: str, disk_dir: Path,
                skip_existing: bool = True) -> bool:
    """上传一个 kind 的所有 chunks → 合并 parquet → OSS."""
    chunk_dir = disk_dir / kind
    chunks = sorted(chunk_dir.glob("*.parquet")) if chunk_dir.exists() else []
    if not chunks:
        log.warning("[%s] no chunks at %s, skip", kind, chunk_dir)
        return False

    year = date_str[:4]
    month = date_str[4:6]
    oss_key = f"{year}/{year}{month}/{date_str}/{date_str}_{kind}.parquet"
    bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")

    # 检查 OSS 是否已有
    if skip_existing:
        try:
            meta = bucket.head_object(oss_key)
            if meta.content_length > 0:
                log.info("[%s] skip (OSS has it): oss://%s/%s (%d bytes)",
                         kind, bucket_name, oss_key, meta.content_length)
                return True
        except Exception:
            pass  # NoSuchKey 或其他，继续上传

    # 加载 daily_basic code map
    import pandas as pd
    y, m = date_str[:4], date_str[4:6]
    db_paths = [
        disk_dir / "daily_basic_data.parquet",
        disk_dir / f"{date_str}_daily_basic_data.parquet",
    ]
    # 也试 OSS
    db_df = None
    for p in db_paths:
        if p.exists():
            db_df = pd.read_parquet(p, columns=["ID_QI", "SECURITY_ID"])
            break
    if db_df is None:
        # OSS 用 oss2 拉 daily_basic。今天没有就回退到昨天（ID_QI↔SECURITY_ID 跨日稳定）
        import tempfile
        from datetime import datetime, timedelta
        d = datetime.strptime(date_str, "%Y%m%d")
        candidates = []
        for back in range(0, 8):
            dd = (d - timedelta(days=back)).strftime("%Y%m%d")
            yy, mm = dd[:4], dd[4:6]
            candidates.append((dd, f"{yy}/{yy}{mm}/{dd}/{dd}_daily_basic_data.parquet"))

        tmp_db = Path(tempfile.mktemp(suffix=".parquet"))
        try:
            got = False
            for dd, db_key in candidates:
                try:
                    log.info("[%s] trying daily_basic oss://%s/%s", kind, bucket_name, db_key)
                    bucket.get_object_to_file(db_key, str(tmp_db))
                    db_df = pd.read_parquet(tmp_db, columns=["ID_QI", "SECURITY_ID"])
                    log.info("[%s] ✓ using daily_basic from %s (%d rows)",
                             kind, dd, len(db_df))
                    got = True
                    break
                except Exception:
                    continue
            if not got:
                log.error("[%s] no daily_basic found in last 8 days", kind)
                return False
        finally:
            tmp_db.unlink(missing_ok=True)

    if db_df is None or db_df.empty:
        log.error("[%s] daily_basic not found, cannot map code", kind)
        return False

    # 决定 map_code_col：优先用零填充字符串
    map_code_col = "ID_QI"
    if "_ID_QI_PAD" in db_df.columns:
        map_code_col = "_ID_QI_PAD"
    map_df = db_df[["_ID_QI_PAD"] if map_code_col == "_ID_QI_PAD" else ["ID_QI", "SECURITY_ID"]].drop_duplicates()
    if map_code_col == "ID_QI":
        map_df = db_df[["ID_QI", "SECURITY_ID"]].drop_duplicates()
        map_df["ID_QI"] = map_df["ID_QI"].astype(str).str.strip()

    map_tmp = disk_dir / f"tmp_code_map_{kind}.parquet"
    map_df.to_parquet(map_tmp, index=False)
    log.info("[%s] code map: %d rows, using col=%s", kind, len(map_df), map_code_col)

    tmp_file = disk_dir / f"tmp_{kind}.parquet"
    tmp_file.unlink(missing_ok=True)

    import duckdb
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET memory_limit='{os.environ.get('ARCHIVE_DUCKDB_MEMORY', '32GB')}'")
        log.info("[%s] merging %d chunks (%.2f GB)...", kind, len(chunks),
                 sum(c.stat().st_size for c in chunks) / 1024**3)
        con.execute(f"""
            COPY (
                SELECT
                    {select_clause(kind)}
                FROM read_parquet('{chunk_dir}/*.parquet') x
                JOIN read_parquet('{map_tmp}') m
                  ON regexp_extract(x.Code, '^\\d+') = m.{map_code_col}::VARCHAR
                ORDER BY m.SECURITY_ID, x.SeqNum
            ) TO '{tmp_file}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)
        con.close()
    except Exception as exc:
        log.error("[%s] duckdb merge failed: %s", kind, exc, exc_info=True)
        tmp_file.unlink(missing_ok=True)
        map_tmp.unlink(missing_ok=True)
        return False

    size_mb = tmp_file.stat().st_size / 1024 / 1024
    log.info("[%s] merged → %s (%.1f MB), uploading...", kind, tmp_file, size_mb)

    # 大文件用 resumable_upload
    try:
        import oss2
        if size_mb >= 100:
            oss2.resumable_upload(
                bucket, oss_key, str(tmp_file),
                multipart_threshold=100 * 1024 * 1024,
                part_size=50 * 1024 * 1024,
                num_threads=4,
            )
        else:
            bucket.put_object_from_file(oss_key, str(tmp_file))
        log.info("[%s] ✓ uploaded oss://%s/%s (%.1f MB, %d chunks)",
                 kind, bucket_name, oss_key, size_mb, len(chunks))
        return True
    except Exception as exc:
        log.error("[%s] upload failed: %s", kind, exc, exc_info=True)
        return False
    finally:
        tmp_file.unlink(missing_ok=True)
        map_tmp.unlink(missing_ok=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True, help="交易日 YYYYMMDD")
    ap.add_argument("--kind", choices=["tick", "order", "deal"],
                    help="只传一个 kind（不传则全传）")
    ap.add_argument("--archive-dir", default="/data/collector_output",
                    help="本地 archive 根目录")
    ap.add_argument("--no-skip-existing", action="store_true",
                    help="强制重传（默认 OSS 已有则跳过）")
    args = ap.parse_args()

    disk_dir = Path(args.archive_dir) / args.date
    if not disk_dir.exists():
        log.error("archive dir not exist: %s", disk_dir)
        sys.exit(2)

    kinds = [args.kind] if args.kind else ["tick", "order", "deal"]
    skip = not args.no_skip_existing

    try:
        bucket = get_oss_bucket()
    except Exception as exc:
        log.error("OSS init failed: %s", exc)
        sys.exit(2)

    log.info("=== manual archive upload for %s (kinds=%s, skip=%s) ===",
             args.date, kinds, skip)

    results = {}
    for kind in kinds:
        results[kind] = upload_kind(bucket, args.date, kind, disk_dir, skip)

    log.info("=== done: %s ===", results)
    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
