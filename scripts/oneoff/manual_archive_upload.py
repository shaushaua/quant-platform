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
        # Keep this in sync with NativeEngine._archive_select_clause("tick").
        # 通联对"无涨跌停限制"代码(新股、停牌等)用 sentinel 填:
        # HighLimitPrice = 999999999.9999。×100 cast INT32 会溢出，
        # 用 LEAST clip 到 INT32_MAX；必须保留 AS {col}，否则 DuckDB
        # 会把表达式文本写成列名。
        def _clip(col):
            return f"LEAST(ROUND(x.{col} * 100), 2147483647)::INTEGER AS {col}"
        exprs = [
            "m.SECURITY_ID::INTEGER AS Code",
            "epoch_us(x.Time) AS Time",
            "epoch_us(x.UpdateTime) AS UpdateTime",
            _clip("CurrentPrice"),
            "x.TotalVolume::BIGINT AS TotalVolume",
        ]
        exprs += [_clip(c) for c in
                  ["PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice",
                   "HighLimitPrice", "LowLimitPrice", "IOPV"]]
        exprs += [
            "COALESCE(x.TradeNum, 0)::INTEGER AS TradeNum",
            "x.TotalBidVolume::BIGINT AS TotalBidVolume",
            "x.TotalAskVolume::BIGINT AS TotalAskVolume",
            _clip("AvgBidPrice"),
            _clip("AvgAskPrice"),
        ]
        exprs += [_clip(f"AskPrice{i}") for i in range(1, 11)]
        exprs += [f"COALESCE(x.AskVolume{i}, 0)::BIGINT AS AskVolume{i}" for i in range(1, 11)]
        exprs += [f"COALESCE(x.AskNum{i}, 0)::INTEGER AS AskNum{i}" for i in range(1, 11)]
        exprs += [_clip(f"BidPrice{i}") for i in range(1, 11)]
        exprs += [f"COALESCE(x.BidVolume{i}, 0)::BIGINT AS BidVolume{i}" for i in range(1, 11)]
        exprs += [f"COALESCE(x.BidNum{i}, 0)::INTEGER AS BidNum{i}" for i in range(1, 11)]
        exprs += ["x.SeqNum::INTEGER AS SeqNum"]
    else:
        raise ValueError(f"unsupported kind: {kind}")
    return ",\n                            ".join(exprs)


def output_columns(kind: str) -> list[str]:
    if kind == "order":
        return [
            "Code", "Time", "UpdateTime", "OrderID", "Side", "Price",
            "Volume", "OrderType", "SeqNum",
        ]
    if kind == "deal":
        return [
            "Code", "Time", "UpdateTime", "SaleOrderID", "BuyOrderID",
            "Side", "Price", "Volume", "SeqNum",
        ]
    if kind == "tick":
        cols = [
            "Code", "Time", "UpdateTime", "CurrentPrice", "TotalVolume",
            "PreClosePrice", "OpenPrice", "HighestPrice", "LowestPrice",
            "HighLimitPrice", "LowLimitPrice", "IOPV", "TradeNum",
            "TotalBidVolume", "TotalAskVolume", "AvgBidPrice",
            "AvgAskPrice",
        ]
        cols += [f"AskPrice{i}" for i in range(1, 11)]
        cols += [f"AskVolume{i}" for i in range(1, 11)]
        cols += [f"AskNum{i}" for i in range(1, 11)]
        cols += [f"BidPrice{i}" for i in range(1, 11)]
        cols += [f"BidVolume{i}" for i in range(1, 11)]
        cols += [f"BidNum{i}" for i in range(1, 11)]
        cols.append("SeqNum")
        return cols
    raise ValueError(f"unsupported kind: {kind}")


def log_duplicate_stats(con, kind: str, chunk_dir: Path, map_tmp: Path, map_code_col: str) -> None:
    raw_rows, distinct_keys, dup_rows = con.execute(f"""
        WITH mapped AS (
            SELECT
                m.SECURITY_ID::INTEGER AS Code,
                x.SeqNum::INTEGER AS SeqNum
            FROM read_parquet('{chunk_dir}/*.parquet') x
            JOIN read_parquet('{map_tmp}') m
              ON regexp_extract(x.Code, '^\\d+') = m.{map_code_col}::VARCHAR
        ),
        grouped AS (
            SELECT Code, SeqNum, COUNT(*) AS n
            FROM mapped
            GROUP BY Code, SeqNum
        )
        SELECT
            COALESCE(SUM(n), 0)::BIGINT AS raw_rows,
            COUNT(*)::BIGINT AS distinct_keys,
            COALESCE(SUM(n - 1), 0)::BIGINT AS dup_rows
        FROM grouped
    """).fetchone()
    log.info("[%s] duplicate stats: raw_rows=%d distinct_keys=%d dup_rows=%d",
             kind, int(raw_rows or 0), int(distinct_keys or 0), int(dup_rows or 0))


def validate_archive_times(con, kind: str, chunk_dir: Path, map_tmp: Path, map_code_col: str, tmp_file: Path) -> bool:
    """Fail fast if merged parquet Time/UpdateTime drift from source chunks.

    20260701 曾经因为自定义 DuckDB epoch_us 宏把秒数重复加了一遍，导致
    tick/order 归档时间相对 SHM 错位。这里用 DuckDB 内置 epoch_us 重新
    对源 chunk 计算期望值，并和最终 tmp parquet 比较；不一致就拒绝上传。
    """
    stats = con.execute(f"""
        WITH src_raw AS (
            SELECT
                m.SECURITY_ID::INTEGER AS Code,
                x.SeqNum::INTEGER AS SeqNum,
                epoch_us(x.Time) AS Time,
                epoch_us(x.UpdateTime) AS UpdateTime
            FROM read_parquet('{chunk_dir}/*.parquet') x
            JOIN read_parquet('{map_tmp}') m
              ON regexp_extract(x.Code, '^\\d+') = m.{map_code_col}::VARCHAR
        ),
        src AS (
            SELECT Code, SeqNum, Time, UpdateTime
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Code, SeqNum
                        ORDER BY Time, UpdateTime
                    ) AS _rn
                FROM src_raw
            )
            WHERE _rn = 1
        ),
        dst AS (
            SELECT Code, SeqNum, Time, UpdateTime
            FROM read_parquet('{tmp_file}')
        )
        SELECT
            COUNT(*) AS rows_checked,
            SUM(CASE WHEN dst.Time != src.Time THEN 1 ELSE 0 END) AS time_bad,
            SUM(CASE WHEN dst.UpdateTime != src.UpdateTime THEN 1 ELSE 0 END) AS update_bad,
            (SELECT COUNT(*) FROM dst) AS dst_rows,
            (SELECT COUNT(*) FROM src) AS src_rows
        FROM dst
        JOIN src USING (Code, SeqNum)
    """).fetchone()
    rows_checked, time_bad, update_bad, dst_rows, src_rows = (
        int(stats[0] or 0), int(stats[1] or 0), int(stats[2] or 0),
        int(stats[3] or 0), int(stats[4] or 0),
    )
    if rows_checked == 0 or rows_checked != dst_rows or rows_checked != src_rows or time_bad or update_bad:
        log.error(
            "[%s] archive time validation failed: checked=%d dst_rows=%d src_rows=%d time_bad=%d update_bad=%d",
            kind, rows_checked, dst_rows, src_rows, time_bad, update_bad,
        )
        return False
    log.info("[%s] archive time validation ok: rows=%d", kind, rows_checked)
    return True


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
        log_duplicate_stats(con, kind, chunk_dir, map_tmp, map_code_col)
        out_cols = ", ".join(output_columns(kind))
        con.execute(f"""
            COPY (
                WITH merged AS (
                    SELECT
                        {select_clause(kind)}
                    FROM read_parquet('{chunk_dir}/*.parquet') x
                    JOIN read_parquet('{map_tmp}') m
                      ON regexp_extract(x.Code, '^\\d+') = m.{map_code_col}::VARCHAR
                ),
                ranked AS (
                    SELECT
                        *,
                        -- Manual uploads may be used after a pod restart or a
                        -- failed automatic upload.  De-dup by exchange sequence
                        -- key so repeated chunks do not contaminate OSS.
                        ROW_NUMBER() OVER (
                            PARTITION BY Code, SeqNum
                            ORDER BY Time, UpdateTime
                        ) AS _rn
                    FROM merged
                )
                SELECT {out_cols}
                FROM ranked
                WHERE _rn = 1
                ORDER BY Code, SeqNum
            ) TO '{tmp_file}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)
        if not validate_archive_times(con, kind, chunk_dir, map_tmp, map_code_col, tmp_file):
            tmp_file.unlink(missing_ok=True)
            map_tmp.unlink(missing_ok=True)
            return False
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
