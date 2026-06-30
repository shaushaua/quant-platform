#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证 mdl_msg_backup CSV (通联 feeder_client 原始) vs C++ archive parquet 是否一致。

两边都源自同一份通联 feed：
  - CSV  (feeder_client, Encoding=5 CSV)  : 显示单位 元/股, 价格带小数
  - archive (native_mdl_collector MKTPRO) : 价格×100=分(int), Volume=BIGINT股

比较策略：按 (Code, SeqNum) join，逐字段 diff。
  - 价格类：abs(csv*100 - archive) <= 1   (容忍 ROUND 半位误差)
  - 量类  ：严格相等
  - SeqNum 是通联 SDK 序列号，两边同源，理论上 100% 对齐；对不上的行 = 丢/重消息

Code 映射：CSV SecurityID 是 "600000" 6 位 code，archive Code 是 SECURITY_ID int。
用 daily_basic_data.parquet 的 ID_QI ↔ SECURITY_ID 做映射。

用法：
  # 默认对比某天 SH tick
  python verify_csv_vs_archive.py --date 20260629 --type sh_tick

  # 指定文件
  python verify_csv_vs_archive.py \\
      --csv /data/quant/mdl_msg_backup/20260629_mdl_4_4_0.csv \\
      --parquet /data/collector_output/20260629/tick/000000.parquet \\
      --type sh_tick --date 20260629

  # 用 OSS 上的合并 parquet (推荐, 已经 Price×100 归一化)
  python verify_csv_vs_archive.py --date 20260629 --type sh_tick \\
      --parquet oss://quant-mdl-data/2026/202606/20260629/20260629_tick.parquet
"""
import argparse
import logging
import os
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("verify-csv-arc")

# ── 消息类型配置（CSV 列名 → archive 列名映射，来自 converter.py）────────
MSG_TYPE_CFG: Dict[str, dict] = {
    "sh_tick": {
        "csv_glob": "mdl_4_4_0.csv",            # SHL2MarketData (MID=4, channel 0)
        # 注: mdl_4_4_1.csv 是 50 档委托队列, 不同 msg schema, 不在对比范围
        "archive_kind": "tick",
        "csv_code": "SecurityID",
        "csv_seq": "SeqNo",
        "field_map": {
            "LastPrice":   "CurrentPrice",
            "TradVolume":  "TotalVolume",
            "PreCloPrice": "PreClosePrice",
            "HighPrice":   "HighestPrice",
            "LowPrice":    "LowestPrice",
            "TradNumber":  "TradeNum",
            "TotalBidVol": "TotalBidVolume",
            "TotalAskVol": "TotalAskVolume",
            "WAvgBidPri":  "AvgBidPrice",
            "WAvgAskPri":  "AvgAskPrice",
        },
        "price_fields":  {"CurrentPrice", "PreClosePrice", "HighestPrice",
                          "LowestPrice", "AvgBidPrice", "AvgAskPrice"},
        "volume_fields": {"TotalVolume", "TotalBidVolume",
                          "TotalAskVolume", "TradeNum"},
    },
    "sz_tick": {
        "csv_glob": "mdl_6_28_0.csv",            # Snapshot300111_v2 (MID=28, channel 0)
        # 注: mdl_6_28_1/2.csv 是 89 列 variant, 不同 schema
        "archive_kind": "tick",
        "csv_code": "SecurityID",
        "csv_seq": "SeqNo",
        "field_map": {
            "LastPrice":          "CurrentPrice",
            "Volume":             "TotalVolume",
            "PreCloPrice":        "PreClosePrice",
            "HighPrice":          "HighestPrice",
            "LowPrice":           "LowestPrice",
            "TurnNum":            "TradeNum",
            "TotalBidQty":        "TotalBidVolume",
            "TotalOfferQty":      "TotalAskVolume",
            "WeightedAvgBidPx":   "AvgBidPrice",
            "WeightedAvgOfferPx": "AvgAskPrice",
        },
        "price_fields":  {"CurrentPrice", "PreClosePrice", "HighestPrice",
                          "LowestPrice", "AvgBidPrice", "AvgAskPrice"},
        "volume_fields": {"TotalVolume", "TotalBidVolume",
                          "TotalAskVolume", "TradeNum"},
    },
    "sh_deal": {
        "csv_glob": "mdl_4_24_*.csv",            # SH NGTS (MID=24, Type=T 成交)
        "archive_kind": "deal",
        "csv_code": "SecurityID",
        "csv_seq": "BizIndex",
        "field_map": {
            "TradPrice":  "Price",
            "TradVolume": "Volume",
        },
        "price_fields":  {"Price"},
        "volume_fields": {"Volume"},
    },
    "sz_deal": {
        "csv_glob": "mdl_6_36_*.csv",            # Transaction300191_v2 (MID=36)
        "archive_kind": "deal",
        "csv_code": "SecurityID",
        "csv_seq": "ApplSeqNum",
        "field_map": {
            "LastPx":  "Price",
            "LastQty": "Volume",
        },
        "price_fields":  {"Price"},
        "volume_fields": {"Volume"},
    },
}


# ── 加载 ────────────────────────────────────────────────────────────
def _csv_usecols(header_cols: List[str], cfg: dict) -> List[str]:
    """只读需要的列，避免加载 100 列 × 7M 行爆内存."""
    wanted = {cfg["csv_code"], cfg["csv_seq"]}
    wanted.update(cfg["field_map"].keys())
    return [c for c in header_cols if c in wanted]


def load_csv(paths: List[Path], cfg: dict,
             sample_codes: Optional[set] = None) -> pd.DataFrame:
    """读一个或多个 CSV（多 channel concat），列裁剪 + 行过滤."""
    # 先拿 header 确定列
    import csv as _csv
    with open(paths[0]) as f:
        reader = _csv.reader(f)
        header = next(reader)
    usecols = _csv_usecols(header, cfg)
    log.info("      CSV usecols: %s", usecols)

    frames = []
    for p in paths:
        log.info("      reading %s ...", p.name)
        if p.suffix == ".zip":
            with zipfile.ZipFile(p) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as f:
                    df = pd.read_csv(f, usecols=usecols, dtype=str,
                                     on_bad_lines="skip")
        else:
            df = pd.read_csv(p, usecols=usecols, dtype=str,
                             on_bad_lines="skip")
        if sample_codes is not None:
            code_col = cfg["csv_code"]
            df = df[df[code_col].isin(sample_codes)]
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_parquet(path: str, sample_sec_ids: Optional[set] = None,
                 columns: Optional[List[str]] = None) -> pd.DataFrame:
    """读 archive parquet。支持本地路径、本地通配、oss:// 协议。
    谓词下推：sample_sec_ids 走 WHERE Code IN (...)，避免全表加载。"""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb required") from exc

    con = duckdb.connect()
    sel = ",".join(columns) if columns else "*"
    where = ""
    if sample_sec_ids:
        ids = ",".join(str(int(x)) for x in sample_sec_ids)
        where = f" WHERE Code IN ({ids})"

    if path.startswith("oss://"):
        ak = os.environ.get("OSS_ACCESS_KEY_ID", "")
        sk = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
        endpoint = os.environ.get("OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com")
        if ak and sk:
            con.execute(f"SET access_key='{ak}'")
            con.execute(f"SET secret_key='{sk}'")
        con.execute(f"SET s3_endpoint='{endpoint}'")
        con.execute("SET s3_url_style='path'")
        con.execute("SET s3_use_ssl=true")
        s3_path = path.replace("oss://", "s3://")
        sql = f"SELECT {sel} FROM read_parquet('{s3_path}'){where}"
    else:
        sql = f"SELECT {sel} FROM read_parquet('{path}'){where}"
    log.info("      SQL: %s", sql)
    return con.execute(sql).fetchdf()


def load_code_map(date_str: str, archive_dir: str, oss_bucket: str) -> pd.DataFrame:
    """从 daily_basic 拿 ID_QI ↔ SECURITY_ID 映射。

    archive 的 Code 是 SECURITY_ID int；CSV 的 SecurityID 是 "600000" 字符串。
    需要 daily_basic 把两者连起来。
    """
    # 优先本地，再 OSS
    candidates = [
        Path(archive_dir) / date_str / "daily_basic_data.parquet",
        Path(archive_dir) / f"{date_str}_daily_basic_data.parquet",
    ]
    for p in candidates:
        if p.exists():
            log.info("loading daily_basic from %s", p)
            return pd.read_parquet(p, columns=["ID_QI", "SECURITY_ID"])

    # OSS
    y, m = date_str[:4], date_str[4:6]
    oss_path = f"{oss_bucket}/{y}/{y}{m}/{date_str}/{date_str}_daily_basic_data.parquet"
    log.info("loading daily_basic from oss: %s", oss_path)
    return load_parquet(oss_path.replace("oss://quant-mdl-data",
                                         "oss://quant-mdl-data"))


# ── 归一化 ──────────────────────────────────────────────────────────
def normalize_csv(df: pd.DataFrame, cfg: dict, code_map: pd.DataFrame) -> pd.DataFrame:
    """把 CSV 字段重命名 + Code 转 SECURITY_ID int."""
    # 把 CSV 列名映射到 archive 名
    rename = {cfg["csv_code"]: "_csv_code_raw",
              cfg["csv_seq"]:  "SeqNum"}
    rename.update(cfg["field_map"])   # csv_name → archive_name
    df = df.rename(columns=rename)

    # Code "600000" → SECURITY_ID int
    code_map_local = code_map.copy()
    code_map_local["ID_QI"] = code_map_local["ID_QI"].astype(str).str.strip()
    df["_csv_code_raw"] = df["_csv_code_raw"].astype(str).str.strip()
    df = df.merge(code_map_local, left_on="_csv_code_raw", right_on="ID_QI",
                  how="left")
    df["Code"] = df["SECURITY_ID"].astype("Int64")
    unmapped = df["Code"].isna().sum()
    if unmapped > 0:
        log.warning("CSV %d/%d rows failed to map to SECURITY_ID (code not in daily_basic)",
                    unmapped, len(df))
    return df


def normalize_archive(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """archive 已经是 SECURITY_ID int + 价格分. 直接选需要的列."""
    keep = ["Code", "SeqNum"] + list(cfg["field_map"].values())
    cols = [c for c in keep if c in df.columns]
    return df[cols].copy()


# ── 对比 ────────────────────────────────────────────────────────────
def diff_field(csv_v: pd.Series, arc_v: pd.Series, is_price: bool) -> dict:
    """返回 match/mismatch 统计 + 最大差异."""
    # 对齐索引，丢 NaN
    pair = pd.concat([csv_v, arc_v], axis=1).dropna()
    if pair.empty:
        return {"total": 0, "match": 0, "mismatch": 0, "max_diff": 0.0,
                "ratio_mode": None}
    cv = pair.iloc[:, 0].astype("float64")
    av = pair.iloc[:, 1].astype("float64")

    if is_price:
        # CSV 元 × 100 = archive 分；ROUND 可能差 ±1 分
        diff = (cv * 100.0 - av).abs()
        match_mask = diff <= 1.0
    else:
        diff = (cv - av).abs()
        match_mask = diff == 0

    mismatch_idx = ~match_mask
    stats = {
        "total":   len(pair),
        "match":   int(match_mask.sum()),
        "mismatch": int(mismatch_idx.sum()),
        "max_diff": float(diff.max()) if len(diff) > 0 else 0.0,
    }
    # 用出现频次最高的 csv/arc 比值判断是否系统性偏差 (×10 / ÷10 / 等)
    if mismatch_idx.any() and (av[mismatch_idx] != 0).all():
        ratios = cv[mismatch_idx] / av[mismatch_idx]
        if is_price:
            ratios = ratios * 100.0  # 换回元→分后再比
        # 圆整到 0.1 看众数
        rmode = ratios.round(1).mode()
        stats["ratio_mode"] = float(rmode.iloc[0]) if not rmode.empty else None
    else:
        stats["ratio_mode"] = None
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="交易日 YYYYMMDD（用于自动找文件 + code map）")
    ap.add_argument("--type", required=True, choices=list(MSG_TYPE_CFG.keys()),
                    help="消息类型")
    ap.add_argument("--csv", help="直接指定 CSV 路径（.csv 或 .csv.zip）")
    ap.add_argument("--parquet", help="直接指定 archive parquet（本地 / oss:// / 通配）")
    ap.add_argument("--backup-dir", default="/data/quant/mdl_msg_backup",
                    help="feeder_client msg_backup 目录")
    ap.add_argument("--archive-dir", default="/data/collector_output",
                    help="collector 本地 archive 根目录")
    ap.add_argument("--oss-bucket", default="oss://quant-mdl-data",
                    help="OSS bucket")
    ap.add_argument("--sample-codes", help="只对比这几只股票，逗号分隔（强烈建议，避免全量加载）")
    ap.add_argument("--dump-unjoined", action="store_true",
                    help="把 join 不上的行写到 /tmp/csv_arc_unjoined.csv")
    args = ap.parse_args()

    cfg = MSG_TYPE_CFG[args.type]
    if not args.date and (not args.csv or not args.parquet):
        ap.error("--date 必填（除非 --csv 和 --parquet 都显式指定）")

    sample_codes = None
    if args.sample_codes:
        sample_codes = {c.strip().zfill(6) for c in args.sample_codes.split(",")
                        if c.strip()}
        log.info("[args] sample_codes = %s", sorted(sample_codes))

    # ── 1. 找 CSV ──────────────────────────────────────────────────
    if args.csv:
        csv_paths = [Path(args.csv)]
    else:
        # mdl_msg_backup layout: {backup_dir}/{date}/mdl_X_Y_Z.csv
        date_dir = Path(args.backup_dir) / args.date
        if not date_dir.exists():
            log.error("backup dir not exist: %s", date_dir)
            sys.exit(2)
        csv_paths = sorted(date_dir.glob(cfg["csv_glob"]))
        if not csv_paths:
            log.error("no csv under %s matching %s", date_dir, cfg["csv_glob"])
            sys.exit(2)
    log.info("[1/5] CSV: %d file(s): %s", len(csv_paths),
             [p.name for p in csv_paths])
    csv_raw = load_csv(csv_paths, cfg, sample_codes)
    log.info("      rows after filter=%d", len(csv_raw))
    if csv_raw.empty:
        log.error("CSV empty after filter — sample_codes not found?")
        sys.exit(1)

    # ── 2. 加载 code map (先于 archive，因为 archive 谓词下推需要 SECURITY_ID)
    log.info("[2/5] Loading daily_basic code map ...")
    code_map = load_code_map(args.date, args.archive_dir, args.oss_bucket)
    log.info("      code_map rows=%d", len(code_map))

    # 把 sample_codes（6 位字符串）转成 SECURITY_ID int（archive 谓词下推用）
    cm = code_map.copy()
    cm["ID_QI"] = cm["ID_QI"].astype(str).str.strip().str.zfill(6)
    sample_sec_ids = None
    if sample_codes is not None:
        sub = cm[cm["ID_QI"].isin(sample_codes)]
        sample_sec_ids = set(sub["SECURITY_ID"].astype(int).tolist())
        log.info("      sample_codes → SECURITY_IDs: %s",
                 dict(zip(sub["ID_QI"], sub["SECURITY_ID"].astype(int))))

    # ── 3. 找 archive parquet ──────────────────────────────────────
    if args.parquet:
        parquet_path = args.parquet
    else:
        kind = cfg["archive_kind"]
        local_dir = Path(args.archive_dir) / args.date / kind
        chunks = sorted(local_dir.glob("*.parquet")) if local_dir.exists() else []
        if chunks:
            parquet_path = f"{local_dir}/*.parquet"
        else:
            y, m = args.date[:4], args.date[4:6]
            parquet_path = (f"{args.oss_bucket}/{y}/{y}{m}/{args.date}/"
                            f"{args.date}_{kind}.parquet")
    log.info("[3/5] Parquet: %s", parquet_path)
    arc_cols = ["Code", "SeqNum"] + list(cfg["field_map"].values())
    arc_df = load_parquet(parquet_path, sample_sec_ids, arc_cols)
    log.info("      rows after filter=%d", len(arc_df))

    # ── 4. 归一化 ──────────────────────────────────────────────────
    log.info("[4/5] Normalizing ...")
    csv_norm = normalize_csv(csv_raw, cfg, code_map)
    arc_norm = normalize_archive(arc_df, cfg)
    log.info("      csv mapped rows=%d (Code non-null=%d)",
             len(csv_norm), csv_norm["Code"].notna().sum())
    log.info("      arc rows=%d", len(arc_norm))

    # ── 5. Join + diff ─────────────────────────────────────────────
    log.info("[5/5] Joining on (Code, SeqNum) and diffing ...")
    keep_csv = ["Code", "SeqNum"] + list(cfg["field_map"].values())
    keep_csv = [c for c in keep_csv if c in csv_norm.columns]
    csv_join = csv_norm[keep_csv].dropna(subset=["Code", "SeqNum"])
    csv_join["Code"] = csv_join["Code"].astype("int64")
    csv_join["SeqNum"] = csv_join["SeqNum"].astype("int64")

    keep_arc = ["Code", "SeqNum"] + list(cfg["field_map"].values())
    keep_arc = [c for c in keep_arc if c in arc_norm.columns]
    arc_join = arc_norm[keep_arc].dropna(subset=["Code", "SeqNum"])
    arc_join["Code"] = arc_join["Code"].astype("int64")
    arc_join["SeqNum"] = arc_join["SeqNum"].astype("int64")

    merged = csv_join.merge(arc_join, on=["Code", "SeqNum"],
                            suffixes=("_csv", "_arc"), how="outer", indicator=True)
    csv_only = (merged["_merge"] == "left_only").sum()
    arc_only = (merged["_merge"] == "right_only").sum()
    both = (merged["_merge"] == "both").sum()
    log.info("      join: both=%d  csv_only=%d  arc_only=%d  (csv=%d, arc=%d)",
             both, csv_only, arc_only, len(csv_join), len(arc_join))

    if csv_only > 0 or arc_only > 0:
        log.warning("⚠ %d rows in CSV but not archive, %d in archive but not CSV",
                    csv_only, arc_only)
        if args.dump_unjoined:
            unj = merged[merged["_merge"] != "both"].copy()
            unj.to_csv("/tmp/csv_arc_unjoined.csv", index=False)
            log.info("      unjoined dumped to /tmp/csv_arc_unjoined.csv")

    both_df = merged[merged["_merge"] == "both"].copy()
    if both_df.empty:
        log.error("no rows joined — check code map / SeqNum column name")
        sys.exit(1)

    print()
    print(f"=== {args.type}  field-level diff (both sides joined: {len(both_df)}) ===")
    print(f"{'field':<20s} {'match/total':<14s} {'mismatch%':<10s} "
          f"{'max_diff':<12s} {'ratio_mode':<10s}")
    print("-" * 70)
    any_fail = False
    for arc_field in cfg["field_map"].values():
        csv_col = f"{arc_field}_csv"
        arc_col = f"{arc_field}_arc"
        if csv_col not in both_df.columns or arc_col not in both_df.columns:
            print(f"{arc_field:<20s} (column missing, skip)")
            continue
        is_price = arc_field in cfg["price_fields"]
        st = diff_field(both_df[csv_col], both_df[arc_col], is_price)
        pct = 100.0 * st["mismatch"] / st["total"] if st["total"] else 0
        flag = "OK" if st["mismatch"] == 0 else "FAIL"
        if st["mismatch"] > 0:
            any_fail = True
        rmode = f"{st['ratio_mode']:.3g}" if st["ratio_mode"] is not None else "-"
        print(f"{arc_field:<20s} {st['match']}/{st['total']:<10d} "
              f"{pct:>7.2f}%    {st['max_diff']:>10.4f}  {rmode:>8s}  {flag}")

    print()
    if any_fail:
        print("❌ 有字段不一致，看上面 FAIL 行的 ratio_mode 判断系统性偏差")
        print("   - ratio_mode≈100 (价格) 或 ≈1 (量)  →  单位/精度错位 (×10, ÷1000 等)")
        print("   - ratio_mode 散乱                   →  parser 解码错误或丢数据")
        sys.exit(1)
    else:
        print("✓ 所有字段在 join 上的行全部匹配（价格容忍 ±1 分 ROUND 误差）")


if __name__ == "__main__":
    main()
