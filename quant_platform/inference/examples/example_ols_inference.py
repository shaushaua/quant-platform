#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run inference from a model.joblib artifact produced by train_model_from_oss.py.
"""

import argparse
import gzip
import io
import json
import math
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional

import joblib
import oss2
import pandas as pd


DEFAULT_ID_COLS = ["date", "code", "_date", "_code6"]

DAILY_RESULT_RE = re.compile(
    r"/(?P<year>\d{4})/(?P<month>\d{6})/(?:\d{8}/)?"
    r"(?P<date>\d{8})(?:_s(?P<shard>\d+))?\.json$"
)

RISK_FACTOR_COLS = [
    "BETA",
    "MOMENTUM",
    "SIZE",
    "EARNYILD",
    "RESVOL",
    "GROWTH",
    "BTOP",
    "LEVERAGE",
    "LIQUIDTY",
    "SIZENL",
]

INDUSTRY_FACTOR_COLS = [
    "Agriculture",
    "Automobiles",
    "Banks",
    "BuildMater",
    "Chemicals",
    "Commerce",
    "Computers",
    "Conglomerates",
    "ConstrDecor",
    "Defense",
    "ElectricalEquip",
    "Electronics",
    "FoodBeverages",
    "HealthCare",
    "HomeAppliances",
    "Leisure",
    "LightIndustry",
    "MachineEquip",
    "Media",
    "Mining",
    "NonbankFinan",
    "NonferrousMetals",
    "RealEstate",
    "Steel",
    "Telecoms",
    "TextileGarment",
    "Transportation",
    "Utilities",
    "BasicChemicals",
    "BeautyCare",
    "Coal",
    "EnvironProtect",
    "Petroleum",
    "PowerEquip",
    "RetailTrade",
    "SocialServices",
]

DAILY_FEATURE_COLS = [
    "open",
    "high",
    "low",
    "close",
    "adj_open",
    "adj_close",
    "adj_high",
    "adj_low",
    "adj_pre_close",
    "deal_amount",
    "volume",
    "amount",
    "mkt_cap",
    "float_mkt_cap",
    "turnover_rate",
    "pe_ttm",
    "pb",
]

CACHE_STATS = {"hits": 0, "misses": 0, "writes": 0, "retries": 0, "list_retries": 0}


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def _make_bucket(bucket_name: str) -> oss2.Bucket:
    endpoint = os.environ.get("OSS_ENDPOINT", "").strip()
    if not endpoint:
        endpoint = "https://oss-cn-hangzhou.aliyuncs.com"
        os.environ["OSS_ENDPOINT"] = endpoint

    auth = oss2.Auth(
        _require_env("OSS_ACCESS_KEY_ID"),
        _require_env("OSS_ACCESS_KEY_SECRET"),
    )
    return oss2.Bucket(auth, endpoint, bucket_name)


def _cached_object_path(cache_dir: Path, bucket_name: str, key: str) -> Path:
    return cache_dir / bucket_name / key


def read_oss_object_cached(
    bucket: oss2.Bucket,
    bucket_name: str,
    key: str,
    cache_dir: Optional[Path],
    refresh_cache: bool,
    retries: int = 5,
    retry_sleep: float = 1.5,
) -> bytes:
    if cache_dir is not None:
        local_path = _cached_object_path(cache_dir, bucket_name, key)
        if local_path.exists() and local_path.stat().st_size > 0 and not refresh_cache:
            CACHE_STATS["hits"] += 1
            return local_path.read_bytes()

    CACHE_STATS["misses"] += 1
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            payload = bucket.get_object(key).read()
            break
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            CACHE_STATS["retries"] += 1
            sleep_seconds = retry_sleep * attempt
            print(
                f"[cache] download failed attempt {attempt}/{retries} "
                f"for oss://{bucket_name}/{key}: {exc}; retrying in {sleep_seconds:.1f}s"
            )
            time.sleep(sleep_seconds)

    if last_exc is not None and "payload" not in locals():
        raise last_exc

    if cache_dir is not None:
        local_path = _cached_object_path(cache_dir, bucket_name, key)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        tmp_path.write_bytes(payload)
        tmp_path.replace(local_path)
        CACHE_STATS["writes"] += 1

    return payload


def _normalize_date(value) -> Optional[str]:
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[:8] if len(digits) >= 8 else None


def _normalize_code6(value) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text.split(".")[0])
    if not digits:
        return None
    return digits[-6:].zfill(6)


def _parse_result_key(key: str) -> Optional[dict]:
    match = DAILY_RESULT_RE.search(key)
    if not match:
        return None
    shard = match.group("shard")
    return {
        "key": key,
        "date": match.group("date"),
        "shard": int(shard) if shard is not None else None,
    }


def _infer_date_from_key(key: str) -> Optional[str]:
    info = _parse_result_key(key)
    if info:
        return info["date"]
    base = key.rsplit("/", 1)[-1]
    digits = re.sub(r"\D", "", base)
    return digits[:8] if len(digits) >= 8 else None


def _month_starts(start_date: str, end_date: str) -> Iterable[str]:
    current = datetime.strptime(start_date, "%Y%m%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y%m%d").replace(day=1)
    while current <= end:
        yield current.strftime("%Y%m")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def iter_oss_keys(
    bucket: oss2.Bucket,
    prefix: str = "",
    retries: int = 5,
    retry_sleep: float = 1.5,
) -> Iterable[str]:
    marker = ""
    while True:
        last_exc = None
        result = None
        for attempt in range(1, retries + 1):
            try:
                result = bucket.list_objects(
                    prefix=prefix,
                    marker=marker,
                    max_keys=1000,
                )
                break
            except Exception as exc:
                last_exc = exc
                if attempt >= retries:
                    break
                CACHE_STATS["list_retries"] += 1
                sleep_seconds = retry_sleep * attempt
                print(
                    f"[oss-list] failed attempt {attempt}/{retries} "
                    f"for prefix={prefix!r} marker={marker!r}: {exc}; "
                    f"retrying in {sleep_seconds:.1f}s"
                )
                time.sleep(sleep_seconds)
        if result is None:
            raise last_exc

        for obj in result.object_list:
            yield obj.key

        if not result.is_truncated:
            break
        marker = result.next_marker


def iter_factor_keys(
    bucket: oss2.Bucket,
    prefix: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> Iterable[str]:
    base_prefix = prefix.strip("/")
    base_prefix = f"{base_prefix}/" if base_prefix else ""
    if start_date and end_date:
        list_prefixes = [
            f"{base_prefix}{year_month[:4]}/{year_month}/"
            for year_month in _month_starts(start_date, end_date)
        ]
    else:
        list_prefixes = [base_prefix]

    seen = set()
    for list_prefix in list_prefixes:
        for key in iter_oss_keys(bucket, prefix=list_prefix):
            if key in seen:
                continue
            seen.add(key)
            if key.startswith(f"{base_prefix}logs/"):
                continue
            if not key.endswith(".json") or key.endswith("/result.json"):
                continue
            info = _parse_result_key(key)
            if info is None:
                continue
            if start_date and info["date"] < start_date:
                continue
            if end_date and info["date"] > end_date:
                continue
            yield key


def list_factor_keys(
    bucket: oss2.Bucket,
    prefix: str,
    start_date: Optional[str],
    end_date: Optional[str],
    max_files: Optional[int],
    shard_mode: str,
) -> list[str]:
    by_date: dict[str, dict[str, list]] = {}
    for key in iter_factor_keys(bucket, prefix, start_date, end_date):
        info = _parse_result_key(key)
        if not info:
            continue
        date = info["date"]
        by_date.setdefault(date, {"full": [], "shards": []})
        if info["shard"] is None:
            by_date[date]["full"].append(key)
        else:
            by_date[date]["shards"].append((info["shard"], key))

    keys = []
    for date in sorted(by_date):
        full_keys = sorted(by_date[date]["full"])
        shard_keys = [key for _, key in sorted(by_date[date]["shards"])]
        if shard_mode == "auto":
            keys.extend(shard_keys if shard_keys else full_keys)
        elif shard_mode == "shards":
            keys.extend(shard_keys)
        elif shard_mode == "full":
            keys.extend(full_keys)
        elif shard_mode == "all":
            keys.extend(full_keys)
            keys.extend(shard_keys)
        else:
            raise RuntimeError(f"Unsupported shard mode: {shard_mode}")

    if start_date or end_date:
        start = start_date or "00000000"
        end = end_date or "99999999"
        keys = [key for key in keys if start <= (_infer_date_from_key(key) or "") <= end]
    keys = sorted(keys)
    if max_files is not None:
        keys = keys[:max_files]
    return keys


def load_factor_results(
    bucket: oss2.Bucket,
    bucket_name: str,
    prefix: str,
    start_date: Optional[str],
    end_date: Optional[str],
    max_files: Optional[int],
    shard_mode: str,
    cache_dir: Optional[Path],
    refresh_cache: bool,
) -> pd.DataFrame:
    keys = list_factor_keys(bucket, prefix, start_date, end_date, max_files, shard_mode)
    records = []
    print(f"[factors] loading {len(keys)} json files from prefix={prefix!r}")
    for key in keys:
        date_from_key = _infer_date_from_key(key)
        payload = read_oss_object_cached(bucket, bucket_name, key, cache_dir, refresh_cache)
        data = json.loads(payload)
        rows = data["records"] if isinstance(data, dict) and "records" in data else data
        if not isinstance(rows, list):
            print(f"[factors] skip unsupported json shape: {key}")
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if "date" not in row and date_from_key:
                row = {**row, "date": date_from_key}
            records.append(row)
    if not records:
        raise RuntimeError(f"No factor records found under prefix={prefix!r}")
    df = pd.DataFrame(records)
    print(f"[factors] loaded shape={df.shape}")
    return df


def _daily_basic_key(date_str: str) -> str:
    date_str = date_str.replace("-", "")
    year_month = date_str[:6]
    base_path = os.getenv("OSS_DATA_PATH", "2025").strip("/")
    return f"{base_path}/{year_month}/{date_str}/{date_str}_daily_basic_data.parquet"


def read_daily_basic_cached(
    bucket: oss2.Bucket,
    bucket_name: str,
    date_str: str,
    cache_dir: Optional[Path],
    refresh_cache: bool,
) -> pd.DataFrame:
    key = _daily_basic_key(date_str)
    payload = read_oss_object_cached(bucket, bucket_name, key, cache_dir, refresh_cache)
    return pd.read_parquet(io.BytesIO(payload))

def get_trading_days_from_oss(bucket: oss2.Bucket, start_date: str, end_date: str) -> list[str]:
    base_path = os.getenv("OSS_DATA_PATH", "2025").strip("/")
    days = set()
    for year_month in _month_starts(start_date, end_date):
        prefix = f"{base_path}/{year_month}/"
        for key in iter_oss_keys(bucket, prefix=prefix):
            if "daily_basic_data" not in key:
                continue
            parts = key.split("/")
            if len(parts) >= 3 and re.fullmatch(r"\d{8}", parts[-2]):
                day = parts[-2]
            else:
                day = _normalize_date(parts[-1])
            if day and start_date <= day <= end_date:
                days.add(day)
    return sorted(days)


def _daily_code_key(df: pd.DataFrame) -> pd.Series:
    if "ID_QI" in df.columns:
        return df["ID_QI"].map(_normalize_code6)
    if "ts_code" in df.columns:
        return df["ts_code"].map(_normalize_code6)
    if "Code" in df.columns:
        return df["Code"].map(_normalize_code6)
    if "code" in df.columns:
        return df["code"].map(_normalize_code6)
    raise RuntimeError(f"Cannot find code column in daily_basic: {list(df.columns)}")


def _market_factor_frame(
    daily: pd.DataFrame,
    columns: list[str],
    start_date: str,
    end_date: str,
    name: str,
) -> pd.DataFrame:
    present_cols = [col for col in columns if col in daily.columns]
    missing_cols = [col for col in columns if col not in daily.columns]
    if missing_cols:
        print(f"[market] missing {name} columns: {missing_cols}")

    base = daily.copy()
    base["_code6"] = _daily_code_key(base)
    base = base.dropna(subset=["_date", "_code6"])
    base = base[(base["_date"] >= start_date) & (base["_date"] <= end_date)]

    result = base[["_date", "_code6", *present_cols]].copy()
    result["date"] = result["_date"]
    result["code"] = result["_code6"]
    result = result[["date", "code", "_date", "_code6", *present_cols]]

    for col in present_cols:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    result = result.sort_values(["_date", "_code6"]).reset_index(drop=True)
    print(f"[market] {name} factors rows={len(result)} columns={len(present_cols)}")
    return result


def read_table(path: Path) -> pd.DataFrame:
    suffixes = "".join(path.suffixes[-2:])
    if path.suffix == ".csv":
        return pd.read_csv(
            path,
            dtype={"date": str, "code": str, "_date": str, "_code6": str},
        )
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if suffixes == ".jsonl.gz":
        rows = []
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    if path.suffix == ".jsonl":
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)
    raise RuntimeError(f"Unsupported input format: {path}")


def _load_feature_transform_callable(value, fmt: Optional[str]) -> Optional[Callable]:
    if value is None:
        return None
    if callable(value):
        return value
    if isinstance(value, (bytes, bytearray)):
        if fmt not in ("cloudpickle", "", None):
            raise RuntimeError(f"Unsupported feature transform format: {fmt}")
        try:
            import cloudpickle
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "cloudpickle is required to load the embedded feature transform callable"
            ) from exc
        return cloudpickle.loads(value)
    if isinstance(value, str):
        raise RuntimeError(
            "model.joblib contains a legacy string feature_transform. "
            "Regenerate the model so feature_transform is an embedded callable payload."
        )
    raise RuntimeError(f"Unsupported feature transform payload type: {type(value).__name__}")


def load_model_artifact(path: Path) -> tuple[object, list[str], Optional[pd.Series], Optional[Callable]]:
    artifact = joblib.load(path)

    if isinstance(artifact, dict) and "model" in artifact:
        model = artifact["model"]
        features = list(artifact.get("features", []))
        medians = artifact.get("medians")
        feature_transform = _load_feature_transform_callable(
            artifact.get("feature_transform"),
            artifact.get("feature_transform_format"),
        )
        if medians is not None and not isinstance(medians, pd.Series):
            medians = pd.Series(medians)
        if not features:
            raise RuntimeError("model.joblib artifact has no feature list")
        return model, features, medians, feature_transform

    raise RuntimeError(
        "Expected model.joblib to be a dict with keys: model, features, medians"
    )


def build_feature_matrix(
    df: pd.DataFrame,
    features: list[str],
    medians: Optional[pd.Series],
    feature_transform: Optional[Callable],
) -> pd.DataFrame:
    if feature_transform is not None:
        x = feature_transform(df=df, features=features, medians=medians)
        if not isinstance(x, pd.DataFrame):
            x = pd.DataFrame(x, index=df.index, columns=features)
        return x.reindex(columns=features).fillna(0.0)

    x = pd.DataFrame(index=df.index)
    for feature in features:
        if feature in df.columns:
            x[feature] = pd.to_numeric(df[feature], errors="coerce")
        else:
            x[feature] = float("nan")

    if medians is not None:
        x = x.fillna(medians.reindex(features))
    return x.fillna(0.0)


def normalize_factor_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    date_col = "date" if "date" in df.columns else "_date"
    if date_col not in df.columns:
        raise RuntimeError("Factor input must contain date or _date")
    code_candidates = ["code", "ID_QI", "ts_code", "Code", "stock_code", "_code6"]
    code_col = next((col for col in code_candidates if col in df.columns), None)
    if code_col is None:
        raise RuntimeError("Factor input must contain a stock code column")

    df["_date"] = df[date_col].map(_normalize_date)
    df["_code6"] = df[code_col].map(_normalize_code6)
    df["date"] = df["_date"]
    df["code"] = df["_code6"]
    return df.dropna(subset=["_date", "_code6"])


def load_market_frames(
    start_date: str,
    end_date: str,
    data_bucket: str,
    daily_lookback_days: int,
    cache_dir: Optional[Path],
    refresh_cache: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    market_bucket = _make_bucket(data_bucket)
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    history_calendar_days = max(365, daily_lookback_days * 3)
    history_start = (start_dt - timedelta(days=history_calendar_days)).strftime("%Y%m%d")
    all_days = get_trading_days_from_oss(market_bucket, history_start, end_date)
    start_idx = next((idx for idx, day in enumerate(all_days) if day >= start_date), None)
    if start_idx is None:
        raise RuntimeError(f"Cannot locate start_date={start_date} in trading days")
    feature_start_idx = max(0, start_idx - max(daily_lookback_days, 0))
    feature_start = all_days[feature_start_idx]
    days_to_load = all_days[feature_start_idx:]

    dfs = []
    for day in days_to_load:
        try:
            daily = read_daily_basic_cached(
                bucket=market_bucket,
                bucket_name=data_bucket,
                date_str=day,
                cache_dir=cache_dir,
                refresh_cache=refresh_cache,
            )
        except Exception as exc:
            print(f"[market] skip daily_basic {day}: {exc}")
            continue
        if daily.empty:
            continue
        daily = daily.copy()
        daily["_date"] = day
        dfs.append(daily)

    if not dfs:
        raise RuntimeError("No daily_basic rows loaded for inference")

    daily_all = pd.concat(dfs, ignore_index=True)
    risk = _market_factor_frame(daily_all, RISK_FACTOR_COLS, start_date, end_date, "risk")
    industry = _market_factor_frame(daily_all, INDUSTRY_FACTOR_COLS, start_date, end_date, "industry")
    daily_current = _market_factor_frame(daily_all, DAILY_FEATURE_COLS, start_date, end_date, "daily-current")
    daily_features = _market_factor_frame(
        daily_all,
        DAILY_FEATURE_COLS,
        feature_start,
        end_date,
        "daily-history",
    )
    return risk, industry, daily_current, daily_features


def merge_market_features(
    factors: pd.DataFrame,
    risk: pd.DataFrame,
    industry: pd.DataFrame,
    daily_current: pd.DataFrame,
) -> pd.DataFrame:
    result = normalize_factor_keys(factors)
    for frame in (risk, industry, daily_current):
        cols = [col for col in frame.columns if col not in {"date", "code"}]
        result = result.merge(
            frame[cols],
            on=["_date", "_code6"],
            how="left",
            suffixes=("", "_market"),
        )
    return result


def save_market_frames(
    output_path: Path,
    risk: pd.DataFrame,
    industry: pd.DataFrame,
    daily_features: pd.DataFrame,
) -> None:
    base_dir = output_path.parent / "market_data"
    base_dir.mkdir(parents=True, exist_ok=True)
    risk.to_csv(base_dir / "risk_factors.csv", index=False)
    industry.to_csv(base_dir / "industry_factors.csv", index=False)
    daily_features.to_csv(base_dir / "daily_features.csv", index=False)
    print(f"[market] wrote {base_dir}")


def make_tail_positions(
    pred: pd.Series,
    dates: pd.Series,
    tail_frac: float,
) -> pd.Series:
    if not 0 < tail_frac <= 0.5:
        raise RuntimeError("--position-tail-frac must be in (0, 0.5]")

    frame = pd.DataFrame(
        {
            "_date": dates.reindex(pred.index),
            "pred": pd.to_numeric(pred, errors="coerce"),
        },
        index=pred.index,
    )
    positions = pd.Series(0.0, index=pred.index, dtype="float64")
    valid = frame.dropna(subset=["_date", "pred"])

    for _, group in valid.groupby("_date", sort=False):
        n = len(group)
        if n < 2:
            continue
        k = max(1, math.ceil(n * tail_frac))
        k = min(k, n // 2)
        ranked = group["pred"].sort_values(kind="mergesort")
        short_idx = ranked.index[:k]
        long_idx = ranked.index[-k:]
        positions.loc[long_idx] = 1.0 / k
        positions.loc[short_idx] = -1.0 / k

    return positions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="Path to model.joblib")
    parser.add_argument("--input", help="Local feature input CSV/JSONL/Parquet")
    parser.add_argument("--prefix", help="OSS factor result prefix, e.g. eillen_protected")
    parser.add_argument("--result-bucket", default="stock-mdl-data-result")
    parser.add_argument("--data-bucket", default="quant-mdl-data")
    parser.add_argument("--start-date", help="YYYYMMDD factor date filter")
    parser.add_argument("--end-date", help="YYYYMMDD factor date filter")
    parser.add_argument(
        "--shard-mode",
        choices=["auto", "shards", "full", "all"],
        default="auto",
    )
    parser.add_argument("--max-files", type=int)
    parser.add_argument("--cache-dir", default="artifacts/oss_cache")
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--daily-lookback-days", type=int, default=180)
    parser.add_argument(
        "--save-market-data",
        action="store_true",
        help="Write risk/industry/daily market data beside prediction output",
    )
    parser.add_argument("--output", required=True, help="Output prediction CSV")
    parser.add_argument(
        "--id-cols",
        default=",".join(DEFAULT_ID_COLS),
        help="Comma-separated columns to keep in prediction output",
    )
    parser.add_argument("--pred-col", default="pred", help="Prediction column name")
    parser.add_argument(
        "--position-col",
        default="position",
        help="Output column for long/short position weights",
    )
    parser.add_argument(
        "--position-tail-frac",
        type=float,
        default=0.10,
        help="Per-date fraction to keep on each side. Default keeps top/bottom 10%%.",
    )
    parser.add_argument(
        "--keep-zero-positions",
        action="store_true",
        help="Keep middle stocks with zero position in the output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    model, features, medians, feature_transform = load_model_artifact(Path(args.model))
    cache_dir = None if args.no_cache else Path(args.cache_dir)

    if args.input:
        df = normalize_factor_keys(read_table(Path(args.input)))
    else:
        if not args.prefix or not args.start_date or not args.end_date:
            raise RuntimeError(
                "Pass either --input, or all of --prefix --start-date --end-date"
            )
        cache_dir = None if args.no_cache else Path(args.cache_dir)
        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)
            print(f"[cache] using {cache_dir.resolve()}")

        result_bucket = _make_bucket(args.result_bucket)
        factors = load_factor_results(
            result_bucket,
            bucket_name=args.result_bucket,
            prefix=args.prefix,
            start_date=_normalize_date(args.start_date),
            end_date=_normalize_date(args.end_date),
            max_files=args.max_files,
            shard_mode=args.shard_mode,
            cache_dir=cache_dir,
            refresh_cache=args.refresh_cache,
        )
        risk, industry, daily_current, daily_features = load_market_frames(
            start_date=_normalize_date(args.start_date),
            end_date=_normalize_date(args.end_date),
            data_bucket=args.data_bucket,
            daily_lookback_days=args.daily_lookback_days,
            cache_dir=cache_dir,
            refresh_cache=args.refresh_cache,
        )
        df = merge_market_features(factors, risk, industry, daily_current)

    x = build_feature_matrix(df, features, medians, feature_transform)
    pred = model.predict(x)

    id_cols = [col.strip() for col in args.id_cols.split(",") if col.strip()]
    output_cols = [col for col in id_cols if col in df.columns]
    out = df[output_cols].copy() if output_cols else pd.DataFrame(index=df.index)
    out[args.pred_col] = pred
    out[args.position_col] = make_tail_positions(
        out[args.pred_col],
        df["_date"],
        args.position_tail_frac,
    )
    if not args.keep_zero_positions:
        out = out[out[args.position_col] != 0.0].copy()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    if not args.input and args.save_market_data:
        save_market_frames(output_path, risk, industry, daily_features)
    print(f"[cache] stats: {CACHE_STATS}")
    print(f"[infer] rows={len(out)} features={len(features)} output={output_path}")


if __name__ == "__main__":
    main()


# ── Native Engine Integration Hook ─────────────────────────────────
#
# The NativeEngine calls this function when INFERENCE_MODULE points to this file.
#
# Contract:
#   inference(date_str, end_time, prev_day_factors, intraday_factors, daily_basic_df) -> pd.DataFrame
#   Returns: DataFrame with at least columns [code, pred, position]
#
_MODEL_ARTIFACT = None  # lazy-loaded on first call


def inference(date_str: str, end_time: str,
              prev_day_factors_df: pd.DataFrame,
              intraday_factors_df: pd.DataFrame,
              daily_basic_df: pd.DataFrame = None) -> pd.DataFrame:
    """Called by engine after each factor computation round.

    Args:
        date_str: Trading day, e.g. "20260605"
        end_time: Time slice, e.g. "093000"
        prev_day_factors_df: Previous trading day's factor results (daily frequency).
                             May be None on first day or in backtest first iteration.
        intraday_factors_df: Current round's factor results (intraday, minute-level).
        daily_basic_df: Engine's daily_basic DataFrame with market data
                        (risk factors, industry factors, daily features).

    Returns:
        DataFrame with code, pred, position columns.
        position > 0 = long, < 0 = short, 0 = no position.
    """
    global _MODEL_ARTIFACT

    if _MODEL_ARTIFACT is None:
        model_path = Path(__file__).parent / "example_model.joblib"
        _MODEL_ARTIFACT = load_model_artifact(model_path)

    model, features, medians, feature_transform = _MODEL_ARTIFACT

    # Use intraday factors as primary input
    df = normalize_factor_keys(intraday_factors_df)

    # Merge previous day's factors if available
    if prev_day_factors_df is not None and not prev_day_factors_df.empty:
        prev_df = normalize_factor_keys(prev_day_factors_df)
        # Rename factor columns to avoid collision
        prev_cols = [c for c in prev_df.columns
                     if c not in {"date", "code", "_date", "_code6"}]
        if prev_cols:
            prev_rename = {c: f"prev_{c}" for c in prev_cols}
            prev_subset = prev_df[["_date", "_code6"] + prev_cols].rename(columns=prev_rename)
            # Use previous date for merge key (shift back one day)
            prev_subset["_date"] = date_str
            df = df.merge(prev_subset, on=["_date", "_code6"], how="left")

    x = build_feature_matrix(df, features, medians, feature_transform)
    df["pred"] = model.predict(x)
    df["position"] = make_tail_positions(df["pred"], df["_date"], tail_frac=0.10)

    out = df[df["position"] != 0.0].copy()
    print(f"[inference] date={date_str} end_time={end_time} "
          f"total={len(df)} positions={len(out)}")
    return out[["code", "_code6", "date", "_date", "pred", "position"]]
