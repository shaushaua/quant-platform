#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Production inference for the tree daily feature model.

All market data and historical daily features are pulled from OSS with a local
cache to avoid repeated OSS traffic.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Optional

# Ensure the compiled Cython extension (calc_predict_tree_model.so) sitting
# next to this file is importable when this module is loaded as part of the
# quant_platform.inference package (otherwise only standalone script use
# would have its directory on sys.path).
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import oss2
import pandas as pd

try:
    from calc_predict_tree_model import (
        POSITION_COLUMNS,
        build_optimizer_exposure_frame,
        extract_current_position_weights,
        normalize_benchmark_weights,
        normalize_daily_feature_czhou1_keys,
        normalize_date_column,
        normalize_universe_codes,
        predict_tree_model,
        resolve_signal_date,
    )
except ModuleNotFoundError:
    from scripts.calc_predict_tree_model import (
        POSITION_COLUMNS,
        build_optimizer_exposure_frame,
        extract_current_position_weights,
        normalize_benchmark_weights,
        normalize_daily_feature_czhou1_keys,
        normalize_date_column,
        normalize_universe_codes,
        predict_tree_model,
        resolve_signal_date,
    )

DEFAULT_OUTPUT = Path("artifacts/tree_model_positions.csv")
OSS_ENDPOINT = "https://oss-cn-hangzhou.aliyuncs.com"
OSS_DATA_BUCKET = "quant-mdl-data"
OSS_RESULT_BUCKET = "stock-mdl-data-result"
OSS_MODEL_BUCKET = OSS_RESULT_BUCKET
OSS_MODEL_KEY = "models/models_20260623_encrypted.pkl"
OSS_DAILY_BASIC_PREFIX = ""
OSS_INDEX_COMPOSITION_PREFIX = ""
OSS_DAILY_FEATURE_PREFIX = "protected-eillen-strategy-v2"
OSS_DAILY_FEATURE_LIVE_BUCKET = "stock-mdl-data-result"
OSS_DAILY_FEATURE_LIVE_PREFIX = "live-factors"
OSS_CACHE_DIR = Path(os.environ.get("TREE_MODEL_OSS_CACHE_DIR", "artifacts/oss_cache/tree_model"))
OSS_METADATA_CACHE_TTL_SECONDS = 60 * 60
DEFAULT_MODEL_PATH = OSS_CACHE_DIR / OSS_MODEL_BUCKET / OSS_MODEL_KEY
DAILY_BASIC_LOOKBACK_DAYS = 400
DAILY_FEATURE_CZHOU1_LOOKBACK_DAYS = 20
TALIB_DROPNA_THRESH = 20
APPLY_TRADE_MASK = True
MIN_ADV = 35000000.0
ADV_WINDOW = 252
ADV_MIN_PERIODS = 10
JUNK_MKT_CAP = 2e9
JUNK_FLOAT_MKT_CAP = 1e9
MIN_PRICE = 1.0
MAX_PRICE = 500.0
POSITION_TAIL_FRAC = 0.10
ENABLE_SHORT = False
KEEP_ZERO_POSITIONS = False
ALLOW_MISSING_FEATURES = False
INCLUDE_DEBUG_COLS = False
USE_OPTIMIZER = True
OPTIMIZER_MAX_TURNOVER = 0.35
OPTIMIZER_MAX_WEIGHT = 0.01
OPTIMIZER_CASH_RATIO = 0.0
OPTIMIZER_TRANS_COST = 0.0003
OPTIMIZER_STYLE_EXPOSURE_TOL = 0.30
OPTIMIZER_INDUSTRY_EXPOSURE_TOL = 0.30
OPTIMIZER_MIN_BENCHMARK_CONSTITUENT_WEIGHT = 0.30
MORNING_SIGNAL_CUTOFF = "093000"
POSITION_OUTPUT: Optional[Path] = None

_OSS_BUCKETS: dict[str, oss2.Bucket] = {}
_DAILY_FEATURE_CACHE: dict[str, pd.DataFrame] = {}


def normalize_date(value) -> Optional[str]:
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[:8] if len(digits) >= 8 else None


def normalize_code6(value) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"\D", "", text.split(".")[0])
    if not digits:
        return None
    return digits[-6:].zfill(6)


def order_code(code6: str) -> str:
    code6 = str(code6).zfill(6)
    return ("SH" if code6.startswith("6") else "SZ") + code6


def parse_hhmmss(value) -> int:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return 0
    return int(digits[:6].ljust(6, "0"))


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def make_bucket(bucket_name: str) -> oss2.Bucket:
    bucket = _OSS_BUCKETS.get(bucket_name)
    if bucket is not None:
        return bucket
    auth = oss2.Auth(require_env("OSS_ACCESS_KEY_ID"), require_env("OSS_ACCESS_KEY_SECRET"))
    bucket = oss2.Bucket(auth, OSS_ENDPOINT, bucket_name)
    _OSS_BUCKETS[bucket_name] = bucket
    return bucket


def cache_path(bucket_name: str, key: str) -> Path:
    return OSS_CACHE_DIR / bucket_name / key


def is_cached_oss_object(bucket_name: str, key: str) -> bool:
    local_path = cache_path(bucket_name, key)
    return local_path.exists() and local_path.stat().st_size > 0


def metadata_cache_path(bucket_name: str, category: str, cache_key: str) -> Path:
    digest = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
    return OSS_CACHE_DIR / "_metadata" / bucket_name / category / f"{digest}.json"


def read_metadata_cache(bucket_name: str, category: str, cache_key: str):
    path = metadata_cache_path(bucket_name, category, cache_key)
    if not path.exists() or path.stat().st_size <= 0:
        return None
    if time.time() - path.stat().st_mtime > OSS_METADATA_CACHE_TTL_SECONDS:
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_metadata_cache(bucket_name: str, category: str, cache_key: str, payload) -> None:
    path = metadata_cache_path(bucket_name, category, cache_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    tmp_path.replace(path)


def read_oss_object_cached(
    bucket_name: str,
    key: str,
    retries: int = 5,
    retry_sleep: float = 1.5,
) -> bytes:
    local_path = cache_path(bucket_name, key)
    if local_path.exists() and local_path.stat().st_size > 0:
        return local_path.read_bytes()

    bucket = make_bucket(bucket_name)
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            payload = bucket.get_object(key).read()
            break
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(retry_sleep * attempt)
    else:
        payload = None

    if last_exc is not None and "payload" not in locals():
        raise last_exc

    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
    tmp_path.write_bytes(payload)
    tmp_path.replace(local_path)
    return payload


def load_model_from_oss(model_key: str = OSS_MODEL_KEY, model_bucket: str = OSS_MODEL_BUCKET) -> Path:
    model_key = model_key.strip("/")
    if not model_key:
        raise RuntimeError("model OSS key cannot be empty")
    read_oss_object_cached(model_bucket, model_key)
    local_path = cache_path(model_bucket, model_key)
    print(f"[tree-infer] model cached bucket={model_bucket} key={model_key} local={local_path}")
    return local_path


def iter_oss_keys(bucket_name: str, prefix: str, use_cache: bool = True) -> Iterable[str]:
    if use_cache:
        cached = read_metadata_cache(bucket_name, "list", prefix)
        if cached is not None:
            yield from cached
            return

    bucket = make_bucket(bucket_name)
    marker = ""
    keys = []
    while True:
        result = bucket.list_objects(prefix=prefix, marker=marker, max_keys=1000)
        for obj in result.object_list:
            keys.append(obj.key)
        if not result.is_truncated:
            break
        marker = result.next_marker
    if keys and use_cache:
        write_metadata_cache(bucket_name, "list", prefix, keys)
    yield from keys


def iter_calendar_dates(start_date: str, end_date: str) -> Iterable[str]:
    current = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def daily_basic_prefix(date_str: str) -> str:
    prefix = OSS_DAILY_BASIC_PREFIX.strip("/")
    suffix = f"{date_str[:4]}/{date_str[:6]}/{date_str}/"
    return f"{prefix}/{suffix}" if prefix else suffix


def daily_basic_key(date_str: str) -> str:
    return f"{daily_basic_prefix(date_str)}{date_str}_daily_basic_data.parquet"


def index_composition_prefix(date_str: str) -> str:
    prefix = OSS_INDEX_COMPOSITION_PREFIX.strip("/")
    suffix = f"{date_str[:4]}/{date_str[:6]}/{date_str}/"
    return f"{prefix}/{suffix}" if prefix else suffix


def index_composition_key(date_str: str) -> str:
    return f"{index_composition_prefix(date_str)}{date_str}_composition.parquet"


def oss_object_exists(bucket_name: str, key: str, use_cache: bool = True) -> bool:
    if is_cached_oss_object(bucket_name, key):
        return True

    if use_cache:
        cached = read_metadata_cache(bucket_name, "exists", key)
        if cached is not None:
            return bool(cached.get("exists"))

    try:
        make_bucket(bucket_name).head_object(key)
        if use_cache:
            write_metadata_cache(bucket_name, "exists", key, {"exists": True})
        return True
    except Exception as exc:
        status = getattr(exc, "status", None)
        if status == 404 or exc.__class__.__name__ in {"NoSuchKey", "NotFound"}:
            if use_cache:
                write_metadata_cache(bucket_name, "exists", key, {"exists": False})
            return False
        raise


def load_daily_basic_from_oss(start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    for date_item in iter_calendar_dates(start_date, end_date):
        key = daily_basic_key(date_item)
        if not oss_object_exists(OSS_DATA_BUCKET, key):
            continue
        payload = read_oss_object_cached(OSS_DATA_BUCKET, key)
        frame = pd.read_parquet(io.BytesIO(payload))
        if "trade_date" not in frame.columns:
            frame["trade_date"] = date_item
        frames.append(frame)

    if not frames:
        raise RuntimeError(f"No daily_basic parquet found on OSS between {start_date} and {end_date}")
    combined = pd.concat(frames, ignore_index=True, copy=False)
    # Coerce numeric columns that concat may have promoted to object (different
    # daily parquets have inconsistent dtypes) back to float64. The compiled
    # .so (value_weight) does Python-level true division on object Series which
    # raises ZeroDivisionError; same op on float64 returns inf.
    _STRING_COLS = {"TS", "ID_QI", "SEC_SHORT_NAME", "SEC_FULL_NAME",
                    "trade_date", "date", "idx_EXCHANGE_CD", "idx_UPDATE_TIME",
                    "UPDATE_TIME"}
    for col in combined.columns:
        if col in _STRING_COLS:
            # Force string dtype — some rows may be pd.Timestamp objects from
            # parquet decode (idx_UPDATE_TIME etc.); pollers/pyarrow choke on
            # mixed str/Timestamp in object columns.
            combined[col] = combined[col].astype(str).replace({"NaT": "", "nan": "", "None": ""})
            continue
        if combined[col].dtype == object:
            combined[col] = pd.to_numeric(combined[col], errors="ignore")
    return combined


def load_index_composition_from_oss(start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    for date_item in iter_calendar_dates(start_date, end_date):
        key = index_composition_key(date_item)
        if not oss_object_exists(OSS_DATA_BUCKET, key):
            continue
        payload = read_oss_object_cached(OSS_DATA_BUCKET, key)
        frame = pd.read_parquet(io.BytesIO(payload))
        if "trade_date" not in frame.columns and "date" not in frame.columns:
            frame["trade_date"] = date_item
        frames.append(frame)

    if not frames:
        raise RuntimeError(f"No index composition parquet found on OSS between {start_date} and {end_date}")
    return pd.concat(frames, ignore_index=True, copy=False)


def daily_feature_czhou1_prefix(date_str: str) -> str:
    return f"{OSS_DAILY_FEATURE_PREFIX.strip('/')}/{date_str[:4]}/{date_str[:6]}/{date_str}/"


def daily_feature_czhou1_live_keys(date_str: str) -> list[str]:
    base = f"{OSS_DAILY_FEATURE_LIVE_PREFIX.strip('/')}/{date_str[:4]}/{date_str[:6]}/{date_str}"
    return [
        f"{base}/daily-feature/daily.parquet",
        f"{base}/daily.parquet",
    ]


def date_from_oss_key(key: str) -> Optional[str]:
    matches = re.findall(r"\d{8}", key)
    return matches[-1] if matches else None


def sort_daily_feature_czhou1_key(key: str) -> tuple:
    name = key.rsplit("/", 1)[-1]
    match = re.search(r"(?P<date>\d{8})_s(?P<shard>\d+)(?:_p(?P<part>\d+))?\.parquet$", name)
    if not match:
        return (key, -1, -1)
    return (
        match.group("date"),
        int(match.group("shard")),
        int(match.group("part") or -1),
        key,
    )


def load_daily_feature_czhou1_key(key: str) -> pd.DataFrame:
    if key not in _DAILY_FEATURE_CACHE:
        payload = read_oss_object_cached(OSS_RESULT_BUCKET, key)
        frame = pd.read_parquet(io.BytesIO(payload))
        date_value = date_from_oss_key(key)
        if "date" not in frame.columns and "trade_date" not in frame.columns and date_value:
            frame["date"] = date_value
        _DAILY_FEATURE_CACHE[key] = frame
    return _DAILY_FEATURE_CACHE[key]


def load_daily_feature_czhou1_live_key(key: str) -> pd.DataFrame:
    cache_key = f"{OSS_DAILY_FEATURE_LIVE_BUCKET}/{key}"
    if cache_key not in _DAILY_FEATURE_CACHE:
        payload = read_oss_object_cached(OSS_DAILY_FEATURE_LIVE_BUCKET, key)
        frame = pd.read_parquet(io.BytesIO(payload))
        date_value = date_from_oss_key(key)
        if "date" not in frame.columns and "trade_date" not in frame.columns and date_value:
            frame["date"] = date_value
        _DAILY_FEATURE_CACHE[cache_key] = frame
    return _DAILY_FEATURE_CACHE[cache_key]


def load_daily_feature_czhou1_from_oss(start_date: str, end_date: str) -> pd.DataFrame:
    dated_keys = []
    today = datetime.now().strftime("%Y%m%d")
    for date_item in iter_calendar_dates(start_date, end_date):
        prefix = daily_feature_czhou1_prefix(date_item)
        use_list_cache = date_item < today
        day_keys = [
            key for key in iter_oss_keys(OSS_RESULT_BUCKET, prefix, use_cache=use_list_cache)
            if key.endswith(".parquet")
        ]
        if day_keys:
            dated_keys.extend(
                (date_item, "old", key)
                for key in sorted(day_keys, key=sort_daily_feature_czhou1_key)
            )
            continue

        for live_key in daily_feature_czhou1_live_keys(date_item):
            if oss_object_exists(OSS_DAILY_FEATURE_LIVE_BUCKET, live_key, use_cache=False):
                dated_keys.append((date_item, "live", live_key))
                break

    if not dated_keys:
        raise RuntimeError(
            f"No daily feature parquet found under {OSS_DAILY_FEATURE_PREFIX!r} "
            f"or {OSS_DAILY_FEATURE_LIVE_PREFIX!r} "
            f"between {start_date} and {end_date}"
        )

    frames = []
    for _, source, key in sorted(dated_keys, key=lambda item: (item[0], item[2])):
        if source == "old":
            frames.append(load_daily_feature_czhou1_key(key))
        else:
            frames.append(load_daily_feature_czhou1_live_key(key))
    return pd.concat(frames, ignore_index=True, copy=False)


def load_production_daily_basic(date_str: str, current_daily_basic: Optional[pd.DataFrame]) -> pd.DataFrame:
    date_str = normalize_date(date_str)
    if current_daily_basic is not None and not current_daily_basic.empty:
        current = normalize_daily_basic(current_daily_basic, date_str=date_str)
        current = current.loc[current["trade_date"].astype(str) <= date_str].copy()
        current_dates = pd.Index(current["trade_date"].dropna().astype(str).unique())
        # Need BOTH multi-day history AND the Barra columns the .so expects.
        # The live engine passes a single-day 22-col market df (no BETA/MOMENTUM)
        # which isn't sufficient for inference — fall through to OSS load.
        has_barra = "BETA" in current.columns
        if (current_dates < date_str).any() and has_barra and len(current_dates) >= 5:
            return current

    start = (datetime.strptime(date_str, "%Y%m%d") - timedelta(days=DAILY_BASIC_LOOKBACK_DAYS)).strftime("%Y%m%d")
    history = normalize_daily_basic(load_daily_basic_from_oss(start, date_str))
    return append_current_daily_basic(history, current_daily_basic, date_str)


def write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        df.to_parquet(path, index=False)
    elif suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix in {".pkl", ".pickle"}:
        df.to_pickle(path)
    else:
        raise RuntimeError(f"Unsupported output format: {path}")


def normalize_daily_basic(df: pd.DataFrame, date_str: Optional[str] = None) -> pd.DataFrame:
    if isinstance(df.index, pd.MultiIndex):
        index_names = set(name for name in df.index.names if name is not None)
        if {"trade_date", "ID_QI"}.issubset(index_names):
            df = df.reset_index()
    else:
        df = df.copy()

    if "trade_date" not in df.columns:
        if "date" in df.columns:
            df["trade_date"] = df["date"]
        elif "_date" in df.columns:
            df["trade_date"] = df["_date"]
        elif "TS" in df.columns:
            df["trade_date"] = pd.to_datetime(
                df["TS"].astype(str).str.replace(" CST", "", regex=False),
                errors="coerce",
            ).dt.strftime("%Y%m%d")
        elif date_str:
            df["trade_date"] = date_str
        else:
            raise RuntimeError("daily_basic must contain trade_date/date/TS, or pass date_str")

    if "ID_QI" not in df.columns:
        for candidate in ("code", "ts_code", "TICKER_SYMBOL"):
            if candidate in df.columns:
                df["ID_QI"] = df[candidate]
                break
    if "ID_QI" not in df.columns:
        raise RuntimeError("daily_basic must contain ID_QI/code/ts_code/TICKER_SYMBOL")

    df = df.copy()
    df["trade_date"] = df["trade_date"].map(normalize_date)
    df["ID_QI"] = df["ID_QI"].map(normalize_code6)
    df = df.dropna(subset=["trade_date", "ID_QI"])
    return df.sort_values(["trade_date", "ID_QI"])


def append_current_daily_basic(history: pd.DataFrame, current: Optional[pd.DataFrame], date_str: str) -> pd.DataFrame:
    if current is None or current.empty:
        return history
    current = normalize_daily_basic(current, date_str=date_str)
    combined = pd.concat([history, current], ignore_index=True, copy=False)
    return combined.drop_duplicates(["trade_date", "ID_QI"], keep="last")


def inference(
    date_str: str,
    end_time: str,
    prev_day_factors_df: Optional[pd.DataFrame],
    intraday_factors_df: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame] = None,
    trading_universe_df: Optional[pd.DataFrame] = None,
    index_composition_df: Optional[pd.DataFrame] = None,
    portfolio_context=None,
) -> pd.DataFrame:
    daily_basic = load_production_daily_basic(date_str, current_daily_basic=daily_basic_df)
    universe = normalize_universe_codes(trading_universe_df)
    # Tolerate backtest-format daily factors that carry `code` (6-digit) instead
    # of `ID_QI` (e.g. merged protected-eillen-strategy-v2 shards). Bridge both
    # formats before any universe filter or .so ingestion. Use normalize_code6
    # so int-typed code columns ("000001" -> 1) still map to "000001".
    if (prev_day_factors_df is not None and not prev_day_factors_df.empty
            and "ID_QI" not in prev_day_factors_df.columns
            and "code" in prev_day_factors_df.columns):
        prev_day_factors_df = prev_day_factors_df.copy()
        prev_day_factors_df["ID_QI"] = (
            prev_day_factors_df["code"].map(normalize_code6)
        )
    if universe is not None:
        daily_basic = daily_basic[daily_basic["ID_QI"].isin(universe)]
        prev_day_factors_df = prev_day_factors_df[prev_day_factors_df["ID_QI"].isin(universe)]
    # prev_day_factors_df is the live T-1 daily parquet (ID_QI + F_* cols).
    # The .so's normalize_daily_feature_czhou1_keys expects a trade_date key
    # column; inject it from the previous trading day derived from daily_basic.
    if prev_day_factors_df is not None and not prev_day_factors_df.empty:
        if "trade_date" not in prev_day_factors_df.columns and "date" not in prev_day_factors_df.columns:
            prev_dates = sorted(daily_basic["trade_date"].dropna().astype(str).unique())
            prev_date = prev_dates[-2] if len(prev_dates) >= 2 else (prev_dates[0] if prev_dates else date_str)
            prev_day_factors_df = prev_day_factors_df.copy()
            prev_day_factors_df["trade_date"] = prev_date

    last_index_csi_all = None
    if index_composition_df is not None and not index_composition_df.empty:
        index_csi_all = index_composition_df
        # Long format (INDEX_CODE + weight columns): filter to 000985 directly
        if "INDEX_CODE" in index_csi_all.columns and "weight" in index_csi_all.columns:
            index_csi_all = index_csi_all[index_csi_all["INDEX_CODE"].astype(str) == "000985"]
            if not index_csi_all.empty:
                date_col = "trade_date" if "trade_date" in index_csi_all.columns else "date" if "date" in index_csi_all.columns else None
                if date_col is not None:
                    index_csi_all = index_csi_all[index_csi_all[date_col].astype(str) == index_csi_all[date_col].astype(str).max()]
                last_index_csi_all = index_csi_all[["ID_QI", "weight"]].set_index("ID_QI")["weight"]
        # Wide format (weight_000985 column from compute_index_composition)
        elif "weight_000985" in index_csi_all.columns:
            sub = index_csi_all[["ID_QI", "weight_000985"]].dropna(subset=["weight_000985"])
            sub = sub[sub["weight_000985"] > 0]
            if not sub.empty:
                last_index_csi_all = sub.set_index("ID_QI")["weight_000985"]
            # Melt wide → long so the .so (which expects INDEX_CODE/trade_date/
            # weight columns) gets the same data shape as load_index_composition_from_oss.
            # Without this, passing wide format causes KeyError 'INDEX_CODE' at
            # predict_tree_model:2252 and passing None changes ~95% of positions.
            weight_cols = [c for c in index_composition_df.columns if c.startswith("weight_")]
            if weight_cols:
                long_frames = []
                for wc in weight_cols:
                    code = wc[len("weight_"):]
                    sub_l = index_composition_df[["ID_QI", wc]].rename(columns={wc: "weight"})
                    sub_l = sub_l[sub_l["weight"].fillna(0) > 0]
                    sub_l["INDEX_CODE"] = code
                    long_frames.append(sub_l)
                if long_frames:
                    index_composition_df = pd.concat(long_frames, ignore_index=True)
                    index_composition_df["trade_date"] = date_str
    signal_date = resolve_signal_date(
        daily_basic=daily_basic,
        trade_date=date_str,
        end_time=end_time,
        morning_cutoff=MORNING_SIGNAL_CUTOFF,
    )
    all_codes = pd.Index(daily_basic["ID_QI"].dropna().astype(str).unique()).sort_values()
    current_weight = extract_current_position_weights(portfolio_context, all_codes)
    benchmark_weight = normalize_benchmark_weights(last_index_csi_all)
    exposure_frame = build_optimizer_exposure_frame(daily_basic, signal_date)
    model_path = (
        DEFAULT_MODEL_PATH
        if Path(DEFAULT_MODEL_PATH).exists() and Path(DEFAULT_MODEL_PATH).stat().st_size > 0
        else load_model_from_oss(OSS_MODEL_KEY, OSS_MODEL_BUCKET)
    )

    out = predict_tree_model(
        daily_basic=daily_basic,
        daily_feature_czhou1=prev_day_factors_df,
        model_path=model_path,
        start_date=signal_date,
        end_date=signal_date,
        output_date=date_str,
        composition_df=(index_composition_df if (index_composition_df is not None
                                                and "INDEX_CODE" in (index_composition_df.columns if hasattr(index_composition_df, "columns") else []))
                        else None),
        talib_dropna_thresh=TALIB_DROPNA_THRESH,
        apply_trade_mask=APPLY_TRADE_MASK,
        min_adv=MIN_ADV,
        adv_window=ADV_WINDOW,
        adv_min_periods=ADV_MIN_PERIODS,
        junk_mkt_cap=JUNK_MKT_CAP,
        junk_float_mkt_cap=JUNK_FLOAT_MKT_CAP,
        min_price=MIN_PRICE,
        max_price=MAX_PRICE,
        tail_frac=POSITION_TAIL_FRAC,
        enable_short=ENABLE_SHORT,
        keep_zero_positions=KEEP_ZERO_POSITIONS,
        allow_missing_features=ALLOW_MISSING_FEATURES,
        include_debug_cols=INCLUDE_DEBUG_COLS,
        use_optimizer=USE_OPTIMIZER,
        current_weight=current_weight,
        optimizer_max_turnover=OPTIMIZER_MAX_TURNOVER,
        optimizer_max_weight=OPTIMIZER_MAX_WEIGHT,
        optimizer_cash_ratio=OPTIMIZER_CASH_RATIO,
        optimizer_trans_cost=OPTIMIZER_TRANS_COST,
        benchmark_weight=benchmark_weight,
        exposure_frame=exposure_frame,
        optimizer_style_exposure_tol=OPTIMIZER_STYLE_EXPOSURE_TOL,
        optimizer_industry_exposure_tol=OPTIMIZER_INDUSTRY_EXPOSURE_TOL,
        optimizer_min_benchmark_constituent_weight=OPTIMIZER_MIN_BENCHMARK_CONSTITUENT_WEIGHT,
    )

    if universe is not None:
        out = out[out["_code6"].isin(set(universe))].copy()

    print(
        f"[tree-inference] date={date_str} end_time={end_time} "
        f"signal_date={signal_date} positions={len(out)} model={model_path}"
    )
    if POSITION_OUTPUT is not None:
        write_table(out, POSITION_OUTPUT)
        print(f"[tree-inference] wrote positions={len(out)} output={POSITION_OUTPUT}")
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model",
        "--model-key",
        dest="model_key",
        default=OSS_MODEL_KEY,
        help="OSS object key for encrypted model.",
    )
    parser.add_argument("--model-bucket", default=OSS_MODEL_BUCKET)
    parser.add_argument("--start-date", default="20260625")
    parser.add_argument("--end-date", default="20260625")
    parser.add_argument("--end-time", default="145000")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--talib-dropna-thresh", type=int, default=20)
    parser.add_argument("--min-adv", type=float, default=35000000)
    parser.add_argument("--adv-window", type=int, default=252)
    parser.add_argument("--adv-min-periods", type=int, default=10)
    parser.add_argument("--junk-mkt-cap", type=float, default=2e9)
    parser.add_argument("--junk-float-mkt-cap", type=float, default=1e9)
    parser.add_argument("--min-price", type=float, default=1.0)
    parser.add_argument("--max-price", type=float, default=500.0)
    parser.add_argument("--position-tail-frac", type=float, default=0.10)
    parser.add_argument("--enable-short", action="store_true")
    parser.add_argument("--long-only", action="store_true")
    parser.add_argument("--no-trade-mask", action="store_true")
    parser.add_argument("--keep-zero-positions", action="store_true")
    parser.add_argument("--allow-missing-features", action="store_true")
    parser.add_argument("--include-debug-cols", action="store_true")
    parser.add_argument("--no-optimizer", action="store_true")
    parser.add_argument("--optimizer-max-turnover", type=float, default=0.35)
    parser.add_argument("--optimizer-max-weight", type=float, default=0.01)
    parser.add_argument("--optimizer-cash-ratio", type=float, default=0.0)
    parser.add_argument("--optimizer-trans-cost", type=float, default=0.0003)
    return parser.parse_args()


def main() -> None:
    global DEFAULT_MODEL_PATH
    global OSS_MODEL_BUCKET, OSS_MODEL_KEY
    global TALIB_DROPNA_THRESH, APPLY_TRADE_MASK, MIN_ADV, ADV_WINDOW, ADV_MIN_PERIODS
    global JUNK_MKT_CAP, JUNK_FLOAT_MKT_CAP, MIN_PRICE, MAX_PRICE
    global POSITION_TAIL_FRAC, ENABLE_SHORT, KEEP_ZERO_POSITIONS, ALLOW_MISSING_FEATURES
    global INCLUDE_DEBUG_COLS, USE_OPTIMIZER
    global OPTIMIZER_MAX_TURNOVER, OPTIMIZER_MAX_WEIGHT, OPTIMIZER_CASH_RATIO, OPTIMIZER_TRANS_COST

    args = parse_args()
    start_date = normalize_date(args.start_date)
    end_date = normalize_date(args.end_date or args.start_date)

    OSS_MODEL_BUCKET = args.model_bucket
    OSS_MODEL_KEY = args.model_key
    DEFAULT_MODEL_PATH = load_model_from_oss(OSS_MODEL_KEY, OSS_MODEL_BUCKET)
    TALIB_DROPNA_THRESH = args.talib_dropna_thresh
    APPLY_TRADE_MASK = not args.no_trade_mask
    MIN_ADV = args.min_adv
    ADV_WINDOW = args.adv_window
    ADV_MIN_PERIODS = args.adv_min_periods
    JUNK_MKT_CAP = args.junk_mkt_cap
    JUNK_FLOAT_MKT_CAP = args.junk_float_mkt_cap
    MIN_PRICE = args.min_price
    MAX_PRICE = args.max_price
    POSITION_TAIL_FRAC = args.position_tail_frac
    ENABLE_SHORT = args.enable_short and not args.long_only
    KEEP_ZERO_POSITIONS = args.keep_zero_positions
    ALLOW_MISSING_FEATURES = args.allow_missing_features
    INCLUDE_DEBUG_COLS = args.include_debug_cols
    USE_OPTIMIZER = not args.no_optimizer
    OPTIMIZER_MAX_TURNOVER = args.optimizer_max_turnover
    OPTIMIZER_MAX_WEIGHT = args.optimizer_max_weight
    OPTIMIZER_CASH_RATIO = args.optimizer_cash_ratio
    OPTIMIZER_TRANS_COST = args.optimizer_trans_cost

    lookback_start = (
        datetime.strptime(start_date, "%Y%m%d") - timedelta(days=DAILY_BASIC_LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    daily_basic_all = normalize_daily_basic(load_daily_basic_from_oss(lookback_start, end_date))
    daily_feature_start = (
        datetime.strptime(start_date, "%Y%m%d") - timedelta(days=DAILY_FEATURE_CZHOU1_LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    daily_feature_czhou1_all = load_daily_feature_czhou1_from_oss(daily_feature_start, end_date)
    daily_feature_czhou1_all = normalize_daily_feature_czhou1_keys(daily_feature_czhou1_all)
    index_composition_all = load_index_composition_from_oss(lookback_start, end_date)
    index_composition_all = normalize_date_column(index_composition_all)

    outputs = []
    last_positions = pd.DataFrame()
    for date_item in iter_calendar_dates(start_date, end_date):
        daily_basic_history = daily_basic_all.loc[daily_basic_all["trade_date"].astype(str) <= date_item].copy()
        daily_feature_czhou1_history = daily_feature_czhou1_all.loc[
            daily_feature_czhou1_all["trade_date"].astype(str) <= date_item
        ].copy()
        index_composition_history = index_composition_all.loc[
            index_composition_all["trade_date"].astype(str) <= date_item
        ].copy()
        portfolio_context = (
            SimpleNamespace(positions=last_positions)
            if not last_positions.empty
            else None
        )
        trading_universe_df = index_composition_history.pipe(lambda df_:
                                            df_[df_["trade_date"]==df_["trade_date"].max()]
                                            )["ID_QI"].unique()

        out_i = inference(
            date_str=date_item,
            end_time=args.end_time,
            prev_day_factors_df=daily_feature_czhou1_history,
            intraday_factors_df=pd.DataFrame(),
            daily_basic_df=daily_basic_history,
            trading_universe_df=trading_universe_df,
            index_composition_df=index_composition_history,
            portfolio_context=portfolio_context,
        )
        outputs.append(out_i)
        last_positions = out_i

    out = pd.concat(outputs, ignore_index=True, copy=False) if outputs else pd.DataFrame(columns=POSITION_COLUMNS)
    output = Path(args.output)
    write_table(out, output)
    print(f"[tree-infer] rows={len(out)} output={output}")


if __name__ == "__main__":
    main()
