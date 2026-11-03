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
import math
import os
import re
import sys
import threading
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
    from .turnover_budget import dynamic_optimizer_max_turnover
except ImportError:  # standalone: python tree_model_inference.py
    from turnover_budget import dynamic_optimizer_max_turnover

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


def filter_st_stocks_from_daily_basic(daily_basic, date_str: str = ""):
    """从 daily_basic 中剔除 ST/*ST/退市股票。

    判定规则与 native_engine._filter_st_codes 一致：当天快照的 SEC_SHORT_NAME
    含 "ST"（含 *ST）或 "退"。必须在 _prepare_daily_basic_for_tree 删除
    SEC_SHORT_NAME 列【之前】调用，否则无法判定 ST 状态。

    这是 inference 层的兜底过滤。trading_universe 模块是第一层。
    """
    import pandas as _pd

    if daily_basic is None or daily_basic.empty:
        return daily_basic

    _st_name_col = next(
        (c for c in ("SEC_SHORT_NAME", "SEC_NAME", "name") if c in daily_basic.columns),
        None,
    )
    _st_id_col = "_ID_QI_PAD" if "_ID_QI_PAD" in daily_basic.columns else (
        "ID_QI" if "ID_QI" in daily_basic.columns else None
    )
    if _st_name_col is None or _st_id_col is None:
        return daily_basic

    # 选定日期的快照（避免多日窗口里历史名称误排已摘帽股）
    _today_rows = daily_basic
    if "trade_date" in daily_basic.columns:
        if date_str:
            _today_rows = daily_basic[daily_basic["trade_date"].astype(str) == date_str]
        if _today_rows.empty:
            _latest = daily_basic["trade_date"].dropna().astype(str).max()
            _today_rows = daily_basic[daily_basic["trade_date"].astype(str) == _latest]

    _names = _today_rows[_st_name_col].astype(str).str.upper()
    _st_mask = _names.str.contains("ST", na=False) | _names.str.contains("退", na=False)
    if not _st_mask.any():
        return daily_basic

    # 归一化 ST 代码到 6 位纯数字（与 ID_QI 列格式一致）
    _st_codes = set(
        _today_rows.loc[_st_mask, _st_id_col]
        .astype(str)
        .str.split(".")
        .str[0]
        .str.zfill(6)
    )
    # 同时匹配纯数字和带后缀的 ID_QI（兼容不同数据源格式）
    _st_patterns = set()
    for c in _st_codes:
        _st_patterns.add(c)
        _st_patterns.add(c + ".SH")
        _st_patterns.add(c + ".SZ")
        _st_patterns.add(c + ".BJ")
        _st_patterns.add(c + ".XSHG")
        _st_patterns.add(c + ".XSHE")
    _before = len(daily_basic)
    _id_norm = daily_basic["ID_QI"].astype(str)
    # 用纯数字匹配（ID_QI 大多数是纯数字格式）
    _id_6digit = _id_norm.str.split(".").str[0].str.zfill(6)
    daily_basic = daily_basic.loc[~_id_6digit.isin(_st_codes)].copy()
    print(
        f"[tree-infer] ST/退市过滤: 排除 {len(_st_codes)} 只 "
        f"({date_str or 'latest'} 当前名称含 ST/退)，daily_basic {_before} → {len(daily_basic)} 行",
        flush=True,
    )
    return daily_basic


DEFAULT_OUTPUT = Path("artifacts/tree_model_positions.csv")
OSS_ENDPOINT = os.environ.get("OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com")
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


def _env_float(name: str, default: float) -> float:
    """Read a float from env, falling back to default on parse error."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_float_list(name: str) -> list[float]:
    """Read a comma/semicolon separated float list from env."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return []
    values: list[float] = []
    for item in re.split(r"[,;]", raw):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(float(item))
        except ValueError:
            print(f"[tree-infer] ignore invalid {name} item: {item!r}", flush=True)
    return values


def _optimizer_style_tol_candidates() -> list[float]:
    """Style exposure tolerance ladder.

    First value is the production constraint (OPTIMIZER_STYLE_EXPOSURE_TOL).
    Configured extra values are considered only after the turnover ladder fails.
    Industry tolerance remains fixed at the original configured value.
    """
    base = OPTIMIZER_STYLE_EXPOSURE_TOL
    fallbacks = _env_float_list("OPTIMIZER_STYLE_EXPOSURE_TOL_FALLBACKS")
    values = [base]
    values.extend(sorted(v for v in fallbacks if v >= base))
    out: list[float] = []
    seen: set[float] = set()
    for value in values:
        if not math.isfinite(value) or value < 0:
            continue
        rounded = round(float(value), 10)
        if rounded in seen:
            continue
        seen.add(rounded)
        out.append(float(value))
    return out or [base]


class RiskConstraintViolation(RuntimeError):
    """A finite target violates the independently verified risk bounds."""


def _is_optimizer_retryable_error(exc: BaseException) -> bool:
    """Return whether a prediction failure is likely an optimizer infeasibility.

    The industry exposure ladder should not mask data/schema/model bugs. Retry
    only when the compiled predictor reports an optimizer/solver infeasibility
    style error. Empty output is handled explicitly by raising a retryable
    RuntimeError with the marker below.
    """
    if isinstance(exc, RiskConstraintViolation):
        return True
    text = str(exc).lower()
    if type(exc).__name__ == "SolverError":
        return True
    # 宽匹配保底: 20261102 实盘 .so 的 user_limit 错误不含 "optimizer" 字样、
    # 异常类型也未证实是 RuntimeError — 收窄判定会让该类错误单发失败。
    # 这里取新旧判定并集: 命中任一 marker 即可重试(多重试的代价只是多跑
    # 几档优化器, 漏重试的代价是 9:30 直接失败)。
    broad_markers = (
        "optimizer_retryable",
        "infeasible",
        "infeasibility",
        "solver",
        "optimization",
        "user_limit",
        "optimal_inaccurate",
        "no positions",
        "empty positions",
    )
    return any(marker in text for marker in broad_markers)


def _optimizer_retry_stages(base_turnover: float):
    """Relax turnover first, then style; industry remains fixed in every stage."""
    styles = sorted(set(_optimizer_style_tol_candidates() + [OPTIMIZER_STYLE_EXPOSURE_TOL * 1.5, OPTIMIZER_STYLE_EXPOSURE_TOL * 2]))
    industry = OPTIMIZER_INDUSTRY_EXPOSURE_TOL
    if (not all(math.isfinite(v) and v >= 0 for v in styles + [industry])
            or not math.isfinite(base_turnover) or base_turnover < 0
            or not math.isfinite(RISK_RETRY_TURNOVER_FLOOR) or RISK_RETRY_TURNOVER_FLOOR < 0):
        raise RuntimeError("risk validation: invalid optimizer retry configuration")
    if os.environ.get("OPTIMIZER_INDUSTRY_EXPOSURE_TOL_FALLBACKS", "").strip():
        print("[tree-risk WARNING] industry tolerance fallbacks are ignored; industry remains fixed", flush=True)
    ceiling = max(base_turnover, RISK_RETRY_TURNOVER_FLOOR)
    turnovers = sorted(set([base_turnover, ceiling] + [v for v in (0.50, 0.75) if base_turnover < v < ceiling]))
    stages = [(OPTIMIZER_STYLE_EXPOSURE_TOL, industry, turnover) for turnover in turnovers]
    stages.extend((style, industry, ceiling) for style in styles if style > OPTIMIZER_STYLE_EXPOSURE_TOL + 1e-12)
    return stages


# Optimizer parameters — overridable via env vars so tuning doesn't require
# a rebuild. Key knobs for the infeasible-on-rebalance issue:
#   - MAX_TURNOVER: how much the target portfolio can differ from current.
#     When current holdings are normalized to sum=1.0 by the C extension
#     (regardless of actual cash), the optimizer sees gross=1.0 and a tight
#     turnover budget leaves no room to adjust → infeasible.
#   - CASH_RATIO: explicit allowance to hold cash (reduces target gross).
OPTIMIZER_MAX_TURNOVER = _env_float("OPTIMIZER_MAX_TURNOVER", 0.35)
OPTIMIZER_TURNOVER_BUFFER = _env_float("OPTIMIZER_TURNOVER_BUFFER", 0.02)
OPTIMIZER_MAX_WEIGHT = _env_float("OPTIMIZER_MAX_WEIGHT", 0.01)
OPTIMIZER_CASH_RATIO = _env_float("OPTIMIZER_CASH_RATIO", 0.0)
OPTIMIZER_TRANS_COST = _env_float("OPTIMIZER_TRANS_COST", 0.0003)
OPTIMIZER_STYLE_EXPOSURE_TOL = _env_float("OPTIMIZER_STYLE_EXPOSURE_TOL", 0.30)
OPTIMIZER_INDUSTRY_EXPOSURE_TOL = _env_float("OPTIMIZER_INDUSTRY_EXPOSURE_TOL", 0.30)
OPTIMIZER_MIN_BENCHMARK_CONSTITUENT_WEIGHT = _env_float(
    "OPTIMIZER_MIN_BENCHMARK_CONSTITUENT_WEIGHT", 0.30
)
MORNING_SIGNAL_CUTOFF = "093000"
POSITION_OUTPUT: Optional[Path] = None

# Risk validation is separate from model feature inputs.
RISK_COVERAGE_LOOKBACK = 5
RISK_COVERAGE_MAX_DROP = 0.20
RISK_MAX_AGE_DAYS = int(os.environ.get("TREE_RISK_MAX_AGE_DAYS", "14"))
RISK_STYLE_COLS = ("BETA", "MOMENTUM", "SIZE", "EARNYILD", "RESVOL",
                   "GROWTH", "BTOP", "LEVERAGE", "LIQUIDTY", "SIZENL")
RISK_INDUSTRY_COLS = (
    "Agriculture", "Automobiles", "Banks", "BuildMater", "Chemicals", "Commerce",
    "Computers", "Conglomerates", "ConstrDecor", "Defense", "ElectricalEquip",
    "Electronics", "FoodBeverages", "HealthCare", "HomeAppliances", "Leisure",
    "LightIndustry", "MachineEquip", "Media", "Mining", "NonbankFinan",
    "NonferrousMetals", "RealEstate", "Steel", "Telecoms", "TextileGarment",
    "Transportation", "Utilities", "BasicChemicals", "BeautyCare", "Coal",
    "EnvironProtect", "Petroleum", "PowerEquip", "RetailTrade", "SocialServices",
    "TextileApparel",
)
RISK_LEGACY_INDUSTRY_COLS = ("Chemicals", "Commerce", "ElectricalEquip", "Leisure", "Mining", "TextileGarment")
RISK_ACTIVE_INDUSTRY_COLS = tuple(c for c in RISK_INDUSTRY_COLS if c not in RISK_LEGACY_INDUSTRY_COLS)
RISK_RETRY_TURNOVER_FLOOR = _env_float("TREE_RISK_RETRY_TURNOVER_FLOOR", 1.0)

_OSS_BUCKETS: dict[str, oss2.Bucket] = {}
_DAILY_FEATURE_CACHE: dict[str, pd.DataFrame] = {}
_DAILY_BASIC_MEM_CACHE: dict[str, pd.DataFrame] = {}


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
    # 唯一化 tmp 文件名：precompute 线程和 fallback inline fork 可能并发下载
    # 同一 OSS key，共用 ".tmp" 后缀会出现一个 rename 走、另一个 FileNotFoundError。
    tmp_path = local_path.with_name(
        f"{local_path.name}.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}.tmp"
    )
    tmp_path.write_bytes(payload)
    tmp_path.replace(local_path)
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass
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


_DAILY_BASIC_STRING_COLS = {
    "TS", "ID_QI", "SEC_SHORT_NAME", "SEC_FULL_NAME",
    "trade_date", "date", "_date", "_ID_QI_PAD",
    "idx_EXCHANGE_CD", "idx_UPDATE_TIME", "UPDATE_TIME",
}


def coerce_daily_basic_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    # Coerce numeric columns that concat may have promoted to object (different
    # daily parquets have inconsistent dtypes) back to float64. The compiled
    # .so (value_weight) does Python-level true division on object Series which
    # raises ZeroDivisionError; same op on float64 returns inf.
    df = df.copy()
    for col in df.columns:
        if col in _DAILY_BASIC_STRING_COLS:
            # Force string dtype — some rows may be pd.Timestamp objects from
            # parquet decode (idx_UPDATE_TIME etc.); pollers/pyarrow choke on
            # mixed str/Timestamp in object columns.
            df[col] = df[col].astype(str).replace({"NaT": "", "nan": "", "None": ""})
            continue
        if df[col].dtype == object:
            numeric = pd.to_numeric(df[col], errors="coerce")
            non_null = df[col].notna()
            if not non_null.any() or numeric[non_null].notna().mean() >= 0.99:
                df[col] = numeric
    return df


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
    return coerce_daily_basic_dtypes(combined)


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

    cached = _DAILY_BASIC_MEM_CACHE.get(date_str)
    if cached is not None:
        # 返回副本，防止调用方 in-place 修改污染缓存
        return cached.copy()

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
    result = append_current_daily_basic(history, current_daily_basic, date_str)
    _DAILY_BASIC_MEM_CACHE[date_str] = result
    print(f"[tree-infer] daily_basic cached for {date_str}: {len(result)} rows, "
          f"{len(result.columns)} cols")
    return result


def clear_daily_basic_cache() -> None:
    _DAILY_BASIC_MEM_CACHE.clear()


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
        return coerce_daily_basic_dtypes(history)
    current = normalize_daily_basic(current, date_str=date_str)
    combined = pd.concat([history, current], ignore_index=True, copy=False)
    combined = combined.drop_duplicates(["trade_date", "ID_QI"], keep="last")
    return coerce_daily_basic_dtypes(combined)


def _risk_held_codes(portfolio_context) -> set[str]:
    """Absent account data is not evidence of an empty portfolio."""
    import numpy as np
    if portfolio_context is None:
        raise RuntimeError("risk validation: portfolio_context is required")
    positions = getattr(portfolio_context, "positions", None)
    if not isinstance(positions, pd.DataFrame):
        raise RuntimeError("risk validation: positions must be a DataFrame")
    if positions.empty:
        account = getattr(portfolio_context, "account", None)
        if not isinstance(account, pd.DataFrame) or account.empty or "market_value" not in account:
            raise RuntimeError("risk validation: cannot verify that an empty portfolio is actually empty")
        mv = pd.to_numeric(account["market_value"], errors="coerce").iloc[0]
        if not np.isfinite(mv) or abs(mv) > 1e-8:
            raise RuntimeError("risk validation: empty positions conflict with account market value")
        return set()
    cc = next((c for c in ("code", "stock_code", "symbol", "ID_QI", "_code6") if c in positions), None)
    vc = next((c for c in ("position", "weight", "market_value", "value", "position_value", "current_volume", "volume", "qty", "quantity") if c in positions), None)
    if cc is None or vc is None:
        raise RuntimeError("risk validation: positions lack code or amount")
    amounts = pd.to_numeric(positions[vc], errors="coerce")
    if not np.isfinite(amounts.to_numpy(dtype=float)).all():
        raise RuntimeError("risk validation: invalid position amount")
    for other in ("position", "weight", "market_value", "value", "position_value", "current_volume", "volume", "qty", "quantity"):
        if other != vc and other in positions:
            other_amount = pd.to_numeric(positions[other], errors="coerce")
            if ((amounts.abs() <= 1e-10) & (other_amount.abs() > 1e-10)).any():
                raise RuntimeError("risk validation: contradictory zero and nonzero position amounts")
    codes = positions.loc[amounts.abs() > 1e-10, cc].map(normalize_code6)
    if codes.isna().any():
        raise RuntimeError("risk validation: invalid held stock code")
    return set(codes)


def _require_live_risk_clock(date_str: str):
    """Historical as-of versions are absent; do not silently introduce lookahead."""
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    if date_str != today:
        raise RuntimeError("risk validation: historical/future replay requires verified point-in-time snapshots; live-only gate")


def _risk_composition(index_composition_df, benchmark: str, risk_date: str):
    """Require an explicitly dated benchmark; undated engine inputs use OSS."""
    comp = index_composition_df
    date_col = next((c for c in ("trade_date", "date", "TS") if isinstance(comp, pd.DataFrame) and c in comp), None)
    if date_col is None or not {"INDEX_CODE", "weight", "ID_QI"}.issubset(comp.columns):
        comp = load_index_composition_from_oss(risk_date, risk_date)
        date_col = next((c for c in ("trade_date", "date", "TS") if c in comp), None)
    if date_col is None or not {"INDEX_CODE", "weight", "ID_QI"}.issubset(comp.columns):
        raise RuntimeError("dated benchmark composition with weights is required")
    frame = comp.loc[comp["INDEX_CODE"].astype(str) == benchmark].copy()
    frame["_risk_date"] = frame[date_col].map(normalize_date)
    frame = frame.loc[frame["_risk_date"] == risk_date].copy()
    if frame.empty and comp is index_composition_df:
        comp = load_index_composition_from_oss(risk_date, risk_date)
        date_col = next((c for c in ("trade_date", "date", "TS") if c in comp), None)
        if date_col is None or not {"INDEX_CODE", "weight", "ID_QI"}.issubset(comp.columns):
            raise RuntimeError("dated benchmark composition with weights is required")
        frame = comp.loc[comp["INDEX_CODE"].astype(str) == benchmark].copy()
        frame["_risk_date"] = frame[date_col].map(normalize_date)
        frame = frame.loc[frame["_risk_date"] == risk_date].copy()
    if frame.empty:
        raise RuntimeError(f"benchmark {benchmark} has no composition for {risk_date}")
    frame["_risk_code"] = frame["ID_QI"].map(normalize_code6)
    raw = pd.to_numeric(frame["weight"], errors="coerce")
    if (frame["_risk_code"].isna().any() or raw.isna().any()
            or (~raw.map(math.isfinite)).any() or (raw < 0).any()
            or frame["_risk_code"].duplicated().any()):
        raise RuntimeError("benchmark contains invalid or duplicate codes/weights")
    positive = raw > 0
    weights = normalize_benchmark_weights(pd.Series(raw[positive].to_numpy(), index=frame.loc[positive, "_risk_code"]))
    if weights is None or weights.empty:
        raise RuntimeError("benchmark has no positive weights")
    return weights


def _risk_reject_mixed_date_sidecars(day: str):
    """Do not treat known per-stock stale-filled inputs as a single-date snapshot."""
    bucket = make_bucket(OSS_DATA_BUCKET)
    for suffix, field in (("stale_fill", "count"), ("index_weight_audit", "stale_prices")):
        key = daily_basic_prefix(day) + day + "_" + suffix + ".json"
        if bucket.object_exists(key):
            response = bucket.get_object(key)
            try:
                audit = json.loads(response.read())
            finally:
                response.close()
            count = audit.get(field)
            if count is None or not isinstance(count, (int, float)) or not math.isfinite(count) or count < 0:
                raise RuntimeError(f"invalid {suffix} sidecar")
            if count > 0:
                raise RuntimeError(f"{suffix} sidecar records {count} stale rows")


def _emit_risk_warning(message: str):
    """Log the warning and reuse the platform notifier when configured."""
    print(f"[tree-risk WARNING] {message}", flush=True)
    try:
        from quant_platform.utils.notify import feishu_alert
        if not feishu_alert("tree model risk data warning", message):
            print("[tree-risk WARNING] notification was not delivered; inspect process logs", flush=True)
    except Exception as exc:
        print(f"[tree-risk WARNING] notification unavailable ({type(exc).__name__}); inspect process logs", flush=True)


def select_validated_risk_snapshot(daily_basic, date_str, index_composition_df, benchmark, held_codes):
    """Select the latest complete prior-day risk snapshot; never fill risks with zero.

    Five preceding independently qualifying snapshots provide the baseline;
    upstream as-of/version provenance cannot be proven from this frame.
    """
    import numpy as np
    required_cols = RISK_STYLE_COLS + RISK_ACTIVE_INDUSTRY_COLS
    if RISK_MAX_AGE_DAYS < 1 or not set(RISK_STYLE_COLS + RISK_INDUSTRY_COLS).issubset(daily_basic.columns):
        raise RuntimeError("risk validation: invalid max age or missing Barra style/industry columns")
    prior = daily_basic.loc[daily_basic["trade_date"].astype(str) < date_str].copy()
    counts = prior.groupby("trade_date")["ID_QI"].nunique().sort_index()
    dates = list(counts.index.astype(str))
    today = datetime.strptime(date_str, "%Y%m%d")
    rejected = []
    cache = {}
    def inspect(day, require_current_holdings):
        cache_key = (day, require_current_holdings)
        if cache_key in cache:
            return cache[cache_key]
        try:
            _risk_reject_mixed_date_sidecars(day)
            snapshot = prior.loc[prior["trade_date"].astype(str) == day].copy()
            if snapshot.empty or snapshot["ID_QI"].duplicated().any():
                raise RuntimeError("empty snapshot or duplicate stock codes")
            if snapshot[list(RISK_LEGACY_INDUSTRY_COLS)].notna().any().any():
                raise RuntimeError("legacy industry fields are populated; classification schema requires review")
            bm = _risk_composition(index_composition_df, benchmark, day)
            needed = pd.Index(sorted(set(bm.index.astype(str)) | (held_codes if require_current_holdings else set())))
            risk = snapshot.set_index("ID_QI").reindex(needed)[list(required_cols)].apply(pd.to_numeric, errors="coerce")
            valid = np.isfinite(risk.to_numpy(dtype=float))
            if not valid.all():
                bad_cols = list(risk.columns[~valid.all(axis=0)])
                raise RuntimeError("missing Barra values for benchmark or held positions; fields=" + ",".join(bad_cols))
            ind = risk[list(RISK_ACTIVE_INDUSTRY_COLS)].to_numpy(dtype=float)
            if not np.isin(ind, [0.0, 1.0]).all() or not np.allclose(ind.sum(axis=1), 1.0, atol=1e-8):
                raise RuntimeError("active industry classification must be complete one-hot")
            result = (snapshot, bm, None)
        except Exception as exc:
            result = (None, None, f"{type(exc).__name__}: {exc}")
        cache[cache_key] = result
        return result
    for candidate in reversed(dates):
        age = (today - datetime.strptime(candidate, "%Y%m%d")).days
        if age > RISK_MAX_AGE_DAYS:
            break
        snapshot, bm, error = inspect(candidate, True)
        if error:
            rejected.append(f"{candidate}: {error}")
            continue
        qualified = []
        for day in reversed([x for x in dates if x < candidate]):
            _, previous_bm, previous_error = inspect(day, False)
            if previous_error is None:
                qualified.append((day, previous_bm))
            if len(qualified) == RISK_COVERAGE_LOOKBACK:
                break
        if len(qualified) < RISK_COVERAGE_LOOKBACK:
            rejected.append(f"{candidate}: fewer than five historical dates")
            continue
        baseline = float(np.median([int(counts.loc[day]) for day, _ in qualified]))
        count = int(counts.loc[candidate])
        drop = 1 - count / baseline if baseline > 0 else float("inf")
        if drop >= RISK_COVERAGE_MAX_DROP - 1e-12:
            rejected.append(f"{candidate}: stock coverage drop {drop:.2%}")
            continue
        bm_baseline = float(np.median([len(previous_bm) for _, previous_bm in qualified]))
        if bm_baseline <= 0 or len(bm) <= bm_baseline * (1 - RISK_COVERAGE_MAX_DROP) + 1e-12:
            rejected.append(f"{candidate}: benchmark member coverage drop")
            continue
        summary = f"date={candidate} age_days={age} stocks={count} baseline={baseline:.0f} rejected={rejected}"
        if rejected or age > 1:
            _emit_risk_warning("using historical risk snapshot; " + summary)
        else:
            print(f"[tree-risk] {summary}", flush=True)
        print("[tree-risk WARNING] independent trading calendar and snapshot availability not verified", flush=True)
        return snapshot, bm, {"risk_date": candidate, "age_days": age, "stock_count": count,
                              "baseline_median": baseline, "stock_drop": drop, "rejected": rejected}
    failure = "risk validation: no qualifying snapshot: " + "; ".join(rejected[-8:])
    _emit_risk_warning(failure)
    raise RuntimeError(failure)


def _validate_prediction_holdings(daily_basic, signal_date, factors, universe, held_codes):
    """Do not allow existing positions to disappear before the compiled optimizer."""
    signal_codes = set(daily_basic.loc[daily_basic["trade_date"].astype(str) == signal_date, "ID_QI"])
    factor_codes = set()
    if factors is not None and not factors.empty:
        key = next((c for c in ("ID_QI", "code", "_code6") if c in factors), None)
        if key:
            factor_codes = set(factors[key].map(normalize_code6).dropna())
    eligible = signal_codes & factor_codes
    if universe is not None:
        eligible &= set(universe)
    missing = held_codes - eligible
    if missing:
        raise RuntimeError(f"risk validation: {len(missing)} held stocks missing from model inputs; examples={sorted(missing)[:8]}")


def _validate_target_risk(out, risk_frame, benchmark_weight, style_tol, industry_tol):
    """Independent complete-benchmark style and industry check."""
    import numpy as np
    code_col = next((c for c in ("_code6", "code", "ID_QI") if c in out), None)
    if code_col is None or "position" not in out:
        raise RuntimeError("risk validation: target lacks code or position")
    codes = out[code_col].map(normalize_code6)
    values = pd.to_numeric(out["position"], errors="coerce")
    if codes.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all() or codes.duplicated().any():
        raise RuntimeError("risk validation: target contains invalid or duplicate positions")
    weights = pd.Series(values.to_numpy(dtype=float), index=codes)
    weights = weights.loc[weights.abs() > 1e-10]
    exposure = risk_frame.reindex(weights.index)[list(RISK_STYLE_COLS)].apply(pd.to_numeric, errors="coerce")
    benchmark = risk_frame.reindex(benchmark_weight.index)[list(RISK_STYLE_COLS)].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(exposure.to_numpy(dtype=float)).all() or not np.isfinite(benchmark.to_numpy(dtype=float)).all():
        raise RuntimeError("risk validation: target or full benchmark has missing Barra values")
    active = exposure.mul(weights, axis=0).sum(axis=0) - benchmark.mul(benchmark_weight, axis=0).sum(axis=0)
    limit = benchmark.std(axis=0) * float(style_tol)
    if not np.isfinite(limit.to_numpy(dtype=float)).all() or (limit <= 0).any():
        raise RuntimeError("risk validation: invalid benchmark style dispersion")
    breaches = active.index[active.abs() > limit + 1e-6]
    if len(breaches):
        raise RiskConstraintViolation("risk validation: target violates full-benchmark style bounds: " + ", ".join(map(str, breaches)))
    other_cols = [c for c in risk_frame.columns if c not in RISK_STYLE_COLS]
    if not set(RISK_ACTIVE_INDUSTRY_COLS + ("XSHG", "XSHE")).issubset(other_cols):
        raise RuntimeError("risk validation: industry or exchange exposures are missing")
    target_other = risk_frame.reindex(weights.index)[other_cols].apply(pd.to_numeric, errors="coerce")
    benchmark_other = risk_frame.reindex(benchmark_weight.index)[other_cols].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(target_other.to_numpy(dtype=float)).all() or not np.isfinite(benchmark_other.to_numpy(dtype=float)).all():
        raise RuntimeError("risk validation: target or benchmark has missing industry/exchange exposures")
    target_exp = target_other.mul(weights, axis=0).sum(axis=0)
    bench_exp = benchmark_other.mul(benchmark_weight, axis=0).sum(axis=0)
    constrained = bench_exp > 1e-8
    low = (1.0 - float(industry_tol)) * bench_exp
    high = (1.0 + float(industry_tol)) * bench_exp
    bad = constrained & ((target_exp < low - 1e-6) | (target_exp > high + 1e-6))
    if bad.any():
        raise RiskConstraintViolation("risk validation: target violates full-benchmark industry/exchange bounds: " + ", ".join(map(str, bad.index[bad])))
    print(f"[tree-risk] independent target style check passed tolerance={style_tol:.4f}", flush=True)


def _validate_target_contract(out, current_weight, stage_turnover):
    """Check the final reported position and actual turnover before returning it."""
    import numpy as np
    code_col = next((c for c in ("_code6", "code", "ID_QI") if c in out), None)
    if code_col is None or "position" not in out:
        raise RuntimeError("risk validation: target lacks position or code")
    codes = out[code_col].map(normalize_code6)
    values = pd.to_numeric(out["position"], errors="coerce")
    if codes.isna().any() or codes.duplicated().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise RuntimeError("risk validation: invalid target positions")
    target = pd.Series(values.to_numpy(dtype=float), index=codes)
    current = pd.to_numeric(current_weight, errors="coerce")
    if not np.isfinite(current.to_numpy(dtype=float)).all() or current.index.duplicated().any():
        raise RuntimeError("risk validation: invalid optimizer current weights")
    current.index = current.index.astype(str)
    idx = current.index.union(target.index)
    current = current.reindex(idx, fill_value=0.)
    target = target.reindex(idx, fill_value=0.)
    if not ENABLE_SHORT and (target < -1e-6).any():
        raise RuntimeError("risk validation: unexpected short position")
    expected = max(0., 1. - OPTIMIZER_CASH_RATIO)
    if abs(float(target.sum()) - expected) > 1e-5:
        raise RuntimeError("risk validation: target sum differs from configured investment budget")
    effective_turnover = max(stage_turnover, expected) if current.abs().sum() < 1e-8 else stage_turnover
    if float((target - current).abs().sum()) > effective_turnover + 1e-5:
        raise RuntimeError("risk validation: target exceeds turnover limit against full current weights")
    if not ENABLE_SHORT and (target > np.maximum(OPTIMIZER_MAX_WEIGHT, current) + 1e-6).any():
        raise RuntimeError("risk validation: target increases a position beyond its limit")


def inference(
    date_str: str,
    end_time: str,
    prev_day_factors_df: Optional[pd.DataFrame],
    intraday_factors_df: pd.DataFrame,
    daily_basic_df: Optional[pd.DataFrame] = None,
    trading_universe_df: Optional[pd.DataFrame] = None,
    index_composition_df: Optional[pd.DataFrame] = None,
    portfolio_context=None,
    benchmark_code: Optional[str] = None,
) -> pd.DataFrame:
    """benchmark_code: 优化基准指数代码(TICKER)。
    None → 读 OPTIMIZER_BENCHMARK env(默认 000985, 全指信号路径)。
    显式传参(如 "932000") → 使用该基准, 不受进程 env 影响 —
    同一引擎进程内并行跑多个产品(全指/中证2000)时互不污染。
    """
    import time as _time
    _t0 = _time.time()

    date_str = normalize_date(date_str)
    if date_str is None:
        raise RuntimeError("risk validation: invalid decision date")
    _require_live_risk_clock(date_str)
    daily_basic = load_production_daily_basic(date_str, current_daily_basic=daily_basic_df)
    held_codes = _risk_held_codes(portfolio_context)
    _benchmark = (benchmark_code or os.environ.get("OPTIMIZER_BENCHMARK", "")).strip() or "000985"
    risk_snapshot, risk_benchmark, risk_audit = select_validated_risk_snapshot(
        daily_basic, date_str, index_composition_df, _benchmark, held_codes,
    )
    _t_daily_basic = _time.time()
    print(f"[tree-infer-timing] daily_basic: {_t_daily_basic - _t0:.3f}s ({len(daily_basic)} rows)", flush=True)
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
    # 优化基准可配: 默认中证全指 000985(现有行为不变); 新产品(如中证2000)
    # 通过 benchmark_code 参数或 OPTIMIZER_BENCHMARK env 传入 —
    # 风格/行业约束、成分下限、信号超配全部切到该基准坐标系。
    # 参数优先于 env: 同进程多产品并行时各自传参互不污染。
    _benchmark = (benchmark_code or os.environ.get("OPTIMIZER_BENCHMARK", "")).strip() or "000985"
    if index_composition_df is not None and not index_composition_df.empty:
        index_csi_all = index_composition_df
        # Long format (INDEX_CODE + weight columns): filter to benchmark directly
        if "INDEX_CODE" in index_csi_all.columns and "weight" in index_csi_all.columns:
            index_csi_all = index_csi_all[index_csi_all["INDEX_CODE"].astype(str) == _benchmark]
            if not index_csi_all.empty:
                date_col = "trade_date" if "trade_date" in index_csi_all.columns else "date" if "date" in index_csi_all.columns else None
                if date_col is not None:
                    index_csi_all = index_csi_all[index_csi_all[date_col].astype(str) == index_csi_all[date_col].astype(str).max()]
                last_index_csi_all = index_csi_all[["ID_QI", "weight"]].set_index("ID_QI")["weight"]
        # Wide format (weight_{benchmark} column from compute_index_composition)
        elif f"weight_{_benchmark}" in index_csi_all.columns:
            sub = index_csi_all[["ID_QI", f"weight_{_benchmark}"]].dropna(subset=[f"weight_{_benchmark}"])
            sub = sub[sub[f"weight_{_benchmark}"] > 0]
            if not sub.empty:
                last_index_csi_all = sub.set_index("ID_QI")[f"weight_{_benchmark}"]
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
    # ST/*ST/退市股票不参与调仓：必须在候选池入口剔除，否则优化器仍会选到 ST
    # （native_engine._filter_st_codes 只作用于分钟因子计算，不作用于 inference）。
    # 判定规则与 _filter_st_codes 一致：当天 daily_basic 的 SEC_SHORT_NAME 含
    # "ST"（含 *ST）或 "退"。用当天数据，避免历史快照误排已摘帽股。
    #
    # 这是第二层（兜底）：第一层是 trading_universe 模块（Pod 启动时调用一次，
    # 把 ST 排除出 universe），见 quant_platform/inference/trading_universe.py。
    # 这里再过滤一次是为了：
    #   1. 即使没配 TRADING_UNIVERSE_MODULE，inference 也能独立保证无 ST
    #   2. 防止 Pod 启动后盘中 ST 状态变化（摘帽/戴帽）导致的遗漏
    daily_basic = filter_st_stocks_from_daily_basic(daily_basic, date_str)
    _validate_prediction_holdings(daily_basic, signal_date, prev_day_factors_df, universe, held_codes)
    all_codes = pd.Index(daily_basic["ID_QI"].dropna().astype(str).unique()).sort_values()
    current_weight = extract_current_position_weights(portfolio_context, all_codes)
    optimizer_max_turnover = OPTIMIZER_MAX_TURNOVER

    # extract_current_position_weights normalizes holdings to sum=1.0
    # (qty*price / Σ qty*price), so the optimizer always thinks we are 100%
    # invested regardless of actual cash. This makes rebalancing infeasible
    # whenever there is uninvested cash — e.g. 100-share rounding loss on
    # small accounts, or right after a deposit.
    #
    # Rescale current_weight by the true invested fraction so the optimizer
    # sees actual gross exposure. Safe because the optimizer already accepts
    # sum(current_weight) < 1.0 (proven by 9:30 fresh-open path where
    # current gross=0.0 succeeds).
    if portfolio_context is not None:
        _acct = getattr(portfolio_context, "account", None)
        if _acct is not None and not _acct.empty:
            _ta = pd.to_numeric(_acct["total_asset"].iloc[0], errors="coerce") \
                if "total_asset" in _acct.columns else float("nan")
            _mv = pd.to_numeric(_acct["market_value"].iloc[0], errors="coerce") \
                if "market_value" in _acct.columns else float("nan")
            _today_flow = pd.to_numeric(
                _acct["today_cash_flow"].iloc[0], errors="coerce") \
                if "today_cash_flow" in _acct.columns else float("nan")
            if math.isfinite(_ta) and _ta > 0 and math.isfinite(_mv) and _mv >= 0:
                invested_frac = min(_mv / _ta, 1.0)
                current_weight = current_weight * invested_frac
                _cw_sum = float(pd.Series(current_weight).sum())
                optimizer_max_turnover = dynamic_optimizer_max_turnover(
                    _cw_sum,
                    today_cash_flow=_today_flow,
                    total_asset=_ta,
                    base_turnover=OPTIMIZER_MAX_TURNOVER,
                    buffer=OPTIMIZER_TURNOVER_BUFFER,
                )
                print(
                    f"[tree-infer] rescaled current_weight by invested_frac="
                    f"{invested_frac:.4f} (market_value={_mv:.0f} / total_asset={_ta:.0f})"
                    f" → current gross={_cw_sum:.4f}; turnover budget="
                    f"{optimizer_max_turnover:.4f} (base={OPTIMIZER_MAX_TURNOVER:.4f}, "
                    f"today_cash_flow={_today_flow:.0f})",
                    flush=True,
                )
    benchmark_weight = risk_benchmark
    exposure_frame = build_optimizer_exposure_frame(risk_snapshot, risk_audit["risk_date"])
    if exposure_frame is None or exposure_frame.empty:
        raise RuntimeError("risk validation: exposure frame is empty")
    # These six legacy columns were verified empty across the entire selected
    # snapshot; do not fill them with 0 or invent a one-to-one mapping.
    exposure_frame = exposure_frame.drop(columns=list(RISK_LEGACY_INDUSTRY_COLS), errors="ignore")
    print("[tree-risk] excluded six fully empty legacy industry fields; 31 current fields validated", flush=True)
    model_path = (
        DEFAULT_MODEL_PATH
        if Path(DEFAULT_MODEL_PATH).exists() and Path(DEFAULT_MODEL_PATH).stat().st_size > 0
        else load_model_from_oss(OSS_MODEL_KEY, OSS_MODEL_BUCKET)
    )
    _t_model = _time.time()
    print(f"[tree-infer-timing] model_path resolve: {_t_model - _t_daily_basic:.3f}s path={model_path}", flush=True)

    _t_pre_predict = _time.time()
    retry_stages = _optimizer_retry_stages(optimizer_max_turnover)
    # 首日建仓(无存量持仓): 对空仓任何非零目标都"超换手", 低档阶梯注定不可行;
    # _validate_target_contract 对空仓本就按满额预算校验 — 直接从顶格开始,
    # 省掉 3 次必败求解(9:30→9:45 截止窗口内每一次求解都很贵)。
    if float(current_weight.abs().sum()) < 1e-8 and retry_stages:
        ceiling = max(stage[2] for stage in retry_stages)
        retry_stages = [(style, industry, ceiling) for style, industry, _ in retry_stages]
    out = None
    last_predict_error: Optional[BaseException] = None
    total_attempts = len(retry_stages)
    attempt = 0
    for stage in retry_stages:
        for style_tol, industry_tol, stage_turnover in [stage]:
            attempt += 1
            if total_attempts > 1:
                print(
                    f"[tree-infer] optimizer attempt {attempt}/{total_attempts}: "
                    f"style_tol={style_tol:.4f}, industry_tol={industry_tol:.4f}, "
                    f"turnover={stage_turnover:.4f}, risk_date={risk_audit['risk_date']}",
                    flush=True,
                )
            try:
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
                    optimizer_max_turnover=stage_turnover,
                    optimizer_max_weight=OPTIMIZER_MAX_WEIGHT,
                    optimizer_cash_ratio=OPTIMIZER_CASH_RATIO,
                    optimizer_trans_cost=OPTIMIZER_TRANS_COST,
                    benchmark_weight=benchmark_weight,
                    exposure_frame=exposure_frame,
                    optimizer_style_exposure_tol=style_tol,
                    optimizer_industry_exposure_tol=industry_tol,
                    optimizer_min_benchmark_constituent_weight=OPTIMIZER_MIN_BENCHMARK_CONSTITUENT_WEIGHT,
                )
                if out is None or out.empty:
                    raise RuntimeError(
                        f"optimizer_retryable: predict_tree_model returned no positions "
                        f"(style_tol={style_tol:.4f}, industry_tol={industry_tol:.4f})"
                    )
                _validate_target_contract(out, current_weight, stage_turnover)
                _validate_target_risk(out, exposure_frame, benchmark_weight, style_tol, industry_tol)
                if attempt > 1:
                    print(
                        f"[tree-infer] optimizer recovered with "
                        f"style_tol={style_tol:.4f}, industry_tol={industry_tol:.4f}",
                        flush=True,
                    )
                break
            except Exception as exc:
                last_predict_error = exc
                # A compiled solver can return a finite target that fails our
                # independent gate. Never retain that target for a later stage.
                out = None
                if not _is_optimizer_retryable_error(exc):
                    raise
                if attempt >= total_attempts:
                    raise
                print(
                    f"[tree-infer] optimizer failed "
                    f"(style_tol={style_tol:.4f}, industry_tol={industry_tol:.4f}): "
                    f"{exc}; advancing ladder",
                    flush=True,
                )
        if out is not None:
            break
    if out is None:
        raise RuntimeError("predict_tree_model produced no output") from last_predict_error

    _t_predict = _time.time()
    print(f"[tree-infer-timing] predict_tree_model(.so): {_t_predict - _t_pre_predict:.3f}s ({len(out)} positions)", flush=True)
    print(f"[tree-infer-timing] TOTAL: {_t_predict - _t0:.3f}s", flush=True)

    if universe is not None:
        out = out[out["_code6"].isin(set(universe))].copy()
    if out.empty:
        raise RuntimeError("risk validation: target is empty after universe filtering")
    _validate_target_contract(out, current_weight, stage_turnover)
    _validate_target_risk(out, exposure_frame, benchmark_weight, style_tol, industry_tol)

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
    parser.add_argument("--optimizer-max-turnover", type=float, default=OPTIMIZER_MAX_TURNOVER)
    parser.add_argument("--optimizer-max-weight", type=float, default=OPTIMIZER_MAX_WEIGHT)
    parser.add_argument("--optimizer-cash-ratio", type=float, default=OPTIMIZER_CASH_RATIO)
    parser.add_argument("--optimizer-trans-cost", type=float, default=OPTIMIZER_TRANS_COST)
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
