# -*- coding: utf-8 -*-
"""
实盘因子验证脚本

收盘后运行，从 OSS 下载原始数据和因子结果，交叉核验：
  1. 覆盖率：因子输出覆盖了多少股票（对比 daily_basic）
  2. 准确性：用原始 tick/deal 数据独立重算因子，与 StreamingEngine 输出对比
  3. 数据延迟：通联数据产生时间 vs chunk 写入时间的差异
  4. 因子计算延迟：e2e_latency_ms 统计

用法：
    python scripts/verify_live_factors.py --date 20260512

需要环境变量：
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

# 添加项目根目录到 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from quant_platform.data.oss_loader import OSSDataLoader
from quant_platform.factor.base import StockState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _load_factor_results_oss(loader: OSSDataLoader, date_str: str):
    """从 OSS 加载因子结果 JSON 文件。"""
    bucket = loader._bucket
    prefix = os.environ.get("OSS_LIVE_PREFIX", "live-factors")

    year = date_str[:4]
    month = date_str[4:6]

    all_records = []
    # 列出该日期下所有 JSON 文件
    marker = ""
    while True:
        ls = oss2.ObjectInfoIterator(
            bucket,
            prefix=f"{prefix}/{year}/{year}{month}/{date_str}_",
            marker=marker,
        )
        batch = list(ls)
        if not batch:
            break
        for obj in batch:
            if not obj.key.endswith(".json"):
                continue
            try:
                data = json.loads(bucket.get_object(obj.key).read())
                if isinstance(data, list):
                    all_records.extend(data)
            except Exception as e:
                logger.warning("读取 %s 失败: %s", obj.key, e)
        if not batch:
            break
        marker = batch[-1].key

    if not all_records:
        return pd.DataFrame()
    return pd.DataFrame(all_records)


def _load_factor_results_local(date_str: str):
    """从本地 /data/factors 加载 CSV 文件。"""
    factor_dir = Path("/data/factors")
    if not factor_dir.exists():
        return pd.DataFrame()

    csvs = sorted(factor_dir.glob(f"{date_str}_*.csv"))
    if not csvs:
        return pd.DataFrame()

    dfs = []
    for f in csvs:
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            logger.warning("读取 %s 失败: %s", f, e)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _recompute_from_raw(tick_df: pd.DataFrame, deal_df: pd.DataFrame,
                        codes: list) -> pd.DataFrame:
    """从原始 tick/deal 数据独立重算因子，用于对比。"""
    results = []

    for code in codes:
        state = StockState(code=str(code))

        # tick 数据
        tick_code = tick_df[tick_df["Code"] == code] if not tick_df.empty else pd.DataFrame()
        if not tick_code.empty:
            state.update_tick(tick_code)

        # deal 数据
        deal_code = deal_df[deal_df["Code"] == code] if not deal_df.empty else pd.DataFrame()
        if not deal_code.empty:
            state.update_deal(deal_code)

        vol_ratio = state.cum_volume / state.deal_count if state.deal_count > 0 else 0
        price_range = state.high - state.low if state.high > state.low else 0
        price_pos = (state.latest_price - state.low) / price_range if price_range > 0 else 0.5
        total_lv = state.bid_volume1 + state.ask_volume1
        buy_pressure = state.bid_volume1 / total_lv if total_lv > 0 else 0.5

        results.append({
            "code": str(code),
            "vwap": state.vwap,
            "change_pct": state.change_pct,
            "total_vol": state.cum_volume,
            "deal_count": state.deal_count,
            "spread": state.spread,
            "latest_price": state.latest_price,
            "high": state.high,
            "low": state.low if state.low != float("inf") else 0.0,
            "vol_ratio": round(vol_ratio, 2),
            "price_pos": round(price_pos, 4),
            "buy_pressure": round(buy_pressure, 4),
        })

    return pd.DataFrame(results)


def _check_coverage(factor_df: pd.DataFrame, daily_basic: pd.DataFrame) -> dict:
    """检查因子覆盖率。"""
    # daily_basic 中 ID_QI 列是 6 位代码
    if "ID_QI" in daily_basic.columns:
        all_codes = set(daily_basic["ID_QI"].dropna().astype(str))
    elif "SECURITY_ID" in daily_basic.columns:
        all_codes = set(daily_basic["SECURITY_ID"].dropna().astype(str))
    else:
        return {"error": "daily_basic 中无 ID_QI 或 SECURITY_ID 列"}

    factor_codes = set(factor_df["code"].dropna().astype(str)) if "code" in factor_df.columns else set()

    # 尝试匹配（factor 中的 code 可能是 int 或带后缀的格式）
    covered = all_codes & factor_codes
    missing = all_codes - factor_codes

    return {
        "total_in_universe": len(all_codes),
        "total_in_factors": len(factor_codes),
        "covered": len(covered),
        "missing_count": len(missing),
        "coverage_pct": round(len(covered) / len(all_codes) * 100, 2) if all_codes else 0,
        "sample_missing": sorted(missing)[:20],
    }


def _check_accuracy(factor_df: pd.DataFrame, truth_df: pd.DataFrame) -> dict:
    """对比因子输出与独立重算结果。"""
    if factor_df.empty or truth_df.empty:
        return {"error": "因子输出或重算结果为空"}

    # 确保 code 列类型一致
    factor_df = factor_df.copy()
    truth_df = truth_df.copy()
    factor_df["code"] = factor_df["code"].astype(str)
    truth_df["code"] = truth_df["code"].astype(str)

    merged = factor_df.merge(truth_df, on="code", suffixes=("_live", "_truth"), how="inner")

    if merged.empty:
        return {"error": "code 列无法匹配，请检查格式"}

    metrics = ["vwap", "total_vol", "deal_count", "latest_price", "high", "low"]
    report = {"matched_stocks": len(merged)}

    for m in metrics:
        col_live = f"{m}_live"
        col_truth = f"{m}_truth"
        if col_live not in merged.columns or col_truth not in merged.columns:
            continue

        live_vals = pd.to_numeric(merged[col_live], errors="coerce")
        truth_vals = pd.to_numeric(merged[col_truth], errors="coerce")

        # 过滤 NaN
        valid = pd.DataFrame({"live": live_vals, "truth": truth_vals}).dropna()
        if valid.empty:
            report[m] = {"status": "all_nan"}
            continue

        # 相对误差
        diff = (valid["live"] - valid["truth"]).abs()
        rel_err = diff / valid["truth"].replace(0, float("nan")).abs()

        exact_match = (valid["live"] == valid["truth"]).sum()

        report[m] = {
            "exact_match": int(exact_match),
            "total_valid": len(valid),
            "max_abs_diff": round(float(diff.max()), 4),
            "mean_rel_err_pct": round(float(rel_err.mean() * 100), 4) if rel_err.notna().any() else None,
            "p99_rel_err_pct": round(float(rel_err.quantile(0.99) * 100), 4) if rel_err.notna().any() else None,
        }

    return report


def _check_latency(factor_df: pd.DataFrame) -> dict:
    """统计延迟。"""
    if factor_df.empty:
        return {"error": "因子输出为空"}

    report = {}

    # e2e_latency_ms: chunk 写入 → 因子计算
    if "e2e_latency_ms" in factor_df.columns:
        lat = pd.to_numeric(factor_df["e2e_latency_ms"], errors="coerce").dropna()
        if not lat.empty:
            report["e2e_latency_ms"] = {
                "count": len(lat),
                "avg_ms": round(float(lat.mean()), 1),
                "p50_ms": round(float(lat.median()), 1),
                "p90_ms": round(float(lat.quantile(0.9)), 1),
                "p99_ms": round(float(lat.quantile(0.99)), 1),
                "max_ms": round(float(lat.max()), 1),
                "min_ms": round(float(lat.min()), 1),
            }

    # _compute_ts - _chunk_ts: 更精确的延迟
    if "_compute_ts" in factor_df.columns and "_chunk_ts" in factor_df.columns:
        compute_ts = pd.to_numeric(factor_df["_compute_ts"], errors="coerce")
        chunk_ts = pd.to_numeric(factor_df["_chunk_ts"], errors="coerce")
        delta = ((compute_ts - chunk_ts) * 1000).dropna()
        delta = delta[delta > 0]  # 过滤无效值
        if not delta.empty:
            report["chunk_to_compute_ms"] = {
                "avg_ms": round(float(delta.mean()), 1),
                "p50_ms": round(float(delta.median()), 1),
                "p90_ms": round(float(delta.quantile(0.9)), 1),
                "max_ms": round(float(delta.max()), 1),
            }

    return report


def main():
    parser = argparse.ArgumentParser(description="实盘因子验证")
    parser.add_argument("--date", required=True, help="交易日 YYYYMMDD")
    parser.add_argument("--local", action="store_true",
                        help="从本地 /data/factors 读 CSV（默认从 OSS 读 JSON）")
    parser.add_argument("--sample", type=int, default=200,
                        help="准确性验证采样股票数（默认 200，0=全市场）")
    args = parser.parse_args()

    date_str = args.date
    logger.info("=" * 60)
    logger.info("实盘因子验证: %s", date_str)
    logger.info("=" * 60)

    # 初始化 OSS 加载器
    loader = OSSDataLoader()

    # ---- 1. 加载因子结果 ----
    logger.info("\n[1] 加载因子结果...")
    if args.local:
        factor_df = _load_factor_results_local(date_str)
    else:
        factor_df = _load_factor_results_oss(loader, date_str)

    if factor_df.empty:
        logger.error("未找到因子结果！请检查 OSS 或本地路径。")
        sys.exit(1)

    logger.info("因子结果: %d 条记录, %d 列", len(factor_df), len(factor_df.columns))
    if "end_time" in factor_df.columns:
        times = sorted(factor_df["end_time"].unique())
        logger.info("时间切片: %s ... %s (%d 个)", times[0], times[-1], len(times))

    # ---- 2. 覆盖率 ----
    logger.info("\n[2] 检查覆盖率...")
    daily_basic = loader.load_daily_basic(date_str)
    if daily_basic is not None and not daily_basic.empty:
        # 取最后一个时间切片的快照（收盘附近）
        last_slice = factor_df
        if "end_time" in factor_df.columns:
            last_time = sorted(factor_df["end_time"].unique())[-1]
            last_slice = factor_df[factor_df["end_time"] == last_time]

        coverage = _check_coverage(last_slice, daily_basic)
        logger.info("  全市场股票: %d", coverage.get("total_in_universe", "?"))
        logger.info("  因子输出:   %d", coverage.get("total_in_factors", "?"))
        logger.info("  覆盖率:     %s%%", coverage.get("coverage_pct", "?"))
        if coverage.get("missing_count", 0) > 0:
            logger.info("  缺失股票数: %d (示例: %s)",
                        coverage["missing_count"], coverage.get("sample_missing", []))
    else:
        logger.warning("  无法加载 daily_basic，跳过覆盖率检查")

    # ---- 3. 准确性 ----
    logger.info("\n[3] 检查准确性（从原始数据独立重算）...")
    try:
        # 加载 tick 和 deal 原始数据
        tick_df = loader.load_data(date_str, "tick", codes=None)
        deal_df = loader.load_data(date_str, "deal", codes=None)

        if tick_df is not None and deal_df is not None:
            # 采样股票做对比
            all_codes_in_data = tick_df["Code"].unique() if not tick_df.empty else []
            sample_count = args.sample if args.sample > 0 else len(all_codes_in_data)
            sample_codes = list(all_codes_in_data[:sample_count])

            logger.info("  采样 %d 只股票进行对比...", len(sample_codes))
            truth_df = _recompute_from_raw(tick_df, deal_df, sample_codes)

            # 取最后一个时间切片对比
            last_slice = factor_df
            if "end_time" in factor_df.columns:
                last_time = sorted(factor_df["end_time"].unique())[-1]
                last_slice = factor_df[factor_df["end_time"] == last_time]

            accuracy = _check_accuracy(last_slice, truth_df)
            logger.info("  匹配股票: %d", accuracy.get("matched_stocks", 0))

            for metric, info in accuracy.items():
                if isinstance(info, dict) and "exact_match" in info:
                    logger.info("  %-15s: 精确匹配 %d/%d, 最大绝对误差 %.4f, 平均相对误差 %s%%",
                                metric, info["exact_match"], info["total_valid"],
                                info.get("max_abs_diff", "?"),
                                info.get("mean_rel_err_pct", "?"))
        else:
            logger.warning("  无法加载 tick/deal 原始数据，跳过准确性检查")
    except Exception as e:
        logger.warning("  准确性检查失败: %s", e)

    # ---- 4. 延迟统计 ----
    logger.info("\n[4] 延迟统计...")
    latency = _check_latency(factor_df)
    for name, stats in latency.items():
        if isinstance(stats, dict) and "avg_ms" in stats:
            logger.info("  %s: avg=%sms p50=%sms p90=%sms max=%sms",
                        name, stats["avg_ms"], stats.get("p50_ms", "?"),
                        stats.get("p90_ms", "?"), stats.get("max_ms", "?"))

    # ---- 5. 数据时间连续性 ----
    logger.info("\n[5] 时间连续性检查...")
    if "end_time" in factor_df.columns:
        times = sorted(factor_df["end_time"].unique())
        logger.info("  因子计算次数: %d", len(times))
        logger.info("  首次计算: %s", times[0] if times else "?")
        logger.info("  末次计算: %s", times[-1] if times else "?")

        # 检查是否有缺失的分钟
        if len(times) >= 2:
            expected_interval = 60  # COMPUTE_INTERVAL 默认 60s
            gaps = []
            for i in range(1, len(times)):
                try:
                    prev = int(times[i - 1])
                    curr = int(times[i])
                    diff_sec = (curr // 100 * 60 + curr % 100) - (prev // 100 * 60 + prev % 100)
                    if diff_sec > expected_interval * 2:  # 超过 2 个间隔算 gap
                        gaps.append((times[i - 1], times[i], diff_sec))
                except ValueError:
                    pass
            if gaps:
                logger.warning("  发现 %d 个时间间隔异常:", len(gaps))
                for start, end, gap in gaps[:5]:
                    logger.warning("    %s -> %s (%ds)", start, end, gap)
            else:
                logger.info("  时间间隔正常，无缺失")

    logger.info("\n" + "=" * 60)
    logger.info("验证完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
