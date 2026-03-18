# -*- coding: utf-8 -*-
"""
Backtest Aggregator 入口

由 backtest-operator 在所有 Worker Job 完成后启动，
合并各分片结果并写入 results/{task_id}/result.json。

用法:
    python -m quant_platform.backtest.aggregator \
        --task-id my-strategy-1234567890 \
        --total-shards 4
"""

import argparse
import json
import logging
import os

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Backtest aggregator")
    p.add_argument("--task-id", required=True)
    p.add_argument("--total-shards", type=int, required=True)
    return p.parse_args()


def aggregate_shards(shards: list[dict]) -> dict:
    """Merge shard results into a single aggregated result."""
    if not shards:
        return {}

    total_return_list = [s.get("total_return", 0.0) for s in shards]
    nav_curves = [s.get("nav_curve", []) for s in shards]
    trades = []
    for s in shards:
        trades.extend(s.get("trades", []))

    # Compound total return across shards
    compound_return = 1.0
    for r in total_return_list:
        compound_return *= (1 + r)
    total_return = compound_return - 1.0

    # Flatten nav curve (concatenate by date order)
    flat_nav = []
    for curve in nav_curves:
        flat_nav.extend(curve)
    flat_nav.sort(key=lambda x: x[0] if isinstance(x, (list, tuple)) else x.get("date", ""))

    # Simple max drawdown across merged nav
    max_dd = 0.0
    peak = 1.0
    for point in flat_nav:
        nav = point[1] if isinstance(point, (list, tuple)) else point.get("nav", 1.0)
        if nav > peak:
            peak = nav
        dd = (peak - nav) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    trading_days = sum(s.get("total_trading_days", 0) for s in shards)
    annual_return = (1 + total_return) ** (250.0 / max(trading_days, 1)) - 1 if trading_days > 0 else 0.0

    return {
        "task_id": shards[0].get("task_id", ""),
        "total_trading_days": trading_days,
        "instances_total": len(shards),
        "instances_completed": len(shards),
        "total_return": round(total_return, 6),
        "annual_return": round(annual_return, 6),
        "max_drawdown": round(max_dd, 6),
        "total_trades": len(trades),
        "nav_curve": flat_nav,
        "trades": trades,
        "is_partial": False,
    }


def main():
    args = parse_args()
    result_bucket = os.getenv("RESULT_BUCKET", "")
    access_key_id = os.getenv("OSS_ACCESS_KEY_ID", "")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET", "")
    endpoint = os.getenv("OSS_ENDPOINT", "")

    import oss2
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, result_bucket)

    shards = []
    for i in range(args.total_shards):
        key = f"results/{args.task_id}/shards/{i}.json"
        try:
            content = bucket.get_object(key).read()
            shards.append(json.loads(content))
            logger.info("Loaded shard %d", i)
        except Exception as e:
            logger.error("Failed to load shard %d: %s", i, e)

    if not shards:
        logger.error("No shard results found for task %s", args.task_id)
        raise SystemExit(1)

    result = aggregate_shards(shards)
    result["task_id"] = args.task_id

    out_key = f"results/{args.task_id}/result.json"
    bucket.put_object(out_key, json.dumps(result, ensure_ascii=False, default=str))
    logger.info("Aggregated result uploaded: %s", out_key)


if __name__ == "__main__":
    main()
