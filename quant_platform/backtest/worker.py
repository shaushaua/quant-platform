# -*- coding: utf-8 -*-
"""
Backtest Worker 入口

由 backtest-operator 通过 K8s Job 启动，执行单个分片回测。
环境变量由 K8s Secret (quant-secrets) 注入。

用法:
    python -m quant_platform.backtest.worker \
        --start-date 2024-01-01 \
        --end-date 2024-06-30 \
        --task-id my-strategy-1234567890 \
        --shard-index 0
"""

import argparse
import logging
import os
import sys

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Backtest worker")
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--task-id", required=True)
    p.add_argument("--shard-index", type=int, required=True)
    return p.parse_args()


def main():
    args = parse_args()
    logger.info(
        "Worker started: task=%s shard=%d range=[%s, %s]",
        args.task_id, args.shard_index, args.start_date, args.end_date,
    )

    from quant_platform.backtest.engine import BacktestEngine
    from quant_platform.core.config import get_config
    from quant_platform.data.api import DataAPI
    from quant_platform.data.oss_loader import OSSDataLoader

    cfg = get_config()
    data_path = os.getenv("DATA_PATH", cfg.oss_data_path)
    result_bucket = os.getenv("RESULT_BUCKET", cfg.oss_result_bucket)
    initial_capital = float(os.getenv("INITIAL_CAPITAL", str(cfg.default_account)))

    oss_loader = OSSDataLoader(data_path=data_path)
    data_api = DataAPI(oss_loader=oss_loader)

    engine = BacktestEngine(
        data_api=data_api,
        start_date=args.start_date,
        end_date=args.end_date,
        initial_capital=initial_capital,
    )

    result = engine.run()

    # Upload shard result to OSS
    import json
    import oss2

    access_key_id = os.getenv("OSS_ACCESS_KEY_ID", "")
    access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET", "")
    endpoint = os.getenv("OSS_ENDPOINT", "")

    if access_key_id and access_key_secret and endpoint:
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket = oss2.Bucket(auth, endpoint, result_bucket)
        key = f"results/{args.task_id}/shards/{args.shard_index}.json"
        payload = json.dumps(result, ensure_ascii=False, default=str)
        bucket.put_object(key, payload)
        logger.info("Shard result uploaded: %s", key)
    else:
        logger.warning("OSS not configured, result not uploaded")
        logger.info("Result: %s", result)


if __name__ == "__main__":
    main()
