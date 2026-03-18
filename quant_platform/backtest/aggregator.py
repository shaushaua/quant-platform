# -*- coding: utf-8 -*-
"""
因子计算聚合器

由 backtest-operator 在所有 worker shard 完成后启动。
从 OSS 读取各 shard 的 parquet 文件，合并为一张完整的因子矩阵，
写回 OSS：{result_prefix}/{task_id}/result.parquet

因子矩阵格式：
    行 (index)  = stock_code  (如 "000001.XSHE")
    列 (columns) = date        (如 "2024-01-02")
    值           = 因子值 float

用法:
    python -m quant_platform.backtest.aggregator \\
        --task-id my-factor-1234567890 \\
        --shards 4
"""

import argparse
import io
import logging
import os

import pandas as pd

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Factor aggregator")
    p.add_argument("--task-id", required=True)
    p.add_argument("--shards", type=int, required=True)
    return p.parse_args()


def main():
    args = parse_args()
    logger.info("Aggregator 启动: task=%s shards=%d", args.task_id, args.shards)

    import oss2
    from ..core.config import get_config

    cfg = get_config()
    result_prefix = os.getenv("RESULT_BUCKET", "factor-results")
    auth = oss2.Auth(cfg.oss_access_key, cfg.oss_secret_key)
    bucket = oss2.Bucket(auth, cfg.oss_endpoint, cfg.oss_bucket)

    # 读取所有 shard
    shards = []
    for i in range(args.shards):
        key = f"{result_prefix}/{args.task_id}/shard-{i}.parquet".lstrip("/")
        logger.info("读取 shard %d: %s", i, key)
        obj = bucket.get_object(key)
        df = pd.read_parquet(io.BytesIO(obj.read()))
        shards.append(df)

    # 按列（日期）合并，去重后按日期排序
    result = pd.concat(shards, axis=1)
    result = result.loc[:, ~result.columns.duplicated()]
    result = result.sort_index(axis=1)
    logger.info("合并完成: %d stocks × %d dates", *result.shape)

    # 写入最终结果
    out_key = f"{result_prefix}/{args.task_id}/result.parquet".lstrip("/")
    buf = io.BytesIO()
    result.to_parquet(buf, engine="pyarrow", compression="snappy")
    buf.seek(0)
    bucket.put_object(out_key, buf.read())
    logger.info("结果写入 oss://%s/%s", cfg.oss_bucket, out_key)


if __name__ == "__main__":
    main()
