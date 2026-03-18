# -*- coding: utf-8 -*-
"""
因子计算 Worker 入口

由 backtest-operator 通过 K8s Job 启动，执行单个分片的因子计算。
交易员的因子代码挂载在 /app/factor.py，必须包含一个 BaseFactor 子类。

用法:
    python -m quant_platform.backtest.worker \\
        --start-date 2024-01-01 \\
        --end-date 2024-06-30 \\
        --task-id my-factor-1234567890 \\
        --shard-index 0
"""

import argparse
import importlib.util
import logging
import os
import sys

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

FACTOR_CODE_PATH = os.getenv("FACTOR_CODE_PATH", "/app/factor.py")


def parse_args():
    p = argparse.ArgumentParser(description="Factor worker")
    p.add_argument("--start-date", required=True)
    p.add_argument("--end-date", required=True)
    p.add_argument("--task-id", required=True)
    p.add_argument("--shard-index", type=int, required=True)
    return p.parse_args()


def load_factor(path: str):
    """从文件加载交易员上传的因子代码，返回 BaseFactor 实例。"""
    from ..factor.base import BaseFactor

    if not os.path.exists(path):
        raise FileNotFoundError(f"因子代码文件不存在: {path}")

    spec = importlib.util.spec_from_file_location("user_factor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # 找到第一个 BaseFactor 子类
    for attr in vars(module).values():
        if (
            isinstance(attr, type)
            and issubclass(attr, BaseFactor)
            and attr is not BaseFactor
        ):
            instance = attr()
            if not instance.name:
                raise ValueError(f"{attr.__name__} 必须设置 name 属性")
            logger.info("加载因子: %s", instance.name)
            return instance

    raise RuntimeError(f"{path} 中未找到 BaseFactor 子类")


def main():
    args = parse_args()
    logger.info(
        "Worker 启动: task=%s shard=%d [%s, %s]",
        args.task_id, args.shard_index, args.start_date, args.end_date,
    )

    from ..data.api import DataAPI
    from ..factor.engine import FactorEngine

    result_prefix = os.getenv("RESULT_BUCKET", "factor-results")

    factor = load_factor(FACTOR_CODE_PATH)
    data_api = DataAPI()
    engine = FactorEngine(data_api, oss_result_prefix=result_prefix)

    result_path = engine.run_history(
        factor=factor,
        start_date=args.start_date,
        end_date=args.end_date,
        task_id=args.task_id,
        shard_index=args.shard_index,
    )
    logger.info("Worker 完成，结果: %s", result_path)


if __name__ == "__main__":
    main()
