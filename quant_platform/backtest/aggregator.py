# -*- coding: utf-8 -*-
"""
Backtest Aggregator

从 OSS 读取各 shard 结果，合并后写回 OSS: {RESULT_BUCKET}/{TASK_ID}/result.json

用法:
    python -m quant_platform.backtest.aggregator \
        --task-id <task_id> \
        --total-shards <n>
"""
import argparse
import json
import os
import sys

import oss2


def get_bucket() -> oss2.Bucket:
    auth = oss2.Auth(
        os.environ["OSS_ACCESS_KEY_ID"],
        os.environ["OSS_ACCESS_KEY_SECRET"],
    )
    return oss2.Bucket(
        auth,
        os.environ["OSS_ENDPOINT"],
        os.environ.get("OSS_RESULT_BUCKET", "quant-backtest-results"),
    )


def read_shard(bucket: oss2.Bucket, task_id: str, shard_index: int) -> dict:
    key = f"{task_id}/{shard_index}.json"
    result = bucket.get_object(key)
    return json.loads(result.read())


def aggregate(shards: list[dict]) -> dict:
    """简单聚合：对数值字段取均值，日期取 min/max。"""
    if not shards:
        return {}

    numeric_keys = [k for k, v in shards[0].items() if isinstance(v, (int, float))]
    date_keys = ["start_date", "end_date"]

    result = {}
    for key in numeric_keys:
        values = [s[key] for s in shards if key in s]
        result[key] = sum(values) / len(values) if values else None

    if "start_date" in shards[0]:
        result["start_date"] = min(s["start_date"] for s in shards)
    if "end_date" in shards[0]:
        result["end_date"] = max(s["end_date"] for s in shards)

    result["total_shards"] = len(shards)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--total-shards", type=int, required=True)
    args = parser.parse_args()

    task_id = args.task_id
    total_shards = args.total_shards

    print(f"[aggregator] task={task_id} total_shards={total_shards}")

    bucket = get_bucket()

    shards = []
    for i in range(total_shards):
        try:
            shard = read_shard(bucket, task_id, i)
            shards.append(shard)
            print(f"[aggregator] shard {i}: {shard}")
        except Exception as e:
            print(f"[aggregator] ERROR reading shard {i}: {e}", file=sys.stderr)
            sys.exit(1)

    result = aggregate(shards)
    print(f"[aggregator] aggregated result: {result}")

    key = f"{task_id}/result.json"
    bucket_name = os.environ.get("OSS_RESULT_BUCKET", "quant-backtest-results")
    bucket.put_object(key, json.dumps(result, ensure_ascii=False).encode("utf-8"))
    print(f"[aggregator] Written to oss://{bucket_name}/{key}")


if __name__ == "__main__":
    main()
