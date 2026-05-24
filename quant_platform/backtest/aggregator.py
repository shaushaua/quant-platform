# -*- coding: utf-8 -*-
"""
Backtest Aggregator

从 OSS 读取 worker 按日期写入的结果文件，汇总统计信息后写回 OSS。

Worker 输出格式:
  - 无股票分片: {TASK_ID}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json
  - 有股票分片: {TASK_ID}/{YYYY}/{YYYYMM}/{YYYYMMDD}_s{N}.json
Aggregator 输出: {TASK_ID}/result.json（汇总统计）
"""
import argparse
import json
import os
import re
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
        os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result"),
    )


def list_daily_results(bucket: oss2.Bucket, task_id: str) -> list[str]:
    """列举 OSS 中该任务的所有日期结果文件。"""
    prefix = f"{task_id}/"
    keys = []
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        # 只取日期文件: {task_id}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json
        key = obj.key
        if key.endswith(".json") and key.count("/") == 3:
            keys.append(key)
    return sorted(keys)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--total-shards", type=int, required=True)
    args = parser.parse_args()

    task_id = args.task_id

    print(f"[aggregator] task={task_id} total_shards={args.total_shards}")

    bucket = get_bucket()
    daily_keys = list_daily_results(bucket, task_id)

    if not daily_keys:
        print(f"[aggregator] ERROR: 未找到任何日期结果文件，prefix={task_id}/", file=sys.stderr)
        sys.exit(1)

    # 读取每日结果，汇总统计
    total_records = 0
    days = set()
    all_stocks = set()

    for key in daily_keys:
        data = json.loads(bucket.get_object(key).read())
        if isinstance(data, list):
            total_records += len(data)
            for record in data:
                if "code" in record:
                    all_stocks.add(record["code"])
            # 从 key 提取日期: {task_id}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json 或 {YYYYMMDD}_s{N}.json
            filename = key.rsplit("/", 1)[-1].replace(".json", "")
            date_str = re.sub(r"_s\d+$", "", filename)  # 去掉 _s{N} 后缀
            days.add(date_str)
        elif isinstance(data, dict) and "records" in data:
            total_records += len(data["records"])
            filename = key.rsplit("/", 1)[-1].replace(".json", "")
            date_str = re.sub(r"_s\d+$", "", filename)
            days.add(date_str)

    sorted_days = sorted(days)
    result = {
        "task_id": task_id,
        "days": sorted_days,
        "day_count": len(sorted_days),
        "total_records": total_records,
        "stock_count": len(all_stocks),
        "stocks": sorted(all_stocks),
    }

    print(f"[aggregator] days={len(sorted_days)} records={total_records} stocks={len(all_stocks)}")

    result_key = f"{task_id}/result.json"
    bucket.put_object(result_key, json.dumps(result, ensure_ascii=False, default=str).encode("utf-8"))
    bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
    print(f"[aggregator] result written to oss://{bucket_name}/{result_key}")


if __name__ == "__main__":
    main()
