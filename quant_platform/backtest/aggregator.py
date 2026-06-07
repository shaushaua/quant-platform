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
        if key.endswith(".json") and key.count("/") == 4:
            keys.append(key)
    return sorted(keys)


def merge_daily_shards(bucket: oss2.Bucket, task_id: str, daily_keys: list[str]) -> dict:
    """
    按日期分组，将同一天的 _s{N}.json 分片合并成一个 {YYYYMMDD}.json，
    然后删除原始分片文件。返回汇总统计信息。
    """
    from collections import defaultdict

    # 按日期分组
    date_groups: dict[str, list[str]] = defaultdict(list)
    for key in daily_keys:
        filename = key.rsplit("/", 1)[-1].replace(".json", "")
        date_str = filename[:8]
        if not re.fullmatch(r"\d{8}", date_str):
            print(f"[aggregator] skip unknown result filename: {key}")
            continue
        date_groups[date_str].append(key)

    total_records = 0
    all_stocks = set()

    for date_str, keys in sorted(date_groups.items()):
        merged_records = []
        for key in sorted(keys):
            data = json.loads(bucket.get_object(key).read())
            if isinstance(data, list):
                merged_records.extend(data)
            elif isinstance(data, dict) and "records" in data:
                merged_records.extend(data["records"])

        total_records += len(merged_records)
        for record in merged_records:
            if "code" in record:
                all_stocks.add(record["code"])

        # 写合并后的文件: {task_id}/{YYYY}/{YYYYMM}/{YYYYMMDD}.json
        year = date_str[:4]
        month = date_str[4:6]
        merged_key = f"{task_id}/{year}/{year}{month}/{date_str}/{date_str}.json"
        payload = json.dumps(merged_records, ensure_ascii=False, default=str).encode("utf-8")
        bucket.put_object(merged_key, payload)
        print(f"[aggregator] merged {len(keys)} shards -> {merged_key} ({len(merged_records)} records)")

        # 保留原始分片文件，不删除

    sorted_days = sorted(date_groups.keys())
    return {
        "days": sorted_days,
        "day_count": len(sorted_days),
        "total_records": total_records,
        "stock_count": len(all_stocks),
        "stocks": sorted(all_stocks),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--strategy-name", default="")
    parser.add_argument("--total-shards", type=int, required=True)
    args = parser.parse_args()

    task_id = args.task_id
    strategy_name = args.strategy_name or task_id  # OSS路径用策略名

    print(f"[aggregator] task={task_id} strategy={strategy_name} total_shards={args.total_shards}")

    bucket = get_bucket()
    daily_keys = list_daily_results(bucket, strategy_name)

    if not daily_keys:
        print(f"[aggregator] ERROR: 未找到任何日期结果文件，prefix={strategy_name}/", file=sys.stderr)
        sys.exit(1)

    # 合并同日分片 + 汇总统计
    stats = merge_daily_shards(bucket, strategy_name, daily_keys)

    result = {"task_id": task_id, "strategy_name": strategy_name, **stats}

    print(f"[aggregator] days={stats['day_count']} records={stats['total_records']} stocks={stats['stock_count']}")

    result_key = f"{strategy_name}/result.json"
    bucket.put_object(result_key, json.dumps(result, ensure_ascii=False, default=str).encode("utf-8"))
    bucket_name = os.environ.get("OSS_RESULT_BUCKET", "stock-mdl-data-result")
    print(f"[aggregator] result written to oss://{bucket_name}/{result_key}")


if __name__ == "__main__":
    main()
