#!/usr/bin/env python3
"""检查 parquet 文件的排序方式"""
import os
import oss2
import pandas as pd
import io

ak = os.environ.get('OSS_ACCESS_KEY_ID')
sk = os.environ.get('OSS_ACCESS_KEY_SECRET')
endpoint = os.environ.get('OSS_ENDPOINT')
bucket_name = 'stock-mdl-data'

auth = oss2.Auth(ak, sk)
bucket = oss2.Bucket(auth, endpoint, bucket_name)

# 读取一个 tick 文件样本
key = '2025/202501/20250106/20250106_tick.parquet'
print(f'正在读取: {key}')
result = bucket.get_object(key)
data = result.read()
df = pd.read_parquet(io.BytesIO(data))

print(f'\n文件总行数: {len(df):,}')
print(f'列名: {list(df.columns)}')

# 检查前100行
sample = df.head(100)
if 'Code' in sample.columns and 'SECURITY_ID' in sample.columns:
    print('\n前20行 Code 和 SECURITY_ID:')
    print(sample[['Code', 'SECURITY_ID']].head(20).to_string())

    # 检查排序
    is_code_sorted = df['Code'].is_monotonic_increasing
    is_security_id_sorted = df['SECURITY_ID'].is_monotonic_increasing

    print(f'\n按 Code 排序: {is_code_sorted}')
    print(f'按 SECURITY_ID 排序: {is_security_id_sorted}')

    # 检查唯一值数量
    print(f'\nCode 唯一值数量: {df["Code"].nunique()}')
    print(f'SECURITY_ID 唯一值数量: {df["SECURITY_ID"].nunique()}')

    # 显示部分唯一值
    print(f'\nCode 前10个唯一值: {sorted(df["Code"].unique())[:10]}')
    print(f'SECURITY_ID 前10个唯一值: {sorted(df["SECURITY_ID"].unique())[:10]}')
