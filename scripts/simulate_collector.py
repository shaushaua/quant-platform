#!/usr/bin/env python3
"""
模拟 collector：从 OSS 下载历史 parquet，转换 Code 后并行写入 ShmStore chunk
tick / deal / order 同时写入，模拟实盘 inotify 同时触发。

用法：
    python simulate_collector.py --date 20250103 --speed 1x
    python simulate_collector.py --date 20250103 --speed max --max-rows 50000
"""

import argparse
import os
import threading
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

SHM_BASE = Path(os.environ.get("SHM_STORE_PATH", "/dev/shm/quant-store"))
CHUNK_ROWS = 5000

_seq = 0
_seq_lock = threading.Lock()


def download_from_oss(date_str: str, data_type: str) -> Path:
    """从 OSS 下载历史 parquet 到 /tmp"""
    import oss2

    endpoint = os.environ.get("OSS_ENDPOINT", "")
    ak_id = os.environ.get("OSS_ACCESS_KEY_ID", "")
    ak_secret = os.environ.get("OSS_ACCESS_KEY_SECRET", "")
    bucket_name = os.environ.get("OSS_DATA_BUCKET", "quant-mdl-data")

    year = date_str[:4]
    month = date_str[4:6]
    key = f"{year}/{year}{month}/{date_str}/{date_str}_{data_type}.parquet"

    local_path = Path(f"/tmp/sim_{date_str}_{data_type}.parquet")
    if local_path.exists():
        print(f"[sim] 已有本地文件: {local_path} ({local_path.stat().st_size/1024/1024:.0f}MB)")
        return local_path

    auth = oss2.Auth(ak_id, ak_secret)
    ep = endpoint.replace("https://", "").replace("http://", "")
    bucket = oss2.Bucket(auth, ep, bucket_name)

    print(f"[sim] 下载 oss://{bucket_name}/{key}")
    bucket.get_object_to_file(key, str(local_path))
    print(f"[sim] 下载完成: {local_path.stat().st_size/1024/1024:.0f}MB")
    return local_path


def load_code_map(date_str: str) -> dict:
    """从 daily_basic 构建 SECURITY_ID(int) → '000001.XSHE' 映射。"""
    print("[sim] 加载 daily_basic 构建代码映射...")
    path = download_from_oss(date_str, "daily_basic_data")
    df = pd.read_parquet(path, columns=["SECURITY_ID", "ID_QI"])
    code_map = {}
    for _, row in df.iterrows():
        sec_id = int(row["SECURITY_ID"])
        id_qi = str(row["ID_QI"]).zfill(6)
        suffix = ".XSHG" if id_qi[0] in ("6", "9") else ".XSHE"
        code_map[sec_id] = id_qi + suffix
    print(f"[sim] 代码映射: {len(code_map)} 只股票")
    return code_map


def convert_codes(df: pd.DataFrame, code_map: dict) -> pd.DataFrame:
    """将 Code 列从 SECURITY_ID(int32) 转换为 '000001.XSHE' 格式。"""
    if "Code" not in df.columns:
        return df
    df = df.copy()
    df["Code"] = df["Code"].map(code_map)
    df = df.dropna(subset=["Code"])
    return df


def write_chunk(df: pd.DataFrame, data_type: str) -> Path:
    """写一个 chunk 到 ShmStore（线程安全）"""
    global _seq

    chunk_dir = SHM_BASE / data_type
    chunk_dir.mkdir(parents=True, exist_ok=True)

    with _seq_lock:
        ts = int(time.time() * 1000)
        _seq += 1
        seq = _seq

    path = chunk_dir / f"chunk_{ts}_{seq:06d}.arrow"

    table = pa.Table.from_pandas(df, preserve_index=False)
    tmp = path.with_suffix(".tmp")
    with ipc.new_file(str(tmp), table.schema) as writer:
        writer.write_table(table)
    tmp.replace(path)
    return path


def writer_thread(data_type: str, parquet_path: Path, code_map: dict,
                  speed: str, max_rows: int, results: dict):
    """单数据类型的写入线程。"""
    total_rows = 0
    chunk_count = 0

    pf = pq.ParquetFile(str(parquet_path))

    for batch in pf.iter_batches(batch_size=CHUNK_ROWS):
        df = batch.to_pandas()
        if df.empty:
            continue

        df = convert_codes(df, code_map)
        if df.empty:
            continue

        if max_rows > 0 and total_rows >= max_rows:
            break

        write_chunk(df, data_type)
        chunk_count += 1
        total_rows += len(df)

        # 速度控制
        if speed == "1x":
            time.sleep(0.1)
        elif speed not in ("max", "1x"):
            try:
                time.sleep(float(speed))
            except ValueError:
                pass

        if chunk_count % 500 == 0:
            print(f"[sim] {data_type}: {chunk_count} chunks, {total_rows} rows")

    results[data_type] = {"chunks": chunk_count, "rows": total_rows}
    print(f"[sim] {data_type} 完成: {chunk_count} chunks, {total_rows} rows")


def simulate(date_str: str, speed: str, data_types: list, max_rows: int):
    """主入口：下载 → 启动多线程并行写入。"""
    global _seq
    _seq = int(time.time() * 1000)

    # 加载代码映射
    code_map = load_code_map(date_str)

    t0 = time.time()

    # 下载所有数据
    parquet_files = {}
    for data_type in data_types:
        try:
            parquet_files[data_type] = download_from_oss(date_str, data_type)
        except Exception as e:
            print(f"[sim] 下载 {data_type} 失败: {e}，跳过")

    if not parquet_files:
        print("[sim] 没有可用的数据")
        return

    # 每种数据类型一个线程，并行写入
    results = {}
    threads = []
    for data_type, path in parquet_files.items():
        t = threading.Thread(
            target=writer_thread,
            args=(data_type, path, code_map, speed, max_rows, results),
            daemon=True,
        )
        threads.append(t)

    print(f"\n[sim] 启动 {len(threads)} 个写入线程: {list(parquet_files.keys())}")
    for t in threads:
        t.start()

    # 等待所有线程完成
    for t in threads:
        t.join()

    elapsed = time.time() - t0
    summary = ", ".join(f"{dt}={r['rows']}" for dt, r in results.items())
    print(f"\n[sim] 全部写入完成，耗时 {elapsed:.1f}s")
    print(f"[sim] 数据量: {summary}")


def main():
    parser = argparse.ArgumentParser(description="模拟 collector 写入 ShmStore")
    parser.add_argument("--date", required=True, help="交易日 YYYYMMDD")
    parser.add_argument("--speed", default="max",
                        help="速度: max=最快, 1x=近似实盘, 数字=秒间隔")
    parser.add_argument("--data-types", default="tick,deal,order",
                        help="数据类型 (逗号分隔，默认 tick,deal,order)")
    parser.add_argument("--max-rows", type=int, default=0,
                        help="每种数据最多读多少行 (0=全部)")
    args = parser.parse_args()

    data_types = [t.strip() for t in args.data_types.split(",")]
    simulate(args.date, args.speed, data_types, args.max_rows)


if __name__ == "__main__":
    main()
