# -*- coding: utf-8 -*-
"""
转换器诊断脚本

在集群节点上运行，检查通联 CSV 数据能否被正确转换。
用法：
    # 在 collector pod 内运行
    crictl exec -it $(crictl ps | grep collector | awk '{print $1}') python3 scripts/diagnose_converter.py

    # 或在宿主机上直接运行（需要项目在 PYTHONPATH 中）
    python scripts/diagnose_converter.py --date 20260512

检查项：
    1. CSV 文件是否存在、列名列表
    2. 转换后的 DataFrame 列名、dtype、样本值
    3. 异常值检测（PreClosePrice > 1000、Volume 有小数、Time 为 epoch）
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# 添加项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from quant_platform.core.constants import TICK_COLUMNS
from quant_platform.data.converter import TonglanceDataConverter

# 通联文件目录
MSG_BACKUP_DIR = Path("/root/mdl/msg_backup")

# 文件类型映射
FILE_TYPES = {
    "mdl_4_4_0.csv": ("SH", "tick", "convert_sh_tick"),
    "mdl_6_28_0.csv": ("SZ", "tick", "convert_sz_tick"),
    "mdl_4_24_0.csv": ("SH", "order_deal", "convert_sh_order_deal"),
    "mdl_6_33_0.csv": ("SZ", "order", "convert_sz_order"),
    "mdl_6_36_0.csv": ("SZ", "deal", "convert_sz_deal"),
}


def diagnose_csv(csv_path: Path, converter: TonglanceDataConverter, n_rows: int = 100):
    """诊断单个 CSV 文件的转换结果。"""
    print(f"\n{'='*60}")
    print(f"文件: {csv_path.name}")
    print(f"大小: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*60}")

    # 读取前 N 行
    try:
        raw_df = pd.read_csv(csv_path, nrows=n_rows)
    except Exception as e:
        print(f"  读取失败: {e}")
        return

    print(f"\n[1] 原始数据: {len(raw_df)} 行, {len(raw_df.columns)} 列")
    print(f"    列名: {list(raw_df.columns)}")
    print(f"\n    dtypes:")
    for col in raw_df.columns:
        dtype = raw_df[col].dtype
        sample = raw_df[col].iloc[0] if len(raw_df) > 0 else "?"
        print(f"      {col:30s} {str(dtype):15s} sample={sample}")

    # 判断文件类型并转换
    fname = csv_path.name
    # 支持带日期前缀的文件名
    for suffix, (market, data_type, method_name) in FILE_TYPES.items():
        if fname.endswith(suffix):
            break
    else:
        print(f"  未识别的文件类型: {fname}")
        return

    print(f"\n[2] 转换: {market} {data_type} ({method_name})")

    trading_day = pd.Timestamp(datetime.now().strftime("%Y-%m-%d"))
    # 尝试从文件名提取日期
    if fname[:8].isdigit():
        d = fname[:8]
        trading_day = pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:8]}")

    method = getattr(converter, method_name)

    try:
        if data_type == "order_deal":
            order_df, deal_df = method(raw_df, trading_day)
            for label, df in [("order", order_df), ("deal", deal_df)]:
                _print_converted(label, df)
        else:
            if data_type == "tick":
                df = method(raw_df, trading_day)
            else:
                df = method(raw_df, trading_day)
            _print_converted(data_type, df)
    except Exception as e:
        print(f"  转换失败: {e}")
        import traceback
        traceback.print_exc()


def _print_converted(label: str, df: pd.DataFrame):
    """打印转换后的 DataFrame 诊断信息。"""
    print(f"\n  [{label}] 转换结果: {len(df)} 行, {len(df.columns)} 列")
    if df.empty:
        print("    ⚠ 结果为空！可能是列名不匹配导致 KeyError")
        return

    # 打印列名和 dtype
    print(f"    列名: {list(df.columns)}")

    # 异常值检测
    anomalies = []

    # Tick 数据检查
    if "PreClosePrice" in df.columns:
        bad = df[df["PreClosePrice"] > 1000]
        if not bad.empty:
            anomalies.append(f"PreClosePrice > 1000: {len(bad)} 行 (max={bad['PreClosePrice'].max():.1f})")

    if "CurrentPrice" in df.columns:
        bad = df[(df["CurrentPrice"] > 1000) & (df["CurrentPrice"] > 0)]
        if not bad.empty:
            anomalies.append(f"CurrentPrice > 1000: {len(bad)} 行 (max={bad['CurrentPrice'].max():.1f})")

    for col in ["AskVolume1", "BidVolume1"]:
        if col in df.columns:
            vals = df[col].dropna()
            if not vals.empty and vals.dtype == np.float64:
                has_frac = (vals != vals.astype(int)).any()
                if has_frac:
                    anomalies.append(f"{col} 有小数值（应为整数）: sample={vals.iloc[0]}")

    if "Time" in df.columns:
        bad = df[df["Time"] < pd.Timestamp("2000-01-01")]
        if not bad.empty:
            sample = bad["Time"].iloc[0]
            anomalies.append(f"Time < 2000（可能 epoch 错误）: {len(bad)} 行, sample={sample}")

    if anomalies:
        print(f"\n    ⚠ 发现 {len(anomalies)} 个异常:")
        for a in anomalies:
            print(f"      - {a}")
    else:
        print(f"\n    ✓ 未发现明显异常")

    # 打印前 3 行的关键字段
    key_cols = [c for c in ["Code", "Time", "CurrentPrice", "PreClosePrice",
                             "TotalVolume", "AskPrice1", "BidPrice1",
                             "AskVolume1", "BidVolume1"] if c in df.columns]
    if key_cols:
        print(f"\n    样本数据（前3行）:")
        print(df[key_cols].head(3).to_string(index=False))


def diagnose_shm_store():
    """检查 ShmStore 中的 Arrow chunk 数据。"""
    from quant_platform.data.shm_store import ShmStore

    print(f"\n{'='*60}")
    print("ShmStore 诊断")
    print(f"{'='*60}")

    store = ShmStore()

    for data_type in ["tick", "deal", "order"]:
        from quant_platform.data.shm_store import SHM_BASE
        chunk_dir = SHM_BASE / data_type
        chunks = sorted(chunk_dir.glob("chunk_*.arrow")) if chunk_dir.exists() else []
        print(f"\n  [{data_type}] chunk 文件数: {len(chunks)}")

        if not chunks:
            continue

        # 只读最新 chunk
        import pyarrow.ipc as ipc
        latest = chunks[-1]
        try:
            reader = ipc.open_file(str(latest))
            df = reader.read_all().to_pandas()
            print(f"    最新 chunk: {latest.name}")
            print(f"    行数: {len(df)}, 列: {list(df.columns[:15])}...")

            if "Code" in df.columns:
                codes = df["Code"].unique()
                print(f"    股票数: {len(codes)}, 样本: {list(codes[:5])}")

            # 检查异常值
            if "PreClosePrice" in df.columns:
                bad = df[df["PreClosePrice"] > 1000]
                if not bad.empty:
                    print(f"    ⚠ PreClosePrice > 1000: {len(bad)} 行")
                    sample = bad[["Code", "PreClosePrice"]].head(3)
                    print(f"    {sample.to_string(index=False)}")

            if "Time" in df.columns:
                bad = df[df["Time"] < pd.Timestamp("2000-01-01")]
                if not bad.empty:
                    print(f"    ⚠ Time < 2000: {len(bad)} 行, sample={bad['Time'].iloc[0]}")

        except Exception as e:
            print(f"    读取失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="转换器诊断")
    parser.add_argument("--date", help="日期 YYYYMMDD，默认今天")
    parser.add_argument("--rows", type=int, default=100, help="读取行数（默认100）")
    parser.add_argument("--shm", action="store_true", help="同时检查 ShmStore")
    args = parser.parse_args()

    date_str = args.date or datetime.now().strftime("%Y%m%d")
    day_dir = MSG_BACKUP_DIR / date_str

    print(f"诊断日期: {date_str}")
    print(f"CSV 目录: {day_dir}")

    if not day_dir.exists():
        print(f"  ⚠ 目录不存在！")
        # 列出可用日期
        if MSG_BACKUP_DIR.exists():
            dirs = sorted([d.name for d in MSG_BACKUP_DIR.iterdir()
                          if d.is_dir() and d.name.isdigit()])
            print(f"  可用日期: {dirs[-5:] if dirs else '无'}")
        return

    converter = TonglanceDataConverter()

    # 找到所有 CSV 文件
    csv_files = sorted(day_dir.glob("*.csv"))
    print(f"找到 {len(csv_files)} 个 CSV 文件: {[f.name for f in csv_files]}")

    for csv_file in csv_files:
        diagnose_csv(csv_file, converter, args.rows)

    # ShmStore 诊断
    if args.shm:
        diagnose_shm_store()

    print(f"\n{'='*60}")
    print("诊断完成")


if __name__ == "__main__":
    main()
