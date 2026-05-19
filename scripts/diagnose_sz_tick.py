#!/usr/bin/env python3
"""
SZ Tick 数据诊断脚本

检查 mdl_6_28_0.csv 文件的列对齐问题。
在集群节点上运行：
    python scripts/diagnose_sz_tick.py --date 20260519
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
import numpy as np

MSG_BACKUP_DIR = Path("/www/wwwroot/mdl/msg_backup")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--rows", type=int, default=20)
    args = parser.parse_args()

    day_dir = MSG_BACKUP_DIR / args.date
    sz_tick = day_dir / f"{args.date}_mdl_6_28_0.csv"
    if not sz_tick.exists():
        # 也试试不带日期前缀的文件名
        sz_tick = day_dir / "mdl_6_28_0.csv"
    if not sz_tick.exists():
        print(f"SZ tick 文件不存在: {day_dir}/mdl_6_28_0.csv")
        # 列出目录下的文件
        if day_dir.exists():
            print(f"目录内容: {[f.name for f in day_dir.glob('*.csv')]}")
        return

    print(f"文件: {sz_tick}")
    print(f"大小: {sz_tick.stat().st_size / 1024 / 1024:.1f} MB")

    # 1. 检查原始 header
    with open(sz_tick, "r", encoding="utf-8", errors="replace") as f:
        header_line = f.readline().strip()
    header_fields = header_line.split(",")
    print(f"\n[1] Header 字段数: {len(header_fields)}")
    print(f"    前10个: {header_fields[:10]}")
    print(f"    后10个: {header_fields[-10:]}")

    # 检查是否有空格
    has_spaces = [h for h in header_fields if h != h.strip()]
    if has_spaces:
        print(f"    ⚠ 有前导/尾随空格的列名: {has_spaces}")
    else:
        print(f"    ✓ 列名无多余空格")

    # 检查 BOM
    if header_fields[0].startswith("\ufeff"):
        print(f"    ⚠ BOM 检测到: 第一列名 = '{repr(header_fields[0])}'")

    # 检查 SecurityID 在第几列
    for i, h in enumerate(header_fields):
        if "SecurityID" in h and "Source" not in h:
            print(f"    SecurityID 在第 {i} 列: '{h}'")
        if "SecurityIDSource" in h:
            print(f"    SecurityIDSource 在第 {i} 列: '{h}'")

    # 2. 读取前 N 行数据
    print(f"\n[2] 读取前 {args.rows} 行数据")
    raw_df = pd.read_csv(sz_tick, nrows=args.rows)
    print(f"    DataFrame shape: {raw_df.shape}")
    print(f"    列名: {list(raw_df.columns[:10])}...")

    # 3. 检查 SecurityID 列的值
    if "SecurityID" in raw_df.columns:
        sid = raw_df["SecurityID"]
        print(f"\n[3] SecurityID 列:")
        print(f"    dtype: {sid.dtype}")
        print(f"    unique 数量: {sid.nunique()}")
        print(f"    前10个值: {list(sid.head(10))}")
        # 检查是否有 102 这个值
        if 102 in sid.values:
            print(f"    ⚠ SecurityID 中有 102（可能是 SecurityIDSource 的值）")
            count_102 = (sid == 102).sum()
            print(f"    102 出现次数: {count_102} / {len(sid)}")
    else:
        print(f"\n[3] ⚠ SecurityID 列不存在！")
        print(f"    可用列: {list(raw_df.columns)}")

    # 4. 检查 SecurityIDSource 列
    if "SecurityIDSource" in raw_df.columns:
        src = raw_df["SecurityIDSource"]
        print(f"\n[4] SecurityIDSource 列:")
        print(f"    dtype: {src.dtype}")
        print(f"    unique: {src.unique()[:10]}")
        print(f"    前10个值: {list(src.head(10))}")
    else:
        print(f"\n[4] SecurityIDSource 列不存在")

    # 5. 模拟 FilePoller 的增量读取
    print(f"\n[5] 模拟 FilePoller 增量读取")
    # 从文件中间开始读取，模拟 skip_existing=True 的场景
    file_size = sz_tick.stat().st_size
    start_offset = min(100 * 1024, file_size // 2)  # 从 100KB 处开始

    with open(sz_tick, "rb") as f:
        f.seek(start_offset)
        chunk = f.read(4096)  # 读 4KB

    last_newline = chunk.rfind(b"\n")
    if last_newline > 0:
        complete_chunk = chunk[:last_newline + 1]
        text = complete_chunk.decode("utf-8", errors="replace")

        # 使用 header 来解析
        df_poller = pd.read_csv(
            __import__("io").StringIO(text),
            header=None,
            names=header_fields
        )
        print(f"    从 offset={start_offset} 读取 {len(df_poller)} 行")

        # 检查列对齐
        if len(df_poller.columns) != len(header_fields):
            print(f"    ⚠ 列数不匹配！data={len(df_poller.columns)} vs header={len(header_fields)}")

        # 检查 SecurityID 的值
        if "SecurityID" in df_poller.columns:
            sid_poller = df_poller["SecurityID"]
            print(f"    SecurityID 前5值: {list(sid_poller.head(5))}")

            # 检查这些值是否像股票代码
            sample = sid_poller.iloc[0]
            if isinstance(sample, (int, np.integer)):
                if sample < 1000:
                    print(f"    ⚠ SecurityID 值过小 ({sample})，可能是 SecurityIDSource 的值被错位了！")
                    print(f"    SecurityIDSource 前5值: {list(df_poller['SecurityIDSource'].head(5)) if 'SecurityIDSource' in df_poller.columns else 'N/A'}")
                else:
                    print(f"    ✓ SecurityID 值看起来正常 ({sample})")
        else:
            print(f"    ⚠ SecurityID 列不存在！")

        # 打印第一行的所有值（与 header 对照）
        print(f"\n    第一行数据对照:")
        for i, (col, val) in enumerate(zip(header_fields[:15], df_poller.iloc[0][:15])):
            print(f"      [{i:2d}] {col:25s} = {val}")

    # 6. 直接用 read_csv 从中间开始读
    print(f"\n[6] 直接用 pd.read_csv 跳行读取")
    # 计算大约 100KB 对应的行数
    with open(sz_tick, "r") as f:
        # 读取 10 行估算行大小
        lines = []
        for i, line in enumerate(f):
            if i == 0:
                continue  # skip header
            lines.append(line)
            if len(lines) >= 100:
                break
    if lines:
        avg_line_size = sum(len(l) for l in lines) / len(lines)
        skip_rows = int(100 * 1024 / avg_line_size)
        print(f"    跳过约 {skip_rows} 行")
        df_skip = pd.read_csv(sz_tick, skiprows=range(1, skip_rows + 1), nrows=args.rows)
        if "SecurityID" in df_skip.columns:
            print(f"    SecurityID 前5值: {list(df_skip['SecurityID'].head(5))}")
            if "SecurityIDSource" in df_skip.columns:
                print(f"    SecurityIDSource 前5值: {list(df_skip['SecurityIDSource'].head(5))}")

    # 7. SH tick 对比
    sh_tick = day_dir / f"{args.date}_mdl_4_4_0.csv"
    if not sh_tick.exists():
        sh_tick = day_dir / "mdl_4_4_0.csv"
    if sh_tick.exists():
        print(f"\n[7] SH tick 对比: {sh_tick.name}")
        with open(sh_tick, "r") as f:
            sh_header = f.readline().strip().split(",")
        print(f"    SH header 前10: {sh_header[:10]}")
        sh_df = pd.read_csv(sh_tick, nrows=5)
        if "SecurityID" in sh_df.columns:
            print(f"    SH SecurityID 前5: {list(sh_df['SecurityID'].head(5))}")
        if "SecurityIDSource" in sh_df.columns:
            print(f"    SH SecurityIDSource 前5: {list(sh_df['SecurityIDSource'].head(5))}")
        else:
            print(f"    SH 无 SecurityIDSource 列")

    print(f"\n{'='*60}")
    print("诊断完成")


if __name__ == "__main__":
    main()
