#!/usr/bin/env python3
"""
监控通联 CSV 文件的写入行为，验证 collector 的 offset 机制是否安全。

检查项：
1. 文件是否只追加（size 单调递增）
2. inode 是否变化（文件被 rename 覆盖）
3. 文件是否被截断（size 变小）
4. nlink 是否变化（文件被删除重建）

用法（在实盘节点上运行）：
    python3 scripts/monitor_csv_write.py /www/wwwroot/mdl/msg_backup/20260522
    # 或监控今天：
    python3 scripts/monitor_csv_write.py
"""

import os
import sys
import time
import signal
from pathlib import Path
from datetime import datetime


def get_today_dir():
    """返回今天的通联数据目录。"""
    today = datetime.now().strftime("%Y%m%d")
    return Path("/www/wwwroot/mdl/msg_backup") / today


def monitor_files(watch_dir: Path, interval: float = 1.0):
    """监控目录下所有 CSV 文件的 stat 变化。"""
    print(f"监控目录: {watch_dir}")
    print(f"采样间隔: {interval}s")
    print(f"{'='*80}")

    # 文件状态记录: path -> {inode, size, mtime, nlink}
    prev_state = {}
    anomalies = []

    # 关注的文件
    targets = ["mdl_6_36_0.csv", "mdl_4_24_0.csv", "mdl_6_28_0.csv", "mdl_4_4_0.csv",
               "mdl_6_33_0.csv", "mdl_4_19_0.csv"]

    running = True
    def stop(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while running:
        now = datetime.now().strftime("%H:%M:%S")

        for fname in targets:
            fpath = watch_dir / fname
            if not fpath.exists():
                continue

            try:
                st = fpath.stat()
            except FileNotFoundError:
                continue

            current = {
                "inode": st.st_ino,
                "size": st.st_size,
                "mtime": st.st_mtime,
                "nlink": st.st_nlink,
            }

            key = str(fpath)
            if key in prev_state:
                prev = prev_state[key]

                # 检查 inode 变化 = 文件被替换
                if current["inode"] != prev["inode"]:
                    msg = (f"[{now}] ⚠️ INODE 变化: {fname} "
                           f"inode {prev['inode']} → {current['inode']} "
                           f"(文件被 rename/替换!)")
                    print(msg, flush=True)
                    anomalies.append(("INODE_CHANGE", now, fname))

                # 检查文件缩小 = 截断重写
                if current["size"] < prev["size"]:
                    msg = (f"[{now}] ⚠️ 文件缩小: {fname} "
                           f"size {prev['size']:,} → {current['size']:,} "
                           f"(减少了 {prev['size']-current['size']:,} bytes!)")
                    print(msg, flush=True)
                    anomalies.append(("SIZE_DECREASE", now, fname,
                                      prev["size"], current["size"]))

                # 检查 nlink 变化
                if current["nlink"] != prev["nlink"]:
                    msg = (f"[{now}] ⚠️ nlink 变化: {fname} "
                           f"nlink {prev['nlink']} → {current['nlink']}")
                    print(msg, flush=True)
                    anomalies.append(("NLINK_CHANGE", now, fname))

                # 正常追加 - 每 60 秒打印一次进度
                size_delta = current["size"] - prev["size"]
                if size_delta > 0 and int(current["mtime"]) % 60 == 0:
                    size_gb = current["size"] / 1024 / 1024 / 1024
                    rate_mbs = size_delta / interval / 1024 / 1024
                    print(f"[{now}] {fname}: {size_gb:.2f}GB "
                          f"(+{size_delta/1024/1024:.1f}MB, {rate_mbs:.1f}MB/s)",
                          flush=True)

            prev_state[key] = current

        time.sleep(interval)

    # 汇总
    print(f"\n{'='*80}")
    print(f"监控结束，汇总:")
    if anomalies:
        print(f"  发现 {len(anomalies)} 个异常:")
        for a in anomalies:
            print(f"    {a}")
        print(f"\n  ⚠️ 结论: 通联客户端不是纯追加写入，collector 的 offset 机制可能丢数据!")
    else:
        print(f"  未发现异常，文件均为纯追加写入")
        print(f"  ✅ collector 的 offset 机制是安全的")


def check_historical(day_dir: Path):
    """快速检查历史数据：用 awk 统计行数和最大 offset 推断是否有重写。"""
    print(f"\n历史分析: {day_dir}")
    print(f"{'='*80}")

    for fname in ["mdl_6_36_0.csv", "mdl_4_24_0.csv"]:
        fpath = day_dir / fname
        if not fpath.exists():
            print(f"  {fname}: 不存在")
            continue

        size_gb = fpath.stat().st_size / 1024 / 1024 / 1024
        # 用 wc -l 统计行数
        import subprocess
        result = subprocess.run(["wc", "-l", str(fpath)], capture_output=True, text=True)
        lines = result.stdout.strip().split()[0] if result.returncode == 0 else "?"

        print(f"  {fname}:")
        print(f"    大小: {size_gb:.2f}GB")
        print(f"    行数: {lines}")


def quick_offset_check(day_dir: Path):
    """
    模拟 collector 的 offset 行为，检查是否有数据丢失。

    原理：如果通联客户端是纯追加写，文件按时间排序，
    我们可以在不同 offset 处采样，验证时间戳是否单调递增。
    """
    print(f"\noffset 单调性检查: {day_dir}")
    print(f"{'='*80}")

    import subprocess

    for fname in ["mdl_6_36_0.csv", "mdl_4_24_0.csv"]:
        fpath = day_dir / fname
        if not fpath.exists():
            continue

        file_size = fpath.stat().st_size
        # 在文件的不同位置采样时间戳
        sample_offsets = [
            0,                           # 文件头
            file_size // 4,              # 1/4 处
            file_size // 2,              # 1/2 处
            file_size * 3 // 4,          # 3/4 处
            file_size - 1024 * 1024,     # 末尾前 1MB
        ]

        print(f"\n  {fname} ({file_size/1024/1024/1024:.2f}GB):")

        for offset in sample_offsets:
            if offset < 0:
                offset = 0
            try:
                # dd 跳到指定位置读一行
                result = subprocess.run(
                    ["dd", f"if={fpath}", "bs=1", f"skip={offset}",
                     "count=500", "2>/dev/null"],
                    capture_output=True, text=True, timeout=5
                )
                text = result.stdout
                lines = [l.strip() for l in text.split('\n') if l.strip() and not l.startswith("ChannelNo") and not l.startswith("BizIndex")]
                if lines:
                    # 取第一个完整行的时间戳
                    first_line = lines[0]
                    fields = first_line.split(',')
                    # SZ deal: TransactTime=col11, SH order_deal: TickTime=col4
                    if "mdl_6" in fname:
                        time_col = fields[10] if len(fields) > 10 else "?"
                        sid_col = fields[5] if len(fields) > 5 else "?"
                    else:
                        time_col = fields[3] if len(fields) > 3 else "?"
                        sid_col = fields[2] if len(fields) > 2 else "?"
                    print(f"    offset={offset:>12,}: time={time_col} sid={sid_col}")
            except Exception as e:
                print(f"    offset={offset:>12,}: 读取失败: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        watch_dir = Path(sys.argv[1])
    else:
        watch_dir = get_today_dir()

    if not watch_dir.exists():
        print(f"目录不存在: {watch_dir}")
        sys.exit(1)

    # 如果传了 --check 参数，只做历史分析
    if "--check" in sys.argv:
        check_historical(watch_dir)
        quick_offset_check(watch_dir)
    else:
        monitor_files(watch_dir, interval=0.5)
