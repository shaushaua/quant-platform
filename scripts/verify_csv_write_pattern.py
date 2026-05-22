#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证通联 CSV 文件写入模式：是否纯追加（append-only）

核心逻辑：
1. 在文件不同 offset 采样，读取该位置的时间戳
2. 如果文件是纯追加写，时间戳必须单调递增
3. 如果文件被截断重写，时间戳会出现跳跃或逆序

同时检查：
- collector 的 offset 是否可能失效
- 实盘因子丢失数据的具体原因

用法（实盘节点）：
    python3 scripts/verify_csv_write_pattern.py --date 20260521

    # 也查看 collector 日志：
    kubectl logs -n quant -l app=collector --tail=5000 | grep "行 队列"
"""

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--msg-dir", default="/www/wwwroot/mdl/msg_backup")
    p.add_argument("--samples", type=int, default=20, help="采样点数量")
    return p.parse_args()


def sample_timestamps(csv_path, n_samples=20):
    """在文件等距位置采样时间戳，检查单调性。"""
    file_size = csv_path.stat().st_size
    fname = csv_path.name

    print(f"\n  {fname} ({file_size/1024/1024/1024:.2f}GB)")
    print(f"  采样 {n_samples} 个点:")

    samples = []
    header_size = 0

    # 先读 header
    try:
        result = subprocess.run(
            ["head", "-1", str(csv_path)],
            capture_output=True, text=True, timeout=5
        )
        header_size = len(result.stdout.encode('utf-8'))
    except Exception:
        pass

    for i in range(n_samples):
        # 等距采样，跳过 header
        offset = header_size + int((file_size - header_size) * i / n_samples)
        if offset >= file_size:
            offset = file_size - 1
        if offset < 0:
            continue

        try:
            # dd 读取 512 字节，足够拿到一行
            result = subprocess.run(
                ["dd", f"if={csv_path}", "bs=1",
                 f"skip={offset}", "count=512", "2>/dev/null"],
                capture_output=True, text=True, timeout=5
            )
            text = result.stdout
            lines = [l.strip() for l in text.split('\n')
                     if l.strip() and not l.startswith(('ChannelNo', 'BizIndex'))]

            if not lines:
                continue

            line = lines[0]
            fields = line.split(',')

            if "mdl_6_36" in fname:
                # SZ deal: TransactTime=col11 (index 10)
                ts = fields[10] if len(fields) > 10 else "?"
                sid = fields[5] if len(fields) > 5 else "?"
                qty = fields[8] if len(fields) > 8 else "?"
            elif "mdl_4_24" in fname:
                # SH order+deal: TickTime=col4 (index 3)
                ts = fields[3] if len(fields) > 3 else "?"
                sid = fields[2] if len(fields) > 2 else "?"
                typ = fields[4] if len(fields) > 4 else "?"
                qty = f"type={typ}" if typ else "?"
            elif "mdl_6_28" in fname:
                # SZ tick:UpdateTime=col3 (index 2)
                ts = fields[2] if len(fields) > 2 else "?"
                sid = fields[0] if len(fields) > 0 else "?"
                qty = "-"
            elif "mdl_4_4" in fname:
                # SH tick:UpdateTime=col4 (index 3)
                ts = fields[3] if len(fields) > 3 else "?"
                sid = fields[0] if len(fields) > 0 else "?"
                qty = "-"
            else:
                ts = "?"
                sid = "?"
                qty = "?"

            samples.append((offset, ts, sid, qty))

        except Exception as e:
            samples.append((offset, f"ERR: {e}", "", ""))

    # 打印采样结果
    print(f"  {'offset':>14s}  {'timestamp':<16s}  {'sid':<8s}  info")
    print(f"  {'-'*14}  {'-'*16}  {'-'*8}  {'-'*20}")

    monotonically_increasing = True
    prev_ts = None
    for offset, ts, sid, info in samples:
        print(f"  {offset:>14,}  {ts:<16s}  {sid:<8s}  {info}")

        # 检查时间戳单调性
        if prev_ts and ts != "?" and prev_ts != "?":
            if ts < prev_ts:
                monotonically_increasing = False
        if ts != "?":
            prev_ts = ts

    return monotonically_increasing, samples


def check_tail_vs_head(csv_path):
    """检查文件头尾的时间戳，确认首尾顺序正确。"""
    fname = csv_path.name
    try:
        # 头部第2行（第一行数据）
        head = subprocess.run(
            ["sed", "-n", "2p", str(csv_path)],
            capture_output=True, text=True, timeout=10
        )
        # 尾部最后1行
        tail = subprocess.run(
            ["tail", "-1", str(csv_path)],
            capture_output=True, text=True, timeout=30
        )

        head_fields = head.stdout.strip().split(',')
        tail_fields = tail.stdout.strip().split(',')

        if "mdl_6_36" in fname:
            head_ts = head_fields[10] if len(head_fields) > 10 else "?"
            tail_ts = tail_fields[10] if len(tail_fields) > 10 else "?"
            ts_col = "TransactTime"
        elif "mdl_4_24" in fname:
            head_ts = head_fields[3] if len(head_fields) > 3 else "?"
            tail_ts = tail_fields[3] if len(tail_fields) > 3 else "?"
            ts_col = "TickTime"
        else:
            return None, None, "?"

        print(f"\n  {fname} 首尾时间戳 ({ts_col}):")
        print(f"    首行: {head_ts}")
        print(f"    尾行: {tail_ts}")

        # 统计行数
        wc = subprocess.run(
            ["wc", "-l", str(csv_path)],
            capture_output=True, text=True, timeout=60
        )
        lines = wc.stdout.strip().split()[0] if wc.returncode == 0 else "?"
        print(f"    行数: {lines}")

        return head_ts, tail_ts, lines

    except Exception as e:
        print(f"  {fname}: 检查失败: {e}")
        return None, None, None


def main():
    args = parse_args()
    day_dir = Path(args.msg_dir) / args.date

    if not day_dir.exists():
        print(f"目录不存在: {day_dir}")
        sys.exit(1)

    print("=" * 80)
    print(f"通联 CSV 写入模式验证 — {args.date}")
    print(f"目的: 确认通联客户端是否纯追加写入，collector offset 是否安全")
    print("=" * 80)

    csv_files = {
        "SZ deal":       "mdl_6_36_0.csv",
        "SH order+deal": "mdl_4_24_0.csv",
        "SZ tick":       "mdl_6_28_0.csv",
        "SH tick":       "mdl_4_4_0.csv",
    }

    all_monotonic = True
    results = {}

    # 1. 首尾时间戳检查
    print(f"\n[1/3] 首尾时间戳检查:")
    for label, fname in csv_files.items():
        fpath = day_dir / fname
        if not fpath.exists():
            print(f"  {label} ({fname}): 不存在，跳过")
            continue
        head_ts, tail_ts, lines = check_tail_vs_head(fpath)
        results[label] = (head_ts, tail_ts, lines)

    # 2. 等距采样检查
    print(f"\n[2/3] 等距采样时间戳单调性检查:")
    for label, fname in csv_files.items():
        fpath = day_dir / fname
        if not fpath.exists():
            continue
        monotonic, samples = sample_timestamps(fpath, n_samples=args.samples)
        status = "✅ 单调递增 (纯追加)" if monotonic else "⚠️ 非单调 (可能有重写!)"
        print(f"  结论: {status}")
        if not monotonic:
            all_monotonic = False

    # 3. collector offset 风险评估
    print(f"\n[3/3] collector offset 风险评估:")
    print(f"  FilePoller 逻辑:")
    print(f"    current_size <= offset → return None (跳过)")
    print(f"    current_size >  offset → 读取增量数据")
    print()

    if all_monotonic:
        print(f"  ✅ 所有文件时间戳单调递增，通联客户端为纯追加写入")
        print(f"  ✅ collector 的 offset 机制是安全的，不会丢数据")
        print()
        print(f"  那为什么实盘因子只有 CSV 的 13-17%?")
        print(f"  可能原因:")
        print(f"    1. collector SKIP_HISTORY=true 启动时跳过了已有数据")
        print(f"    2. collector 启动时间和通联客户端写入有延迟")
        print(f"    3. 通联客户端可能有多个写入阶段（追加+后续更新）")
        print(f"    4. 实盘因子只计算了部分时间段的数据")
        print()
        print(f"  建议检查:")
        print(f"    - collector 启动时间 vs 通联 CSV 首行时间")
        print(f"    - kubectl logs -n quant -l app=collector --tail=5000 | grep '行 队列'")
        print(f"    - 对比 collector 累计读取行数 vs CSV 总行数")
    else:
        print(f"  ⚠️ 文件时间戳非单调递增!")
        print(f"  ⚠️ 通联客户端可能不是纯追加写入!")
        print(f"  ⚠️ collector 的 offset 机制可能丢数据!")
        print()
        print(f"  需要进一步确认:")
        print(f"    - 在交易时间运行 scripts/monitor_csv_write.py 实时监控")
        print(f"    - 检查是否有 inode 变化或文件缩小")

    print(f"\n{'=' * 80}")


if __name__ == "__main__":
    main()
