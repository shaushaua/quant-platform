#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Replay captured PML binary data to test Rust parser.

Usage:
    # Record (auto during trading if PML_RECORD_DIR is set)
    # Replay:
    python scripts/replay_pml.py /data/collector_output/pml_capture_20260601_093000.bin

Format per frame:
    [8B timestamp][4B mid][4B seq_id][4B buf_len][buf_len bytes: raw PML]
"""
import struct
import sys
import time
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from quant_platform.core.constants import TICK_COLUMNS, DEAL_COLUMNS, ORDER_COLUMNS


def read_frames(path):
    """Read all frames from a capture file."""
    frames = []
    with open(path, "rb") as f:
        while True:
            header = f.read(20)  # 8 + 4 + 4 + 4
            if len(header) < 20:
                break
            ts, mid, seq_id, buf_len = struct.unpack("<dIII", header)
            buf = f.read(buf_len)
            if len(buf) < buf_len:
                break
            frames.append((ts, mid, seq_id, buf))
    return frames


def replay(path):
    """Replay captured PML data through Rust parser."""
    import mdl_parser

    # Message IDs (must match combined_engine.py constants)
    # SH
    MID_SH_TICK = 24001   # MDLMID_SHL2MarketData
    MID_SH_NGTS = 24002   # MDLMID_NGTSTick
    # SZ
    MID_SZ_TICK = 300111  # MDLMID_Snapshot300111_v2
    MID_SZ_ORDER = 300192 # MDLMID_Order300192_v2
    MID_SZ_DEAL = 300193 # MDLMID_Transaction300191_v2

    # Read all frames
    print(f"Reading {path}...")
    frames = read_frames(path)
    print(f"Total frames: {len(frames)}")

    if not frames:
        print("No frames found.")
        return

    # Classify by type
    by_type = {}
    for ts, mid, seq_id, buf in frames:
        by_type.setdefault(mid, []).append((ts, seq_id, buf))

    for mid, items in sorted(by_type.items()):
        type_name = {
            MID_SH_TICK: "SH_tick", MID_SH_NGTS: "SH_ngts",
            MID_SZ_TICK: "SZ_tick", MID_SZ_ORDER: "SZ_order", MID_SZ_DEAL: "SZ_deal",
        }.get(mid, f"unknown_{mid}")
        print(f"  {type_name} (mid={mid}): {len(items)} frames")

    # Replay
    trading_day = time.strftime("%Y%m%d")
    tick_count = 0
    order_count = 0
    deal_count = 0
    error_count = 0

    t0 = time.perf_counter()
    for ts, mid, seq_id, buf in frames:
        try:
            if mid == MID_SH_TICK:
                result = mdl_parser.parse_sh_tick(buf, trading_day, seq_id)
                if result:
                    tick_count += 1
            elif mid == MID_SH_NGTS:
                result = mdl_parser.parse_sh_ngts(buf, trading_day)
                if result:
                    code, order_tup, deal_tup = result
                    if order_tup:
                        order_count += 1
                    if deal_tup:
                        deal_count += 1
            elif mid == MID_SZ_TICK:
                result = mdl_parser.parse_sz_tick(buf, trading_day, seq_id)
                if result:
                    tick_count += 1
            elif mid == MID_SZ_ORDER:
                result = mdl_parser.parse_sz_order(buf, trading_day, seq_id)
                if result:
                    order_count += 1
            elif mid == MID_SZ_DEAL:
                result = mdl_parser.parse_sz_deal(buf, trading_day, seq_id)
                if result:
                    deal_count += 1
        except Exception as exc:
            error_count += 1
            if error_count <= 10:
                print(f"  ERROR mid={mid} seq={seq_id}: {exc}")

    elapsed = time.perf_counter() - t0

    print(f"\n=== Replay Results ===")
    print(f"Frames:    {len(frames)}")
    print(f"Ticks:     {tick_count}")
    print(f"Orders:    {order_count}")
    print(f"Deals:     {deal_count}")
    print(f"Errors:    {error_count}")
    print(f"Time:      {elapsed*1000:.1f}ms")
    print(f"Throughput: {len(frames)/elapsed:.0f} frames/sec")
    if error_count == 0:
        print("ALL PASSED")
    else:
        print(f"FAILURES: {error_count}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <capture.bin>")
        sys.exit(1)
    replay(sys.argv[1])
