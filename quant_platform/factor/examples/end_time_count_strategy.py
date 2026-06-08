# -*- coding: utf-8 -*-
"""Minimal strategy used to verify batched end_time delivery."""

factor_info = {
    "market_count": 1,
    "need_l1_tick": False,
    "need_l2_deal": False,
    "need_l2_order": False,
    "compute_interval": 60,
}

securities = ["000001.XSHE"]


def factor_calculation(data, code, date, end_time):
    codes = code if isinstance(code, list) else [code]
    end_times = end_time if isinstance(end_time, list) else [end_time]

    print(
        "[end-time-check]",
        "date=", date,
        "codes=", len(codes),
        "end_times=", len(end_times),
        "is_245=", len(end_times) == 245,
        "first=", end_times[:3],
        "last=", end_times[-3:],
    )

    return {
        c: {
            "code": c,
            "date": date,
            "F_end_time_count": len(end_times),
            "F_end_time_is_245": 1.0 if len(end_times) == 245 else 0.0,
        }
        for c in codes
    }
