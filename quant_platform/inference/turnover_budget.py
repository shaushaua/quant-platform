"""Portfolio turnover budget derived from the current cash exposure."""

from __future__ import annotations

import math


def dynamic_optimizer_max_turnover(
    current_gross: object,
    *,
    today_cash_flow: object,
    total_asset: object,
    base_turnover: float = 0.35,
    buffer: float = 0.02,
) -> float:
    """Cover the optimizer's actual target-weight gap, with a small buffer."""
    try:
        raw_base = float(base_turnover)
        raw_buffer = float(buffer)
        gross = float(current_gross)
        cash_flow = float(today_cash_flow)
        total = float(total_asset)
    except (TypeError, ValueError):
        return 0.35
    base = min(max(raw_base, 0.0), 1.0) if math.isfinite(raw_base) else 0.35
    if (not math.isfinite(gross) or gross < 0 or not math.isfinite(total) or total <= 0 or
            not math.isfinite(cash_flow) or cash_flow <= 0):
        return base

    target_gap = 1.0 - min(gross, 1.0)
    inflow_ratio = min(cash_flow / total, 1.0)
    extra = max(raw_buffer, 0.0) if math.isfinite(raw_buffer) else 0.02
    required = target_gap + extra
    inflow_cap = base + inflow_ratio
    return min(1.0, max(base, min(required, inflow_cap)))
