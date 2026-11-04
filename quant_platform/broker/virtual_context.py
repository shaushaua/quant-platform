# -*- coding: utf-8 -*-
"""虚盘 portfolio context — 无网关环境下的推理供数(测试 Pod / 离线验证)。

经 PORTFOLIO_CONTEXT_MODULE=quant_platform.broker.virtual_context 启用,
替换 order_context(需 ORDER_GATEWAY_URL 查真实持仓)。返回空持仓 +
1000 万虚拟账户: inference() 的空持仓校验要求 account.market_value≈0
(order_context 无网关时账户为空会被风险校验拒绝), 这里给出可验证的
空仓虚拟账户, 优化器按虚拟资金 sizing。

仅测试场景使用 — 生产必须用 order_context 取真实持仓/资金。
"""
from __future__ import annotations

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

VIRTUAL_TOTAL_CAPITAL = float(os.environ.get("VIRTUAL_CONTEXT_CAPITAL", "10000000"))


def get_portfolio_context(date_str: str, end_time: str,
                          request_timeout: float | None = None):
    """与 order_context.get_portfolio_context 同签名/同返回类型。"""
    from quant_platform.inference.interface import PortfolioContext

    account = pd.DataFrame([{
        "total_asset": VIRTUAL_TOTAL_CAPITAL,
        "available_cash": VIRTUAL_TOTAL_CAPITAL,
        "market_value": 0.0,
    }])
    logger.info("[virtual-context] 空仓虚拟账户 %.0f万 (%s %s)",
                VIRTUAL_TOTAL_CAPITAL / 1e4, date_str, end_time)
    return PortfolioContext(
        account_id="virtual-test",
        broker="virtual",
        account_type="",
        as_of=end_time,
        source="virtual-context",
        positions=pd.DataFrame(),
        account=account,
        orders=pd.DataFrame(),
        deals=pd.DataFrame(),
        meta={"positions_usable": False, "virtual": True},
    )
