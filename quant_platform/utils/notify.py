# -*- coding: utf-8 -*-
"""飞书机器人告警。

轻量 webhook 通知：urllib 直发，5 秒超时，任何异常只打日志——
告警失败绝不影响主流程。

安全约定(P1 修复): webhook 只从环境变量 FEISHU_WEBHOOK_URL 读取,
代码/仓库中不保留任何真实地址。集群侧通过 Secret 注入:
    kubectl -n quant create secret generic feishu-webhook \
        --from-literal=url='https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
deployment 引用:
    env:
    - name: FEISHU_WEBHOOK_URL
      valueFrom:
        secretKeyRef: {name: feishu-webhook, key: url}
未配置时静默降级(只记一次 warning), 不发送。
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

_warned_missing = False


def feishu_alert(title: str, text: str, webhook: str | None = None) -> bool:
    """发送飞书文本告警。返回是否成功(失败仅记日志,不抛异常)。"""
    global _warned_missing
    url = (webhook or os.environ.get("FEISHU_WEBHOOK_URL", "")).strip()
    if not url:
        if not _warned_missing:
            logger.warning("[feishu] FEISHU_WEBHOOK_URL 未配置,告警将不发送(仅此提示一次)")
            _warned_missing = True
        return False
    payload = json.dumps({
        "msg_type": "text",
        "content": {"text": f"【{title}】\n{text}"},
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            ok = resp.status == 200
        if not ok:
            logger.warning("[feishu] alert http status != 200")
        return ok
    except Exception as exc:
        logger.warning("[feishu] alert failed: %s", exc)
        return False
