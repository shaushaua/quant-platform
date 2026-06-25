# -*- coding: utf-8 -*-
"""通联数据 (DataYes) API 客户端：拉取 SW21 因子暴露数据。

移植自 deeptrade Go 端 dataconv/datayes_client.go，逻辑严格一致。

用于 dy1d_exposure_sw21 表数据滞后场景：MySQL hermes 库的因子暴露数据
在 2026-03-06 之后不再更新，需要从通联 API 直接拉取以保证 daily_basic_data
的 barra / 行业字段非空。

API 文档: /api/equity/getRMExposureDaySW21.json
响应格式: {"retCode":1, "data":[{"ticker":"000001", "tradeDate":"20260305", "BETA":..., ...}]}
"""
import json
import logging
from typing import Dict, Optional

import requests

logger = logging.getLogger(__name__)

DATAYES_BASE_URL = "https://api.wmcloud.com"
DATAYES_PATH = "/data/v1/api/equity/getRMExposureDaySW21.json"


class ExposureData:
    """单只股票当日的全部因子暴露。

    fields 是字段名→数值的映射，字段名与 MySQL dy1d_exposure_sw21 列名一致
    (BETA / MOMENTUM / SIZE / ... / Agriculture / Banks / ... / COUNTRY)。
    """

    __slots__ = ("trade_date", "ticker", "update_time", "fields")

    def __init__(self, trade_date: str = "", ticker: str = "",
                 update_time: str = "", fields: Optional[Dict[str, float]] = None):
        self.trade_date = trade_date
        self.ticker = ticker
        self.update_time = update_time
        self.fields = fields or {}


class DatayesClient:
    """通联数据 API 客户端。"""

    def __init__(self, token: str, timeout: int = 60):
        if not token or not token.strip():
            raise ValueError("datayes token 为空")
        self.token = token.strip()
        self.timeout = timeout

    def get_rm_exposure_day_sw21(self, trade_date: str) -> Dict[str, ExposureData]:
        """拉取指定交易日 (YYYYMMDD) 的全部股票 SW21 因子暴露。

        返回 dict: {ticker(6位): ExposureData}, ticker 为 6 位股票代码（无交易所后缀）。
        """
        url = (f"{DATAYES_BASE_URL}{DATAYES_PATH}"
               f"?field=&ticker=&secID=&tradeDate={trade_date}&beginDate=&endDate=")
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            resp = requests.get(url, headers=headers, timeout=self.timeout)
        except requests.RequestException as e:
            raise RuntimeError(f"调用通联 API 失败: {e}") from e

        if resp.status_code != 200:
            raise RuntimeError(
                f"通联 API 返回 HTTP {resp.status_code}: {resp.text[:512]}"
            )

        # requests 自动处理 gzip 解压 (基于 Content-Encoding 响应头);
        # 不要再手动 gzip.decompress resp.content,否则会对已解压的 JSON
        # 二次解压导致 "Not a gzipped file" 错误。
        content = resp.content

        try:
            result = json.loads(content)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"解析 JSON 失败: {e}, body: {content[:300]!r}"
            ) from e

        if result.get("retCode") != 1:
            raise RuntimeError(
                f"通联 API retCode={result.get('retCode')} retMsg={result.get('retMsg')}"
            )

        rows = result.get("data", []) or []
        out: Dict[str, ExposureData] = {}
        for item in rows:
            ticker = _as_string(item.get("ticker"))
            if not ticker:
                continue
            # ticker 形如 "000001.SZ" 或 "000001"，统一取 6 位代码
            if "." in ticker:
                ticker = ticker.split(".", 1)[0]
            if len(ticker) > 6:
                ticker = ticker[-6:]

            fields: Dict[str, float] = {}
            for k, v in item.items():
                if k in ("ticker", "tradeDate", "updateTime", "secID") or v is None:
                    continue
                f = _as_float(v)
                if f is not None:
                    fields[k] = f

            out[ticker] = ExposureData(
                trade_date=_as_string(item.get("tradeDate")),
                ticker=ticker,
                update_time=_as_string(item.get("updateTime")),
                fields=fields,
            )
        return out


def _as_string(v) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v) if isinstance(v, float) else str(v)
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)


def _as_float(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, str):
        if not v:
            return None
        try:
            return float(v)
        except ValueError:
            return None
    return None
