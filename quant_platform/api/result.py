# -*- coding: utf-8 -*-
"""
Result API endpoints
"""

import json
import logging
from typing import Optional

import oss2
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/result", tags=["result"])


# Response models
class ResultSummary(BaseModel):
    task_id: str
    strategy_name: str
    start_date: str
    end_date: str
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe_ratio: float
    total_trades: int
    completed_at: str


class ResultListResponse(BaseModel):
    results: list
    total: int


class ResultDetail(BaseModel):
    task_id: str
    total_trading_days: int
    start_date: str
    end_date: str
    instances_completed: int
    instances_total: int
    total_return: float
    annual_return: float
    max_drawdown: float
    sharpe_ratio: float
    total_trades: int
    nav_curve: list
    trades: list
    aggregated_at: str
    result_url: Optional[str]
    is_partial: bool


_oss_bucket = None


def _simple_html_report(task_id: str, data: dict) -> str:
    """Generate a minimal HTML report from result JSON."""
    rows = "".join(
        f"<tr><td>{k}</td><td>{v}</td></tr>"
        for k, v in data.items()
        if not isinstance(v, (list, dict))
    )
    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'>
<title>Backtest Result: {task_id}</title>
<style>body{{font-family:sans-serif;padding:2rem}}table{{border-collapse:collapse}}td{{border:1px solid #ccc;padding:6px 12px}}</style>
</head><body>
<h2>Backtest Result: {task_id}</h2>
<table>{rows}</table>
</body></html>"""


def get_oss_bucket():
    """Get OSS bucket instance"""
    global _oss_bucket
    if _oss_bucket is None:
        import oss2
        import os

        access_key_id = os.getenv("OSS_ACCESS_KEY_ID")
        access_key_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
        endpoint = os.getenv("OSS_ENDPOINT", "oss-cn-shanghai.aliyuncs.com")
        bucket_name = os.getenv("OSS_RESULT_BUCKET", "quant-backtest-results")

        if access_key_id and access_key_secret:
            auth = oss2.Auth(access_key_id, access_key_secret)
            _oss_bucket = oss2.Bucket(auth, endpoint, bucket_name)

    return _oss_bucket




@router.get("/list", response_model=ResultListResponse, summary="列出回测结果")
async def list_results(
    limit: int = 100,
    offset: int = 0
):
    """
    列出所有已完成的回测结果
    
    ## 查询参数
    - limit: 返回数量限制（默认100）
    - offset: 偏移量（用于分页）
    
    ## 返回结果
    - results: 结果列表
    - total: 总数
    """
    bucket = get_oss_bucket()

    results = []

    if bucket:
        try:
            # List result.json files
            prefix = "results/"
            delimiter = "/"

            for obj in oss2.ObjectIterator(bucket, prefix=prefix, delimiter=delimiter):
                if obj.key.endswith("result.json"):
                    # Extract task_id from path
                    # Format: results/{task_id}/result.json
                    parts = obj.key.split("/")
                    if len(parts) >= 3:
                        task_id = parts[1]

                        # Fetch result
                        try:
                            content = bucket.get_object(obj.key).read()
                            data = json.loads(content)

                            results.append({
                                "task_id": task_id,
                                "strategy_name": data.get("strategy_name", ""),
                                "start_date": data.get("start_date", ""),
                                "end_date": data.get("end_date", ""),
                                "total_return": data.get("total_return", 0),
                                "annual_return": data.get("annual_return", 0),
                                "max_drawdown": data.get("max_drawdown", 0),
                                "sharpe_ratio": data.get("sharpe_ratio", 0),
                                "total_trades": data.get("total_trades", 0),
                                "completed_at": data.get("aggregated_at", "")
                            })
                        except Exception as e:
                            logger.warning(f"Failed to read result {obj.key}: {e}")

        except Exception as e:
            logger.exception(f"Failed to list results: {e}")

    # Sort by completion time, newest first
    results.sort(key=lambda x: x.get("completed_at", ""), reverse=True)

    # Paginate
    total = len(results)
    results = results[offset:offset + limit]

    return ResultListResponse(results=results, total=total)


@router.get("/{task_id}", response_model=ResultDetail, summary="获取回测结果详情")
async def get_result(task_id: str):
    """
    获取指定任务的详细回测结果
    
    ## 返回结果
    - task_id: 任务ID
    - strategy_name: 策略名称
    - start_date: 开始日期
    - end_date: 结束日期
    - metrics: 绩效指标
    - positions: 持仓数据
    - signals: 信号数据
    """
    bucket = get_oss_bucket()

    if not bucket:
        raise HTTPException(status_code=503, detail="OSS not configured")

    result_key = f"results/{task_id}/result.json"

    try:
        content = bucket.get_object(result_key).read()
        data = json.loads(content)

        return ResultDetail(**data)

    except Exception as e:
        logger.exception(f"Failed to get result {task_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Result not found: {task_id}")


@router.get("/{task_id}/download", summary="下载回测结果")
async def download_result(task_id: str, format: str = "json"):
    """
    下载回测结果文件
    
    ## 查询参数
    - format: 下载格式 (json/html)，默认 json
    
    ## 返回
    文件下载响应
    """
    bucket = get_oss_bucket()

    if not bucket:
        raise HTTPException(status_code=503, detail="OSS not configured")

    if format == "html":
        result_key = f"results/{task_id}/result.json"
        try:
            content = bucket.get_object(result_key).read()
            data = json.loads(content)
            html = _simple_html_report(task_id, data)
            return HTMLResponse(content=html)
        except Exception as e:
            logger.exception(f"Failed to generate HTML report: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    else:
        # Return JSON
        result_key = f"results/{task_id}/result.json"

        try:
            content = bucket.get_object(result_key).read()

            return JSONResponse(
                content=json.loads(content),
                headers={
                    "Content-Disposition": f"attachment; filename={task_id}_result.json"
                }
            )

        except Exception as e:
            logger.exception(f"Failed to download result: {e}")
            raise HTTPException(status_code=404, detail=f"Result not found: {task_id}")


@router.get("/{task_id}/nav", summary="获取净值曲线")
async def get_nav_curve(task_id: str):
    """
    获取任务净值曲线（用于图表展示）
    
    ## 返回结果
    - task_id: 任务ID
    - nav_curve: 净值曲线数据 [(date, nav), ...]
    """
    bucket = get_oss_bucket()

    if not bucket:
        raise HTTPException(status_code=503, detail="OSS not configured")

    result_key = f"results/{task_id}/result.json"

    try:
        content = bucket.get_object(result_key).read()
        data = json.loads(content)

        return {
            "task_id": task_id,
            "nav_curve": data.get("nav_curve", [])
        }

    except Exception as e:
        logger.exception(f"Failed to get NAV curve: {e}")
        raise HTTPException(status_code=404, detail=f"Result not found: {task_id}")


@router.get("/{task_id}/trades", summary="获取交易记录")
async def get_trades(
    task_id: str,
    limit: int = 1000,
    offset: int = 0
):
    """
    获取任务的交易记录
    
    ## 查询参数
    - limit: 返回数量限制（默认1000）
    - offset: 偏移量（用于分页）
    
    ## 返回结果
    - task_id: 任务ID
    - trades: 交易记录列表
    - total: 总数
    """
    bucket = get_oss_bucket()

    if not bucket:
        raise HTTPException(status_code=503, detail="OSS not configured")

    result_key = f"results/{task_id}/result.json"

    try:
        content = bucket.get_object(result_key).read()
        data = json.loads(content)

        trades = data.get("trades", [])
        total = len(trades)
        trades = trades[offset:offset + limit]

        return {
            "task_id": task_id,
            "trades": trades,
            "total": total
        }

    except Exception as e:
        logger.exception(f"Failed to get trades: {e}")
        raise HTTPException(status_code=404, detail=f"Result not found: {task_id}")
