# -*- coding: utf-8 -*-
"""
Backtest API endpoints

将回测任务委托给 backtest-operator（Go Operator），
通过 K8s BacktestTask CRD 或 operator HTTP API 触发。
"""

import os
import logging
from typing import Optional

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/backtest", tags=["backtest"])

OPERATOR_URL = os.getenv("BACKTEST_OPERATOR_URL", "http://backtest-operator:8080")


# ── Request / Response models ──────────────────────────────────────────────

class SubmitBacktestRequest(BaseModel):
    image_tag: str = Field(..., description="Docker 镜像标签")
    start_date: str = Field(..., description="回测开始日期 YYYY-MM-DD")
    end_date: str = Field(..., description="回测结束日期 YYYY-MM-DD")
    strategy_name: str = Field(default="strategy", description="策略名称")
    initial_capital: float = Field(default=100_000_000.0, description="初始资金")
    instances: Optional[int] = Field(default=None, description="实例数（不填则 operator 自动计算）")


class SubmitBacktestResponse(BaseModel):
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    task_id: str
    strategy_name: str
    image_tag: str
    start_date: str
    end_date: str
    status: str
    result_url: Optional[str] = None
    error_message: Optional[str] = None
    created_at: str
    updated_at: str


class TaskListResponse(BaseModel):
    tasks: list
    total: int


# ── Helpers ────────────────────────────────────────────────────────────────

def _operator_post(path: str, payload: dict) -> dict:
    try:
        resp = requests.post(f"{OPERATOR_URL}{path}", json=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Operator request failed: {e}")
        raise HTTPException(status_code=502, detail=f"backtest-operator unavailable: {e}")


def _operator_get(path: str) -> dict:
    try:
        resp = requests.get(f"{OPERATOR_URL}{path}", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Operator request failed: {e}")
        raise HTTPException(status_code=502, detail=f"backtest-operator unavailable: {e}")


# ── Endpoints ──────────────────────────────────────────────────────────────

@router.post("/submit", response_model=SubmitBacktestResponse, summary="提交分布式回测任务")
async def submit_backtest(request: SubmitBacktestRequest):
    """
    提交回测任务给 backtest-operator。
    Operator 负责拆分分片、创建 K8s Job、聚合结果。
    """
    data = _operator_post("/tasks", request.model_dump())
    return SubmitBacktestResponse(
        task_id=data["task_id"],
        status=data["status"],
        message=data.get("message", "Task submitted"),
    )


@router.get("/status/{task_id}", response_model=TaskStatusResponse, summary="查询任务状态")
async def get_task_status(task_id: str):
    data = _operator_get(f"/tasks/{task_id}")
    return TaskStatusResponse(**data)


@router.delete("/cancel/{task_id}", summary="取消任务")
async def cancel_task(task_id: str):
    data = _operator_post(f"/tasks/{task_id}/cancel", {})
    return data


@router.get("/list", response_model=TaskListResponse, summary="列出所有任务")
async def list_tasks(status: Optional[str] = None, limit: int = 100):
    path = f"/tasks?limit={limit}"
    if status:
        path += f"&status={status}"
    data = _operator_get(path)
    return TaskListResponse(tasks=data.get("tasks", []), total=data.get("total", 0))


@router.post("/callback/complete", summary="Worker 回调（由 Operator 内部调用）")
async def handle_completion_callback(callback_data: dict):
    """
    Worker 完成后由 backtest-operator 调用此接口汇报结果。
    """
    data = _operator_post("/callback/complete", callback_data)
    return data
