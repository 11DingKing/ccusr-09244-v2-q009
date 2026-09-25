from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.schemas.operation import OperationDataBase, OperationDataResponse


class OperationIngestRequest(OperationDataBase):
    """带业务键的单条作业接入请求。"""

    idempotency_key: str = Field(
        ..., min_length=1, max_length=200, description="调用方提供的业务幂等键"
    )
    ttl_seconds: Optional[int] = Field(
        None,
        ge=1,
        le=365 * 24 * 3600,
        description="幂等记录有效期（秒），缺省使用服务端默认策略",
    )


class OperationIngestResponse(BaseModel):
    idempotency_key: str
    scope: str
    # saved：首次请求保存；replayed：命中既有结果
    status: str
    replayed: bool
    replay_count: int
    conflict_count: int
    created_at: datetime
    expires_at: datetime
    reusable_after: datetime
    data: OperationDataResponse


class BatchOperationIngestItem(BaseModel):
    """批量接入中的单条载荷，位置由其在 items 数组中的下标决定。"""

    idempotency_key: Optional[str] = Field(
        None, max_length=200, description="业务幂等键，缺失则该条目判定为无效"
    )
    ttl_seconds: Optional[int] = Field(None, ge=1, le=365 * 24 * 3600)

    robot_model_id: int = Field(..., description="机型ID")
    scene_id: int = Field(..., description="场景ID")
    skill_id: int = Field(..., description="技能ID")
    robot_serial: Optional[str] = Field(None, max_length=100)
    motion_trajectory: Dict[str, Any]
    perception_records: Dict[str, Any]
    grasp_result: Optional[Dict[str, Any]] = None
    timestamp_start: datetime
    timestamp_end: datetime
    duration_ms: Optional[int] = None
    environment_conditions: Optional[Dict[str, Any]] = None
    hardware_status: Optional[Dict[str, Any]] = None


class BatchOperationIngestRequest(BaseModel):
    items: List[BatchOperationIngestItem] = Field(..., min_length=1)


class BatchOperationIngestResultItem(BaseModel):
    # 原输入位置（items 数组下标），保持稳定
    index: int
    idempotency_key: Optional[str] = None
    # saved / replayed / conflict / expired / invalid / error
    outcome: str
    success: bool
    data: Optional[OperationDataResponse] = None
    error_code: Optional[str] = None
    error: Optional[str] = None
    replay_count: Optional[int] = None
    conflict_count: Optional[int] = None


class BatchOperationIngestResponse(BaseModel):
    total: int
    saved_count: int
    replayed_count: int
    success_count: int
    failure_count: int
    conflict_count: int
    expired_count: int
    results: List[BatchOperationIngestResultItem]


class IdempotencyRecordResponse(BaseModel):
    id: int
    scope: str
    idempotency_key: str
    record_hash: str
    operation_id: int
    status: str
    replay_count: int
    conflict_count: int
    last_conflict_hash: Optional[str] = None
    created_at: datetime
    last_replayed_at: Optional[datetime] = None
    expires_at: datetime
    reusable_after: datetime
    expired: bool
    reusable: bool

    class Config:
        from_attributes = True


class IdempotencyRecordListResponse(BaseModel):
    total: int
    items: List[IdempotencyRecordResponse]


class IdempotencyReapResponse(BaseModel):
    now: datetime
    grace_seconds: int
    cutoff: datetime
    reaped: int
    scanned: int
    in_grace: int
    dry_run: bool
