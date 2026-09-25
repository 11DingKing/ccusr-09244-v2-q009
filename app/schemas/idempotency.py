from pydantic import BaseModel
from typing import Optional, Any, Dict, List
from datetime import datetime


class IdempotencyRecordResponse(BaseModel):
    id: int
    scope: str
    idempotency_key: str
    payload_hash: str
    operation_data_id: Optional[int] = None
    hit_count: int
    conflict_count: int
    created_at: datetime
    expires_at: datetime
    expired: bool


class IdempotencyRecordDetailResponse(IdempotencyRecordResponse):
    response_body: Dict[str, Any]


class IdempotencyRecordListResponse(BaseModel):
    total: int
    items: List[IdempotencyRecordResponse]
    page: int
    page_size: int


class IdempotencyRecycleResponse(BaseModel):
    recycled: int
    remaining: int
