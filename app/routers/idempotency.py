from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import IdempotencyRecord
from app.schemas.idempotency import (
    IdempotencyRecordResponse, IdempotencyRecordDetailResponse,
    IdempotencyRecordListResponse, IdempotencyRecycleResponse
)
from app.services import idempotency as idem

router = APIRouter()


def _policy() -> idem.IdempotencyPolicy:
    return idem.IdempotencyPolicy(
        ttl=timedelta(hours=settings.IDEMPOTENCY_TTL_HOURS)
    ).validate()


def _to_response(record: IdempotencyRecord, now) -> IdempotencyRecordResponse:
    return IdempotencyRecordResponse(
        id=record.id,
        scope=record.scope,
        idempotency_key=record.idempotency_key,
        payload_hash=record.payload_hash,
        operation_data_id=record.operation_data_id,
        hit_count=record.hit_count,
        conflict_count=record.conflict_count,
        created_at=record.created_at,
        expires_at=record.expires_at,
        expired=_policy().is_expired(record.expires_at, now),
    )


@router.get("/idempotency-records", response_model=IdempotencyRecordListResponse, tags=["幂等记录"])
def list_idempotency_records(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    scope: Optional[str] = Query(None, description="接入范围过滤"),
    idempotency_key: Optional[str] = Query(None, description="业务键精确过滤"),
    expired: Optional[bool] = Query(None, description="是否已过期"),
    db: Session = Depends(get_db)
):
    now = idem.utcnow()
    query = db.query(IdempotencyRecord)
    if scope:
        query = query.filter(IdempotencyRecord.scope == scope)
    if idempotency_key:
        query = query.filter(IdempotencyRecord.idempotency_key == idempotency_key)
    if expired is True:
        query = query.filter(IdempotencyRecord.expires_at <= now)
    elif expired is False:
        query = query.filter(IdempotencyRecord.expires_at > now)

    total = query.count()
    skip = (page - 1) * page_size
    items = query.order_by(IdempotencyRecord.created_at.desc()).offset(skip).limit(page_size).all()

    return IdempotencyRecordListResponse(
        total=total,
        items=[_to_response(record, now) for record in items],
        page=page,
        page_size=page_size
    )


@router.get("/idempotency-records/{record_id}", response_model=IdempotencyRecordDetailResponse, tags=["幂等记录"])
def get_idempotency_record(record_id: int, db: Session = Depends(get_db)):
    record = db.query(IdempotencyRecord).filter(IdempotencyRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="幂等记录不存在")
    now = idem.utcnow()
    detail = IdempotencyRecordDetailResponse(
        **_to_response(record, now).model_dump(),
        response_body=record.response_body
    )
    return detail


@router.post("/idempotency-records/recycle", response_model=IdempotencyRecycleResponse, tags=["幂等记录"])
def recycle_idempotency_records(db: Session = Depends(get_db)):
    """按明确策略回收过期键：expires_at <= 当前时间（到期边界含）即回收。"""
    recycled = idem.recycle_expired(db, idem.utcnow())
    remaining = db.query(IdempotencyRecord).count()
    return IdempotencyRecycleResponse(recycled=recycled, remaining=remaining)
