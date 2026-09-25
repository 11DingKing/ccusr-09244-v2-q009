"""统一幂等接入接口与幂等记录观测接口。"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import IdempotencyRecord
from app.schemas.idempotency import (
    BatchOperationIngestItem,
    BatchOperationIngestRequest,
    BatchOperationIngestResponse,
    BatchOperationIngestResultItem,
    IdempotencyRecordListResponse,
    IdempotencyRecordResponse,
    IdempotencyReapResponse,
    OperationIngestRequest,
    OperationIngestResponse,
)
from app.services.idempotency import (
    BusinessDataMissing,
    KeyExpired,
    PayloadConflict,
    STATUS_REPLAYED,
    SCOPE_OPERATION,
    idempotency_stats,
    ingest_operation,
    reap_expired_records,
    record_conflict_hit,
    utc_now,
    validate_operation_references,
)

router = APIRouter()


def _operation_payload(data) -> tuple[dict, dict]:
    """返回 (用于写库的 python 对象载荷, 用于摘要的简单载荷)。"""
    write_payload = data.model_dump(exclude={"idempotency_key", "ttl_seconds"})
    hash_payload = data.model_dump(
        mode="json", exclude={"idempotency_key", "ttl_seconds"}
    )
    return write_payload, hash_payload


@router.post(
    "/ingest/operations",
    response_model=OperationIngestResponse,
    tags=["幂等接入"],
)
def ingest_single_operation(
    data: OperationIngestRequest, db: Session = Depends(get_db)
):
    """带业务键的单条作业接入：首次保存，同键同载荷重放，同键异载荷冲突。"""
    write_payload, hash_payload = _operation_payload(data)

    # 只有全新键才做引用校验，避免对合法重放请求返回 400
    existing = (
        db.query(IdempotencyRecord.id)
        .filter(
            IdempotencyRecord.scope == SCOPE_OPERATION,
            IdempotencyRecord.idempotency_key == data.idempotency_key,
        )
        .first()
    )
    if existing is None:
        errors = validate_operation_references(db, hash_payload)
        if errors:
            raise HTTPException(status_code=400, detail={"message": "; ".join(errors), "error_code": "INVALID_PAYLOAD"})

    try:
        outcome = ingest_operation(
            db,
            idempotency_key=data.idempotency_key,
            payload=write_payload,
            hash_payload=hash_payload,
            ttl_seconds=data.ttl_seconds,
        )
        db.commit()
    except PayloadConflict as exc:
        # 回滚业务事务，但单独持久化冲突计数以便观测键冲突
        db.rollback()
        record_conflict_hit(db, exc.record.id, exc.incoming_hash)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "相同业务键对应不同载荷",
                "error_code": "PAYLOAD_CONFLICT",
                "idempotency_key": data.idempotency_key,
                "existing_operation_id": exc.record.operation_id,
            },
        )
    except KeyExpired as exc:
        db.rollback()
        raise HTTPException(
            status_code=410,
            detail={
                "message": "幂等记录已过期，请在宽限期结束后更换业务键重试",
                "error_code": "KEY_EXPIRED",
                "idempotency_key": data.idempotency_key,
                "reusable_after": exc.reusable_after.isoformat(),
            },
        )
    except BusinessDataMissing as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"message": str(exc), "error_code": "BUSINESS_DATA_MISSING"})
    except Exception:
        db.rollback()
        raise

    record = outcome.record
    reusable_after = record.expires_at + timedelta(
        seconds=settings.IDEMPOTENCY_GRACE_SECONDS
    )
    return OperationIngestResponse(
        idempotency_key=record.idempotency_key,
        scope=record.scope,
        status=record.status,
        replayed=outcome.replayed,
        replay_count=record.replay_count,
        conflict_count=record.conflict_count,
        created_at=record.created_at,
        expires_at=record.expires_at,
        reusable_after=reusable_after,
        data=outcome.operation,
    )


@router.post(
    "/ingest/operations/batch",
    response_model=BatchOperationIngestResponse,
    tags=["幂等接入"],
)
def ingest_operations_batch(
    body: BatchOperationIngestRequest, db: Session = Depends(get_db)
):
    """批量幂等接入。

    每个条目独立保存点处理：无效条目或冲突条目不影响其他条目；results 严格
    按输入位置返回。全部条目在同一数据库事务内提交，保证业务数据与幂等记录
    原子落库。
    """
    moment = utc_now()
    results: list[BatchOperationIngestResultItem] = []
    saved = replayed = conflicts = expired = failures = 0

    for index, item in enumerate(body.items):
        result = _process_batch_item(db, index, item, moment)
        results.append(result)
        if result.outcome == "saved":
            saved += 1
        elif result.outcome == "replayed":
            replayed += 1
        elif result.outcome == "conflict":
            conflicts += 1
            failures += 1
        elif result.outcome == "expired":
            expired += 1
            failures += 1
        else:
            failures += 1

    try:
        db.commit()
    except Exception:
        db.rollback()
        raise

    return BatchOperationIngestResponse(
        total=len(body.items),
        saved_count=saved,
        replayed_count=replayed,
        success_count=saved + replayed,
        failure_count=failures,
        conflict_count=conflicts,
        expired_count=expired,
        results=results,
    )


def _process_batch_item(
    db: Session, index: int, item: BatchOperationIngestItem, moment
) -> BatchOperationIngestResultItem:
    base_kwargs = {"index": index, "idempotency_key": item.idempotency_key}

    if not item.idempotency_key:
        return BatchOperationIngestResultItem(
            **base_kwargs,
            outcome="invalid",
            success=False,
            error_code="MISSING_IDEMPOTENCY_KEY",
            error="条目缺少 idempotency_key",
        )

    write_payload, hash_payload = _operation_payload(item)

    # 条目级保存点：单条失败/冲突/过期不影响同批其他条目
    captured: dict = {}
    try:
        with db.begin_nested():
            existing = (
                db.query(IdempotencyRecord.id)
                .filter(
                    IdempotencyRecord.scope == SCOPE_OPERATION,
                    IdempotencyRecord.idempotency_key == item.idempotency_key,
                )
                .first()
            )
            if existing is None:
                errors = validate_operation_references(db, hash_payload)
                if errors:
                    return BatchOperationIngestResultItem(
                        **base_kwargs,
                        outcome="invalid",
                        success=False,
                        error_code="INVALID_PAYLOAD",
                        error="; ".join(errors),
                    )

            try:
                captured["outcome"] = ingest_operation(
                    db,
                    idempotency_key=item.idempotency_key,
                    payload=write_payload,
                    hash_payload=hash_payload,
                    ttl_seconds=item.ttl_seconds,
                    now=moment,
                )
            except PayloadConflict as exc:
                # 在 with 内消化异常，保存点正常释放，冲突计数随批次提交
                captured["conflict"] = exc
            except KeyExpired as exc:
                captured["expired"] = exc
    except Exception as exc:  # 数据库级异常：保存点已回滚，不影响其他条目
        return BatchOperationIngestResultItem(
            **base_kwargs,
            outcome="error",
            success=False,
            error_code="INTERNAL_ERROR",
            error=str(exc),
        )

    if "conflict" in captured:
        record = captured["conflict"].record
        return BatchOperationIngestResultItem(
            **base_kwargs,
            outcome="conflict",
            success=False,
            error_code="PAYLOAD_CONFLICT",
            error=f"相同业务键对应不同载荷，既有作业ID {record.operation_id}",
            replay_count=record.replay_count,
            conflict_count=record.conflict_count,
        )
    if "expired" in captured:
        exc = captured["expired"]
        return BatchOperationIngestResultItem(
            **base_kwargs,
            outcome="expired",
            success=False,
            error_code="KEY_EXPIRED",
            error=f"幂等记录已过期，可于 {exc.reusable_after.isoformat()} 后回收复用",
        )

    outcome = captured["outcome"]
    return BatchOperationIngestResultItem(
        **base_kwargs,
        outcome="replayed" if outcome.replayed else "saved",
        success=True,
        data=outcome.operation,
        replay_count=outcome.record.replay_count,
        conflict_count=outcome.record.conflict_count,
    )


def _to_record_view(record: IdempotencyRecord, moment) -> IdempotencyRecordResponse:
    reusable_after = record.expires_at + timedelta(
        seconds=settings.IDEMPOTENCY_GRACE_SECONDS
    )
    return IdempotencyRecordResponse(
        id=record.id,
        scope=record.scope,
        idempotency_key=record.idempotency_key,
        record_hash=record.record_hash,
        operation_id=record.operation_id,
        status=record.status,
        replay_count=record.replay_count,
        conflict_count=record.conflict_count,
        last_conflict_hash=record.last_conflict_hash,
        created_at=record.created_at,
        last_replayed_at=record.last_replayed_at,
        expires_at=record.expires_at,
        reusable_after=reusable_after,
        expired=moment >= record.expires_at,
        reusable=moment >= reusable_after,
    )


@router.get("/idempotency/records", response_model=IdempotencyRecordListResponse, tags=["幂等观测"])
def list_idempotency_records(
    scope: Optional[str] = Query(None, description="按作用域过滤"),
    idempotency_key: Optional[str] = Query(None, description="按业务键精确过滤"),
    operation_id: Optional[int] = Query(None, description="按业务数据ID过滤"),
    state: Optional[str] = Query(
        None,
        description="active（未到期）/ expired（已到期未回收）/ reusable（已过宽限期）",
    ),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    moment = utc_now()
    grace = timedelta(seconds=settings.IDEMPOTENCY_GRACE_SECONDS)
    query = db.query(IdempotencyRecord)
    if scope:
        query = query.filter(IdempotencyRecord.scope == scope)
    if idempotency_key:
        query = query.filter(IdempotencyRecord.idempotency_key == idempotency_key)
    if operation_id is not None:
        query = query.filter(IdempotencyRecord.operation_id == operation_id)
    if state == "active":
        query = query.filter(IdempotencyRecord.expires_at > moment)
    elif state == "expired":
        query = query.filter(
            IdempotencyRecord.expires_at <= moment,
            IdempotencyRecord.expires_at > moment - grace,
        )
    elif state == "reusable":
        query = query.filter(IdempotencyRecord.expires_at <= moment - grace)
    elif state is not None:
        raise HTTPException(status_code=400, detail="state 必须是 active/expired/reusable")

    total = query.count()
    records = (
        query.order_by(IdempotencyRecord.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return IdempotencyRecordListResponse(
        total=total, items=[_to_record_view(record, moment) for record in records]
    )


@router.get("/idempotency/stats", tags=["幂等观测"])
def get_idempotency_stats(db: Session = Depends(get_db)):
    """观测并发重试命中、冲突命中与到期分布。"""
    moment = utc_now()
    stats = idempotency_stats(db, now=moment)
    grace = timedelta(seconds=settings.IDEMPOTENCY_GRACE_SECONDS)
    reusable = (
        db.query(IdempotencyRecord)
        .filter(IdempotencyRecord.expires_at <= moment - grace)
        .count()
    )
    stats["now"] = moment.isoformat()
    stats["reusable"] = reusable
    stats["in_grace"] = stats["expired"] - reusable
    stats["policy"] = {
        "ttl_seconds": settings.IDEMPOTENCY_TTL_SECONDS,
        "grace_seconds": settings.IDEMPOTENCY_GRACE_SECONDS,
    }
    return stats


@router.post("/idempotency/reap", response_model=IdempotencyReapResponse, tags=["幂等观测"])
def reap_idempotency_records(
    dry_run: bool = Query(False, description="只统计将回收的数量，不实际删除"),
    grace_seconds: Optional[int] = Query(None, ge=0, description="覆盖默认宽限期"),
    batch_size: Optional[int] = Query(None, ge=1, le=10000),
    db: Session = Depends(get_db),
):
    """按 TTL+宽限期策略回收过期幂等记录（业务数据不受影响）。"""
    moment = utc_now()
    report = reap_expired_records(
        db,
        now=moment,
        grace_seconds=grace_seconds,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    db.commit()
    return IdempotencyReapResponse(
        now=moment,
        grace_seconds=(
            grace_seconds
            if grace_seconds is not None
            else settings.IDEMPOTENCY_GRACE_SECONDS
        ),
        cutoff=report.cutoff,
        reaped=report.reaped,
        scanned=report.scanned,
        in_grace=report.in_grace,
        dry_run=dry_run,
    )
