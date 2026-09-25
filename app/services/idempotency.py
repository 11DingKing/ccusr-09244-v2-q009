"""统一请求幂等语义的服务层。

策略要点：
- 调用方提供业务键（scope + idempotency_key），首次请求在同一事务内写入
  业务数据（operation_data）与幂等记录；
- 相同键 + 相同规范载荷：不产生新数据，返回首次结果，累计 replay_count；
- 相同键 + 不同载荷：判定为冲突（409），累计 conflict_count；
- 键到期（expires_at）后不再重放（410）；经过额外宽限期并由回收接口
  显式删除后，业务键才允许被重新使用。回收只删幂等记录，不删业务数据；
  键被复用后，携带旧载荷的请求因载荷摘要不同仍会得到冲突，不会误拿新数据。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.models import IdempotencyRecord, OperationData, RobotModel, Scene, Skill

SCOPE_OPERATION = "operation"
STATUS_SAVED = "saved"
STATUS_REPLAYED = "replayed"


class IdempotencyError(Exception):
    """幂等处理基类。"""


class PayloadConflict(IdempotencyError):
    """同键不同载荷冲突。"""

    def __init__(self, record: IdempotencyRecord, incoming_hash: str):
        self.record = record
        self.incoming_hash = incoming_hash
        super().__init__("相同业务键对应不同载荷")


class KeyExpired(IdempotencyError):
    """幂等记录已过期，处于不可重放、宽限期内不可复用的状态。"""

    def __init__(self, record: IdempotencyRecord, reusable_after: datetime):
        self.record = record
        self.reusable_after = reusable_after
        super().__init__("幂等记录已过期")


class BusinessDataMissing(IdempotencyError):
    """幂等记录指向的业务数据不存在（防御性异常，正常同事务不会出现）。"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def canonical_hash(payload: Mapping[str, Any]) -> str:
    """对载荷做稳定规范化后取 sha256。

    sort_keys 消除字段顺序差异，紧凑分隔符消除空白差异，ensure_ascii=False
    保证中文等非 ASCII 内容稳定。入参应为 JSON 兼容的字典（pydantic mode="json"）。
    """
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_operation_references(db: Session, payload: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    robot_model = db.query(RobotModel.id).filter(RobotModel.id == payload["robot_model_id"]).first()
    if not robot_model:
        errors.append(f"机型ID {payload['robot_model_id']} 不存在")
    scene = db.query(Scene.id).filter(Scene.id == payload["scene_id"]).first()
    if not scene:
        errors.append(f"场景ID {payload['scene_id']} 不存在")
    skill = db.query(Skill.id).filter(Skill.id == payload["skill_id"]).first()
    if not skill:
        errors.append(f"技能ID {payload['skill_id']} 不存在")
    return errors


@dataclass
class IngestOutcome:
    operation: OperationData
    record: IdempotencyRecord
    replayed: bool


def ingest_operation(
    db: Session,
    idempotency_key: str,
    payload: Mapping[str, Any],
    ttl_seconds: Optional[int] = None,
    scope: str = SCOPE_OPERATION,
    now: Optional[datetime] = None,
    hash_payload: Optional[Mapping[str, Any]] = None,
) -> IngestOutcome:
    """在调用方事务内完成单条作业的幂等写入或重放。

    依赖 (scope, idempotency_key) 上的唯一索引解决并发竞争：竞争失败的一方
    回滚到保存点后按已提交记录走重放/冲突分支，因此并发重试只会落一条业务数据。

    payload 为写入 OperationData 的 Python 对象载荷；hash_payload 为参与摘要
    的 JSON 兼容载荷（缺省与 payload 相同）。
    """
    moment = now or utc_now()
    ttl = ttl_seconds if ttl_seconds is not None else settings.IDEMPOTENCY_TTL_SECONDS
    digest_source = hash_payload if hash_payload is not None else payload
    payload_hash = canonical_hash(digest_source)

    existing = (
        db.query(IdempotencyRecord)
        .filter(
            IdempotencyRecord.scope == scope,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        .first()
    )
    if existing is not None:
        return _resolve_existing(db, existing, payload_hash, moment)

    record = IdempotencyRecord(
        scope=scope,
        idempotency_key=idempotency_key,
        record_hash=payload_hash,
        status=STATUS_SAVED,
        replay_count=0,
        conflict_count=0,
        created_at=moment,
        expires_at=moment + timedelta(seconds=ttl),
    )
    operation = OperationData(**dict(payload))

    savepoint = db.begin_nested()
    try:
        db.add(operation)
        db.flush()  # 拿到 operation.id
        record.operation_id = operation.id
        db.add(record)
        db.flush()  # 触发唯一索引竞争
    except IntegrityError:
        savepoint.rollback()
        winner = (
            db.query(IdempotencyRecord)
            .filter(
                IdempotencyRecord.scope == scope,
                IdempotencyRecord.idempotency_key == idempotency_key,
            )
            .first()
        )
        if winner is None:
            raise
        return _resolve_existing(db, winner, payload_hash, moment)

    return IngestOutcome(operation=operation, record=record, replayed=False)


def _bump_counters(
    db: Session,
    record: IdempotencyRecord,
    moment: datetime,
    *,
    replay: bool = False,
    conflict_hash: Optional[str] = None,
) -> IdempotencyRecord:
    """以原子 UPDATE 累计重放/冲突计数，避免并发下读改写丢失更新。"""
    values: dict[str, Any] = {}
    if replay:
        values[IdempotencyRecord.status] = STATUS_REPLAYED
        values[IdempotencyRecord.replay_count] = IdempotencyRecord.replay_count + 1
        values[IdempotencyRecord.last_replayed_at] = moment
    if conflict_hash is not None:
        values[IdempotencyRecord.conflict_count] = IdempotencyRecord.conflict_count + 1
        values[IdempotencyRecord.last_conflict_hash] = conflict_hash
    db.query(IdempotencyRecord).filter(IdempotencyRecord.id == record.id).update(
        values, synchronize_session=False
    )
    db.refresh(record)
    return record


def _resolve_existing(
    db: Session,
    record: IdempotencyRecord,
    payload_hash: str,
    moment: datetime,
) -> IngestOutcome:
    grace = timedelta(seconds=settings.IDEMPOTENCY_GRACE_SECONDS)
    reusable_after = record.expires_at + grace
    if moment >= record.expires_at:
        raise KeyExpired(record, reusable_after)

    if record.record_hash != payload_hash:
        _bump_counters(db, record, moment, conflict_hash=payload_hash)
        raise PayloadConflict(record, payload_hash)

    operation = (
        db.query(OperationData)
        .filter(OperationData.id == record.operation_id)
        .first()
    )
    if operation is None:
        raise BusinessDataMissing(f"幂等记录 {record.id} 指向的作业数据不存在")

    _bump_counters(db, record, moment, replay=True)
    return IngestOutcome(operation=operation, record=record, replayed=True)


def record_conflict_hit(
    db: Session,
    record_id: int,
    incoming_hash: str,
) -> None:
    """在独立事务中原子记录一次冲突命中（用于单条接口回滚后的观测持久化）。"""
    db.query(IdempotencyRecord).filter(IdempotencyRecord.id == record_id).update(
        {
            IdempotencyRecord.conflict_count: IdempotencyRecord.conflict_count + 1,
            IdempotencyRecord.last_conflict_hash: incoming_hash,
        },
        synchronize_session=False,
    )
    db.commit()


@dataclass
class ReapReport:
    cutoff: datetime
    reaped: int
    scanned: int
    in_grace: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "cutoff": self.cutoff,
            "reaped": self.reaped,
            "scanned": self.scanned,
            "in_grace": self.in_grace,
        }


def reap_expired_records(
    db: Session,
    now: Optional[datetime] = None,
    grace_seconds: Optional[int] = None,
    batch_size: Optional[int] = None,
    dry_run: bool = False,
) -> ReapReport:
    """回收超过 TTL + 宽限期的幂等记录（只删幂等行，业务数据保留）。

    边界：expires_at <= now - grace 才回收；已过期但仍在宽限期内的记录
    只计数不删除，保证旧键不会在宽限期内被新请求占用。
    """
    moment = now or utc_now()
    grace = timedelta(
        seconds=settings.IDEMPOTENCY_GRACE_SECONDS
        if grace_seconds is None
        else grace_seconds
    )
    limit = batch_size or settings.IDEMPOTENCY_REAP_BATCH_SIZE
    cutoff = moment - grace

    expired_q = db.query(IdempotencyRecord).filter(IdempotencyRecord.expires_at <= moment)
    in_grace = expired_q.filter(IdempotencyRecord.expires_at > cutoff).count()

    ripe_q = (
        db.query(IdempotencyRecord)
        .filter(IdempotencyRecord.expires_at <= cutoff)
        .order_by(IdempotencyRecord.expires_at)
    )
    ripe = ripe_q.limit(limit).all()
    if not dry_run:
        for record in ripe:
            db.delete(record)
        db.flush()
    return ReapReport(
        cutoff=cutoff,
        reaped=0 if dry_run else len(ripe),
        scanned=len(ripe),
        in_grace=in_grace,
    )


def idempotency_stats(db: Session, now: Optional[datetime] = None) -> dict[str, Any]:
    moment = now or utc_now()
    total = db.query(func.count(IdempotencyRecord.id)).scalar() or 0
    active = (
        db.query(func.count(IdempotencyRecord.id))
        .filter(IdempotencyRecord.expires_at > moment)
        .scalar()
        or 0
    )
    expired = total - active
    replayed = (
        db.query(func.count(IdempotencyRecord.id))
        .filter(IdempotencyRecord.status == STATUS_REPLAYED)
        .scalar()
        or 0
    )
    replay_hits = (
        db.query(func.coalesce(func.sum(IdempotencyRecord.replay_count), 0)).scalar() or 0
    )
    conflict_hits = (
        db.query(func.coalesce(func.sum(IdempotencyRecord.conflict_count), 0)).scalar() or 0
    )

    per_scope_rows = (
        db.query(
            IdempotencyRecord.scope,
            func.count(IdempotencyRecord.id),
            func.coalesce(func.sum(IdempotencyRecord.replay_count), 0),
            func.coalesce(func.sum(IdempotencyRecord.conflict_count), 0),
        )
        .group_by(IdempotencyRecord.scope)
        .all()
    )
    per_scope = {
        scope: {"records": count, "replay_hits": replays, "conflict_hits": conflicts}
        for scope, count, replays, conflicts in per_scope_rows
    }
    return {
        "now": moment,
        "total": total,
        "active": active,
        "expired": expired,
        "replayed_records": replayed,
        "replay_hits": int(replay_hits),
        "conflict_hits": int(conflict_hits),
        "per_scope": per_scope,
    }
