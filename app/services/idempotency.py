"""接入幂等的领域判定与记录存取。

边缘采集网关在网络抖动后会重发最近一批作业数据。服务以调用方提供的
业务键（idempotency_key）识别重试：首次请求把业务数据与幂等记录写入
同一事务；之后相同键的请求按载荷指纹决定重放原结果或判定冲突。记录
带过期时间，过期键按“到期即失效（含边界）”回收；键被回收并复用后，
携带旧载荷的迟到请求只会被判为冲突，不会误命中新数据。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Mapping, Optional

from sqlalchemy.orm import Session

from app.models import IdempotencyRecord


class IdempotencyError(ValueError):
    """幂等策略参数不合法。"""


# 单条与批量接入共用一个键空间：同一业务记录无论从哪个入口重试，
# 都命中同一条幂等记录。
OPERATION_CREATE_SCOPE = "operation_create"


class IdempotencyDecision(str, Enum):
    NEW = "new"            # 无存活记录：正常处理并写入新记录
    RECLAIM = "reclaim"    # 记录已过期：回收旧记录后按新请求处理
    REPLAY = "replay"      # 键与载荷一致：返回首次保存的结果
    CONFLICT = "conflict"  # 键相同但载荷不同：判冲突，拒绝误命中


@dataclass(frozen=True)
class IdempotencyPolicy:
    """幂等记录的存活策略：写入起 ttl 后过期，到期边界（含）即失效。"""

    ttl: timedelta

    def validate(self) -> "IdempotencyPolicy":
        if self.ttl <= timedelta(0):
            raise IdempotencyError("幂等记录的存活时长必须为正")
        return self

    def expires_at(self, now: datetime) -> datetime:
        return now + self.ttl

    def is_expired(self, expires_at: datetime, now: datetime) -> bool:
        return expires_at <= now


def decide(
    record_hash: Optional[str],
    record_expires_at: Optional[datetime],
    payload_hash: str,
    now: datetime,
    policy: IdempotencyPolicy,
) -> IdempotencyDecision:
    """根据已存记录与当前载荷指纹给出幂等判定。"""
    if record_hash is None or record_expires_at is None:
        return IdempotencyDecision.NEW
    if policy.is_expired(record_expires_at, now):
        return IdempotencyDecision.RECLAIM
    if record_hash == payload_hash:
        return IdempotencyDecision.REPLAY
    return IdempotencyDecision.CONFLICT


def fingerprint_payload(payload: Mapping[str, Any]) -> str:
    """载荷的确定性指纹：键序无关、进程重启后保持稳定。"""
    canonical = json.dumps(
        payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def utcnow() -> datetime:
    """与 func.now() 一致使用朴素 UTC，避免 SQLite 下时区比较出错。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# 服务单进程运行（见 README），进程内写锁把同一键的并发重试串行化；
# 数据库唯一约束 uq_idempotency_scope_key 是跨写入方的最终兜底。
write_lock = threading.RLock()


def find_record(db: Session, scope: str, key: str) -> Optional[IdempotencyRecord]:
    return (
        db.query(IdempotencyRecord)
        .filter(
            IdempotencyRecord.scope == scope,
            IdempotencyRecord.idempotency_key == key,
        )
        .first()
    )


def build_record(
    *,
    scope: str,
    key: str,
    payload_hash: str,
    response_body: Mapping[str, Any],
    operation_data_id: Optional[int],
    policy: IdempotencyPolicy,
    now: datetime,
) -> IdempotencyRecord:
    return IdempotencyRecord(
        scope=scope,
        idempotency_key=key,
        payload_hash=payload_hash,
        response_body=dict(response_body),
        operation_data_id=operation_data_id,
        hit_count=0,
        conflict_count=0,
        expires_at=policy.expires_at(now),
    )


def register_replay(db: Session, record: IdempotencyRecord) -> None:
    """重放计数原子自增，并发重试下不丢计数。"""
    db.query(IdempotencyRecord).filter(IdempotencyRecord.id == record.id).update(
        {IdempotencyRecord.hit_count: IdempotencyRecord.hit_count + 1},
        synchronize_session=False,
    )


def register_conflict(db: Session, record: IdempotencyRecord) -> None:
    """冲突计数原子自增，便于观察接口核对误发情况。"""
    db.query(IdempotencyRecord).filter(IdempotencyRecord.id == record.id).update(
        {IdempotencyRecord.conflict_count: IdempotencyRecord.conflict_count + 1},
        synchronize_session=False,
    )


def recycle_expired(db: Session, now: datetime) -> int:
    """回收全部过期键（expires_at <= now），返回回收条数。"""
    recycled = (
        db.query(IdempotencyRecord)
        .filter(IdempotencyRecord.expires_at <= now)
        .delete(synchronize_session=False)
    )
    db.commit()
    return recycled
