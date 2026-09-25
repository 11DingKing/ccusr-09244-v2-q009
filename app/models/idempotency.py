from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index
from sqlalchemy.sql import func

from app.database import Base


class IdempotencyRecord(Base):
    """请求幂等记录，与业务数据（operation_data）在同一事务内落库。

    - scope + idempotency_key 构成业务唯一键（由数据库唯一索引保证并发安全）；
    - record_hash 保存首次请求的规范载荷摘要，同键重试时比对，载荷不同即冲突；
    - operation_id 指向首次请求保存的业务数据，重放时还会回查该行是否仍存在，
      避免把新数据误返回给旧请求；
    - expires_at 为 TTL 到期点。到期记录不参与重放（返回 410），只有超过额外
      宽限期并被显式回收后，业务键才允许被重新使用。

    时间统一存储 naive UTC（SQLite 无时区转换能力）。
    """

    __tablename__ = "idempotency_records"
    __table_args__ = (
        Index("ix_idempotency_scope_key", "scope", "idempotency_key", unique=True),
        Index("ix_idempotency_expires", "expires_at"),
    )

    id = Column(Integer, primary_key=True, index=True)
    scope = Column(String(50), nullable=False)
    idempotency_key = Column(String(200), nullable=False)
    record_hash = Column(String(64), nullable=False)

    operation_id = Column(
        Integer, ForeignKey("operation_data.id"), nullable=False, index=True
    )

    # saved：首次请求落库；replayed：至少被一次重试命中
    status = Column(String(20), nullable=False, default="saved")
    replay_count = Column(Integer, nullable=False, default=0)
    # 同键不同载荷的冲突次数，用于在本地接口观察键冲突
    conflict_count = Column(Integer, nullable=False, default=0)
    last_conflict_hash = Column(String(64), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    last_replayed_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=False)
