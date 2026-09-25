"""幂等接入端到端测试：并发重试、部分无效批次、键冲突、回收边界、重启持久化。"""

import threading
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.config import settings


def _enable_wal(engine):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()


def test_single_first_save_then_replay_same_payload(client, make_payload):
    payload = make_payload()
    body = {"idempotency_key": "job-1", **payload}

    r1 = client.post("/api/v1/ingest/operations", json=body)
    assert r1.status_code == 200, r1.text
    first = r1.json()
    assert first["status"] == "saved"
    assert first["replayed"] is False
    assert first["replay_count"] == 0
    operation_id = first["data"]["id"]

    # 相同键、相同载荷（即使字段顺序/空白不同）返回原结果
    r2 = client.post("/api/v1/ingest/operations", json=dict(reversed(list(body.items()))))
    assert r2.status_code == 200
    second = r2.json()
    assert second["status"] == "replayed"
    assert second["replayed"] is True
    assert second["replay_count"] == 1
    assert second["data"]["id"] == operation_id

    # 业务库里只有一条数据
    r3 = client.get("/api/v1/operations", params={"page_size": 200})
    assert r3.json()["total"] == 1


def test_single_same_key_different_payload_conflicts(client, make_payload):
    body1 = {"idempotency_key": "job-2", **make_payload(robot_serial="SN-A")}
    r1 = client.post("/api/v1/ingest/operations", json=body1)
    assert r1.status_code == 200

    body2 = {"idempotency_key": "job-2", **make_payload(robot_serial="SN-B")}
    r2 = client.post("/api/v1/ingest/operations", json=body2)
    assert r2.status_code == 409
    detail = r2.json()["detail"]
    assert detail["error_code"] == "PAYLOAD_CONFLICT"
    assert detail["existing_operation_id"] == r1.json()["data"]["id"]

    # 冲突计数持久化，可通过观测接口看到
    stats = client.get("/api/v1/idempotency/stats").json()
    assert stats["conflict_hits"] == 1

    records = client.get(
        "/api/v1/idempotency/records", params={"idempotency_key": "job-2"}
    ).json()["items"]
    assert records[0]["conflict_count"] == 1

    # 原始载荷仍能正常重放，拿到的仍是第一次的结果
    r3 = client.post("/api/v1/ingest/operations", json=body1)
    assert r3.status_code == 200
    assert r3.json()["data"]["robot_serial"] == "SN-A"


def test_single_invalid_payload_is_400_and_leaves_no_record(client, make_payload):
    body = {"idempotency_key": "job-x", **make_payload(robot_model_id=99999)}
    r = client.post("/api/v1/ingest/operations", json=body)
    assert r.status_code == 400
    listing = client.get(
        "/api/v1/idempotency/records", params={"idempotency_key": "job-x"}
    ).json()
    assert listing["total"] == 0

    # 修正后同一键仍可首次保存
    good = {"idempotency_key": "job-x", **make_payload()}
    r2 = client.post("/api/v1/ingest/operations", json=good)
    assert r2.status_code == 200
    assert r2.json()["status"] == "saved"


def test_concurrent_retry_creates_exactly_one_record(db_env, make_payload, seed_refs):
    """并发相同重试：数据库唯一索引仲裁，只落一条业务数据。"""
    from main import app
    from app.database import get_db

    engine = create_engine(
        f"sqlite:///{db_env['path']}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    _enable_wal(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    payload = make_payload()
    body = {"idempotency_key": "concurrent-1", **payload}
    outcomes: list[dict] = []
    barrier = threading.Barrier(8)

    def worker():
        local_client = TestClient(app)
        barrier.wait()
        r = local_client.post("/api/v1/ingest/operations", json=body)
        outcomes.append({"status": r.status_code, "json": r.json() if r.status_code == 200 else r.json()})

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(outcomes) == 8
    assert all(o["status"] == 200 for o in outcomes), outcomes
    saved = [o for o in outcomes if o["json"]["status"] == "saved"]
    replayed = [o for o in outcomes if o["json"]["status"] == "replayed"]
    assert len(saved) == 1
    assert len(replayed) == 7
    operation_ids = {o["json"]["data"]["id"] for o in outcomes}
    assert operation_ids == {saved[0]["json"]["data"]["id"]}

    with SessionLocal() as db:
        from app.models import OperationData, IdempotencyRecord

        assert db.query(IdempotencyRecord).count() == 1
        assert db.query(OperationData).count() == 1
        record = db.query(IdempotencyRecord).one()
        assert record.replay_count == 7

    engine.dispose()
    app.dependency_overrides.pop(get_db)


def test_batch_preserves_positions_and_partial_failures(client, make_payload):
    good1 = {"idempotency_key": "b-1", **make_payload()}
    missing_key = make_payload()  # 无业务键
    bad_ref = {"idempotency_key": "b-2", **make_payload(scene_id=99999)}
    good2 = {"idempotency_key": "b-3", **make_payload()}

    r = client.post(
        "/api/v1/ingest/operations/batch",
        json={"items": [good1, missing_key, bad_ref, good2]},
    )
    assert r.status_code == 200, r.text
    resp = r.json()
    assert resp["total"] == 4
    assert resp["success_count"] == 2
    assert resp["saved_count"] == 2
    assert resp["failure_count"] == 2

    results = resp["results"]
    assert [item["index"] for item in results] == [0, 1, 2, 3]
    assert results[0]["outcome"] == "saved"
    assert results[1]["outcome"] == "invalid"
    assert results[1]["error_code"] == "MISSING_IDEMPOTENCY_KEY"
    assert results[2]["outcome"] == "invalid"
    assert results[2]["error_code"] == "INVALID_PAYLOAD"
    assert results[3]["outcome"] == "saved"

    # 有效条目确实落库，无效条目没有产生数据
    listing = client.get("/api/v1/operations", params={"page_size": 200}).json()
    assert listing["total"] == 2


def test_batch_mixes_saved_replayed_conflict_and_counts(client, make_payload):
    first = {"idempotency_key": "mix-1", **make_payload(robot_serial="AAA")}
    assert client.post("/api/v1/ingest/operations", json=first).status_code == 200

    same = dict(first)  # 重放
    conflict = {"idempotency_key": "mix-1", **make_payload(robot_serial="ZZZ")}
    fresh = {"idempotency_key": "mix-2", **make_payload()}

    r = client.post(
        "/api/v1/ingest/operations/batch",
        json={"items": [same, conflict, fresh]},
    )
    resp = r.json()
    assert [item["outcome"] for item in resp["results"]] == [
        "replayed",
        "conflict",
        "saved",
    ]
    assert resp["replayed_count"] == 1
    assert resp["conflict_count"] == 1
    assert resp["saved_count"] == 1
    assert resp["failure_count"] == 1
    # 冲突条目带既有作业ID，位置不变
    assert resp["results"][1]["index"] == 1
    assert "既有作业ID" in resp["results"][1]["error"]

    # 冲突计数随批次提交落库
    stats = client.get("/api/v1/idempotency/stats").json()
    assert stats["conflict_hits"] == 1


def test_batch_duplicate_keys_within_one_batch(client, make_payload):
    """同一批内出现重复业务键：一个 saved，其余按同载荷重放。"""
    p = make_payload()
    items = [
        {"idempotency_key": "dup-1", **p},
        {"idempotency_key": "dup-1", **p},
        {"idempotency_key": "dup-1", **p},
    ]
    r = client.post("/api/v1/ingest/operations/batch", json={"items": items})
    resp = r.json()
    outcomes = [item["outcome"] for item in resp["results"]]
    assert outcomes[0] == "saved"
    assert outcomes.count("replayed") == 2
    assert resp["saved_count"] == 1
    assert client.get("/api/v1/operations", params={"page_size": 10}).json()["total"] == 1


def test_idempotency_persists_across_restart(db_env, client, make_payload):
    """模拟进程重启：新建引擎指向同一数据库文件，旧键仍然有效。"""
    body = {"idempotency_key": "restart-1", **make_payload()}
    r1 = client.post("/api/v1/ingest/operations", json=body)
    assert r1.status_code == 200
    operation_id = r1.json()["data"]["id"]

    from main import app
    from app.database import get_db

    new_engine = create_engine(
        f"sqlite:///{db_env['path']}",
        connect_args={"check_same_thread": False},
    )
    NewSession = sessionmaker(autocommit=False, autoflush=False, bind=new_engine)

    def restarted_get_db():
        db = NewSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = restarted_get_db
    restarted_client = TestClient(app)

    r2 = restarted_client.post("/api/v1/ingest/operations", json=body)
    assert r2.status_code == 200
    assert r2.json()["status"] == "replayed"
    assert r2.json()["data"]["id"] == operation_id

    app.dependency_overrides.pop(get_db)
    new_engine.dispose()


def test_expiry_grace_and_reclaim_boundaries(db_env, make_payload):
    """直接驱动服务层验证时间边界：未到期 / 已到期宽限 / 可回收 / 复用不误命中。"""
    from app.models import IdempotencyRecord
    from app.services.idempotency import (
        KeyExpired,
        PayloadConflict,
        ingest_operation,
        reap_expired_records,
        utc_now,
    )

    SessionLocal = db_env["SessionLocal"]
    ttl = settings.IDEMPOTENCY_TTL_SECONDS
    grace = settings.IDEMPOTENCY_GRACE_SECONDS

    from app.schemas.operation import OperationDataBase

    def parsed(raw):
        model = OperationDataBase(**raw)
        return model.model_dump(), model.model_dump(mode="json")

    payload, payload_json = parsed(make_payload())
    t0 = utc_now()

    db = SessionLocal()
    outcome = ingest_operation(
        db, "edge-1", payload, hash_payload=payload_json, ttl_seconds=ttl, now=t0
    )
    original_id = outcome.operation.id
    db.commit()
    db.close()

    # 未到期：同载荷重放
    db = SessionLocal()
    out = ingest_operation(
        db, "edge-1", payload, hash_payload=payload_json,
        ttl_seconds=ttl, now=t0 + timedelta(seconds=1),
    )
    assert out.replayed is True
    assert out.operation.id == original_id
    db.commit()
    db.close()

    # 到期但未超过宽限：410，不允许重放也不允许复用
    expired_at = t0 + timedelta(seconds=ttl + 1)
    db = SessionLocal()
    try:
        ingest_operation(
            db, "edge-1", payload, hash_payload=payload_json,
            ttl_seconds=ttl, now=expired_at,
        )
        assert False, "应当抛出 KeyExpired"
    except KeyExpired:
        db.rollback()
    db.close()

    # 宽限期内回收：删除 0 条，in_grace 计数 1
    db = SessionLocal()
    report = reap_expired_records(db, now=expired_at)
    db.commit()
    assert report.reaped == 0
    assert report.in_grace == 1
    db.close()

    # 超过宽限期：dry_run 不删除，正式回收删除记录但保留业务数据
    reusable_at = t0 + timedelta(seconds=ttl + grace + 1)
    db = SessionLocal()
    dry = reap_expired_records(db, now=reusable_at, dry_run=True)
    assert dry.reaped == 0
    assert db.query(IdempotencyRecord).count() == 1
    report = reap_expired_records(db, now=reusable_at)
    db.commit()
    assert report.reaped == 1
    assert db.query(IdempotencyRecord).count() == 0
    from app.models import OperationData

    assert db.query(OperationData).filter(OperationData.id == original_id).count() == 1

    # 键复用后：旧载荷请求得到冲突，不会误拿到新数据
    new_payload, new_payload_json = parsed(make_payload(robot_serial="NEW-SERIAL"))
    out2 = ingest_operation(
        db, "edge-1", new_payload, hash_payload=new_payload_json,
        ttl_seconds=ttl, now=reusable_at,
    )
    assert out2.operation.id != original_id
    assert out2.operation.robot_serial == "NEW-SERIAL"
    db.commit()

    try:
        ingest_operation(
            db, "edge-1", payload, hash_payload=payload_json,
            ttl_seconds=ttl, now=reusable_at,
        )
        assert False, "旧载荷应被判定为冲突"
    except PayloadConflict:
        db.rollback()

    # 新载荷自己重放拿到的是新数据
    out3 = ingest_operation(
        db, "edge-1", new_payload, hash_payload=new_payload_json,
        ttl_seconds=ttl, now=reusable_at,
    )
    assert out3.replayed is True
    assert out3.operation.robot_serial == "NEW-SERIAL"
    db.commit()
    db.close()


def test_expired_key_returns_410_over_http(db_env, make_payload):
    SessionLocal = db_env["SessionLocal"]
    from app.schemas.operation import OperationDataBase
    from app.services.idempotency import ingest_operation, utc_now

    raw = make_payload()
    model = OperationDataBase(**raw)
    payload = model.model_dump()
    payload_json = model.model_dump(mode="json")
    db = SessionLocal()
    ingest_operation(
        db, "http-exp-1", payload, hash_payload=payload_json,
        ttl_seconds=60, now=utc_now(),
    )
    db.commit()
    db.close()
    # 手工把到期时间调到过去
    db = SessionLocal()
    from app.models import IdempotencyRecord

    record = db.query(IdempotencyRecord).filter_by(idempotency_key="http-exp-1").one()
    record.expires_at = utc_now() - timedelta(seconds=1)
    db.commit()
    db.close()

    from main import app  # noqa: F401

    from fastapi.testclient import TestClient

    c = TestClient(app)
    r = c.post("/api/v1/ingest/operations", json={"idempotency_key": "http-exp-1", **raw})
    assert r.status_code == 410
    assert r.json()["detail"]["error_code"] == "KEY_EXPIRED"


def test_records_and_stats_observability(client, make_payload):
    payload = make_payload()
    assert client.post(
        "/api/v1/ingest/operations",
        json={"idempotency_key": "obs-1", **payload},
    ).status_code == 200
    assert client.post(
        "/api/v1/ingest/operations",
        json={"idempotency_key": "obs-1", **payload},
    ).status_code == 200

    stats = client.get("/api/v1/idempotency/stats").json()
    assert stats["total"] == 1
    assert stats["replay_hits"] == 1
    assert stats["replayed_records"] == 1
    assert stats["active"] == 1
    assert stats["per_scope"]["operation"]["records"] == 1

    listing = client.get("/api/v1/idempotency/records", params={"state": "active"}).json()
    assert listing["total"] == 1
    view = listing["items"][0]
    assert view["expired"] is False
    assert view["reusable"] is False
    assert view["reusable_after"] > view["expires_at"]

    reap = client.post("/api/v1/idempotency/reap", params={"dry_run": True}).json()
    assert reap["dry_run"] is True
    assert reap["reaped"] == 0


def test_deleting_operation_removes_idempotency_record(client, make_payload):
    body = {"idempotency_key": "del-1", **make_payload()}
    created = client.post("/api/v1/ingest/operations", json=body).json()
    operation_id = created["data"]["id"]

    r = client.delete(f"/api/v1/operations/{operation_id}")
    assert r.status_code == 200
    listing = client.get(
        "/api/v1/idempotency/records", params={"idempotency_key": "del-1"}
    ).json()
    assert listing["total"] == 0


def test_canonical_hash_ignores_field_order_and_whitespace():
    from app.services.idempotency import canonical_hash

    a = {"x": 1, "y": {"z": [1, 2]}, "name": "作业"}
    b = {"name": "作业", "y": {"z": [1, 2]}, "x": 1}
    assert canonical_hash(a) == canonical_hash(b)

    c = {"x": 1, "y": {"z": [1, 3]}, "name": "作业"}
    assert canonical_hash(a) != canonical_hash(c)
