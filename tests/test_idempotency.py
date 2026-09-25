from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.database import Base, SessionLocal, engine
from app.models import IdempotencyRecord, OperationData
from app.services.idempotency import (
    IdempotencyDecision,
    IdempotencyPolicy,
    decide,
    fingerprint_payload,
    utcnow,
)
from main import app

API = settings.API_V1_PREFIX


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_tables():
    db = SessionLocal()
    try:
        for table in reversed(Base.metadata.sorted_tables):
            db.execute(table.delete())
        db.commit()
    finally:
        db.close()
    yield


@pytest.fixture()
def refs(client):
    rm = client.post(f"{API}/robot-models", json={"name": "ARM-01", "manufacturer": "ACME"}).json()
    sc = client.post(f"{API}/scenes", json={"name": "装配线A", "category": "生产制造"}).json()
    sk = client.post(f"{API}/skills", json={"name": "抓取", "category": "操作"}).json()
    return {"robot_model_id": rm["id"], "scene_id": sc["id"], "skill_id": sk["id"]}


def make_payload(refs, key=None, serial="RB-0001", waypoint=0.5):
    payload = {
        "robot_model_id": refs["robot_model_id"],
        "scene_id": refs["scene_id"],
        "skill_id": refs["skill_id"],
        "robot_serial": serial,
        "motion_trajectory": {"waypoints": [[waypoint, 0.1, 0.2]]},
        "perception_records": {"camera_1": {"frames": 12}},
        "grasp_result": {"success": True},
        "timestamp_start": "2026-09-25T08:00:00",
        "timestamp_end": "2026-09-25T08:00:05",
        "duration_ms": 5000,
    }
    if key is not None:
        payload["idempotency_key"] = key
    return payload


def count_operations():
    db = SessionLocal()
    try:
        return db.query(OperationData).count()
    finally:
        db.close()


def get_record(key):
    db = SessionLocal()
    try:
        record = (
            db.query(IdempotencyRecord)
            .filter(IdempotencyRecord.idempotency_key == key)
            .first()
        )
        if record:
            db.expunge(record)
        return record
    finally:
        db.close()


def test_single_create_stores_then_replays(client, refs):
    payload = make_payload(refs, key="gw-key-1")
    first = client.post(f"{API}/operations", json=payload)
    assert first.status_code == 200
    assert first.headers["x-idempotency-status"] == "stored"

    second = client.post(f"{API}/operations", json=payload)
    assert second.status_code == 200
    assert second.headers["x-idempotency-status"] == "replayed"
    assert second.json() == first.json()

    assert count_operations() == 1
    record = get_record("gw-key-1")
    assert record.hit_count == 1
    assert record.conflict_count == 0
    assert record.operation_data_id == first.json()["id"]

    listing = client.get(f"{API}/idempotency-records").json()
    assert listing["total"] == 1
    assert listing["items"][0]["expired"] is False
    detail = client.get(f"{API}/idempotency-records/{listing['items'][0]['id']}").json()
    assert detail["response_body"]["id"] == first.json()["id"]
    assert detail["hit_count"] == 1


def test_same_key_different_payload_conflicts(client, refs):
    first = client.post(f"{API}/operations", json=make_payload(refs, key="gw-key-2"))
    assert first.status_code == 200

    changed = make_payload(refs, key="gw-key-2", waypoint=9.9)
    resp = client.post(f"{API}/operations", json=changed)
    assert resp.status_code == 409
    assert "gw-key-2" in resp.json()["detail"]

    assert count_operations() == 1
    record = get_record("gw-key-2")
    assert record.hit_count == 0
    assert record.conflict_count == 1


def test_requests_without_key_are_not_deduplicated(client, refs):
    payload = make_payload(refs)
    a = client.post(f"{API}/operations", json=payload)
    b = client.post(f"{API}/operations", json=payload)
    assert a.status_code == 200 and b.status_code == 200
    assert a.json()["id"] != b.json()["id"]
    assert count_operations() == 2

    db = SessionLocal()
    try:
        assert db.query(IdempotencyRecord).count() == 0
    finally:
        db.close()


def test_concurrent_retries_store_exactly_once(client, refs):
    payload = make_payload(refs, key="gw-storm")

    def send(_):
        return client.post(f"{API}/operations", json=payload)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(send, range(8)))

    assert all(r.status_code == 200 for r in responses)
    assert len({r.json()["id"] for r in responses}) == 1
    statuses = [r.headers["x-idempotency-status"] for r in responses]
    assert statuses.count("stored") == 1
    assert statuses.count("replayed") == 7

    assert count_operations() == 1
    record = get_record("gw-storm")
    assert record.hit_count == 7


def test_batch_partial_invalid_and_replay_keeps_positions(client, refs):
    good1 = make_payload(refs, key="batch-1", serial="RB-1")
    bad = make_payload(refs, key="batch-2", serial="RB-2")
    bad["robot_model_id"] = 99999
    good2 = make_payload(refs, key="batch-3", serial="RB-3")

    first = client.post(f"{API}/operations/batch", json=[good1, bad, good2])
    assert first.status_code == 200
    body = first.json()
    assert body["total"] == 3
    assert body["success_count"] == 2
    assert body["failure_count"] == 1
    assert [r["index"] for r in body["results"]] == [0, 1, 2]
    assert body["results"][0]["success"] is True
    assert body["results"][0]["replayed"] is False
    assert body["results"][1]["success"] is False
    assert "不存在" in body["results"][1]["error"]
    assert body["results"][2]["success"] is True

    # 网关原样重发整个批次：有效条目重放原结果且不重复入库，无效条目仍失败，位置不变
    second = client.post(f"{API}/operations/batch", json=[good1, bad, good2])
    replayed = second.json()
    assert [r["index"] for r in replayed["results"]] == [0, 1, 2]
    assert replayed["results"][0]["replayed"] is True
    assert replayed["results"][2]["replayed"] is True
    assert replayed["results"][0]["data"]["id"] == body["results"][0]["data"]["id"]
    assert replayed["results"][2]["data"]["id"] == body["results"][2]["data"]["id"]
    assert replayed["results"][1]["success"] is False

    assert count_operations() == 2


def test_batch_item_key_conflict(client, refs):
    first = client.post(f"{API}/operations/batch", json=[make_payload(refs, key="bk-1")])
    assert first.json()["results"][0]["success"] is True

    changed = make_payload(refs, key="bk-1", waypoint=3.3)
    second = client.post(f"{API}/operations/batch", json=[changed])
    item = second.json()["results"][0]
    assert item["index"] == 0
    assert item["success"] is False
    assert "已关联不同的请求载荷" in item["error"]

    assert count_operations() == 1
    record = get_record("bk-1")
    assert record.conflict_count == 1


def test_batch_same_key_reused_within_one_request(client, refs):
    item = make_payload(refs, key="bk-dup")
    resp = client.post(f"{API}/operations/batch", json=[item, item])
    body = resp.json()
    assert body["success_count"] == 2
    assert body["results"][0]["replayed"] is False
    assert body["results"][1]["replayed"] is True
    assert body["results"][0]["data"]["id"] == body["results"][1]["data"]["id"]
    assert count_operations() == 1


def test_expired_key_reclaim_and_old_payload_never_hits_new_data(client, refs):
    payload_a = make_payload(refs, key="recycle-key", waypoint=1.0)
    first = client.post(f"{API}/operations", json=payload_a)
    assert first.headers["x-idempotency-status"] == "stored"

    # 把记录拨到过期边界之外（等价于存活时长到期）
    db = SessionLocal()
    try:
        record = db.query(IdempotencyRecord).filter(
            IdempotencyRecord.idempotency_key == "recycle-key"
        ).one()
        record.expires_at = utcnow() - timedelta(seconds=1)
        db.commit()
    finally:
        db.close()

    listing = client.get(f"{API}/idempotency-records", params={"expired": True}).json()
    assert listing["total"] == 1
    assert listing["items"][0]["expired"] is True

    # 过期后同键按新请求处理，旧记录被回收
    payload_b = make_payload(refs, key="recycle-key", waypoint=2.0)
    reused = client.post(f"{API}/operations", json=payload_b)
    assert reused.headers["x-idempotency-status"] == "stored"
    assert reused.json()["id"] != first.json()["id"]

    # 旧请求迟到：同键不同载荷判 409，不会误命中新数据
    late_old = client.post(f"{API}/operations", json=payload_a)
    assert late_old.status_code == 409

    assert count_operations() == 2


def test_recycle_endpoint_removes_only_expired_keys(client, refs):
    client.post(f"{API}/operations", json=make_payload(refs, key="keep-key"))
    client.post(f"{API}/operations", json=make_payload(refs, key="drop-key"))

    db = SessionLocal()
    try:
        record = db.query(IdempotencyRecord).filter(
            IdempotencyRecord.idempotency_key == "drop-key"
        ).one()
        record.expires_at = utcnow() - timedelta(seconds=1)
        db.commit()
    finally:
        db.close()

    resp = client.post(f"{API}/idempotency-records/recycle")
    assert resp.status_code == 200
    assert resp.json() == {"recycled": 1, "remaining": 1}

    # 未过期键仍然重放，被回收的键可安全复用
    kept = client.post(f"{API}/operations", json=make_payload(refs, key="keep-key"))
    assert kept.headers["x-idempotency-status"] == "replayed"
    again = client.post(f"{API}/operations", json=make_payload(refs, key="drop-key"))
    assert again.headers["x-idempotency-status"] == "stored"


def test_records_survive_connection_restart(client, refs):
    payload = make_payload(refs, key="restart-key")
    first = client.post(f"{API}/operations", json=payload)
    assert first.headers["x-idempotency-status"] == "stored"

    # 模拟服务重启：丢弃全部数据库连接，进程内不保留任何记录
    engine.dispose()

    second = client.post(f"{API}/operations", json=payload)
    assert second.headers["x-idempotency-status"] == "replayed"
    assert second.json() == first.json()
    assert count_operations() == 1


def test_decide_covers_all_boundaries():
    policy = IdempotencyPolicy(ttl=timedelta(hours=1)).validate()
    now = utcnow()
    expires = policy.expires_at(now)

    assert decide(None, None, "h", now, policy) == IdempotencyDecision.NEW
    assert decide("h", expires, "h", now, policy) == IdempotencyDecision.REPLAY
    assert decide("h", expires, "other", now, policy) == IdempotencyDecision.CONFLICT
    # 到期边界（含）即过期，与回收策略一致
    assert decide("h", now, "h", now, policy) == IdempotencyDecision.RECLAIM
    assert policy.is_expired(now, now) is True
    assert policy.is_expired(now + timedelta(microseconds=1), now) is False


def test_fingerprint_is_stable_and_order_independent():
    a = {"x": 1, "y": {"b": 2, "a": 1}, "t": "2026-09-25T08:00:00"}
    b = {"t": "2026-09-25T08:00:00", "y": {"a": 1, "b": 2}, "x": 1}
    assert fingerprint_payload(a) == fingerprint_payload(b)
    assert fingerprint_payload(a) != fingerprint_payload({**a, "x": 2})
