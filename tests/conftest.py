import os
import tempfile

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@pytest.fixture()
def db_env():
    """每个测试使用独立的临时 SQLite 文件，便于多连接/线程并发与重启验证。"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    engine = create_engine(
        f"sqlite:///{path}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    import app.database as database
    from app import models  # noqa: F401  确保所有表已注册

    database.engine = engine
    database.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    database.Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = database.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    from main import app
    from app.database import get_db

    app.dependency_overrides[get_db] = override_get_db

    yield {
        "path": path,
        "engine": engine,
        "SessionLocal": database.SessionLocal,
    }

    app.dependency_overrides.clear()
    engine.dispose()
    os.remove(path)


@pytest.fixture()
def client(db_env):
    from main import app

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def seed_refs(db_env):
    """写入机型/场景/技能，返回其 ID。"""
    db = db_env["SessionLocal"]()
    from app.models import RobotModel, Scene, Skill

    robot = RobotModel(name="RM-TEST", manufacturer="Acme")
    scene = Scene(name="工位一", category="制造")
    skill = Skill(name="抓取", category="操作")
    db.add_all([robot, scene, skill])
    db.commit()
    ids = {"robot_model_id": robot.id, "scene_id": scene.id, "skill_id": skill.id}
    db.close()
    return ids


@pytest.fixture()
def make_payload(seed_refs):
    counter = {"n": 0}

    def _make(**overrides):
        counter["n"] += 1
        n = counter["n"]
        payload = {
            **seed_refs,
            "robot_serial": f"SN-{n}",
            "motion_trajectory": {"points": [n, n + 1]},
            "perception_records": {"frames": n},
            "grasp_result": {"ok": True},
            "timestamp_start": f"2026-01-01T0{n}:00:00",
            "timestamp_end": f"2026-01-01T0{n}:00:10",
            "duration_ms": 10000,
            "environment_conditions": {"temp": 25},
            "hardware_status": {"battery": 0.9},
        }
        payload.update(overrides)
        return payload

    return _make
