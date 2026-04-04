import os
import sys
from pathlib import Path

# Ensure app imports use SQLite during tests instead of default Postgres.
os.environ["DATABASE_URL"] = "sqlite://"
os.environ["ENABLE_SIP_SCHEDULER"] = "0"

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.main import app, get_db

TEST_ENGINE = create_engine(
    "sqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=TEST_ENGINE)


@pytest.fixture(autouse=True)
def reset_database():
    Base.metadata.drop_all(bind=TEST_ENGINE)
    Base.metadata.create_all(bind=TEST_ENGINE)
    yield


@pytest.fixture
def db_session():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def client():
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers(client):
    client.post(
        "/auth/signup",
        json={
            "username": "testuser",
            "email": "testuser@example.com",
            "password": "strongpass123",
        },
    )
    response = client.post(
        "/auth/login",
        json={
            "login": "testuser@example.com",
            "password": "strongpass123",
        },
    )
    token = response.json()["token"]
    return {"Authorization": f"Bearer {token}"}
