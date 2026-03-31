import os

os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BUCKET_NAME"] = "test-bucket"
os.environ["CPI_TABLE_NAME"] = "test-cpi-table"
os.environ["UNEMPLOYMENT_TABLE_NAME"] = "test-unemployment-table"
os.environ["GDP_TABLE_NAME"] = "test-gdp-table"
os.environ["USERS_TABLE_NAME"] = "test-users-table"
os.environ["JWT_SECRET"] = "test-secret-key-that-is-long-enough-for-hs256"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

import boto3
import jwt
from moto import mock_aws
from fastapi.testclient import TestClient
from main import app
import routers.auth as auth_module

client = TestClient(app)

USERS_TABLE_NAME = "test-users-table"


def _create_users_table(db):
    table = db.create_table(
        TableName=USERS_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "email", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return table


@mock_aws
def test_register_success():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    resp = client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })
    assert resp.status_code == 200

    body = resp.json()
    assert body["message"] == "User registered successfully"
    assert body["user"]["email"] == "test@example.com"
    assert body["user"]["name"] == "Test User"
    assert "token" in body


@mock_aws
def test_register_duplicate_email():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = _create_users_table(db)
    auth_module.users_table = table

    client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })

    resp = client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "other",
        "name": "Other",
    })
    assert resp.status_code == 409
    assert "already registered" in resp.json()["detail"]


@mock_aws
def test_register_invalid_email():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    resp = client.post("/auth/register", json={
        "email": "not-an-email",
        "password": "password123",
        "name": "Test User",
    })
    assert resp.status_code == 422


@mock_aws
def test_login_success():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })

    resp = client.post("/auth/login", json={
        "email": "test@example.com",
        "password": "password123",
    })
    assert resp.status_code == 200

    body = resp.json()
    assert body["message"] == "Login successful."
    assert body["user"]["email"] == "test@example.com"
    assert "token" in body


@mock_aws
def test_login_wrong_password():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })

    resp = client.post("/auth/login", json={
        "email": "test@example.com",
        "password": "wrongpassword",
    })
    assert resp.status_code == 401
    assert "Invalid email or password" in resp.json()["detail"]


@mock_aws
def test_login_nonexistent_user():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    resp = client.post("/auth/login", json={
        "email": "nobody@example.com",
        "password": "password123",
    })
    assert resp.status_code == 401
    assert "Invalid email or password" in resp.json()["detail"]


@mock_aws
def test_me_with_valid_token():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    register_resp = client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })
    token = register_resp.json()["token"]

    resp = client.get("/auth/details", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert resp.json()["user"]["email"] == "test@example.com"
    assert resp.json()["user"]["name"] == "Test User"


def test_me_without_token():
    resp = client.get("/auth/details")
    assert resp.status_code == 403


def test_me_with_invalid_token():
    resp = client.get("/auth/details", headers={"Authorization": "Bearer invalid.token.here"})
    assert resp.status_code == 401
    assert "Invalid token" in resp.json()["detail"]


def test_me_with_expired_token():
    from datetime import datetime, timezone, timedelta
    payload = {
        "sub": "test@example.com",
        "name": "Test User",
        "iat": datetime.now(timezone.utc) - timedelta(hours=48),
        "exp": datetime.now(timezone.utc) - timedelta(hours=24),
    }
    expired_token = jwt.encode(payload, "test-secret-key-that-is-long-enough-for-hs256", algorithm="HS256")

    resp = client.get("/auth/details", headers={"Authorization": f"Bearer {expired_token}"})
    assert resp.status_code == 401
    assert "expired" in resp.json()["detail"]


@mock_aws
def test_update_name():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    register_resp = client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })
    token = register_resp.json()["token"]

    resp = client.put("/auth/update",
        json={"name": "New Name"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    assert resp.json()["user"]["name"] == "New Name"
    assert "token" in resp.json()


@mock_aws
def test_update_password():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "oldpassword",
        "name": "Test User",
    })

    login_resp = client.post("/auth/login", json={
        "email": "test@example.com",
        "password": "oldpassword",
    })
    token = login_resp.json()["token"]

    resp = client.put("/auth/update",
        json={"password": "newpassword"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200

    # old password should no longer work
    resp = client.post("/auth/login", json={
        "email": "test@example.com",
        "password": "oldpassword",
    })
    assert resp.status_code == 401

    # new password should work
    resp = client.post("/auth/login", json={
        "email": "test@example.com",
        "password": "newpassword",
    })
    assert resp.status_code == 200


@mock_aws
def test_update_nothing():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    auth_module.users_table = _create_users_table(db)

    register_resp = client.post("/auth/register", json={
        "email": "test@example.com",
        "password": "password123",
        "name": "Test User",
    })
    token = register_resp.json()["token"]

    resp = client.put("/auth/update",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 400
    assert "Nothing to update" in resp.json()["detail"]


def test_update_without_token():
    resp = client.put("/auth/update", json={"name": "New Name"})
    assert resp.status_code == 403
