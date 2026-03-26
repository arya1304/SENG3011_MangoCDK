import os
from decimal import Decimal

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

# fake AWS credentials for moto
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BUCKET_NAME"] = "test-bucket"
os.environ["TABLE_NAME"] = "test-table"
os.environ["CPI_TABLE_NAME"] = "test-cpi-table"
os.environ["UNEMPLOYMENT_TABLE_NAME"] = "test-unemployment-table"
os.environ["GDP_TABLE_NAME"] = "test-gdp-table"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from main import app
import routers.public as public_module

client = TestClient(app)

UNEMPLOYMENT_TABLE_NAME = "test-unemployment-table"

MOCK_ROWS = [
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2023-01",
        "year": "2023",
        "region": "AUS",
        "obs_value": Decimal("3.5"),
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2023-02",
        "year": "2023",
        "region": "AUS",
        "obs_value": Decimal("3.6"),
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2023-03",
        "year": "2023",
        "region": "AUS",
        "obs_value": Decimal("3.8"),
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2024-01",
        "year": "2024",
        "region": "AUS",
        "obs_value": Decimal("4.1"),
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2024-06",
        "year": "2024",
        "region": "AUS",
        "obs_value": Decimal("4.0"),
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
]


def _create_table(db):
    table = db.create_table(
        TableName=UNEMPLOYMENT_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "dataset_id", "KeyType": "HASH"},
            {"AttributeName": "time_period", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "dataset_id", "AttributeType": "S"},
            {"AttributeName": "time_period", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in MOCK_ROWS:
        table.put_item(Item=row)
    return table


@mock_aws
def test_get_unemployment_returns_events_in_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = _create_table(db)

    resp = client.get("/public/unemployment?start=2023-01&end=2023-03")
    assert resp.status_code == 200

    body = resp.json()
    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert body["dataset_id"] == "ABS:LF"
    assert body["query"] == {"start": "2023-01", "end": "2023-03"}
    assert body["count"] == 3
    assert "timestamp" in body
    assert len(body["events"]) == 3


@mock_aws
def test_get_unemployment_event_shape():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = _create_table(db)

    resp = client.get("/public/unemployment?start=2023-01&end=2023-01")
    assert resp.status_code == 200

    event = resp.json()["events"][0]
    assert event["time_period"] == "2023-01"
    assert event["year"] == "2023"
    assert event["month"] == "01"
    assert event["region"] == "AUS"
    assert event["unemployment_value"] == 3.5


@mock_aws
def test_get_unemployment_excludes_outside_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = _create_table(db)

    resp = client.get("/public/unemployment?start=2023-02&end=2023-03")
    assert resp.status_code == 200

    body = resp.json()
    assert body["count"] == 2
    months = [e["month"] for e in body["events"]]
    assert "02" in months
    assert "03" in months


@mock_aws
def test_get_unemployment_empty_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = _create_table(db)

    resp = client.get("/public/unemployment?start=2020-01&end=2020-12")
    assert resp.status_code == 200
    assert resp.json()["events"] == []
    assert resp.json()["count"] == 0


@mock_aws
def test_get_unemployment_empty_table():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = db.create_table(
        TableName=UNEMPLOYMENT_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "dataset_id", "KeyType": "HASH"},
            {"AttributeName": "time_period", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "dataset_id", "AttributeType": "S"},
            {"AttributeName": "time_period", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    public_module.unemployment_table = table

    resp = client.get("/public/unemployment?start=2023-01&end=2024-12")
    assert resp.status_code == 200
    assert resp.json()["events"] == []
    assert resp.json()["count"] == 0


def test_get_unemployment_missing_params():
    resp = client.get("/public/unemployment")
    assert resp.status_code == 422


def test_get_unemployment_missing_end():
    resp = client.get("/public/unemployment?start=2023-01")
    assert resp.status_code == 422


def test_get_unemployment_start_after_end():
    resp = client.get("/public/unemployment?start=2024-12&end=2023-01")
    assert resp.status_code == 400
    assert "start must not be after end" in resp.json()["detail"]


def test_get_unemployment_invalid_start_format():
    resp = client.get("/public/unemployment?start=2023-Q1&end=2024-12")
    assert resp.status_code == 400
    assert "start" in resp.json()["detail"]


def test_get_unemployment_invalid_end_format():
    resp = client.get("/public/unemployment?start=2023-01&end=2024-4")
    assert resp.status_code == 400
    assert "end" in resp.json()["detail"]
