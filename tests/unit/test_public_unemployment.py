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
        "time_period": "2023-Q1",
        "year": "2023",
        "quarter": "Q1",
        "region": "AUS",
        "obs_value": Decimal("3.5"),
        "obs_status": "A",
        "freq": "M",
        "unit_measure": "PC",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2023-Q2",
        "year": "2023",
        "quarter": "Q2",
        "region": "AUS",
        "obs_value": Decimal("3.6"),
        "obs_status": "A",
        "freq": "M",
        "unit_measure": "PC",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2023-Q3",
        "year": "2023",
        "quarter": "Q3",
        "region": "AUS",
        "obs_value": Decimal("3.8"),
        "obs_status": "A",
        "freq": "M",
        "unit_measure": "PC",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2024-Q1",
        "year": "2024",
        "quarter": "Q1",
        "region": "AUS",
        "obs_value": Decimal("4.1"),
        "obs_status": "A",
        "freq": "M",
        "unit_measure": "PC",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/...",
        "time_period": "2024-Q2",
        "year": "2024",
        "quarter": "Q2",
        "region": "AUS",
        "obs_value": Decimal("4.0"),
        "obs_status": "A",
        "freq": "M",
        "unit_measure": "PC",
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
    public_module.unemployment_table = db.Table(UNEMPLOYMENT_TABLE_NAME)
    _create_table(db)

    resp = client.get("/public/unemployment?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200

    body = resp.json()
    assert len(body["events"]) == 3


@mock_aws
def test_get_unemployment_event_shape():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = db.Table(UNEMPLOYMENT_TABLE_NAME)
    _create_table(db)

    resp = client.get("/public/unemployment?start=2023-Q1&end=2023-Q1")
    assert resp.status_code == 200

    event = resp.json()["events"][0]
    assert event["year"] == "2023"
    assert event["quarter"] == "Q1"
    assert event["region"] == "AUS"
    assert event["unemployment_value"] == 3.5


@mock_aws
def test_get_unemployment_excludes_outside_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = db.Table(UNEMPLOYMENT_TABLE_NAME)
    _create_table(db)

    resp = client.get("/public/unemployment?start=2023-Q2&end=2023-Q3")
    assert resp.status_code == 200

    events = resp.json()["events"]
    assert len(events) == 2
    periods = [e["quarter"] for e in events]
    assert "Q2" in periods
    assert "Q3" in periods


@mock_aws
def test_get_unemployment_empty_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = db.Table(UNEMPLOYMENT_TABLE_NAME)
    _create_table(db)

    resp = client.get("/public/unemployment?start=2020-Q1&end=2020-Q4")
    assert resp.status_code == 200
    assert resp.json()["events"] == []


@mock_aws
def test_get_unemployment_404_when_table_empty():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.unemployment_table = db.Table(UNEMPLOYMENT_TABLE_NAME)
    # create table but don't insert any rows
    db.create_table(
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

    resp = client.get("/public/unemployment?start=2023-Q1&end=2024-Q4")
    assert resp.status_code == 200
    assert resp.json()["events"] == []


def test_get_unemployment_missing_params():
    resp = client.get("/public/unemployment")
    assert resp.status_code == 422


def test_get_unemployment_missing_end():
    resp = client.get("/public/unemployment?start=2023-Q1")
    assert resp.status_code == 422
