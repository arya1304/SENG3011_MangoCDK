import os
from decimal import Decimal

import boto3
import pytest
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

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from main import app
import routers.public as public_module

client = TestClient(app)

TABLE_NAME = "test-table"

# Rows matching the real DynamoDB structure observed in AWS console
MOCK_ROWS = [
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q",
        "time_period": "2023-Q1",
        "year": "2023",
        "quarter": "Q1",
        "region": "50",
        "obs_value": Decimal("132.6"),
        "obs_status": "A",
        "freq": "Q",
        "unit_measure": "IDX",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q",
        "time_period": "2023-Q2",
        "year": "2023",
        "quarter": "Q2",
        "region": "50",
        "obs_value": Decimal("133.7"),
        "obs_status": "A",
        "freq": "Q",
        "unit_measure": "IDX",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q",
        "time_period": "2023-Q3",
        "year": "2023",
        "quarter": "Q3",
        "region": "50",
        "obs_value": Decimal("135.3"),
        "obs_status": "A",
        "freq": "Q",
        "unit_measure": "IDX",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q",
        "time_period": "2023-Q4",
        "year": "2023",
        "quarter": "Q4",
        "region": "50",
        "obs_value": Decimal("136.1"),
        "obs_status": "A",
        "freq": "Q",
        "unit_measure": "IDX",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
    {
        "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q",
        "time_period": "2024-Q1",
        "year": "2024",
        "quarter": "Q1",
        "region": "50",
        "obs_value": Decimal("137.8"),
        "obs_status": "A",
        "freq": "Q",
        "unit_measure": "IDX",
        "data_source": "Australian Bureau of Statistics (ABS)",
    },
]


def _create_table(db):
    table = db.create_table(
        TableName=TABLE_NAME,
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
def test_get_cpi_returns_events_in_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.dynamodb = db
    _create_table(db)

    resp = client.get("/public/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = resp.json()
    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert len(body["events"]) == 4


@mock_aws
def test_get_cpi_events_sorted_by_time_period():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.dynamodb = db
    _create_table(db)

    resp = client.get("/public/cpi?start=2023-Q1&end=2024-Q1")
    assert resp.status_code == 200

    periods = [e["time_object"]["timestamp"] for e in resp.json()["events"]]
    assert periods == sorted(periods)


@mock_aws
def test_get_cpi_excludes_outside_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.dynamodb = db
    _create_table(db)

    # only 2023-Q2 and 2023-Q3 fall inside this range
    resp = client.get("/public/cpi?start=2023-Q2&end=2023-Q3")
    assert resp.status_code == 200

    periods = [e["time_object"]["timestamp"] for e in resp.json()["events"]]
    assert periods == ["2023-Q2", "2023-Q3"]


@mock_aws
def test_get_cpi_event_shape():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.dynamodb = db
    _create_table(db)

    resp = client.get("/public/cpi?start=2023-Q1&end=2023-Q1")
    assert resp.status_code == 200

    event = resp.json()["events"][0]
    assert event["event_type"] == "cpi_observation"
    assert event["time_object"]["timestamp"] == "2023-Q1"
    assert event["time_object"]["duration"] == 1
    assert event["time_object"]["duration_unit"] == "quarter"
    assert event["attribute"]["obs_value"] == 132.6
    assert event["attribute"]["region"] == "50"
    assert event["attribute"]["freq"] == "Q"


@mock_aws
def test_get_cpi_empty_range():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.dynamodb = db
    _create_table(db)

    # no data exists for 2020
    resp = client.get("/public/cpi?start=2020-Q1&end=2020-Q4")
    assert resp.status_code == 200
    assert resp.json()["events"] == []


def test_get_cpi_invalid_start_format():
    resp = client.get("/public/cpi?start=2023-01&end=2024-Q4")
    assert resp.status_code == 400
    assert "start" in resp.json()["detail"]


def test_get_cpi_invalid_end_format():
    resp = client.get("/public/cpi?start=2023-Q1&end=2024-4")
    assert resp.status_code == 400
    assert "end" in resp.json()["detail"]


def test_get_cpi_start_after_end():
    resp = client.get("/public/cpi?start=2024-Q4&end=2023-Q1")
    assert resp.status_code == 400


def test_get_cpi_missing_params():
    resp = client.get("/public/cpi")
    assert resp.status_code == 422


def test_get_cpi_missing_end():
    resp = client.get("/public/cpi?start=2023-Q1")
    assert resp.status_code == 422


def test_get_cpi_missing_table_name(monkeypatch):
    import routers.public as public_module
    monkeypatch.setattr(public_module, "TABLE_NAME", None)
    resp = client.get("/public/cpi?start=2023-Q1&end=2024-Q4")
    assert resp.status_code == 500
    assert "TABLE_NAME" in resp.json()["detail"]
