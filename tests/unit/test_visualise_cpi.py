import os
from decimal import Decimal
from unittest.mock import patch, MagicMock

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
os.environ["USERS_TABLE_NAME"] = "test-users-table"
os.environ["JWT_SECRET"] = "test-secret-key-that-is-long-enough-for-hs256"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from main import app
import routers.public as public_module

client = TestClient(app)

TABLE_NAME = "test-cpi-table"

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


def _mock_omega_response():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "url": "https://seng3031-charts.s3.amazonaws.com/images/fake-test-image.png"
    }
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_returns_url(mock_post):
    mock_post.return_value = _mock_omega_response()
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.cpi_table = _create_table(db)

    resp = client.get("/visualise/cpi?start=2023-Q1&end=2023-Q2")
    assert resp.status_code == 200
    assert "url" in resp.json()


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_sends_correct_data_to_omega(mock_post):
    mock_post.return_value = _mock_omega_response()
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.cpi_table = _create_table(db)

    resp = client.get("/visualise/cpi?start=2023-Q1&end=2023-Q2")
    assert resp.status_code == 200

    call_kwargs = mock_post.call_args
    body = call_kwargs.kwargs["json"] if "json" in call_kwargs.kwargs else call_kwargs[1]["json"]

    assert body["title"] == "CPI (2023-Q1 to 2023-Q2)"
    assert body["yAxisTitle"] == "CPI Value"
    assert len(body["datasets"]) == 1
    assert len(body["datasets"][0]["events"]) == 2


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_timestamp_format(mock_post):
    mock_post.return_value = _mock_omega_response()
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.cpi_table = _create_table(db)

    resp = client.get("/visualise/cpi?start=2023-Q1&end=2023-Q1")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"] if "json" in mock_post.call_args.kwargs else mock_post.call_args[1]["json"]
    event = body["datasets"][0]["events"][0]

    assert event["time_object"]["timestamp"] == "2023-01-01 00:00:00.0000000"
    assert event["attribute"]["value"] == 132.6


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_quarter_mapping(mock_post):
    mock_post.return_value = _mock_omega_response()
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.cpi_table = _create_table(db)

    resp = client.get("/visualise/cpi?start=2023-Q1&end=2023-Q2")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"] if "json" in mock_post.call_args.kwargs else mock_post.call_args[1]["json"]
    events = body["datasets"][0]["events"]

    timestamps = [e["time_object"]["timestamp"] for e in events]
    assert "2023-01-01 00:00:00.0000000" in timestamps
    assert "2023-04-01 00:00:00.0000000" in timestamps


def test_visualise_cpi_invalid_params():
    resp = client.get("/visualise/cpi?start=2023-01&end=2024-Q4")
    assert resp.status_code == 400


def test_visualise_cpi_missing_params():
    resp = client.get("/visualise/cpi")
    assert resp.status_code == 422


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_omega_error(mock_post):
    from requests.exceptions import HTTPError
    mock_post.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
    db = boto3.resource("dynamodb", region_name="us-east-1")
    public_module.cpi_table = _create_table(db)

    resp = client.get("/visualise/cpi?start=2023-Q1&end=2023-Q2")
    assert resp.status_code == 502