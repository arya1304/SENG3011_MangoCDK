import os
import sys
from decimal import Decimal
from unittest.mock import patch, MagicMock

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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient
from main import app
import routers.public as public_module

client = TestClient(app)


def _create_unemployment_table():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = db.create_table(
        TableName="test-unemployment-table",
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
    for t, v in [("2023-01", 3.7), ("2023-02", 3.5), ("2023-03", 3.6)]:
        table.put_item(Item={
            "dataset_id": "ABS:LF",
            "time_period": t,
            "year": "2023",
            "region": "AUS",
            "obs_value": Decimal(str(v)),
            "obs_status": "A",
            "freq": "M",
            "unit_measure": "PERCENT",
            "data_source": "Australian Bureau of Statistics (ABS)",
        })
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
def test_visualise_unemployment_returns_url(mock_post):
    mock_post.return_value = _mock_omega_response()
    public_module.unemployment_table = _create_unemployment_table()

    resp = client.get("/visualise/unemployment?start=2023-01&end=2023-03")
    assert resp.status_code == 200
    assert "url" in resp.json()


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_unemployment_sends_correct_data_to_omega(mock_post):
    mock_post.return_value = _mock_omega_response()
    public_module.unemployment_table = _create_unemployment_table()

    resp = client.get("/visualise/unemployment?start=2023-01&end=2023-03")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert body["title"] == "Unemployment (2023-01 to 2023-03)"
    assert body["yAxisTitle"] == "Unemployment Rate"
    assert len(body["datasets"]) == 1
    assert len(body["datasets"][0]["events"]) == 3


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_unemployment_timestamp_format(mock_post):
    mock_post.return_value = _mock_omega_response()
    public_module.unemployment_table = _create_unemployment_table()

    resp = client.get("/visualise/unemployment?start=2023-01&end=2023-01")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    event = body["datasets"][0]["events"][0]
    assert event["time_object"]["timestamp"] == "2023-01-01 00:00:00.0000000"
    assert event["time_object"]["duration"] == 1
    assert event["time_object"]["duration_unit"] == "month"
    assert event["attribute"]["value"] == 3.7


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_unemployment_month_mapping(mock_post):
    mock_post.return_value = _mock_omega_response()
    public_module.unemployment_table = _create_unemployment_table()

    resp = client.get("/visualise/unemployment?start=2023-01&end=2023-03")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    timestamps = [e["time_object"]["timestamp"] for e in body["datasets"][0]["events"]]
    assert "2023-01-01 00:00:00.0000000" in timestamps
    assert "2023-02-01 00:00:00.0000000" in timestamps
    assert "2023-03-01 00:00:00.0000000" in timestamps


def test_visualise_unemployment_invalid_params():
    resp = client.get("/visualise/unemployment?start=2023-Q1&end=2024-Q4")
    assert resp.status_code == 400


def test_visualise_unemployment_missing_params():
    resp = client.get("/visualise/unemployment")
    assert resp.status_code == 422


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_unemployment_omega_error(mock_post):
    from requests.exceptions import HTTPError
    mock_post.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
    public_module.unemployment_table = _create_unemployment_table()

    resp = client.get("/visualise/unemployment?start=2023-01&end=2023-03")
    assert resp.status_code == 502