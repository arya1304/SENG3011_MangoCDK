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


def create_dynamo_table():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[
            {"AttributeName": "dataset_id", "KeyType": "HASH"},
            {"AttributeName": "time_period", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "dataset_id", "AttributeType": "S"},
            {"AttributeName": "time_period", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    return table


def _populate_gdp(table):
    for t, v in [("2023-Q1", 48016), ("2023-Q2", 49200), ("2023-Q3", 50100)]:
        table.put_item(Item={
            "dataset_id": "ABS:ANA_IND_GVA(1.0.0)",
            "time_period": t,
            "data_source": "source",
            "data_item": "GPM",
            "adjustment_type": "20",
            "industry": "TOTAL",
            "region": "AUS",
            "obs_value": Decimal(str(v)),
            "obs_status": None,
            "freq": "Q",
        })


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
def test_visualise_gdp_returns_url(mock_post):
    mock_post.return_value = _mock_omega_response()
    table = create_dynamo_table()
    public_module.gdp_table = table
    _populate_gdp(table)

    resp = client.get("/visualise/gdp?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200
    assert "url" in resp.json()


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_sends_correct_data_to_omega(mock_post):
    mock_post.return_value = _mock_omega_response()
    table = create_dynamo_table()
    public_module.gdp_table = table
    _populate_gdp(table)

    resp = client.get("/visualise/gdp?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert body["title"] == "GDP (2023-Q1 to 2023-Q3)"
    assert body["yAxisTitle"] == "GDP Value"
    assert len(body["datasets"]) == 1
    assert len(body["datasets"][0]["events"]) == 3


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_timestamp_format(mock_post):
    mock_post.return_value = _mock_omega_response()
    table = create_dynamo_table()
    public_module.gdp_table = table
    _populate_gdp(table)

    resp = client.get("/visualise/gdp?start=2023-Q1&end=2023-Q1")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    event = body["datasets"][0]["events"][0]
    assert event["time_object"]["timestamp"] == "2023-01-01 00:00:00.0000000"
    assert event["attribute"]["value"] == 48016.0


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_quarter_mapping(mock_post):
    mock_post.return_value = _mock_omega_response()
    table = create_dynamo_table()
    public_module.gdp_table = table
    _populate_gdp(table)

    resp = client.get("/visualise/gdp?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    timestamps = [e["time_object"]["timestamp"] for e in body["datasets"][0]["events"]]
    assert "2023-01-01 00:00:00.0000000" in timestamps
    assert "2023-04-01 00:00:00.0000000" in timestamps
    assert "2023-07-01 00:00:00.0000000" in timestamps


def test_visualise_gdp_invalid_params():
    resp = client.get("/visualise/gdp?start=2023-01&end=2024-Q4")
    assert resp.status_code == 400


def test_visualise_gdp_missing_params():
    resp = client.get("/visualise/gdp")
    assert resp.status_code == 422


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_omega_error(mock_post):
    from requests.exceptions import HTTPError
    mock_post.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
    table = create_dynamo_table()
    public_module.gdp_table = table
    _populate_gdp(table)

    resp = client.get("/visualise/gdp?start=2023-Q1&end=2023-Q2")
    assert resp.status_code == 502