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
import routers.analysis as analysis_module

client = TestClient(app)


def _create_cpi_table():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = db.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for t, v in [("2023-Q1", 132.6), ("2023-Q2", 133.7), ("2023-Q3", 135.3), ("2023-Q4", 136.1)]:
        table.put_item(Item={
            "time_period": t,
            "region": "50",
            "obs_value": Decimal(str(v)),
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
def test_visualise_cpi_trend_returns_url(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.cpi_table = _create_cpi_table()

    resp = client.get("/visualise/trend/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200
    assert "url" in resp.json()


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_trend_sends_change_pct(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.cpi_table = _create_cpi_table()

    resp = client.get("/visualise/trend/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert body["yAxisTitle"] == "Change (%)"
    assert len(body["datasets"]) == 1
    assert body["datasets"][0]["datasetName"] == "CPI Change (%)"


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_trend_excludes_first_period(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.cpi_table = _create_cpi_table()

    resp = client.get("/visualise/trend/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    events = body["datasets"][0]["events"]
    assert len(events) == 3


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_trend_title_includes_summary(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.cpi_table = _create_cpi_table()

    resp = client.get("/visualise/trend/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert "CPI Trend" in body["title"]
    assert "growing" in body["title"]
    assert "avg" in body["title"]


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_trend_quarter_format(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.cpi_table = _create_cpi_table()

    resp = client.get("/visualise/trend/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    event = body["datasets"][0]["events"][0]
    assert event["time_object"]["timestamp"] == "2023-04-01 00:00:00.0000000"
    assert event["time_object"]["duration"] == 3
    assert event["time_object"]["duration_unit"] == "month"


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_cpi_trend_omega_error(mock_post):
    from requests.exceptions import HTTPError
    mock_post.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
    analysis_module.cpi_table = _create_cpi_table()

    resp = client.get("/visualise/trend/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 502