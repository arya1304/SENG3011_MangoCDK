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


def _create_gdp_table():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = db.create_table(
        TableName="test-gdp-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for t, v in [("2023-Q1", 520000), ("2023-Q2", 525000), ("2023-Q3", 530000), ("2023-Q4", 528000)]:
        table.put_item(Item={
            "time_period": t,
            "region": "AUS",
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
def test_visualise_gdp_trend_returns_url(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.gdp_table = _create_gdp_table()

    resp = client.get("/visualise/trend/gdp?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200
    assert "url" in resp.json()


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_trend_dataset_name(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.gdp_table = _create_gdp_table()

    resp = client.get("/visualise/trend/gdp?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert body["datasets"][0]["datasetName"] == "GDP Change (%)"
    assert body["yAxisTitle"] == "Change (%)"


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_trend_title(mock_post):
    mock_post.return_value = _mock_omega_response()
    analysis_module.gdp_table = _create_gdp_table()

    resp = client.get("/visualise/trend/gdp?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert "GDP Trend" in body["title"]


@mock_aws
@patch("routers.visualise.requests.post")
def test_visualise_gdp_trend_omega_error(mock_post):
    from requests.exceptions import HTTPError
    mock_post.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
    analysis_module.gdp_table = _create_gdp_table()

    resp = client.get("/visualise/trend/gdp?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 502