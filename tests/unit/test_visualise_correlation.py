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

def _create_cpi_table():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = db.create_table(
        TableName="test-cpi-table",
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
    for t, v in [("2023-Q1", 100.0), ("2023-Q2", 102.0), ("2023-Q3", 104.0)]:
        table.put_item(Item={
            "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q",
            "time_period": t,
            "year": t.split("-")[0],
            "quarter": t.split("-")[1],
            "region": "50",
            "obs_value": Decimal(str(v)),
            "obs_status": "A",
            "freq": "Q",
            "unit_measure": "IDX",
            "data_source": "Australian Bureau of Statistics (ABS)",
        })
    return table


def _create_gdp_table():
    db = boto3.resource("dynamodb", region_name="us-east-1")
    table = db.create_table(
        TableName="test-gdp-table",
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
    for t, v in [("2023-Q1", 50000.0), ("2023-Q2", 51000.0), ("2023-Q3", 52000.0)]:
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
    return table


def _setup_tables():
    cpi = _create_cpi_table()
    gdp = _create_gdp_table()
    public_module.cpi_table = cpi
    public_module.gdp_table = gdp

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
def test_correlation_returns_url(mock_post):
    mock_post.return_value = _mock_omega_response()
    _setup_tables()

    resp = client.get("/visualise/cpi-gdp-correlation?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200
    assert "url" in resp.json()


@mock_aws
@patch("routers.visualise.requests.post")
def test_correlation_sends_two_datasets(mock_post):
    mock_post.return_value = _mock_omega_response()
    _setup_tables()

    resp = client.get("/visualise/cpi-gdp-correlation?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert len(body["datasets"]) == 2
    dataset_names = [d["datasetName"] for d in body["datasets"]]
    assert "CPI" in dataset_names
    assert "GDP" in dataset_names


@mock_aws
@patch("routers.visualise.requests.post")
def test_correlation_normalization(mock_post):
    mock_post.return_value = _mock_omega_response()
    _setup_tables()

    resp = client.get("/visualise/cpi-gdp-correlation?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    for dataset in body["datasets"]:
        first_value = dataset["events"][0]["attribute"]["value"]
        assert first_value == 100.0


@mock_aws
@patch("routers.visualise.requests.post")
def test_correlation_title_includes_coefficient(mock_post):
    mock_post.return_value = _mock_omega_response()
    _setup_tables()

    resp = client.get("/visualise/cpi-gdp-correlation?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 200

    body = mock_post.call_args.kwargs["json"]
    assert "CPI vs GDP" in body["title"]
    assert "Correlation:" in body["title"]
    assert "strong positive" in body["title"]
    assert body["yAxisTitle"] == "Index (Base = 100)"


def test_correlation_missing_params():
    resp = client.get("/visualise/cpi-gdp-correlation")
    assert resp.status_code == 422


@mock_aws
@patch("routers.visualise.requests.post")
def test_correlation_omega_error(mock_post):
    from requests.exceptions import HTTPError
    mock_post.return_value.raise_for_status.side_effect = HTTPError("500 Server Error")
    _setup_tables()

    resp = client.get("/visualise/cpi-gdp-correlation?start=2023-Q1&end=2023-Q3")
    assert resp.status_code == 502