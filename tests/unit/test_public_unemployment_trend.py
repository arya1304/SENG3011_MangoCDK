import os
from decimal import Decimal

os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BUCKET_NAME"] = "test-bucket"
os.environ["CPI_TABLE_NAME"] = "test-cpi-table"
os.environ["UNEMPLOYMENT_TABLE_NAME"] = "test-unemployment-table"
os.environ["GDP_TABLE_NAME"] = "test-gdp-table"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

from main import app
import routers.analysis as analysis_module

client = TestClient(app)

UNEMPLOYMENT_ROWS = [
    {"time_period": "2023-01", "region": "AUS", "obs_value": Decimal("3.5")},
    {"time_period": "2023-02", "region": "AUS", "obs_value": Decimal("3.6")},
    {"time_period": "2023-03", "region": "AUS", "obs_value": Decimal("3.8")},
    {"time_period": "2023-04", "region": "AUS", "obs_value": Decimal("3.7")},
    {"time_period": "2023-05", "region": "AUS", "obs_value": Decimal("3.9")},
]


def _setup_table(dynamodb):
    table = dynamodb.create_table(
        TableName="test-unemployment-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in UNEMPLOYMENT_ROWS:
        table.put_item(Item=row)
    analysis_module.unemployment_table = table
    return table


@mock_aws
def test_unemployment_trend_returns_correct_structure():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/unemployment")
    assert response.status_code == 200

    body = response.json()
    assert body["analysis_type"] == "unemployment_trend"
    assert body["dataset"] == "unemployment"
    assert "trend" in body
    assert "summary" in body
    assert body["summary"]["total_periods"] == 5


@mock_aws
def test_unemployment_trend_growing():
    """upward trend overall, so direction should be growing."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/unemployment")
    assert response.status_code == 200

    body = response.json()
    assert body["summary"]["overall_direction"] == "growing"
    assert body["summary"]["periods_growing"] > body["summary"]["periods_shrinking"]


@mock_aws
def test_unemployment_trend_period_values():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/unemployment")
    assert response.status_code == 200

    periods = response.json()["trend"]

    # first period has no change
    assert periods[0]["change"] is None
    assert periods[0]["direction"] is None

    # subsequent periods have change values
    for p in periods[1:]:
        assert p["change"] is not None
        assert p["change_pct"] is not None
        assert p["direction"] in ["growing", "shrinking", "stable"]


@mock_aws
def test_unemployment_trend_filters_by_date_range():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/unemployment?start=2023-02&end=2023-04")
    assert response.status_code == 200

    body = response.json()
    assert body["summary"]["total_periods"] == 3
    assert body["start"] == "2023-02"
    assert body["end"] == "2023-04"


@mock_aws
def test_unemployment_trend_filters_by_region():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    # region "AUS" exists
    response = client.get("/public/analysis/trend/unemployment?region=AUS")
    assert response.status_code == 200
    assert response.json()["summary"]["total_periods"] == 5

    # region "99" does not exist
    response = client.get("/public/analysis/trend/unemployment?region=99")
    assert response.status_code == 400
    assert "At least 2" in response.json()["detail"]


@mock_aws
def test_unemployment_trend_not_enough_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    # only 1 month in range
    response = client.get("/public/analysis/trend/unemployment?start=2023-05&end=2023-05")
    assert response.status_code == 400
    assert "At least 2" in response.json()["detail"]


@mock_aws
def test_unemployment_trend_no_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.create_table(
        TableName="test-unemployment-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    analysis_module.unemployment_table = table

    response = client.get("/public/analysis/trend/unemployment")
    assert response.status_code == 404
    assert "No unemployment data found" in response.json()["detail"]
