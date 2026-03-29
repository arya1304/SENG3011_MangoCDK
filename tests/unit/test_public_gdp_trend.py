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

GDP_ROWS = [
    {"time_period": "2023-Q1", "region": "AUS", "obs_value": Decimal("48016")},
    {"time_period": "2023-Q2", "region": "AUS", "obs_value": Decimal("48500")},
    {"time_period": "2023-Q3", "region": "AUS", "obs_value": Decimal("49100")},
    {"time_period": "2023-Q4", "region": "AUS", "obs_value": Decimal("49800")},
    {"time_period": "2024-Q1", "region": "AUS", "obs_value": Decimal("50200")},
]


def _setup_table(dynamodb):
    table = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in GDP_ROWS:
        table.put_item(Item=row)
    analysis_module.gdp_table = table
    return table


@mock_aws
def test_gdp_trend_returns_correct_structure():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/gdp")
    assert response.status_code == 200

    body = response.json()
    assert body["analysis_type"] == "gdp_trend"
    assert body["dataset"] == "gdp"
    assert "trend" in body
    assert "summary" in body
    assert body["summary"]["total_periods"] == 5


@mock_aws
def test_gdp_trend_growing():
    """upward trend, so overall direction should be growing"""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/gdp")
    assert response.status_code == 200

    body = response.json()
    assert body["summary"]["overall_direction"] == "growing"
    assert body["summary"]["periods_growing"] > body["summary"]["periods_shrinking"]


@mock_aws
def test_gdp_trend_period_values():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/gdp")
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
def test_gdp_trend_filters_by_date_range():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    response = client.get("/public/analysis/trend/gdp?start=2023-Q2&end=2023-Q4")
    assert response.status_code == 200

    body = response.json()
    assert body["summary"]["total_periods"] == 3
    assert body["start"] == "2023-Q2"
    assert body["end"] == "2023-Q4"


@mock_aws
def test_gdp_trend_filters_by_region():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    # region "AUS" exists
    response = client.get("/public/analysis/trend/gdp?region=AUS")
    assert response.status_code == 200
    assert response.json()["summary"]["total_periods"] == 5

    # region "99" does not exist
    response = client.get("/public/analysis/trend/gdp?region=99")
    assert response.status_code == 400
    assert "At least 2" in response.json()["detail"]


@mock_aws
def test_gdp_trend_not_enough_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_table(dynamodb)

    # only 1 quarter in range
    response = client.get("/public/analysis/trend/gdp?start=2024-Q1&end=2024-Q1")
    assert response.status_code == 400
    assert "At least 2" in response.json()["detail"]


@mock_aws
def test_gdp_trend_no_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    analysis_module.gdp_table = table

    response = client.get("/public/analysis/trend/gdp")
    assert response.status_code == 404
    assert "No GDP data found" in response.json()["detail"]
