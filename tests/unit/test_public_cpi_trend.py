import os
from decimal import Decimal

# fake aws credentials for testing
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BUCKET_NAME"] = "test-bucket"
os.environ["CPI_TABLE_NAME"] = "test-cpi-table"
os.environ["UNEMPLOYMENT_TABLE_NAME"] = "test-unemployment-table"
os.environ["GDP_TABLE_NAME"] = "test-gdp-table"
os.environ["USERS_TABLE_NAME"] = "test-users-table"
os.environ["JWT_SECRET"] = "test-secret-key-that-is-long-enough-for-hs256"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

from main import app
import routers.analysis as analysis_module

client = TestClient(app)

CPI_ROWS = [
    {"time_period": "2023-Q1", "region": "50", "obs_value": Decimal("132.6")},
    {"time_period": "2023-Q2", "region": "50", "obs_value": Decimal("133.7")},
    {"time_period": "2023-Q3", "region": "50", "obs_value": Decimal("135.3")},
    {"time_period": "2023-Q4", "region": "50", "obs_value": Decimal("136.1")},
    {"time_period": "2024-Q1", "region": "50", "obs_value": Decimal("137.8")},
]

def _setup_tables(dynamodb):
    cpi = dynamodb.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in CPI_ROWS:
        cpi.put_item(Item=row)
    analysis_module.cpi_table = cpi

@mock_aws
def test_cpi_trend_returns_correct_structure():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)
 
    response = client.get("/public/analysis/trend/cpi")
    assert response.status_code == 200
 
    body = response.json()
    assert body["dataset"] == "cpi"
    assert "trend" in body
    assert "summary" in body
    assert body["summary"]["total_periods"] == 5
 
 
@mock_aws
def test_cpi_trend_growing():
    """CPI data trends upward, so overall direction should be growing."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)
 
    response = client.get("/public/analysis/trend/cpi")
    assert response.status_code == 200
 
    body = response.json()
    assert body["summary"]["overall_direction"] == "growing"
    assert body["summary"]["periods_growing"] > body["summary"]["periods_shrinking"]
 
 
@mock_aws
def test_cpi_trend_period_values():
    """Check that each period has the expected fields."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)
 
    response = client.get("/public/analysis/trend/cpi")
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
def test_cpi_trend_filters_by_date_range():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)
 
    response = client.get("/public/analysis/trend/cpi?start=2023-Q2&end=2023-Q4")
    assert response.status_code == 200
 
    body = response.json()
    assert body["summary"]["total_periods"] == 3
    assert body["start"] == "2023-Q2"
    assert body["end"] == "2023-Q4"
 
 
@mock_aws
def test_cpi_trend_filters_by_region():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)
 
    # region "50" exists
    response = client.get("/public/analysis/trend/cpi?region=50")
    assert response.status_code == 200
    assert response.json()["summary"]["total_periods"] == 5
 
    # region "99" does not exist, so should return 400 since we require at least 2 data points to calculate trend
    response = client.get("/public/analysis/trend/cpi?region=99")
    assert response.status_code == 400
    assert "At least 2" in response.json()["detail"]
 
 
@mock_aws
def test_cpi_trend_not_enough_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)
 
    # only 1 quarter in range
    response = client.get("/public/analysis/trend/cpi?start=2024-Q1&end=2024-Q1")
    assert response.status_code == 400
    assert "At least 2" in response.json()["detail"]
 
 
@mock_aws
def test_cpi_trend_no_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    cpi = dynamodb.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    analysis_module.cpi_table = cpi
 
    response = client.get("/public/analysis/trend/cpi")
    assert response.status_code == 404
    assert "No CPI data found" in response.json()["detail"]
 
 