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

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

from main import app
import routers.public as public_module

client = TestClient(app)

CPI_ROWS = [
    {"time_period": "2023-Q1", "region": "50", "obs_value": Decimal("132.6")},
    {"time_period": "2023-Q2", "region": "50", "obs_value": Decimal("133.7")},
    {"time_period": "2023-Q3", "region": "50", "obs_value": Decimal("135.3")},
    {"time_period": "2023-Q4", "region": "50", "obs_value": Decimal("136.1")},
    {"time_period": "2024-Q1", "region": "50", "obs_value": Decimal("137.8")},
]

GDP_ROWS = [
    {"time_period": "2023-Q1", "region": "AUS", "obs_value": Decimal("520000")},
    {"time_period": "2023-Q2", "region": "AUS", "obs_value": Decimal("525000")},
    {"time_period": "2023-Q3", "region": "AUS", "obs_value": Decimal("530000")},
    {"time_period": "2023-Q4", "region": "AUS", "obs_value": Decimal("528000")},
    {"time_period": "2024-Q1", "region": "AUS", "obs_value": Decimal("535000")},
]


def _setup_tables(dynamodb):
    cpi = dynamodb.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    gdp = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in CPI_ROWS:
        cpi.put_item(Item=row)
    for row in GDP_ROWS:
        gdp.put_item(Item=row)
    public_module.cpi_table = cpi
    public_module.gdp_table = gdp


@mock_aws
def test_correlation_returns_correct_structure():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2024-Q1")
    assert response.status_code == 200

    import json
    print(json.dumps(response.json(), indent=2))

    body = response.json()
    assert body["analysis_type"] == "pearson_correlation"
    assert body["datasets"] == ["CPI", "GDP"]
    assert body["start"] == "2023-Q1"
    assert body["end"] == "2024-Q1"
    assert body["num_data_points"] == 5
    assert "correlation_coefficient" in body
    assert "interpretation" in body


@mock_aws
def test_correlation_coefficient_in_valid_range():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2024-Q1")
    assert response.status_code == 200

    r = response.json()["correlation_coefficient"]
    assert -1 <= r <= 1


@mock_aws
def test_correlation_strong_positive():
    # CPI and GDP both trend upward so should be strongly positively correlated
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2024-Q1")
    assert response.status_code == 200

    body = response.json()
    assert body["correlation_coefficient"] > 0.7
    assert body["interpretation"] == "strong positive"


@mock_aws
def test_correlation_filters_by_date_range():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2023-Q3")
    assert response.status_code == 200

    body = response.json()
    assert body["num_data_points"] == 3


@mock_aws
def test_correlation_missing_params():
    response = client.get("/public/analysis/cpi_gdp_correlation")
    assert response.status_code == 422


@mock_aws
def test_correlation_not_enough_overlapping_quarters():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)

    # range that only covers 1 quarter
    response = client.get("/public/analysis/cpi_gdp_correlation?start=2024-Q1&end=2024-Q1")
    assert response.status_code == 400
    assert "at least 2" in response.json()["detail"]


@mock_aws
def test_correlation_no_cpi_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    cpi = dynamodb.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    gdp = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in GDP_ROWS:
        gdp.put_item(Item=row)
    public_module.cpi_table = cpi
    public_module.gdp_table = gdp

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2024-Q1")
    assert response.status_code == 404
    assert "No CPI data found" in response.json()["detail"]


@mock_aws
def test_correlation_no_gdp_data():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    cpi = dynamodb.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    gdp = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[{"AttributeName": "time_period", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    for row in CPI_ROWS:
        cpi.put_item(Item=row)
    public_module.cpi_table = cpi
    public_module.gdp_table = gdp

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2024-Q1")
    assert response.status_code == 404
    assert "No GDP data found" in response.json()["detail"]


@mock_aws
def test_correlation_returns_valid_coefficient():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_tables(dynamodb)

    response = client.get("/public/analysis/cpi_gdp_correlation?start=2023-Q1&end=2023-Q2")
    assert response.status_code == 200

    body = response.json()
    assert body["num_data_points"] == 2
    assert body["start"] == "2023-Q1"
    assert body["end"] == "2023-Q2"
    assert -1 <= body["correlation_coefficient"] <= 1
