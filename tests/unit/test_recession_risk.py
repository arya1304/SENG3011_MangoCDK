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

# CPI stable
CPI_STABLE = [
    {"dataset_id": "ABS:CPI", "time_period": "2024-Q2", "obs_value": Decimal("132.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2024-Q3", "obs_value": Decimal("132.2")},
    {"dataset_id": "ABS:CPI", "time_period": "2024-Q4", "obs_value": Decimal("132.3")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q1", "obs_value": Decimal("132.5")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q2", "obs_value": Decimal("132.6")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q3", "obs_value": Decimal("132.8")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q4", "obs_value": Decimal("132.9")},
    {"dataset_id": "ABS:CPI", "time_period": "2026-Q1", "obs_value": Decimal("133.0")},
]

# CPI rising fast (high inflation)
CPI_RISING = [
    {"dataset_id": "ABS:CPI", "time_period": "2024-Q2", "obs_value": Decimal("130.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2024-Q3", "obs_value": Decimal("133.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2024-Q4", "obs_value": Decimal("136.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q1", "obs_value": Decimal("139.5")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q2", "obs_value": Decimal("143.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q3", "obs_value": Decimal("147.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2025-Q4", "obs_value": Decimal("151.0")},
    {"dataset_id": "ABS:CPI", "time_period": "2026-Q1", "obs_value": Decimal("155.0")},
]

# Unemployment stable
UNEMP_STABLE = [
    {"dataset_id": "ABS:LF", "time_period": "2025-05", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-06", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-07", "obs_value": Decimal("3.4")},
    {"dataset_id": "ABS:LF", "time_period": "2025-08", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-09", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-10", "obs_value": Decimal("3.4")},
    {"dataset_id": "ABS:LF", "time_period": "2025-11", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-12", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2026-01", "obs_value": Decimal("3.4")},
    {"dataset_id": "ABS:LF", "time_period": "2026-02", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2026-03", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2026-04", "obs_value": Decimal("3.4")},
]

# Unemployment rising sharply
UNEMP_RISING = [
    {"dataset_id": "ABS:LF", "time_period": "2025-05", "obs_value": Decimal("3.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-06", "obs_value": Decimal("3.7")},
    {"dataset_id": "ABS:LF", "time_period": "2025-07", "obs_value": Decimal("3.9")},
    {"dataset_id": "ABS:LF", "time_period": "2025-08", "obs_value": Decimal("4.2")},
    {"dataset_id": "ABS:LF", "time_period": "2025-09", "obs_value": Decimal("4.5")},
    {"dataset_id": "ABS:LF", "time_period": "2025-10", "obs_value": Decimal("4.8")},
    {"dataset_id": "ABS:LF", "time_period": "2025-11", "obs_value": Decimal("5.1")},
    {"dataset_id": "ABS:LF", "time_period": "2025-12", "obs_value": Decimal("5.4")},
    {"dataset_id": "ABS:LF", "time_period": "2026-01", "obs_value": Decimal("5.7")},
    {"dataset_id": "ABS:LF", "time_period": "2026-02", "obs_value": Decimal("6.0")},
    {"dataset_id": "ABS:LF", "time_period": "2026-03", "obs_value": Decimal("6.3")},
    {"dataset_id": "ABS:LF", "time_period": "2026-04", "obs_value": Decimal("6.6")},
]


def _create_tables(dynamodb):
    cpi = dynamodb.create_table(
        TableName="test-cpi-table",
        KeySchema=[{"AttributeName": "dataset_id", "KeyType": "HASH"}, {"AttributeName": "time_period", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "dataset_id", "AttributeType": "S"}, {"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    unemp = dynamodb.create_table(
        TableName="test-unemployment-table",
        KeySchema=[{"AttributeName": "dataset_id", "KeyType": "HASH"}, {"AttributeName": "time_period", "KeyType": "RANGE"}],
        AttributeDefinitions=[{"AttributeName": "dataset_id", "AttributeType": "S"}, {"AttributeName": "time_period", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return cpi, unemp


def _setup_low_risk(dynamodb):
    cpi, unemp = _create_tables(dynamodb)
    for row in CPI_STABLE:
        cpi.put_item(Item=row)
    for row in UNEMP_STABLE:
        unemp.put_item(Item=row)
    analysis_module.cpi_table = cpi
    analysis_module.unemployment_table = unemp


def _setup_high_risk(dynamodb):
    cpi, unemp = _create_tables(dynamodb)
    for row in CPI_RISING:
        cpi.put_item(Item=row)
    for row in UNEMP_RISING:
        unemp.put_item(Item=row)
    analysis_module.cpi_table = cpi
    analysis_module.unemployment_table = unemp


# ── recession-risk tests ────────────────────────────────────────────────


@mock_aws
def test_risk_returns_correct_structure():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_low_risk(dynamodb)

    response = client.get("/public/analysis/recession-risk")
    assert response.status_code == 200

    body = response.json()
    assert "risk_level" in body
    assert "confidence" in body
    assert "signals" in body
    assert "timestamp" in body
    assert body["risk_level"] in ("Low", "Moderate", "High")


@mock_aws
def test_risk_low_in_healthy_economy():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_low_risk(dynamodb)

    body = client.get("/public/analysis/recession-risk").json()
    assert body["risk_level"] == "Low"
    assert len(body["signals"]) == 2

    severities = [s["severity"] for s in body["signals"]]
    assert "High" not in severities


@mock_aws
def test_risk_high_in_weak_economy():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_high_risk(dynamodb)

    body = client.get("/public/analysis/recession-risk").json()
    assert body["risk_level"] == "High"

    unemp_signal = next(s for s in body["signals"] if s["indicator"] == "Unemployment")
    assert unemp_signal["severity"] == "High"

    cpi_signal = next(s for s in body["signals"] if s["indicator"] == "Inflation")
    assert cpi_signal["severity"] == "High"


@mock_aws
def test_risk_confidence_in_valid_range():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_high_risk(dynamodb)

    body = client.get("/public/analysis/recession-risk").json()
    assert 0.0 <= body["confidence"] <= 1.0


@mock_aws
def test_risk_no_data_returns_404():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    cpi, unemp = _create_tables(dynamodb)
    analysis_module.cpi_table = cpi
    analysis_module.unemployment_table = unemp

    response = client.get("/public/analysis/recession-risk")
    assert response.status_code == 404


@mock_aws
def test_risk_partial_data_unemployment_only():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    cpi, unemp = _create_tables(dynamodb)
    for row in UNEMP_RISING:
        unemp.put_item(Item=row)
    analysis_module.cpi_table = cpi
    analysis_module.unemployment_table = unemp

    response = client.get("/public/analysis/recession-risk")
    assert response.status_code == 200

    body = response.json()
    unemp_signal = next(s for s in body["signals"] if s["indicator"] == "Unemployment")
    assert unemp_signal["severity"] == "High"

    cpi_signal = next(s for s in body["signals"] if s["indicator"] == "Inflation")
    assert cpi_signal["severity"] == "Unknown"

    # Confidence should be lower with only one indicator
    assert body["confidence"] < 0.7


@mock_aws
def test_risk_partial_data_cpi_only():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    cpi, unemp = _create_tables(dynamodb)
    for row in CPI_RISING:
        cpi.put_item(Item=row)
    analysis_module.cpi_table = cpi
    analysis_module.unemployment_table = unemp

    response = client.get("/public/analysis/recession-risk")
    assert response.status_code == 200

    body = response.json()
    cpi_signal = next(s for s in body["signals"] if s["indicator"] == "Inflation")
    assert cpi_signal["severity"] == "High"

    unemp_signal = next(s for s in body["signals"] if s["indicator"] == "Unemployment")
    assert unemp_signal["severity"] == "Unknown"

    # Confidence should be lower with only one indicator
    assert body["confidence"] < 0.7


@mock_aws
def test_risk_signals_have_two_indicators():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_low_risk(dynamodb)

    body = client.get("/public/analysis/recession-risk").json()
    indicators = [s["indicator"] for s in body["signals"]]
    assert "Unemployment" in indicators
    assert "Inflation" in indicators
    assert "GDP" not in indicators
    assert len(indicators) == 2
