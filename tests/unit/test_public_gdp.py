import os
import sys
from decimal import Decimal

# fake aws credentials for testing
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


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient
from main import app
import routers.public as public_module



#helper function for the tests
def create_dyanmo_table():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.create_table(
        TableName="test-gdp-table",
        KeySchema=[
            {"AttributeName": "dataset_id", "KeyType": "HASH"},  # partition key
            {"AttributeName": "time_period", "KeyType": "RANGE"}, # sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": "dataset_id", "AttributeType": "S"},
            {"AttributeName": "time_period", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    return table



@mock_aws
def test_public_gdp_returns_correct_data():

    table = create_dyanmo_table()
    public_module.gdp_table = table
    table.put_item(Item={
        "dataset_id": "ABS:ANA_IND_GVA(1.0.0)",
        "time_period": "2023-Q1",
        "data_source": "source",
        "data_item": "GPM",
        "adjustment_type": "20",
        "industry": "TOTAL",
        "region": "AUS",
        "obs_value": Decimal("48016"),
        "obs_status": None,
        "freq": "Q",
    })

    client = TestClient(app)

    response = client.get("/public/gdp?start=2023-Q1&end=2023-Q4")
    assert response.status_code == 200

    body = response.json()

    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert body["dataset_id"] == "ABS:ANA_IND_GVA"
    assert body["query"] == {"start": "2023-Q1", "end": "2023-Q4"}
    assert body["count"] == 1
    assert "timestamp" in body

    event = body["events"][0]

    assert event["time_period"] == "2023-Q1"
    assert event["region"] == "AUS"
    assert event["gdp_value"] == 48016.0
    assert event["data_item"] == "GPM"
    assert event["adjustment_type"] == "20"
    assert event["industry"] == "TOTAL"
    assert event["obs_status"] is None
    assert event["freq"] == "Q"


@mock_aws
def test_public_gdp_missing_params():
    client = TestClient(app)
    
    # missing start and end
    response = client.get("/public/gdp")
    assert response.status_code == 422

    # missing start
    response = client.get("/public/gdp?end=2023-Q4")
    assert response.status_code == 422

    # missing end
    response = client.get("/public/gdp?start=2023-Q1")
    assert response.status_code == 422

@mock_aws
def test_public_gdp_no_events():
    table = create_dyanmo_table()
    public_module.gdp_table = table
    # leave the table empty

    client = TestClient(app)

    response = client.get("/public/gdp?start=2025-Q1&end=2025-Q4")
    assert response.status_code == 200

    body = response.json()
    assert body["events"] == []
    assert body["count"] == 0


@mock_aws
def test_public_gdp_filters():
    table = create_dyanmo_table()
    public_module.gdp_table = table

    for t in ["2023-Q1", "2024-Q3", "2025-Q2", "2025-Q3"]:
        table.put_item(Item={
            "dataset_id": "ABS:ANA_IND_GVA(1.0.0)",
            "time_period": t,
            "data_source": "source",
            "data_item": "GPM",
            "adjustment_type": "20",
            "industry": "TOTAL",
            "region": "AUS",
            "obs_value": Decimal("48016"),
            "obs_status": None,
        })

    client = TestClient(app)

    response = client.get("/public/gdp?start=2024-Q1&end=2025-Q2")
    assert response.status_code == 200

    body = response.json()

    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_id"] == "ABS:ANA_IND_GVA"
    assert body["count"] == 2

    for event in body["events"]:
        assert event["time_period"] >= "2024-Q1"
        assert event["time_period"] <= "2025-Q2"
