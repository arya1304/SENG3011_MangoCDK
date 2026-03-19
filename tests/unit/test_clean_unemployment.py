import json
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
import routers.preprocess as preprocess_module

client = TestClient(app)

MOCK_PREPROCESSED = {
    "data_source": "Australian Bureau of Statistics (ABS)",
    "dataset_type": "Government Economic Indicator",
    "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,LF/all",
    "time_object": {
        "timestamp": "2026-03-19 11:30:00.000000",
        "timezone": "GMT+11"
    },
    "events": [
        {
            "time_object": {"timestamp": "2023-Q1", "duration": 1, "duration_unit": "quarter", "timezone": "GMT+11"},
            "event_type": "unemployment_observation",
            "attribute": {
                "dataflow": "ABS,LF",
                "measure": "M14",
                "sex": "1",
                "age": "1599",
                "adjustment_type": "10",
                "region": "AUS",
                "freq": "M",
                "time_period": "2023-Q1",
                "obs_value": 3.5,
                "unit_measure": None,
                "unit_mult": None,
            }
        },
        {
            "time_object": {"timestamp": "2023-Q2", "duration": 1, "duration_unit": "quarter", "timezone": "GMT+11"},
            "event_type": "unemployment_observation",
            "attribute": {
                "dataflow": "ABS,LF",
                "measure": "M14",
                "sex": "1",
                "age": "1599",
                "adjustment_type": "10",
                "region": "AUS",
                "freq": "M",
                "time_period": "2023-Q2",
                "obs_value": 3.6,
                "unit_measure": None,
                "unit_mult": None,
            }
        },
    ]
}


def _setup_s3_and_table(s3_client, dynamodb):
    s3_client.create_bucket(Bucket="test-bucket")
    s3_client.put_object(
        Bucket="test-bucket",
        Key="preprocessed/ABS,LF/all/2026-03-19T00-00-00Z.json",
        Body=json.dumps(MOCK_PREPROCESSED)
    )
    preprocess_module.s3 = s3_client

    table = dynamodb.create_table(
        TableName="test-unemployment-table",
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
    preprocess_module.unemployment_table = table
    return table


@mock_aws
def test_clean_unemployment_returns_success():
    s3_client = boto3.client("s3", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    _setup_s3_and_table(s3_client, dynamodb)

    response = client.post("/preprocess/cleanUnemployment?dataflowIdentifier=ABS,LF&dataKey=all")
    assert response.status_code == 200
    assert response.json()["message"] == "success"


@mock_aws
def test_clean_unemployment_stores_correct_rows_in_dynamodb():
    s3_client = boto3.client("s3", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = _setup_s3_and_table(s3_client, dynamodb)

    client.post("/preprocess/cleanUnemployment?dataflowIdentifier=ABS,LF&dataKey=all")

    items = table.scan()["Items"]
    assert len(items) == 2

    item = sorted(items, key=lambda x: x["time_period"])[0]
    assert item["dataset_id"] == "https://data.api.abs.gov.au/rest/data/ABS,LF/all"
    assert item["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert item["time_period"] == "2023-Q1"
    assert item["year"] == "2023"
    assert item["sex"] == "1"
    assert item["age"] == "1599"
    assert item["adjustment_type"] == "10"
    assert item["region"] == "AUS"
    assert item["measure"] == "M14"
    assert item["obs_value"] == Decimal("3.5")


@mock_aws
def test_clean_unemployment_404_no_preprocessed_data():
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-bucket")
    preprocess_module.s3 = s3_client

    response = client.post("/preprocess/cleanUnemployment?dataflowIdentifier=ABS,LF&dataKey=all")
    assert response.status_code == 404
    assert "No Preprocessed unemployment data" in response.json()["detail"]


@mock_aws
def test_clean_unemployment_404_no_events():
    s3_client = boto3.client("s3", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-bucket")
    preprocess_module.s3 = s3_client

    # only put the empty-events file 
    empty_data = {**MOCK_PREPROCESSED, "events": []}
    s3_client.put_object(
        Bucket="test-bucket",
        Key="preprocessed/ABS,LF/all/2026-03-19T00-00-00Z.json",
        Body=json.dumps(empty_data)
    )

    dynamodb.create_table(
        TableName="test-unemployment-table",
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
    preprocess_module.unemployment_table = dynamodb.Table("test-unemployment-table")

    response = client.post("/preprocess/cleanUnemployment?dataflowIdentifier=ABS,LF&dataKey=all")
    assert response.status_code == 404
    assert "No events found" in response.json()["detail"]


@mock_aws
def test_clean_unemployment_handles_null_obs_value():
    s3_client = boto3.client("s3", region_name="us-east-1")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    s3_client.create_bucket(Bucket="test-bucket")
    preprocess_module.s3 = s3_client

    null_data = {
        **MOCK_PREPROCESSED,
        "events": [{
            "time_object": {"timestamp": "2023-Q1"},
            "event_type": "unemployment_observation",
            "attribute": {
                "measure": "M14", "sex": "1", "age": "1599",
                "adjustment_type": "10", "region": "AUS",
                "time_period": "2023-Q1", "obs_value": None,
            }
        }]
    }
    s3_client.put_object(
        Bucket="test-bucket",
        Key="preprocessed/ABS,LF/all/2026-03-19T00-00-00Z.json",
        Body=json.dumps(null_data)
    )

    table = dynamodb.create_table(
        TableName="test-unemployment-table",
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
    preprocess_module.unemployment_table = table

    response = client.post("/preprocess/cleanUnemployment?dataflowIdentifier=ABS,LF&dataKey=all")
    assert response.status_code == 200

    items = table.scan()["Items"]
    assert len(items) == 1
    assert items[0].get("obs_value") is None


def test_clean_unemployment_missing_params():
    response = client.post("/preprocess/cleanUnemployment")
    assert response.status_code == 422
