import json
import os
import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

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
os.environ["USERS_TABLE_NAME"] = "test-users-table"
os.environ["JWT_SECRET"] = "test-secret-key-that-is-long-enough-for-hs256"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from main import app
import routers.preprocess as preprocess_module

client = TestClient(app)

MOCK_RAW_UNEMPLOYMENT = {
    "data": {
        "structures": [{
            "dimensions": {
                "series": [
                    {"id": "MEASURE", "keyPosition": 0, "values": [
                        {"id": "M13", "name": "Unemployment rate"},
                        {"id": "M3", "name": "Employed persons"}
                    ]},
                    {"id": "SEX", "keyPosition": 1, "values": [
                        {"id": "3", "name": "Persons"}
                    ]},
                    {"id": "AGE", "keyPosition": 2, "values": [
                        {"id": "1599", "name": "Total (age)"}
                    ]},
                    {"id": "TSEST", "keyPosition": 3, "values": [
                        {"id": "20", "name": "Seasonally Adjusted"}
                    ]},
                    {"id": "REGION", "keyPosition": 4, "values": [
                        {"id": "AUS", "name": "Australia"}
                    ]},
                    {"id": "FREQ", "keyPosition": 5, "values": [
                        {"id": "M", "name": "Monthly"}
                    ]}
                ],
                "observation": [{
                    "id": "TIME_PERIOD",
                    "keyPosition": 6,
                    "values": [
                        {"id": "2022-01", "name": "2022-01"},
                        {"id": "2022-02", "name": "2022-02"}
                    ]
                }]
            },
            "attributes": {
                "dataSet": [],
                "series": [
                    {"id": "UNIT_MEASURE", "values": [
                        {"id": "PCT", "name": "Percent"},
                        {"id": "NUM", "name": "Number"}
                    ]},
                    {"id": "UNIT_MULT", "values": [
                        {"id": "0", "name": "Units"},
                        {"id": "3", "name": "Thousands"}
                    ]}
                ],
                "observation": []
            }
        }],
        "dataSets": [{
            "series": {
                "0:0:0:0:0:0": {
                    "attributes": [0, 0],
                    "observations": {
                        "0": [4.03, None, None, 0],
                        "1": [3.95, None, None, 0]
                    }
                },
                "1:0:0:0:0:0": {
                    "attributes": [1, 1],
                    "observations": {
                        "0": [1136.5, None, None, 0],
                        "1": [1144.0, None, None, 0]
                    }
                }
            }
        }]
    }
}

@mock_aws
def test_preprocess_unemployment_returns_correct_data_model():
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,LF,1.0.0/M13.3.1599.20.AUS.M/2024-01-01T00-00-00Z.json",
        Body=json.dumps(MOCK_RAW_UNEMPLOYMENT)
    )

    response = client.post("/preprocess/unemployment?dataflowIdentifier=ABS,LF,1.0.0&dataKey=M13.3.1599.20.AUS.M")
    assert response.status_code == 200

    body = response.json()
    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert len(body["events"]) == 4

    rate_events = [e for e in body["events"] if e["attribute"]["measure"] == "M13"]
    assert len(rate_events) == 2
    rate = rate_events[0]
    assert rate["event_type"] == "unemployment_observation"
    assert rate["attribute"]["dataflow"] == "ABS:LF(1.0.0)"
    assert rate["attribute"]["sex"] == "3"
    assert rate["attribute"]["age"] == "1599"
    assert rate["attribute"]["adjustment_type"] == "20"
    assert rate["attribute"]["region"] == "AUS"
    assert rate["attribute"]["freq"] == "M"
    assert rate["attribute"]["time_period"] == "2022-01"
    assert rate["attribute"]["obs_value"] == 4.03
    assert rate["attribute"]["unit_measure"] == "PCT"
    assert rate["attribute"]["unit_mult"] == "0"
    assert rate["time_object"]["duration_unit"] == "month"
    assert rate["time_object"]["duration"] == 1

    emp_events = [e for e in body["events"] if e["attribute"]["measure"] == "M3"]
    assert len(emp_events) == 2
    emp = emp_events[0]
    assert emp["attribute"]["obs_value"] == 1136.5
    assert emp["attribute"]["unit_measure"] == "NUM"
    assert emp["attribute"]["unit_mult"] == "3"


@mock_aws
def test_preprocess_unemployment_no_data():
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")

    response = client.post("/preprocess/unemployment?dataflowIdentifier=ABS,LF,1.0.0&dataKey=M13.3.1599.20.AUS.M")
    assert response.status_code == 404


@mock_aws
def test_preprocess_unemployment_invalid_format():
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,LF,1.0.0/M13.3.1599.20.AUS.M/2024-01-01T00-00-00Z.json",
        Body=json.dumps({"data": {"structures": [], "dataSets": []}})
    )

    response = client.post("/preprocess/unemployment?dataflowIdentifier=ABS,LF,1.0.0&dataKey=M13.3.1599.20.AUS.M")
    assert response.status_code == 500
    assert "Unexpected SDMX-JSON" in response.json()["detail"]


def test_preprocess_unemployment_missing_params():
    response = client.post("/preprocess/unemployment")
    assert response.status_code == 422


@mock_aws
def test_preprocess_unemployment_saves_to_s3():
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,LF,1.0.0/M13.3.1599.20.AUS.M/2024-01-01T00-00-00Z.json",
        Body=json.dumps(MOCK_RAW_UNEMPLOYMENT)
    )

    response = client.post("/preprocess/unemployment?dataflowIdentifier=ABS,LF,1.0.0&dataKey=M13.3.1599.20.AUS.M")
    assert response.status_code == 200

    listing = s3.list_objects_v2(
        Bucket="test-bucket",
        Prefix="preprocessed/ABS,LF,1.0.0/M13.3.1599.20.AUS.M/"
    )
    assert "Contents" in listing
    assert len(listing["Contents"]) == 1

    saved = json.loads(
        s3.get_object(Bucket="test-bucket", Key=listing["Contents"][0]["Key"])["Body"].read()
    )
    assert saved["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert len(saved["events"]) == 4