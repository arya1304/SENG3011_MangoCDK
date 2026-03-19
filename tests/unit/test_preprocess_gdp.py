import json
import os
import sys


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
import routers.preprocess as preprocess_module



MOCK_RAW_GDP = {
      "data": {
        "structures": [{
            "dimensions": {
                "observation": [
                    {"id": "MEASURE",     "keyPosition": 0, "values": [{"id": "TCH"}, {"id": "VCH"}]},
                    {"id": "DATA_ITEM",   "keyPosition": 1, "values": [{"id": "GPM"}]},
                    {"id": "SECTOR",      "keyPosition": 2, "values": [{"id": "SSS"}]},
                    {"id": "TSEST",       "keyPosition": 3, "values": [{"id": "20"}]},
                    {"id": "INDUSTRY",    "keyPosition": 4, "values": [{"id": "TOTAL"}]},
                    {"id": "REGION",      "keyPosition": 5, "values": [{"id": "AUS"}]},
                    {"id": "FREQ",        "keyPosition": 6, "values": [{"id": "Q"}]},
                    {"id": "TIME_PERIOD", "keyPosition": 7, "values": [{"id": "2025-Q1"}]},
                ]
            },
            "attributes": {
                "observation": [
                    {"id": "UNIT_MEASURE", "values": [{"id": "NA"}]},
                    {"id": "UNIT_MULT",    "values": [{"id": "0"}]},
                    {"id": "OBS_STATUS",   "values": [{"id": "n"}]},
                    {"id": "OBS_COMMENT",  "values": []},
                ]
            }
        }],
        "dataSets": [{
            "observations": {
                "1:0:0:0:0:0:0:0": [48016, 0, 0, None, None],
                "0:0:0:0:0:0:0:0": [None, 0, 0, 0, None],
            }
        }]
    }
}


@mock_aws
def test_preprocess_gdp_returns_correct_data_model():
    s3 = boto3.client("s3", region_name="us-east-1")

    preprocess_module.s3 = s3  
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,ANA_IND_GVA,1.0.0/......Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(MOCK_RAW_GDP)
    )
    client = TestClient(app)

    response = client.post("/preprocess/gdp")
    assert response.status_code == 200

    body = response.json()
    # print(body)
    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert len(body["events"]) == 2

    event = body["events"][0]
    assert event["event_type"] == "gdp_observation"
    assert event["attribute"]["region"] == "AUS"
    assert event["attribute"]["freq"] == "Q"
    assert event["attribute"]["time_period"] == "2025-Q1"
    assert event["attribute"]["obs_value"] == 48016
    assert event["attribute"]["dataflow"] == "ABS:ANA_IND_GVA(1.0.0)"
    assert event["attribute"]["measure"] == "VCH"
    assert event["attribute"]["data_item"] == "GPM"
    assert event["attribute"]["sector"] == "SSS"
    assert event["attribute"]["adjustment_type"] == "20"
    assert event["attribute"]["unit_measure"] == "NA"
    assert event["attribute"]["unit_mult"] == "0"
    assert event["attribute"]["obs_status"] is None



@mock_aws
def test_preprocess_gdp_no_data():
    s3 = boto3.client("s3", region_name="us-east-1")
    # run thye function with the empty bucket
    preprocess_module.s3 = s3  
    s3.create_bucket(Bucket="test-bucket")
    
    client = TestClient(app)
    response = client.post("/preprocess/gdp")
    assert response.status_code == 404


@mock_aws
def test_preprocess_gdp_invalid_raw_data_format():
    """Should return 500 when S3 file has bad structure"""
    # should return 500 when S3 file has bad structure
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    bad_data = {"data": {}}
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,ANA_IND_GVA,1.0.0/......Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(bad_data)
    )

    client = TestClient(app)
    response = client.post("/preprocess/gdp")
    assert response.status_code == 500
    assert "Unexpected SDMX-JSON" in response.json()["detail"]


@mock_aws
def test_preprocess_gdp_bucket_name():
    preprocess_module.BUCKET_NAME = None
    client = TestClient(app)
    response = client.post("/preprocess/gdp")
    assert response.status_code == 500
    assert "Server configuration error" in response.json()["detail"]
    
    #setting it back to original
    preprocess_module.BUCKET_NAME = "test-bucket"


