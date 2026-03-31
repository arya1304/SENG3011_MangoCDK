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

# this is just made up
MOCK_SDMX = {
    "data": {
        "structures": [{
            "dimensions": {
                "series": [
                    {"id": "MEASURE", "values": [{"id": "1"}]},
                    {"id": "REGION",  "values": [{"id": "AUS"}]},
                    {"id": "FREQ",    "values": [{"id": "Q"}]}
                ],
                "observation": [
                    {"id": "TIME_PERIOD", "values": [
                        {"id": "2024-Q1"},
                        {"id": "2024-Q2"}
                    ]}
                ]
            }
        }],
        "dataSets": [{
            "series": {
                "0:0:0": {
                    "observations": {
                        "0": [136.1],
                        "1": [137.4]
                    }
                }
            }
        }]
    }
}


@mock_aws
def test_preprocess_cpi_returns_correct_format():
    s3 = boto3.client("s3", region_name="us-east-1")
    # point the module at the mocked client
    preprocess_module.s3 = s3  
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(MOCK_SDMX)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 200

    body = response.json()
    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert len(body["events"]) == 2

    event = body["events"][0]
    assert event["event_type"] == "cpi_observation"
    assert event["attribute"]["region"] == "AUS"
    assert event["attribute"]["freq"] == "Q"
    assert event["attribute"]["time_period"] == "2024-Q1"
    assert event["attribute"]["obs_value"] == 136.1
    assert event["attribute"]["dataflow"] == "ABS:CPI(1.0.0)"


# this was fetched using the ABS API 
REAL_ABS_SDMX = {
    "meta": {
        "schema": "https://raw.githubusercontent.com/sdmx-twg/sdmx-json/master/data-message/tools/schemas/2.0.0/sdmx-json-data-schema.json",
        "id": "IREF000006",
        "prepared": "2026-03-12T11:32:57Z",
        "test": True,
        "contentLanguages": ["en"],
        "sender": {"id": "Stable_-_DotStat_v8", "name": "unknown", "names": {"en": "unknown"}}
    },
    "data": {
        "dataSets": [{
            "structure": 0,
            "action": "Information",
            "links": [{"urn": "urn:sdmx:org.sdmx.infomodel.datastructure.DataStructure=ABS:CPI(2.0.0)", "rel": "DataStructure"}],
            "annotations": [0, 1, 2, 3, 4, 5],
            "series": {
                "0:0:0:0:0": {
                    "attributes": [None],
                    "annotations": [],
                    "observations": {
                        "0": [95.41],
                        "1": [96.41],
                        "2": [96.62],
                        "3": [96.81]
                    }
                }
            }
        }],
        "structures": [{
            "name": "Consumer Price Index (CPI)",
            "names": {"en": "Consumer Price Index (CPI)"},
            "dimensions": {
                "dataSet": [],
                "series": [
                    {"id": "MEASURE", "name": "Measure", "keyPosition": 0, "values": [{"id": "1", "name": "Index numbers"}]},
                    {"id": "INDEX", "name": "Index", "keyPosition": 1, "values": [{"id": "10001", "name": "All groups CPI"}]},
                    {"id": "TSEST", "name": "Adjustment Type", "keyPosition": 2, "values": [{"id": "10", "name": "Original"}]},
                    {"id": "REGION", "name": "Region", "keyPosition": 3, "values": [{"id": "50", "name": "Australia"}]},
                    {"id": "FREQ", "name": "Frequency", "keyPosition": 4, "values": [{"id": "Q", "name": "Quarterly"}]}
                ],
                "observation": [{
                    "id": "TIME_PERIOD",
                    "name": "Time Period",
                    "keyPosition": 5,
                    "values": [
                        {"id": "2024-Q1", "name": "2024-Q1", "start": "2024-01-01T00:00:00", "end": "2024-03-31T00:00:00"},
                        {"id": "2024-Q2", "name": "2024-Q2", "start": "2024-04-01T00:00:00", "end": "2024-06-30T00:00:00"},
                        {"id": "2024-Q3", "name": "2024-Q3", "start": "2024-07-01T00:00:00", "end": "2024-09-30T00:00:00"},
                        {"id": "2024-Q4", "name": "2024-Q4", "start": "2024-10-01T00:00:00", "end": "2024-12-31T00:00:00"}
                    ]
                }]
            },
            "attributes": {
                "dataSet": [],
                "series": [{"id": "UNIT_MEASURE", "values": []}],
                "observation": [{"id": "OBS_STATUS", "values": []}, {"id": "DECIMALS", "values": []}, {"id": "OBS_COMMENT", "values": []}]
            },
            "dataSets": [0]
        }]
    },
    "errors": []
}


@mock_aws
def test_preprocess_cpi_returns_correct_format_real():
    s3 = boto3.client("s3", region_name="us-east-1")
    # point the module at the mocked client
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI/1.10001.10.50.Q/2026-03-12T11-32-57Z.json",
        Body=json.dumps(REAL_ABS_SDMX)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI&dataKey=1.10001.10.50.Q")
    assert response.status_code == 200

    body = response.json()
    assert body["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert body["dataset_type"] == "Government Economic Indicator"
    assert body["dataset_id"] == "https://data.api.abs.gov.au/rest/data/ABS,CPI/1.10001.10.50.Q"

    # should have 4 events (one per quarter)
    assert len(body["events"]) == 4

    # check first event (2024-Q1)
    e0 = body["events"][0]
    assert e0["event_type"] == "cpi_observation"
    assert e0["time_object"]["timestamp"] == "2024-Q1"
    assert e0["time_object"]["duration"] == 1
    assert e0["time_object"]["duration_unit"] == "quarter"
    assert e0["attribute"]["dataflow"] == "ABS,CPI"
    assert e0["attribute"]["measure"] == "1"
    assert e0["attribute"]["region"] == "50"
    assert e0["attribute"]["freq"] == "Q"
    assert e0["attribute"]["obs_value"] == 95.41

    # check remaining observations
    assert body["events"][1]["attribute"]["obs_value"] == 96.41
    assert body["events"][1]["attribute"]["time_period"] == "2024-Q2"
    assert body["events"][2]["attribute"]["obs_value"] == 96.62
    assert body["events"][2]["attribute"]["time_period"] == "2024-Q3"
    assert body["events"][3]["attribute"]["obs_value"] == 96.81
    assert body["events"][3]["attribute"]["time_period"] == "2024-Q4"


@mock_aws
def test_preprocess_cpi_no_data():
    s3 = boto3.client("s3", region_name="us-east-1")
    # point the module at the mocked client
    preprocess_module.s3 = s3  
    s3.create_bucket(Bucket="test-bucket")

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 404


def test_preprocess_cpi_missing_params():
    # hould return 422 when query params are missing
    response = client.post("/preprocess/cpi")
    assert response.status_code == 422


@mock_aws
def test_preprocess_cpi_invalid_sdmx_format():
    # should return 500 when S3 file has bad structure
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps({"data": {"structures": [], "dataSets": []}})
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 500
    assert "Unexpected SDMX-JSON" in response.json()["detail"]


@mock_aws
def test_preprocess_cpi_empty_observation_value():
    # should handle empty observation arrays gracefully (obs_value = None)
    sdmx = {
        "data": {
            "structures": [{"dimensions": {
                "series": [
                    {"id": "MEASURE", "values": [{"id": "1"}]},
                    {"id": "REGION", "values": [{"id": "AUS"}]},
                    {"id": "FREQ", "values": [{"id": "Q"}]}
                ],
                "observation": [{"id": "TIME_PERIOD", "values": [{"id": "2024-Q1"}]}]
            }}],
            "dataSets": [{"series": {"0:0:0": {"observations": {"0": []}}}}]
        }
    }

    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(sdmx)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 200
    assert response.json()["events"][0]["attribute"]["obs_value"] is None


@mock_aws
def test_preprocess_cpi_no_observation_dimensions():
    # should handle missing observation dimensions (obs_dims is empty)
    sdmx = {
        "data": {
            "structures": [{"dimensions": {
                "series": [
                    {"id": "MEASURE", "values": [{"id": "1"}]},
                    {"id": "REGION", "values": [{"id": "AUS"}]},
                    {"id": "FREQ", "values": [{"id": "Q"}]}
                ],
                "observation": []
            }}],
            "dataSets": [{"series": {"0:0:0": {"observations": {"0": [100.0]}}}}]
        }
    }

    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(sdmx)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 200
    # with no obs_dims, time_period falls back to str(obs_idx)
    assert response.json()["events"][0]["attribute"]["time_period"] == "0"


@mock_aws
def test_preprocess_cpi_saves_to_s3():
    # should save preprocessed result back to S3 under preprocessed prefix
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(MOCK_SDMX)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 200

    # verify preprocessed file was saved to S3
    listing = s3.list_objects_v2(Bucket="test-bucket", Prefix="preprocessed/ABS,CPI,1.0.0/1.AUS.Q/")
    assert "Contents" in listing
    assert len(listing["Contents"]) == 1

    saved = json.loads(s3.get_object(Bucket="test-bucket", Key=listing["Contents"][0]["Key"])["Body"].read())
    assert saved["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert len(saved["events"]) == 2


@mock_aws
def test_preprocess_cpi_monthly_frequency():
    """Should map monthly freq to 'month' duration unit"""
    sdmx = {
        "data": {
            "structures": [{"dimensions": {
                "series": [
                    {"id": "MEASURE", "values": [{"id": "1"}]},
                    {"id": "REGION", "values": [{"id": "AUS"}]},
                    {"id": "FREQ", "values": [{"id": "M"}]}
                ],
                "observation": [{"id": "TIME_PERIOD", "values": [{"id": "2024-01"}]}]
            }}],
            "dataSets": [{"series": {"0:0:0": {"observations": {"0": [110.5]}}}}]
        }
    }

    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.M/2024-01-01T00-00-00Z.json",
        Body=json.dumps(sdmx)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.M")
    assert response.status_code == 200
    assert response.json()["events"][0]["time_object"]["duration_unit"] == "month"


@mock_aws
def test_preprocess_cpi_picks_latest_file():
    # when multiple files exist, should use the most recent one
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")

    old_sdmx = json.loads(json.dumps(MOCK_SDMX))
    old_sdmx["data"]["dataSets"][0]["series"]["0:0:0"]["observations"]["0"] = [100.0]
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2023-01-01T00-00-00Z.json",
        Body=json.dumps(old_sdmx)
    )
    import time
    # brute forced a small delay so "LastModified" field is not identical
    time.sleep(1)
    s3.put_object(
        Bucket="test-bucket",
        Key="ABS,CPI,1.0.0/1.AUS.Q/2024-06-01T00-00-00Z.json",
        Body=json.dumps(MOCK_SDMX)
    )

    response = client.post("/preprocess/cpi?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 200
    assert response.json()["events"][0]["attribute"]["obs_value"] == 136.1
