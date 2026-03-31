import json
import os
from unittest.mock import MagicMock, patch

import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

# Fake AWS credentials for moto — must be set before importing app
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

from main import app
import routers.collect as collect_module

client = TestClient(app)

BUCKET_NAME = "test-bucket"
DATAFLOW_ID = "ABS,LF"
DATA_KEY = "1.3.3.1.4.AUS.M"
FAKE_ABS_RESPONSE = {"dataSets": [{"observations": {"0:0": [5.2]}}]}


def _make_mock_response(status_code=200, json_data=None, text=""):
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data if json_data is not None else FAKE_ABS_RESPONSE
    mock_resp.content = json.dumps(json_data if json_data is not None else FAKE_ABS_RESPONSE).encode()
    mock_resp.text = text
    return mock_resp


def _create_s3_bucket(s3_client):
    s3_client.create_bucket(Bucket=BUCKET_NAME)


# ---------------------------------------------------------------------------
# Case 1: Successful call — ABS returns data, saved to S3, returned to client
# ---------------------------------------------------------------------------

@mock_aws
def test_collect_unemployment_success():
    s3_client = boto3.client("s3", region_name="us-east-1")
    _create_s3_bucket(s3_client)
    collect_module.s3 = s3_client
    collect_module.BUCKET_NAME = BUCKET_NAME

    with patch("routers.collect.requests.get", return_value=_make_mock_response()):
        resp = client.post(
            f"/collect/unemployment?dataflowIdentifier={DATAFLOW_ID}&dataKey={DATA_KEY}"
        )

    assert resp.status_code == 200
    assert resp.json() == FAKE_ABS_RESPONSE

    # Verify object was stored in S3
    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=f"{DATAFLOW_ID}/{DATA_KEY}/")
    assert objects["KeyCount"] == 1


# ---------------------------------------------------------------------------
# Case 2: ABS API returns non-200 — should raise HTTPException
# ---------------------------------------------------------------------------

@mock_aws
def test_collect_unemployment_abs_api_404():
    s3_client = boto3.client("s3", region_name="us-east-1")
    _create_s3_bucket(s3_client)
    collect_module.s3 = s3_client
    collect_module.BUCKET_NAME = BUCKET_NAME

    mock_resp = _make_mock_response(status_code=404, json_data=None, text="Not Found")
    with patch("routers.collect.requests.get", return_value=mock_resp):
        resp = client.post(
            f"/collect/unemployment?dataflowIdentifier={DATAFLOW_ID}&dataKey={DATA_KEY}"
        )

    assert resp.status_code == 404
    assert "ABS API error" in resp.json()["detail"]


@mock_aws
def test_collect_unemployment_abs_api_500():
    s3_client = boto3.client("s3", region_name="us-east-1")
    _create_s3_bucket(s3_client)
    collect_module.s3 = s3_client
    collect_module.BUCKET_NAME = BUCKET_NAME

    mock_resp = _make_mock_response(status_code=500, json_data=None, text="Internal Server Error")
    with patch("routers.collect.requests.get", return_value=mock_resp):
        resp = client.post(
            f"/collect/unemployment?dataflowIdentifier={DATAFLOW_ID}&dataKey={DATA_KEY}"
        )

    assert resp.status_code == 500
    assert "ABS API error" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Case 3: Missing BUCKET_NAME — should return 500
# ---------------------------------------------------------------------------

def test_collect_unemployment_missing_bucket_name(monkeypatch):
    monkeypatch.setattr(collect_module, "BUCKET_NAME", None)

    with patch("routers.collect.requests.get", return_value=_make_mock_response()):
        resp = client.post(
            f"/collect/unemployment?dataflowIdentifier={DATAFLOW_ID}&dataKey={DATA_KEY}"
        )

    assert resp.status_code == 500
    assert "BUCKET_NAME" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Case 4: startPeriod/endPeriod passed correctly to ABS API params
# ---------------------------------------------------------------------------

@mock_aws
def test_collect_unemployment_with_period_params():
    s3_client = boto3.client("s3", region_name="us-east-1")
    _create_s3_bucket(s3_client)
    collect_module.s3 = s3_client
    collect_module.BUCKET_NAME = BUCKET_NAME

    with patch("routers.collect.requests.get", return_value=_make_mock_response()) as mock_get:
        resp = client.post(
            f"/collect/unemployment"
            f"?dataflowIdentifier={DATAFLOW_ID}&dataKey={DATA_KEY}"
            f"&startPeriod=2023-Q1&endPeriod=2023-Q4"
        )

    assert resp.status_code == 200
    _, kwargs = mock_get.call_args
    params = kwargs.get("params", {})
    assert params["startPeriod"] == "2023-Q1"
    assert params["endPeriod"] == "2023-Q4"


# ---------------------------------------------------------------------------
# Case 5: startPeriod/endPeriod omitted — should NOT be in ABS API params
# ---------------------------------------------------------------------------

@mock_aws
def test_collect_unemployment_without_period_params():
    s3_client = boto3.client("s3", region_name="us-east-1")
    _create_s3_bucket(s3_client)
    collect_module.s3 = s3_client
    collect_module.BUCKET_NAME = BUCKET_NAME

    with patch("routers.collect.requests.get", return_value=_make_mock_response()) as mock_get:
        resp = client.post(
            f"/collect/unemployment?dataflowIdentifier={DATAFLOW_ID}&dataKey={DATA_KEY}"
        )

    assert resp.status_code == 200
    _, kwargs = mock_get.call_args
    params = kwargs.get("params", {})
    assert "startPeriod" not in params
    assert "endPeriod" not in params


# ---------------------------------------------------------------------------
# Case 6: Missing required params — should return 422
# ---------------------------------------------------------------------------

def test_collect_unemployment_missing_dataflow_identifier():
    resp = client.post(f"/collect/unemployment?dataKey={DATA_KEY}")
    assert resp.status_code == 422


def test_collect_unemployment_missing_data_key():
    resp = client.post(f"/collect/unemployment?dataflowIdentifier={DATAFLOW_ID}")
    assert resp.status_code == 422


def test_collect_unemployment_missing_all_required_params():
    resp = client.post("/collect/unemployment")
    assert resp.status_code == 422
