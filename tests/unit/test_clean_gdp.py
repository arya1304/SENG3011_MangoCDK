import os
from decimal import Decimal
import json
import boto3
from moto import mock_aws
from fastapi.testclient import TestClient

# fake AWS credentials for moto
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["BUCKET_NAME"] = "test-bucket"
os.environ["UNEMPLOYMENT_TABLE_NAME"] = "test-unemployment-table"
os.environ["GDP_TABLE_NAME"] = "test-gdp-table"
os.environ["USERS_TABLE_NAME"] = "test-users-table"
os.environ["JWT_SECRET"] = "test-secret-key-that-is-long-enough-for-hs256"
os.environ["CPI_TABLE_NAME"] = "test-cpi-table"

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda"))

from main import app
import routers.preprocess as preprocess_module

client = TestClient(app)

TABLE_NAME = "test-gdp-table"

# extract the preprocessed cpi data
# check if it is stored in the bucket
# Rows matching the real DynamoDB structure observed in AWS console


# preprocessed data that is stored in the S3 Bucket already
MOCK_PREPROCESSED = {
    'data_source': 'Australian Bureau of Statistics (ABS)', 
    'dataset_type': 'Government Economic Indicator', 
    'dataset_id':'https://data.api.abs.gov.au/rest/data/ABS,ANA_IND_GVA,1.0.0/......Q', 
    'time_object': {
        'timestamp': '2026-03-19 09:42:18.276944', 
        'timezone': 'GMT+11'
    }, 
    'events': [
        {
            'time_object': {
                'timestamp': '2025-Q1', 
                'duration': 1, 
                'duration_unit': 'quarter', 
                'timezone': 'GMT+11'
            }, 
        
        'event_type': 'gdp_observation', 
        'attribute': {
            'dataflow': 'ABS:ANA_IND_GVA(1.0.0)', 
            'measure': 'VCH', 
            'data_item': 'GPM', 
            'sector': 'SSS', 
            'adjustment_type': '20', 
            'industry': 'TOTAL', 
            'region': 'AUS', 
            'freq': 'Q', 
            'time_period': '2025-Q1', 
            'obs_value': 48016, 
            'unit_measure': 'NA', 
            'unit_mult': '0', 
            'obs_status': None   
    }
    }

    ]
}

MOCK_ROWS = [
    {"dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,ANA_IND_GVA,1.0.0/......Q.json",
    "data_source": "Australian Bureau of Statistics (ABS)",
    "year": "2024",
    "quarter": "Q3",
    "industry": "A",
    "region": "AUS",
    "time_period": "2024-Q3",
    "obs_value": Decimal("137.4"),
    "data_item": "1",
    "adjustment_type": "10",
    "obs_status": "A",}
]

def _create_table(db):
    table = db.create_table(
        TableName=TABLE_NAME,
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
    for row in MOCK_ROWS:
        table.put_item(Item=row)
    return table

# no preprocessed cpi data avaiable to put inside the db
@mock_aws
def test_preprocess_cpi_no_data():
    s3 = boto3.client("s3", region_name="us-east-1")
    # point the module at the mocked client
    preprocess_module.s3 = s3  
    s3.create_bucket(Bucket="test-bucket")

    response = client.post("/preprocess/cleanGdp?dataflowIdentifier=ABS,CPI,1.0.0&dataKey=1.AUS.Q")
    assert response.status_code == 404

# no events in the preprocessed data
@mock_aws
def test_preprocess_cpi_no_events():
    # should return 500 when S3 file has bad structure
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")

    s3.put_object(
        Bucket="test-bucket",
        Key="preprocessed/ABS,ANA_IND_GVA,1.0.0/......Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps({"dataset_id": "some-id", "data_source": "ABS"}),
    )

    response = client.post("/preprocess/cleanGdp?dataflowIdentifier=ABS,ANA_IND_GVA,1.0.0&dataKey=......Q")
    assert response.status_code == 404
    assert "No events found in preprocessed data" in response.json()["detail"]



# invalid preprocessed data model to put inside the db
@mock_aws
def test_clean_cpi_invalid_preprocessed_data():
    # should return 500 when S3 file has bad structure
    s3 = boto3.client("s3", region_name="us-east-1")
    preprocess_module.s3 = s3
    s3.create_bucket(Bucket="test-bucket")

    response = client.post("/preprocess/cleanGdp?dataflowIdentifier=ABS,ANA_IND_GVA,1.0.0&dataKey=......Q")
    assert response.status_code == 404
    assert "No Preprocessed GDP data found at s3://" in response.json()["detail"]


# success case
# valid preprocessed data found in the S3 bucket and can put it in the db
@mock_aws
def test_clean_cpi_sucess():
    s3 = boto3.client("s3", region_name="us-east-1")
    db = db = boto3.resource("dynamodb", region_name="us-east-1")
    preprocess_module.s3 = s3
    preprocess_module.cpi_table = _create_table(db)

    s3.create_bucket(Bucket="test-bucket")
    
    # put the mock data in the table
    s3.put_object(
        Bucket="test-bucket",
        Key="preprocessed/ABS,ANA_IND_GVA,1.0.0/......Q/2024-01-01T00-00-00Z.json",
        Body=json.dumps(MOCK_PREPROCESSED),
    )

    # call the clean function
    response = client.post("/preprocess/cleanGdp?dataflowIdentifier=ABS,ANA_IND_GVA,1.0.0&dataKey=......Q")
    assert response.status_code == 200
    assert response.json()["message"] == "success"

    # check if the data was populated into the db
    test_tbl = preprocess_module.cpi_table.get_item(
        Key={
            "dataset_id": "https://data.api.abs.gov.au/rest/data/ABS,ANA_IND_GVA,1.0.0/......Q.json",
            "time_period": "2024-Q3",
        }
    )["Item"]
    assert test_tbl["year"] == "2024"
    assert test_tbl["quarter"] == "Q3"
    assert test_tbl["obs_value"] == Decimal("137.4")
    assert test_tbl["region"] == "AUS"