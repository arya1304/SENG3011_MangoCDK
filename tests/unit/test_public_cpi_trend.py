import os
from decimal import Decimal
import sys
import boto3
from moto import mock_aws
from fastapi.testclient import TestClient
from main import app
import routers.analysis as public_module

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

CPI_ROWS = [
    {"time_period": "2023-Q1", "region": "50", "obs_value": Decimal("132.6")},
    {"time_period": "2023-Q2", "region": "50", "obs_value": Decimal("133.7")},
    {"time_period": "2023-Q3", "region": "50", "obs_value": Decimal("135.3")},
    {"time_period": "2023-Q4", "region": "50", "obs_value": Decimal("136.1")},
    {"time_period": "2024-Q1", "region": "50", "obs_value": Decimal("137.8")},
]