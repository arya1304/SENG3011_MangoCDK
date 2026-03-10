import json
import os
import requests
from datetime import UTC, datetime
from fastapi import FastAPI, HTTPException
from mangum import Mangum
import boto3

ABS_API_URL = "https://data.api.abs.gov.au/rest/data"
BUCKET_NAME = os.environ.get("BUCKET_NAME")

app = FastAPI()
s3 = boto3.client('s3')

@app.get("/cpi/{dataflowIdentifier}/{dataKey}")
def get_cpi(
    dataflowIdentifier: str,
    dataKey: str,
    startPeriod: str = None,
    endPeriod: str = None,
    response_format: str = "jsondata",
    detail: str = "dataonly"
    ):
    """
    GET / CPI data from ABS API and return
    Query parameters:
    - startPeriod:             e.g., "2023-Q1"
    - endPeriod:               e.g., "2023-Q4"
    - format:                   e.g., "json"
    - detail:                   e.g., "dataonly"
    """
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")

    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")
    
    abs_query_params = {
        "format": response_format,
        "detail": detail,
    }

    if startPeriod:
        abs_query_params["startPeriod"] = startPeriod
    if endPeriod:
        abs_query_params["endPeriod"] = endPeriod

    response = requests.get(f"{ABS_API_URL}/{dataflowIdentifier}/{dataKey}", params=abs_query_params, timeout=10)

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"ABS API error: {response.text}")
    
    raw = response.json()
    
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{dataflowIdentifier}/{dataKey}/{timestamp}.json", Body=response.content)

    return raw

handler = Mangum(app)