import os
import requests
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
import boto3

ABS_API_URL = "https://data.api.abs.gov.au/rest/data"
# BUCKET_NAME = os.environ.get("BUCKET_NAME")

router = APIRouter(prefix="/collect")
s3 = boto3.client('s3')

@router.post("/cpi")
def collect_cpi(
    dataflowIdentifier: str,
    dataKey: str,
    startPeriod: str = None,
    endPeriod: str = None,
    response_format: str = "jsondata",
    detail: str = "dataonly"
    ):
    """
    POST /collect/cpi to get CPI data from ABS API and return
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

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

@router.post("/gdp")
def collect_gdp(
    startPeriod: str = None,
    endPeriod: str = None,
    response_format: str = "jsondata",
    detail: str = "full"
):
    """
    POST /collect/gdp to get gross domestic product data from ABS API and return
    """

    dataflowIdentifier = "ABS,ANA_IND_GVA,1.0.0"
    dataKey = "......Q"

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    # if not BUCKET_NAME:
    #     raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")
    
    abs_query_params = {
        "format": response_format,
        "detail": detail,
        "dimensionAtObservation": "AllDimensions"
    }

    if startPeriod:
        abs_query_params["startPeriod"] = startPeriod
    if endPeriod:
        abs_query_params["endPeriod"] = endPeriod

    response = requests.get(f"{ABS_API_URL}/{dataflowIdentifier}/{dataKey}", params=abs_query_params, timeout=10)

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"ABS API error: {response.text}")
    
    raw = response.json()
    
    # s3.put_object(Bucket=BUCKET_NAME, Key=f"{dataflowIdentifier}/{dataKey}/{timestamp}.json", Body=response.content)

    return raw

@router.post("/stocks")
def collect_stocks():
    """
    POST /collect/stocks to get stock data from Yahoo Finance API and return
    """
    return {"message": "Stock data collected successfully"}

@router.post("/unemployment")
def collect_unemployment():
    """
    POST /collect/unemployment to get unemployment data from ABS API and return
    """
    return {"message": "Unemployment data collected successfully"}


