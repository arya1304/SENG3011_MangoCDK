import os
import boto3
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/public")
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME'])

@router.get("/cpi")
def get_cpi():
    """
    GET /public/cpi?start=2023-Q1&end=2024-Q4
    Retrieve CPI data from the database
    """
    return {"message": "CPI data retrieved successfully"}

@router.get("/unemployment")
def get_unemployment(start: str = None, end: str = None):
    """
    GET /public/unemployment?start=2023-Q1&end=2024-Q4
    Retrieve unemployment data from the database
    """
    response = unemployment_table.scan()
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = unemployment_table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        items.extend(response.get("Items", []))

    if not items:
        raise HTTPException(status_code=404, detail="No unemployment data found")

    # filter by time period range if provided
    if start or end:
        items = [
            item for item in items
            if (not start or item.get("time_period", "") >= start)
            and (not end or item.get("time_period", "") <= end)
        ]

    # JSON serialization
    for item in items:
        if item.get("obs_value") is not None:
            item["obs_value"] = float(item["obs_value"])

    return {"data": items}

@router.get("/gdp")
def get_gdp():
    """
    GET /public/gdp?start=2023-Q1&end=2024-Q4
    Retrieve GDP data from the database
    """
    return {"message": "GDP data retrieved successfully"}