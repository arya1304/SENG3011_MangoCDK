import os
import re
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from fastapi import APIRouter, HTTPException, Query

TABLE_NAME = os.environ.get("CPI_TABLE_NAME") 
router = APIRouter(prefix="/public")
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME'])

dynamodb = boto3.resource("dynamodb")


def _validate_quarter(value: str, param_name: str) -> str:
    if not re.fullmatch(r"\d{4}-Q[1-4]", value):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} format. Expected YYYY-QN (e.g. 2023-Q1)."
        )
    return value


@router.get("/cpi")
def get_cpi(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /public/cpi?start=2023-Q1&end=2024-Q4
    Retrieve CPI data from DynamoDB for the given quarter range.
    """
    _validate_quarter(start, "start")
    _validate_quarter(end, "end")

    if start > end:
        raise HTTPException(status_code=400, detail="start must not be after end.")

    if not TABLE_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: TABLE_NAME not set.")

    table = dynamodb.Table(TABLE_NAME)

    scan_kwargs = {
        "FilterExpression": Attr("time_period").between(start, end),
    }

    items = []
    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    # Convert Decimal to float for JSON serialisation
    def _to_serialisable(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return obj

    events = [
        {
            "year": item.get("year"),
            "quarter": item.get("quarter"),
            "region": item.get("region"),
            "cpi_value": _to_serialisable(item.get("obs_value")),
        }
        for item in items
    ]

    events.sort(key=lambda e: (e["year"], e["quarter"]))

    return {
        "data_source": "Australian Bureau of Statistics (ABS)",
        "dataset_type": "Government Economic Indicator",
        "events": events,
    }

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