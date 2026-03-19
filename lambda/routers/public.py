import math
import os
import re
from decimal import Decimal

import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from fastapi import APIRouter, HTTPException, Query

TABLE_NAME = os.environ.get("CPI_TABLE_NAME")
router = APIRouter(prefix="/public")
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME']) # type: ignore
cpi_table = boto3.resource('dynamodb').Table(os.environ['CPI_TABLE_NAME']) # type: ignore
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME']) # type: ignore

dynamodb = boto3.resource("dynamodb")

############################################################################
# retrieval endpoints

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
        raise HTTPException(status_code=500, detail="Server configuration error: CPI_TABLE_NAME not set.")

    table = dynamodb.Table(TABLE_NAME) # type: ignore

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
def get_unemployment(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /public/unemployment?start=2023-Q1&end=2024-Q4
    Retrieve unemployment data from DynamoDB for the given quarter range.
    """
    _validate_quarter(start, "start")
    _validate_quarter(end, "end")

    if start > end:
        raise HTTPException(status_code=400, detail="start must not be after end.")

    scan_kwargs = {
        "FilterExpression": Attr("time_period").between(start, end),
    }

    items = []
    while True:
        response = unemployment_table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    def _to_serialisable(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return obj

    events = [
        {
            "year": item.get("year"),
            "quarter": item.get("quarter"),
            "region": item.get("region"),
            "unemployment_value": _to_serialisable(item.get("obs_value")),
        }
        for item in items
    ]

    events.sort(key=lambda e: (e["year"], e["quarter"]))

    return {
        "data_source": "Australian Bureau of Statistics (ABS)",
        "dataset_type": "Government Economic Indicator",
        "events": events,
    }

@router.get("/gdp")
def get_gdp(
    start: str,
    end: str,
):
    """
    GET /public/gdp?start=2023-Q1&end=2024-Q4
    Retrieve GDP data from the database
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    result = gdp_table.query(
        KeyConditionExpression=Key("dataset_id").eq("ABS:ANA_IND_GVA(1.0.0)") & Key("time_period").between(start, end)
    )

    items = result["Items"]

    if not items:
        raise HTTPException(status_code=404, detail="No gross domestic product data found")

    events = []
    for item in items:
        events.append({
            "id": item.get("dataset_id"),
            "time_period": item.get("time_period"),
            "source": item.get("data_source"),
            "industry": item.get("industry"),
            "region": item.get("region"),
            "gdp_value": float(item.get("obs_value")) if item.get("obs_value") is not None else None,
            "data_item": item.get("data_item"),
            "adjustment_type": item.get("adjustment_type"),
            "obs_status": item.get("obs_status"),
        })

    return {
        "timestamp": timestamp,
        "dataset": "ABS - Gross Domestic Product (GDP)",
        "count": len(events),
        "events": events,
    }
