import math
import os
import re
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from fastapi import APIRouter, HTTPException, Query

TABLE_NAME = os.environ.get("CPI_TABLE_NAME") 
router = APIRouter(prefix="/public")
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME'])
cpi_table = boto3.resource('dynamodb').Table(os.environ['CPI_TABLE_NAME'])
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME'])

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
def get_gdp():
    """
    GET /public/gdp?start=2023-Q1&end=2024-Q4
    Retrieve GDP data from the database
    """
    return {"message": "GDP data retrieved successfully"}

############################################################################
# analysis endpoints

def _scan_table(table):
    """helper to scan an entire DynamoDB table"""
    response = table.scan()
    items = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return items


def _pearson_correlation(x, y):
    """calculate pearson correlation coefficient between two lists"""
    n = len(x)
    if n < 2:
        return None

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

    if denom_x == 0 or denom_y == 0:
        return None

    return numerator / (denom_x * denom_y)


@router.get("/analysis/cpi_gdp_correlation")
def get_cpi_gdp_correlation(start: str, end: str):
    """
    GET /public/analysis/correlation?start=2023-Q1&end=2024-Q4
    Calculate the Pearson correlation coefficient between CPI and GDP
    over a given quarterly time range.
    """
    # scan both tables
    cpi_items = _scan_table(cpi_table)
    gdp_items = _scan_table(gdp_table)

    if not cpi_items:
        raise HTTPException(status_code=404, detail="No CPI data found")
    if not gdp_items:
        raise HTTPException(status_code=404, detail="No GDP data found")

    # filter by time range 
    cpi_by_period = {}
    for item in cpi_items:
        tp = item.get("time_period", "")
        if tp >= start and tp <= end and item.get("obs_value") is not None:
            cpi_by_period[tp] = float(item["obs_value"])

    gdp_by_period = {}
    for item in gdp_items:
        tp = item.get("time_period", "")
        if tp >= start and tp <= end and item.get("obs_value") is not None:
            gdp_by_period[tp] = float(item["obs_value"])

    # find quarters that exist in both datasets
    common_periods = sorted(set(cpi_by_period.keys()) & set(gdp_by_period.keys()))

    if len(common_periods) < 2:
        raise HTTPException(
            status_code=400,
            detail=f"require at least 2 overlapping quarters to calculate correlation, found {len(common_periods)}"
        )

    cpi_values = [cpi_by_period[p] for p in common_periods]
    gdp_values = [gdp_by_period[p] for p in common_periods]

    correlation = _pearson_correlation(cpi_values, gdp_values)

    if correlation is None:
        raise HTTPException(status_code=400, detail="unable to calculate correlation")

    return {
        "analysis_type": "pearson_correlation",
        "datasets": ["CPI", "GDP"],
        "start": start,
        "end": end,
        "num_data_points": len(common_periods),
        "correlation_coefficient": round(correlation, 4),
        "interpretation": (
            "strong positive" if correlation >= 0.7
            else "moderate positive" if correlation >= 0.3
            else "weak positive" if correlation >= 0
            else "weak negative" if correlation >= -0.3
            else "moderate negative" if correlation >= -0.7
            else "strong negative"
        ),
    }