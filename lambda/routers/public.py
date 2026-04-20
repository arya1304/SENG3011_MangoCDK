import os
import re
from datetime import datetime, timezone
from decimal import Decimal
import json
import time
import logging

import boto3
from boto3.dynamodb.conditions import Attr
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/public", tags=["Public"])
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME']) # type: ignore
cpi_table = boto3.resource('dynamodb').Table(os.environ['CPI_TABLE_NAME']) # type: ignore
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME']) # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)


dynamodb = boto3.resource("dynamodb")

############################################################################
# helpers

def _to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


def _validate_quarter(value: str, param_name: str) -> str:
    if not re.fullmatch(r"\d{4}-Q[1-4]", value):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} format. Expected YYYY-QN (e.g. 2023-Q1)."
        )
    return value


def _validate_month(value: str, param_name: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} format. Expected YYYY-MM (e.g. 2023-01)."
        )
    return value


def _validate_year(value: str, param_name: str) -> str:
    if not re.fullmatch(r"\d{4}", value):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {param_name} format. Expected YYYY (e.g. 2023)."
        )
    return value


def _scan_with_filter(table, start: str, end: str) -> list:
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
    return items


def _build_response(dataset_id: str, start: str, end: str, events: list) -> dict:
    return {
        "data_source": "Australian Bureau of Statistics (ABS)",
        "dataset_type": "Government Economic Indicator",
        "dataset_id": dataset_id,
        "query": {
            "start": start,
            "end": end,
        },
        "count": len(events),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "events": events,
    }


############################################################################
# retrieval endpoints

@router.get("/cpi")
def get_cpi(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /public/cpi?start=2023-Q1&end=2024-Q4
    Retrieve CPI data from DynamoDB for the given quarter range.
    """
    t0 = time.time()
    _validate_quarter(start, "start")
    _validate_quarter(end, "end")

    if start > end:
        raise HTTPException(status_code=400, detail="start must not be after end.")

    items = _scan_with_filter(cpi_table, start, end)

    events = [
        {
            "time_period": item.get("time_period"),
            "year": item.get("year"),
            "quarter": item.get("quarter"),
            "region": item.get("region"),
            "cpi_value": _to_float(item.get("obs_value")),
            "unit_measure": item.get("unit_measure"),
            "obs_status": item.get("obs_status"),
            "freq": item.get("freq"),
        }
        for item in items
    ]

    events.sort(key=lambda e: e.get("time_period", ""))
    
    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/analysis/cpi-gdp-correlation",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000),
        "start":       start,   
        "end":         end
    }))

    return _build_response(
        dataset_id="ABS:CPI",
        start=start,
        end=end,
        events=events,
    )


@router.get("/unemployment")
def get_unemployment(
    start: str = Query(..., description="Start month, e.g. 2023-01"),
    end: str = Query(..., description="End month, e.g. 2024-12"),
):
    """
    GET /public/unemployment?start=2023-01&end=2024-12
    Retrieve unemployment data from DynamoDB for the given month range.
    """
    t0 = time.time()
    _validate_month(start, "start")
    _validate_month(end, "end")

    if start > end:
        raise HTTPException(status_code=400, detail="start must not be after end.")

    items = _scan_with_filter(unemployment_table, start, end)

    events = [
        {
            "time_period": item.get("time_period"),
            "year": item.get("year"),
            "month": item.get("time_period", "").split("-")[1] if "-" in item.get("time_period", "") else None,
            "region": item.get("region"),
            "unemployment_value": _to_float(item.get("obs_value")),
            "unit_measure": item.get("unit_measure"),
            "obs_status": item.get("obs_status"),
            "freq": item.get("freq"),
        }
        for item in items
    ]

    events.sort(key=lambda e: e.get("time_period", ""))
    
    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/analysis/cpi-gdp-correlation",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000),
        "start":       start,   
        "end":         end
    }))

    return _build_response(
        dataset_id="ABS:LF",
        start=start,
        end=end,
        events=events,
    )


@router.get("/gdp")
def get_gdp(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /public/gdp?start=2023-Q1&end=2024-Q4
    Retrieve GDP data from DynamoDB for the given quarter range.
    """
    t0 = time.time()
    _validate_quarter(start, "start")
    _validate_quarter(end, "end")

    if start > end:
        raise HTTPException(status_code=400, detail="start must not be after end.")

    items = _scan_with_filter(gdp_table, start, end)

    events = [
        {
            "time_period": item.get("time_period"),
            "region": item.get("region"),
            "gdp_value": _to_float(item.get("obs_value")),
            "industry": item.get("industry"),
            "data_item": item.get("data_item"),
            "adjustment_type": item.get("adjustment_type"),
            "unit_measure": item.get("unit_measure"),
            "obs_status": item.get("obs_status"),
            "freq": item.get("freq"),
        }
        for item in items
    ]

    events.sort(key=lambda e: e.get("time_period", ""))
    
    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/analysis/cpi-gdp-correlation",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000),
        "start":       start,   
        "end":         end
    }))

    return _build_response(
        dataset_id="ABS:ANA_IND_GVA",
        start=start,
        end=end,
        events=events,
    )


@router.get("/indicators")
def get_indicators(
    start_year: str = Query(None, description="Start year, e.g. 2022"),
    end_year: str = Query(None, description="End year, e.g. 2024"),
):
    """
    GET /public/indicators?start_year=2022&end_year=2024
    Retrieve CPI, unemployment, and GDP data in a single response.
    start_year and end_year are optional; omitting them returns all available data.
    """
    t0 = time.time()

    if start_year:
        _validate_year(start_year, "start_year")
    if end_year:
        _validate_year(end_year, "end_year")
    if start_year and end_year and start_year > end_year:
        raise HTTPException(status_code=400, detail="start_year must not be after end_year.")

    # derive per-indicator range strings
    cpi_gdp_start = f"{start_year}-Q1" if start_year else None
    cpi_gdp_end = f"{end_year}-Q4" if end_year else None
    unemp_start = f"{start_year}-01" if start_year else None
    unemp_end = f"{end_year}-12" if end_year else None

    def _full_scan(table):
        items = []
        scan_kwargs = {}
        while True:
            response = table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key
        return items

    # CPI
    if cpi_gdp_start and cpi_gdp_end:
        cpi_items = _scan_with_filter(cpi_table, cpi_gdp_start, cpi_gdp_end)
    else:
        cpi_items = _full_scan(cpi_table)

    cpi_events = [
        {
            "time_period": item.get("time_period"),
            "year": item.get("year"),
            "quarter": item.get("quarter"),
            "region": item.get("region"),
            "cpi_value": _to_float(item.get("obs_value")),
            "unit_measure": item.get("unit_measure"),
            "obs_status": item.get("obs_status"),
            "freq": item.get("freq"),
        }
        for item in cpi_items
    ]
    cpi_events.sort(key=lambda e: e.get("time_period", ""))

    # Unemployment
    if unemp_start and unemp_end:
        unemp_items = _scan_with_filter(unemployment_table, unemp_start, unemp_end)
    else:
        unemp_items = _full_scan(unemployment_table)

    unemp_events = [
        {
            "time_period": item.get("time_period"),
            "year": item.get("year"),
            "month": item.get("time_period", "").split("-")[1] if "-" in item.get("time_period", "") else None,
            "region": item.get("region"),
            "unemployment_value": _to_float(item.get("obs_value")),
            "unit_measure": item.get("unit_measure"),
            "obs_status": item.get("obs_status"),
            "freq": item.get("freq"),
        }
        for item in unemp_items
    ]
    unemp_events.sort(key=lambda e: e.get("time_period", ""))

    # GDP
    if cpi_gdp_start and cpi_gdp_end:
        gdp_items = _scan_with_filter(gdp_table, cpi_gdp_start, cpi_gdp_end)
    else:
        gdp_items = _full_scan(gdp_table)

    gdp_events = [
        {
            "time_period": item.get("time_period"),
            "region": item.get("region"),
            "gdp_value": _to_float(item.get("obs_value")),
            "industry": item.get("industry"),
            "data_item": item.get("data_item"),
            "adjustment_type": item.get("adjustment_type"),
            "unit_measure": item.get("unit_measure"),
            "obs_status": item.get("obs_status"),
            "freq": item.get("freq"),
        }
        for item in gdp_items
    ]
    gdp_events.sort(key=lambda e: e.get("time_period", ""))

    logger.info(json.dumps({
        "service": "mango-api",
        "endpoint": "/public/indicators",
        "status": 200,
        "duration_ms": int((time.time() - t0) * 1000),
        "start_year": start_year,
        "end_year": end_year,
    }))

    return {
        "start_year": start_year,
        "end_year": end_year,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cpi": _build_response(
            dataset_id="ABS:CPI",
            start=cpi_gdp_start,
            end=cpi_gdp_end,
            events=cpi_events,
        ),
        "unemployment": _build_response(
            dataset_id="ABS:LF",
            start=unemp_start,
            end=unemp_end,
            events=unemp_events,
        ),
        "gdp": _build_response(
            dataset_id="ABS:ANA_IND_GVA",
            start=cpi_gdp_start,
            end=cpi_gdp_end,
            events=gdp_events,
        ),
    }
