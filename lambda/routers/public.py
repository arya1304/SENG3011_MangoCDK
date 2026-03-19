import os
import boto3
from fastapi import APIRouter, HTTPException
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

import boto3


router = APIRouter(prefix="/public")
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME'])
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME'])

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
def get_gdp(
    start: str,
    end: str,
):
    """
    GET /public/gdp?start=2023-Q1&end=2024-Q4
    Retrieve GDP data from the database
    """

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    
    # if start and end:
    result = gdp_table.query(
        KeyConditionExpression = Key("dataset_id").eq("ABS:ANA_IND_GVA(1.0.0)") & Key("time_period").between(start, end)
    )
    
    # elif start:
    #     result = gdp_table.query(
    #         KeyConditionExpression = Key("dataset_id").eq("ABS:ANA_IND_GVA(1.0.0)") & Key("time_period").gte(start)
    #     )

    # elif end:
    #     result = gdp_table.query(
    #         KeyConditionExpression = Key("dataset_id").eq("ABS:ANA_IND_GVA(1.0.0)") & Key("time_period").lte(end)
    #     )
    
    # else:
    #     result = gdp_table.query(
    #         KeyConditionExpression=Key("dataset_id").eq("ABS:ANA_IND_GVA(1.0.0)")
    #     )

    items = result["Items"]

    if not items:
        raise HTTPException(status_code=404, detail="No gross domestic product data found")
    

    events = []

    for item in items:
        # if item.get("obs_value") is not None:
        #     item["obs_value"] = float(item["obs_value"])

        time_period = item.get("time_period")
        events.append({
            "id": item.get("dataset_id"),
            "time_period": time_period,
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
        "events": events
    }