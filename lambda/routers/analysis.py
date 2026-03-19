import os
import boto3
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/public/analysis")

cpi_table = boto3.resource('dynamodb').Table(os.environ['CPI_TABLE_NAME'])
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME'])
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME'])

# helper function to retrieve data from DynamoDB tables
def _scan_table(table):
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return items

# helper function to filter items by time_period range
def _filter_by_time_period(items, start: str = None, end: str = None):
    return [
        item for item in items
        if (start is None or item.get("time_period", "") >= start) and
           (end is None or item.get("time_period", "") <= end)
    ]

# helper function to sort items by time_period
def _sort_by_time_period(items):
    return sorted(items, key=lambda x: x.get("time_period", ""))

# helper function to convert value to float
def to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def _calculate_trend(sorted_items):
    periods = []
    valid_changes = []
 
    for i, item in enumerate(sorted_items):
        current_val = to_float(item.get("obs_value"))
        time_period = item.get("time_period", "")
 
        entry = {
            "time_period": time_period,
            "obs_value": current_val,
            "change": None,
            "change_pct": None,
            "direction": None,
        }
 
        if i > 0 and current_val is not None:
            prev_val = to_float(sorted_items[i - 1].get("obs_value"))
            if prev_val is not None:
                change = current_val - prev_val
                change_pct = (change / abs(prev_val) * 100) if prev_val != 0 else None
 
                if change > 0:
                    direction = "growing"
                elif change < 0:
                    direction = "shrinking"
                else:
                    direction = "stable"
 
                entry["change"] = round(change, 2)
                entry["change_pct"] = round(change_pct, 2) if change_pct is not None else None
                entry["direction"] = direction
                valid_changes.append(change_pct if change_pct is not None else 0)
 
        periods.append(entry)
 
    if valid_changes:
        avg_change_pct = sum(valid_changes) / len(valid_changes)
        positive = sum(1 for c in valid_changes if c > 0)
        negative = sum(1 for c in valid_changes if c < 0)
 
        if positive > negative:
            overall_direction = "growing"
        elif negative > positive:
            overall_direction = "shrinking"
        else:
            overall_direction = "stable"
 
        summary = {
            "overall_direction": overall_direction,
            "avg_change_pct": round(avg_change_pct, 2),
            "periods_growing": positive,
            "periods_shrinking": negative,
            "periods_stable": len(valid_changes) - positive - negative,
            "total_periods": len(sorted_items),
        }
    else:
        summary = {
            "overall_direction": "insufficient_data",
            "avg_change_pct": None,
            "periods_growing": 0,
            "periods_shrinking": 0,
            "periods_stable": 0,
            "total_periods": len(sorted_items),
        }
 
    return periods, summary
 
@router.get("/ananlysis/cpi-trend")
def get_cpi_trend(start: str = None, end: str = None, region: str = None):
    items = _scan_table(cpi_table)
    if not items:
        raise HTTPException(status_code=404, detail="No CPI data found")
    
    if region:
        items = [item for item in items if item.get("region") == region]

    items = _filter_by_time_period(items, start, end)

    if len(items) < 2:
        raise HTTPException(status_code=400, detail="At least 2 data points are required to calculate trend")
    
    sorted_items = _sort_by_time_period(items)
    trend, summary = _calculate_trend(sorted_items)
    return {"trend": trend, "summary": summary}