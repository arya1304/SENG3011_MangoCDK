import math
import os
from datetime import datetime, timezone
import boto3
from boto3.dynamodb.conditions import Attr
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
import json
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("CPI_TABLE_NAME")
router = APIRouter(prefix="/public/analysis", tags=["Analysis"])
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME']) # type: ignore
cpi_table = boto3.resource('dynamodb').Table(os.environ['CPI_TABLE_NAME']) # type: ignore
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME']) # type: ignore

dynamodb = boto3.resource("dynamodb")

def _scan_table_filtered(table, start: str, end: str):
    """scan a DynamoDB table with a FilterExpression on time_period range"""
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


@router.get("/cpi-gdp-correlation")
def get_cpi_gdp_correlation(start: str, end: str):
    """
    GET /public/analysis/cpi-gdp-correlation?start=2023-Q1&end=2024-Q4
    Calculate the Pearson correlation coefficient between CPI and GDP
    over a given quarterly time range.
    """
    t0 = time.time()
    # query both tables filtered by time range
    cpi_items = _scan_table_filtered(cpi_table, start, end)
    gdp_items = _scan_table_filtered(gdp_table, start, end)

    if not cpi_items:
        raise HTTPException(status_code=404, detail="No CPI data found")
    if not gdp_items:
        raise HTTPException(status_code=404, detail="No GDP data found")

    cpi_by_period = {}
    for item in cpi_items:
        tp = item.get("time_period", "")
        if item.get("obs_value") is not None:
            cpi_by_period[tp] = float(item["obs_value"])

    gdp_by_period = {}
    for item in gdp_items:
        tp = item.get("time_period", "")
        if item.get("obs_value") is not None:
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
    
    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/analysis/cpi-gdp-correlation",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000),
        "start":       start,   
        "end":         end
    }))

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

def _filter_by_time_period(items, start: str = None, end: str = None):
    """filter items by time_period range"""
    return [
        item for item in items
        if (start is None or item.get("time_period", "") >= start) and
           (end is None or item.get("time_period", "") <= end)
    ]

def _sort_by_time_period(items):
    """sort items by time_period"""
    return sorted(items, key=lambda x: x.get("time_period", ""))

def to_float(value):
    """convert value to float"""
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
 
@router.get("/trend/cpi")
def get_cpi_trend(start: str = None, end: str = None, region: str = None):
    """
    GET /public/analysis/cpi-trend?start=2023-Q1&end=2024-Q4&region=50
    Calculate the trend of CPI over a given quarterly time range, optionally filtered by region.
    Returns the direction of change (growing/shrinking/stable).
    """
    t0 = time.time()
    if start and end:
        items = _scan_table_filtered(cpi_table, start, end)
    else:
        # fallback to full scan when no range provided
        scan_kwargs = {}
        if start:
            scan_kwargs["FilterExpression"] = Attr("time_period").gte(start)
        elif end:
            scan_kwargs["FilterExpression"] = Attr("time_period").lte(end)
        items = []
        while True:
            response = cpi_table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key

    if not items:
        raise HTTPException(status_code=404, detail="No CPI data found")

    if region:
        items = [item for item in items if item.get("region") == region]

    if len(items) < 2:
        raise HTTPException(status_code=400, detail="At least 2 data points are required to calculate trend")
    
    sorted_items = _sort_by_time_period(items)
    trend, summary = _calculate_trend(sorted_items)
    
    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/analysis/cpi-gdp-correlation",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000),
        "start":       start,   
        "end":         end
    }))

    return {
        "analysis_type": "cpi_trend",
        "dataset": "cpi",
        "start": start,
        "end": end,
        "region": region,
        "trend": trend,
        "summary": summary
    }


@router.get("/trend/unemployment")
def get_unemployment_trend(start: str = None, end: str = None, region: str = None):
    """
    GET /public/analysis/trend/unemployment?start=2023-01&end=2024-12&region=AUS
    Calculate the trend of unemployment over a given monthly time range, optionally filtered by region.
    Returns the direction of change (growing/shrinking/stable).
    """
    t0 = time.time()
    if start and end:
        items = _scan_table_filtered(unemployment_table, start, end)
    else:
        scan_kwargs = {}
        if start:
            scan_kwargs["FilterExpression"] = Attr("time_period").gte(start)
        elif end:
            scan_kwargs["FilterExpression"] = Attr("time_period").lte(end)
        items = []
        while True:
            response = unemployment_table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key

    if not items:
        raise HTTPException(status_code=404, detail="No unemployment data found")

    if region:
        items = [item for item in items if item.get("region") == region]

    if len(items) < 2:
        raise HTTPException(status_code=400, detail="At least 2 data points are required to calculate trend")

    sorted_items = _sort_by_time_period(items)
    trend, summary = _calculate_trend(sorted_items)

    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/public/analysis/trend/unemployment",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000)
    }))

    return {
        "analysis_type": "unemployment_trend",
        "dataset": "unemployment",
        "start": start,
        "end": end,
        "region": region,
        "trend": trend,
        "summary": summary
    }


@router.get("/trend/gdp")
def get_gdp_trend(start: str = None, end: str = None, region: str = None):
    """
    GET /public/analysis/trend/gdp?start=2023-Q1&end=2024-Q4&region=AUS
    Calculate the trend of GDP over a given quarterly time range, optionally filtered by region.
    Returns the direction of change (growing/shrinking/stable).
    """
    t0 = time.time()
    
    if start and end:
        items = _scan_table_filtered(gdp_table, start, end)
    else:
        scan_kwargs = {}
        if start:
            scan_kwargs["FilterExpression"] = Attr("time_period").gte(start)
        elif end:
            scan_kwargs["FilterExpression"] = Attr("time_period").lte(end)
        items = []
        while True:
            response = gdp_table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_key

    if not items:
        raise HTTPException(status_code=404, detail="No GDP data found")

    if region:
        items = [item for item in items if item.get("region") == region]

    if len(items) < 2:
        raise HTTPException(status_code=400, detail="At least 2 data points are required to calculate trend")

    sorted_items = _sort_by_time_period(items)
    trend, summary = _calculate_trend(sorted_items)
    
    logger.info(json.dumps({
        "service":     "mango-api",
        "endpoint":    "/analysis/cpi-gdp-correlation",      
        "status":      200,                 
        "duration_ms": int((time.time()-t0)*1000),
        "start":       start,   
        "end":         end
    }))

    return {
        "analysis_type": "gdp_trend",
        "dataset": "gdp",
        "start": start,
        "end": end,
        "region": region,
        "trend": trend,
        "summary": summary
    }


def _get_recent_quarters(n: int) -> list[str]:
    now = datetime.now(timezone.utc)
    quarters = []
    year, quarter = now.year, (now.month - 1) // 3 + 1
    for _ in range(n):
        quarters.append(f"{year}-Q{quarter}")
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return list(reversed(quarters))


def _get_recent_months(n: int) -> list[str]:
    now = datetime.now(timezone.utc)
    months = []
    year, month = now.year, now.month
    for _ in range(n):
        months.append(f"{year}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return list(reversed(months))


def _pct_changes(items: list) -> list[float]:
    sorted_items = sorted(items, key=lambda x: x.get("time_period", ""))
    changes = []
    for i in range(1, len(sorted_items)):
        curr = to_float(sorted_items[i].get("obs_value"))
        prev = to_float(sorted_items[i - 1].get("obs_value"))
        if curr is not None and prev is not None and prev != 0:
            changes.append((curr - prev) / abs(prev) * 100)
    return changes

@router.get("/recession-risk")
def get_recession_risk():
    """
    GET /public/analysis/recession-risk

    Detects recession signals by analysing unemployment trends and inflation.
    Returns a risk level and confidence score.
    """
    t0 = time.time()

    quarters = _get_recent_quarters(8)
    months = _get_recent_months(12)
    q_start, q_end = quarters[0], quarters[-1]
    m_start, m_end = months[0], months[-1]

    cpi_items = _scan_table_filtered(cpi_table, q_start, q_end)
    unemp_items = _scan_table_filtered(unemployment_table, m_start, m_end)

    if not cpi_items and not unemp_items:
        raise HTTPException(status_code=404, detail="No economic data available. Please collect and preprocess CPI and unemployment data first.")

    cpi_changes = _pct_changes(cpi_items)
    unemp_changes = _pct_changes(unemp_items)

    # --- Recession signals ---
    signals = []
    signal_count = 0

    # 1. Rising unemployment
    if unemp_changes:
        avg_unemp_change = sum(unemp_changes) / len(unemp_changes)
        recent_unemp = unemp_changes[-1]
        if avg_unemp_change > 0.5 or recent_unemp > 1.0:
            signals.append({"indicator": "Unemployment", "signal": f"Unemployment rising (avg change: {round(avg_unemp_change, 2)}%)", "severity": "High"})
            signal_count += 2
        elif avg_unemp_change > 0:
            signals.append({"indicator": "Unemployment", "signal": f"Unemployment slightly rising (avg change: {round(avg_unemp_change, 2)}%)", "severity": "Medium"})
            signal_count += 1
        else:
            signals.append({"indicator": "Unemployment", "signal": "Unemployment stable or falling", "severity": "Low"})
    else:
        signals.append({"indicator": "Unemployment", "signal": "Insufficient data", "severity": "Unknown"})

    # 2. High inflation
    if cpi_changes:
        avg_cpi_change = sum(cpi_changes) / len(cpi_changes)
        if avg_cpi_change > 2.0:
            signals.append({"indicator": "Inflation", "signal": f"High inflation (avg change: {round(avg_cpi_change, 2)}%)", "severity": "High"})
            signal_count += 2
        elif avg_cpi_change > 1.0:
            signals.append({"indicator": "Inflation", "signal": f"Elevated inflation (avg change: {round(avg_cpi_change, 2)}%)", "severity": "Medium"})
            signal_count += 1
        else:
            signals.append({"indicator": "Inflation", "signal": "Inflation within normal range", "severity": "Low"})
    else:
        signals.append({"indicator": "Inflation", "signal": "Insufficient data", "severity": "Unknown"})

    # Calculate confidence based on how much data we have
    total_available = 2
    total_with_data = sum(1 for s in signals if s["severity"] != "Unknown")
    data_confidence = total_with_data / total_available

    # Risk level and confidence
    max_signal = 4  # 2 per indicator
    raw_risk = signal_count / max_signal
    confidence = round(data_confidence * (0.6 + 0.4 * min(1.0, (len(cpi_changes) + len(unemp_changes)) / 10)), 2)

    if raw_risk >= 0.5:
        risk_level = "High"
    elif raw_risk >= 0.25:
        risk_level = "Moderate"
    else:
        risk_level = "Low"

    logger.info(json.dumps({
        "service": "mango-api",
        "endpoint": "/public/analysis/recession-risk",
        "status": 200,
        "duration_ms": int((time.time() - t0) * 1000),
    }))

    return {
        "risk_level": risk_level,
        "confidence": confidence,
        "signals": signals,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }