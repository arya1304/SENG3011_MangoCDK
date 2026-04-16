import requests
from fastapi import APIRouter, HTTPException, Query
from requests.exceptions import HTTPError

from routers.public import get_cpi, get_gdp
from routers.analysis import get_cpi_gdp_correlation

router = APIRouter(prefix="/visualise", tags=["Visualise"])

OMEGA_URL = "https://a683sqnr5m.execute-api.ap-southeast-2.amazonaws.com/visualise"


def visualise(title, y_axis_title, datasets, return_url=True):
    try:
        response = requests.post(
            OMEGA_URL,
            json={
                "title": title,
                "yAxisTitle": y_axis_title,
                "returnURL": return_url,
                "datasets": datasets,
            },
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"OMEGA API error: {str(e)}")

def _quarter_to_timestamp(quarter_str: str) -> str:
    """'2023-Q1' -> '2023-01-01 00:00:00.0000000'"""
    quarter_map = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}
    year, q = quarter_str.split("-")
    month = quarter_map[q]
    return f"{year}-{month}-01 00:00:00.0000000"

@router.get("/cpi")
def visualise_cpi(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /visualise/cpi?start=2023-Q1&end=2024-Q4
    Visualise CPI data for the given quarter range using OMEGA.
    """
    cpi_data = get_cpi(start=start, end=end)

    omega_events = []
    for event in cpi_data["events"]:
        omega_events.append({
            "time_object": {
            "timestamp": _quarter_to_timestamp(event["time_period"]),
            "timezone": "+11:00",
            "duration": 3,
            "duration_unit": "month",
        },
            "event_type": "cpi",
            "attribute": {
                "value": event["cpi_value"],
                "unit": event.get("unit_measure", "Index"),
            },
        })

    result = visualise(
        title=f"CPI ({start} to {end})",
        y_axis_title="CPI Value",
        datasets=[
            {
                "datasetName": "CPI",
                "attributeName": "value",
                "events": omega_events,
            }
        ],
    )

    return result

@router.get("/gdp")
def visualise_gdp(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /visualise/gdp?start=2023-Q1&end=2024-Q4
    Visualise GDP data for the given quarter range using OMEGA.
    """
    gdp_data = get_gdp(start=start, end=end)

    omega_events = []
    for event in gdp_data["events"]:
        omega_events.append({
            "time_object": {
                "timestamp": _quarter_to_timestamp(event["time_period"]),
                "timezone": "+11:00",
                "duration": 3,
                "duration_unit": "month",
            },
            "event_type": "gdp",
            "attribute": {
                "value": event["gdp_value"],
                "unit": event.get("unit_measure", ""),
            },
        })

    result = visualise(
        title=f"GDP ({start} to {end})",
        y_axis_title="GDP Value",
        datasets=[
            {
                "datasetName": "GDP",
                "attributeName": "value",
                "events": omega_events,
            }
        ],
    )

    return result

@router.get("/cpi-gdp-correlation")
def visualise_cpi_gdp_correlation(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
    """
    GET /visualise/cpi-gdp-correlation?start=2023-Q1&end=2024-Q4
    Visualise CPI and GDP on the same graph (normalised to base 100),
    with the Pearson correlation coefficient shown in the title.
    """
    correlation_result = get_cpi_gdp_correlation(start=start, end=end)
    coef = correlation_result["correlation_coefficient"]
    interpretation = correlation_result["interpretation"]

    cpi_data = get_cpi(start=start, end=end)
    gdp_data = get_gdp(start=start, end=end)

    def _normalize(events, value_key):
        if not events:
            return []
        base = events[0][value_key]
        return [
            {
                "time_object": {
                    "timestamp": _quarter_to_timestamp(e["time_period"]),
                    "timezone": "+11:00",
                    "duration": 3,
                    "duration_unit": "month",
                },
                "event_type": "normalized",
                "attribute": {
                    "value": (e[value_key] / base) * 100,
                },
            }
            for e in events
        ]

    cpi_events = _normalize(cpi_data["events"], "cpi_value")
    gdp_events = _normalize(gdp_data["events"], "gdp_value")

    result = visualise(
        title=f"CPI vs GDP ({start} to {end}) - Correlation: {coef} ({interpretation})",
        y_axis_title="Index (Base = 100)",
        datasets=[
            {
                "datasetName": "CPI",
                "attributeName": "value",
                "events": cpi_events,
            },
            {
                "datasetName": "GDP",
                "attributeName": "value",
                "events": gdp_events,
            },
        ],
    )

    return result