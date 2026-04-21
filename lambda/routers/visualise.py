import requests
from fastapi import APIRouter, HTTPException, Query
from requests.exceptions import HTTPError

from routers.public import get_cpi, get_gdp, get_unemployment
from routers.analysis import (
    get_cpi_gdp_correlation,
    get_cpi_trend,
    get_gdp_trend,
    get_unemployment_trend,
)

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
            timeout=60,
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        raise HTTPException(status_code=502, detail=f"OMEGA API error: {str(e)}")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"OMEGA request failed: {str(e)}")


def _quarter_to_timestamp(quarter_str: str) -> str:
    """'2023-Q1' -> '2023-01-01 00:00:00.0000000'"""
    quarter_map = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}
    year, q = quarter_str.split("-")
    month = quarter_map[q]
    return f"{year}-{month}-01 00:00:00.0000000"


def _month_to_timestamp(month_str: str) -> str:
    """'2023-01' -> '2023-01-01 00:00:00.0000000'"""
    return f"{month_str}-01 00:00:00.0000000"


def _time_object_from_period(time_period: str):
    """Build OMEGA time object from YYYY-Q# or YYYY-MM."""
    if "Q" in time_period:
        return {
            "timestamp": _quarter_to_timestamp(time_period),
            "timezone": "+10:00",
            "duration": 3,
            "duration_unit": "month",
        }
    return {
        "timestamp": _month_to_timestamp(time_period),
        "timezone": "+10:00",
        "duration": 1,
        "duration_unit": "month",
    }


def _headline_for_story(indicator_name: str, overall_direction: str) -> str:
    if indicator_name == "CPI":
        if overall_direction == "growing":
            return "Inflation pressure is building"
        if overall_direction == "shrinking":
            return "Inflation pressure is easing"
        return "Inflation remained broadly stable"

    if indicator_name == "Unemployment":
        if overall_direction == "growing":
            return "Labour market conditions are softening"
        if overall_direction == "shrinking":
            return "Labour market conditions are tightening"
        return "Labour market conditions remained stable"

    return f"{indicator_name} story view"


def _build_story_text(summary: dict, indicator_name: str):
    overall = summary.get("overall_direction", "stable")
    avg_change_pct = summary.get("avg_change_pct", 0)
    growing = summary.get("periods_growing", 0)
    shrinking = summary.get("periods_shrinking", 0)
    stable = summary.get("periods_stable", 0)

    headline = _headline_for_story(indicator_name, overall)
    bullets = [
        f"Overall direction: {overall}",
        f"Average change: {avg_change_pct}%",
        f"Growing periods: {growing}",
        f"Shrinking periods: {shrinking}",
        f"Stable periods: {stable}",
    ]
    return headline, bullets


def _build_story_overlay_dataset(
    trend_data: dict,
    dataset_name: str,
    accent_event_type: str = "story_overlay",
):
    """
    Build a main observed-value dataset from trend analysis output.
    This gives the newsroom-style card an actual main chart, not only % changes.
    """
    events = []
    for row in trend_data["trend"]:
        events.append({
            "time_object": _time_object_from_period(row["time_period"]),
            "event_type": accent_event_type,
            "attribute": {
                "value": row["obs_value"],
            },
        })

    return {
        "datasetName": dataset_name,
        "attributeName": "value",
        "events": events,
    }


def _build_change_dataset(
    trend_data: dict,
    dataset_name: str,
    event_type: str = "change_pct",
):
    events = []
    for row in trend_data["trend"]:
        if row["change_pct"] is None:
            continue
        events.append({
            "time_object": _time_object_from_period(row["time_period"]),
            "event_type": event_type,
            "attribute": {
                "value": row["change_pct"],
            },
        })

    return {
        "datasetName": dataset_name,
        "attributeName": "value",
        "events": events,
    }


@router.get("/cpi")
def visualise_cpi(
    start: str = Query(..., description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(..., description="End quarter, e.g. 2024-Q4"),
):
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


@router.get("/unemployment")
def visualise_unemployment(
    start: str = Query(..., description="Start month, e.g. 2023-01"),
    end: str = Query(..., description="End month, e.g. 2024-12"),
):
    unemployment_data = get_unemployment(start=start, end=end)

    omega_events = []
    for event in unemployment_data["events"]:
        omega_events.append({
            "time_object": {
                "timestamp": _month_to_timestamp(event["time_period"]),
                "timezone": "+10:00",
                "duration": 1,
                "duration_unit": "month",
            },
            "event_type": "unemployment",
            "attribute": {
                "value": event["unemployment_value"],
                "unit": event.get("unit_measure", "%"),
            },
        })

    result = visualise(
        title=f"Unemployment ({start} to {end})",
        y_axis_title="Unemployment Rate",
        datasets=[
            {
                "datasetName": "Unemployment",
                "attributeName": "value",
                "events": omega_events,
            }
        ],
    )
    return result


def _build_trend_datasets(trend_data, dataset_name):
    periods = trend_data["trend"]

    change_events = []
    for p in periods:
        if p["change_pct"] is None:
            continue

        time_period = p["time_period"]
        if "Q" in time_period:
            ts = _quarter_to_timestamp(time_period)
            duration, unit = 3, "month"
        else:
            ts = _month_to_timestamp(time_period)
            duration, unit = 1, "month"

        change_events.append({
            "time_object": {
                "timestamp": ts,
                "timezone": "+10:00",
                "duration": duration,
                "duration_unit": unit,
            },
            "event_type": "change_pct",
            "attribute": {"value": p["change_pct"]},
        })

    return [
        {
            "datasetName": f"{dataset_name} Change (%)",
            "attributeName": "value",
            "events": change_events,
        },
    ]


@router.get("/trend/cpi")
def visualise_cpi_trend(
    start: str = Query(None, description="Start month, e.g. 2023-01"),
    end: str = Query(None, description="End month, e.g. 2024-12"),
    region: str = Query(None, description="Region filter (optional)"),
):
    trend_data = get_cpi_trend(start=start, end=end, region=region)
    datasets = _build_trend_datasets(trend_data, "CPI")
    summary = trend_data["summary"]

    result = visualise(
        title=f"CPI Trend ({start} to {end}) - {summary['overall_direction']}, avg {summary['avg_change_pct']}%",
        y_axis_title="Change (%)",
        datasets=datasets,
    )
    return result


@router.get("/trend/gdp")
def visualise_gdp_trend(
    start: str = Query(None),
    end: str = Query(None),
    region: str = Query(None),
):
    trend_data = get_gdp_trend(start=start, end=end, region=region)
    datasets = _build_trend_datasets(trend_data, "GDP")
    summary = trend_data["summary"]

    result = visualise(
        title=f"GDP Trend ({start} to {end}) - {summary['overall_direction']}, avg {summary['avg_change_pct']}%",
        y_axis_title="Change (%)",
        datasets=datasets,
    )
    return result


@router.get("/trend/unemployment")
def visualise_unemployment_trend(
    start: str = Query(None),
    end: str = Query(None),
    region: str = Query(None),
):
    trend_data = get_unemployment_trend(start=start, end=end, region=region)
    datasets = _build_trend_datasets(trend_data, "Unemployment")
    summary = trend_data["summary"]

    result = visualise(
        title=f"Unemployment Trend ({start} to {end}) - {summary['overall_direction']}, avg {summary['avg_change_pct']}%",
        y_axis_title="Change (%)",
        datasets=datasets,
    )
    return result


@router.get("/cpi-story")
def visualise_cpi_story(
    start: str = Query(..., description="Start month, e.g. 2023-01"),
    end: str = Query(..., description="End month, e.g. 2024-12"),
    region: str = Query(None, description="Region filter, e.g. 50"),
):
    """
    Newsroom-style CPI card:
    main observed-value series + change% series,
    with a headline-like title for editorial use.
    """
    trend_data = get_cpi_trend(start=start, end=end, region=region)
    summary = trend_data["summary"]

    headline, bullets = _build_story_text(summary, "CPI")
    bullet_suffix = " | ".join(bullets[:3])

    datasets = [
        _build_story_overlay_dataset(trend_data, "CPI Level"),
        _build_change_dataset(trend_data, "CPI Change (%)"),
    ]

    result = visualise(
        title=f"{headline} · {bullet_suffix}",
        y_axis_title="Observed value / change %",
        datasets=datasets,
    )
    return result


@router.get("/unemployment-story")
def visualise_unemployment_story(
    start: str = Query(..., description="Start month, e.g. 2023-01"),
    end: str = Query(..., description="End month, e.g. 2024-12"),
    region: str = Query(None, description="Region filter, e.g. AUS"),
):
    """
    Newsroom-style unemployment card:
    main observed-value series + change% series,
    with a headline-like title for editorial use.
    """
    trend_data = get_unemployment_trend(start=start, end=end, region=region)
    summary = trend_data["summary"]

    headline, bullets = _build_story_text(summary, "Unemployment")
    bullet_suffix = " | ".join(bullets[:3])

    datasets = [
        _build_story_overlay_dataset(trend_data, "Unemployment Level"),
        _build_change_dataset(trend_data, "Unemployment Change (%)"),
    ]

    result = visualise(
        title=f"{headline} · {bullet_suffix}",
        y_axis_title="Observed value / change %",
        datasets=datasets,
    )
    return result


@router.get("/cost-of-living-comparison")
def visualise_cost_of_living_comparison(
    cpi_start: str = Query(..., description="CPI start month, e.g. 2023-01"),
    cpi_end: str = Query(..., description="CPI end month, e.g. 2024-12"),
    unemployment_start: str = Query(..., description="Unemployment start month, e.g. 2023-01"),
    unemployment_end: str = Query(..., description="Unemployment end month, e.g. 2024-12"),
    cpi_region: str = Query(None, description="CPI region, e.g. 50"),
    unemployment_region: str = Query(None, description="Unemployment region, e.g. AUS"),
):
    """
    Comparison export graphic for journalists:
    CPI and unemployment together for cost-of-living reporting.
    """
    cpi_trend = get_cpi_trend(start=cpi_start, end=cpi_end, region=cpi_region)
    unemployment_trend = get_unemployment_trend(
        start=unemployment_start,
        end=unemployment_end,
        region=unemployment_region,
    )

    cpi_summary = cpi_trend["summary"]
    unemployment_summary = unemployment_trend["summary"]

    cpi_dir = cpi_summary["overall_direction"]
    unemp_dir = unemployment_summary["overall_direction"]

    if cpi_dir == "growing" and unemp_dir == "shrinking":
        headline = "Inflation pressure rises as labour market stays tight"
    elif cpi_dir == "growing" and unemp_dir == "growing":
        headline = "Prices and unemployment rise together"
    elif cpi_dir == "shrinking" and unemp_dir == "growing":
        headline = "Inflation eases as labour market softens"
    else:
        headline = "Macro conditions remain mixed"

    subtitle = (
        f"CPI latest: {cpi_summary.get('avg_change_pct', 0)}% avg change | "
        f"Unemployment latest: {unemployment_summary.get('avg_change_pct', 0)}% avg change"
    )

    datasets = [
        _build_story_overlay_dataset(cpi_trend, "CPI Level"),
        _build_story_overlay_dataset(unemployment_trend, "Unemployment Level"),
        _build_change_dataset(cpi_trend, "CPI Change (%)"),
        _build_change_dataset(unemployment_trend, "Unemployment Change (%)"),
    ]

    result = visualise(
        title=f"{headline} · {subtitle}",
        y_axis_title="Observed value / change %",
        datasets=datasets,
    )
    return result