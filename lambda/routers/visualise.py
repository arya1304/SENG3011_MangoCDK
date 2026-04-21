import requests
from fastapi import APIRouter, HTTPException, Query
from requests.exceptions import HTTPError
import io
import base64
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

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


def _plot_story_card(points, title, headline, bullets, color, unit_label):
    fig = plt.figure(figsize=(12, 7), dpi=160)
    fig.patch.set_facecolor("#0d1117")

    # Background canvas
    ax_bg = fig.add_axes([0, 0, 1, 1])
    ax_bg.set_axis_off()

    # Main card
    card = FancyBboxPatch(
        (0.03, 0.05), 0.94, 0.90,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.2,
        edgecolor="#1e2d3d",
        facecolor="#111820",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(card)

    # Data prep
    x = list(range(len(points)))
    labels = [p["time_period"] for p in points]
    y = [p["obs_value"] for p in points]

    latest_value = y[-1]
    start_value = y[0]
    total_change = latest_value - start_value
    direction = "Rising" if total_change > 0 else "Falling" if total_change < 0 else "Stable"

    max_idx = y.index(max(y))
    min_idx = y.index(min(y))

    # Header
    fig.text(0.06, 0.90, title, color="white", fontsize=21, fontweight="bold")
    fig.text(0.06, 0.855, headline, color=color, fontsize=13, fontweight="bold")

    # Latest value box
    latest_box = FancyBboxPatch(
        (0.06, 0.68), 0.22, 0.12,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.0,
        edgecolor="#2a3f55",
        facecolor="#0d1117",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(latest_box)

    fig.text(0.075, 0.765, "LATEST VALUE", color="#7d92a6", fontsize=9, fontweight="bold")
    fig.text(0.075, 0.715, f"{latest_value:.2f} {unit_label}", color="white", fontsize=24, fontweight="bold")

    # Direction badge
    badge_color = "#00ffa3" if direction == "Rising" else "#ff6677" if direction == "Falling" else "#ffd166"
    badge_box = FancyBboxPatch(
        (0.30, 0.705), 0.12, 0.05,
        boxstyle="round,pad=0.01,rounding_size=0.03",
        linewidth=0.8,
        edgecolor=badge_color,
        facecolor="#0d1117",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(badge_box)
    fig.text(0.322, 0.72, direction.upper(), color=badge_color, fontsize=10, fontweight="bold")

    # Bullet insight box
    insight_box = FancyBboxPatch(
        (0.06, 0.42), 0.32, 0.20,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.0,
        edgecolor="#2a3f55",
        facecolor="#0d1117",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(insight_box)

    fig.text(0.075, 0.585, "KEY TAKEAWAYS", color="#7d92a6", fontsize=9, fontweight="bold")
    for i, bullet in enumerate(bullets[:3]):
        fig.text(0.078, 0.545 - i * 0.045, f"• {bullet}", color="#c8d8e8", fontsize=10)

    # Main chart
    ax = fig.add_axes([0.43, 0.19, 0.50, 0.62])
    ax.set_facecolor("#111820")
    ax.plot(x, y, color=color, linewidth=2.8, marker="o", markersize=4)
    ax.fill_between(x, y, [min(y)] * len(y), color=color, alpha=0.08)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=8, color="#9fb2c4")
    ax.tick_params(axis="y", colors="#9fb2c4", labelsize=9)
    ax.grid(True, linestyle="--", alpha=0.15)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#334455")
    ax.spines["bottom"].set_color("#334455")

    # Annotations
    ax.scatter([0], [y[0]], color=color, s=45, zorder=5)
    ax.scatter([len(y)-1], [y[-1]], color=color, s=45, zorder=5)
    ax.scatter([max_idx], [y[max_idx]], color="#ffffff", s=28, zorder=5)
    ax.scatter([min_idx], [y[min_idx]], color="#ffffff", s=28, zorder=5)

    ax.annotate(
        f"Start {y[0]:.2f}",
        (0, y[0]),
        xytext=(8, 10),
        textcoords="offset points",
        color="white",
        fontsize=8
    )
    ax.annotate(
        f"End {y[-1]:.2f}",
        (len(y)-1, y[-1]),
        xytext=(8, -14),
        textcoords="offset points",
        color="white",
        fontsize=8
    )
    ax.annotate(
        f"Max {max(y):.2f}",
        (max_idx, y[max_idx]),
        xytext=(8, 8),
        textcoords="offset points",
        color="white",
        fontsize=8
    )
    ax.annotate(
        f"Min {min(y):.2f}",
        (min_idx, y[min_idx]),
        xytext=(8, -14),
        textcoords="offset points",
        color="white",
        fontsize=8
    )

    # Footer
    fig.text(0.06, 0.09, "Source: ABS · Generated by Mangonomics", color="#6f8294", fontsize=9)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)

    encoded = base64.b64encode(buf.read()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"

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

@router.get("/unemployment")
def visualise_unemployment(
    start: str = Query(..., description="Start month, e.g. 2023-01"),
    end: str = Query(..., description="End month, e.g. 2024-12"),
):
    """
    GET /visualise/unemployment?start=2023-01&end=2024-12
    Visualise unemployment data for the given month range using OMEGA.
    """
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
    start: str = Query(None, description="Start quarter, e.g. 2023-Q1"),
    end: str = Query(None, description="End quarter, e.g. 2024-Q4"),
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
def visualise_cpi_story(start: str, end: str, region: str = None):
    trend_data = get_cpi_trend(start=start, end=end, region=region)
    summary = trend_data["summary"]

    headline, bullets = _build_story_text(summary, "CPI")

    image = _plot_story_card(
        trend_data["trend"],
        title="Inflation Snapshot",
        headline=headline,
        bullets=bullets,
        color="#00e5ff",
        unit_label="IDX",
    )

    return {"url": image}

@router.get("/unemployment-story")
def visualise_unemployment_story(start: str, end: str, region: str = None):
    trend_data = get_unemployment_trend(start=start, end=end, region=region)
    summary = trend_data["summary"]

    headline, bullets = _build_story_text(summary, "Unemployment")

    image = _plot_story_card(
        trend_data["trend"],
        title="Labour Market Snapshot",
        headline=headline,
        bullets=bullets,
        color="#00ffa3",
        unit_label="%",
    )

    return {"url": image}

@router.get("/cost-of-living-comparison")
def visualise_cost_of_living_comparison(
    cpi_start: str,
    cpi_end: str,
    unemployment_start: str,
    unemployment_end: str,
):
    cpi = get_cpi_trend(start=cpi_start, end=cpi_end)
    unemp = get_unemployment_trend(start=unemployment_start, end=unemployment_end)

    cpi_summary = cpi["summary"]
    unemp_summary = unemp["summary"]

    cpi_vals = [p["obs_value"] for p in cpi["trend"]]
    cpi_labels = [p["time_period"] for p in cpi["trend"]]
    unemp_vals = [p["obs_value"] for p in unemp["trend"]]
    unemp_labels = [p["time_period"] for p in unemp["trend"]]

    cpi_dir = cpi_summary["overall_direction"]
    unemp_dir = unemp_summary["overall_direction"]

    if cpi_dir == "growing" and unemp_dir == "shrinking":
        comparison_headline = "Inflation pressure rises as labour market stays tight"
    elif cpi_dir == "growing" and unemp_dir == "growing":
        comparison_headline = "Prices and unemployment rise together"
    elif cpi_dir == "shrinking" and unemp_dir == "growing":
        comparison_headline = "Inflation eases as labour market softens"
    else:
        comparison_headline = "Macro conditions remain mixed"

    fig = plt.figure(figsize=(13, 8), dpi=160)
    fig.patch.set_facecolor("#0d1117")

    ax_bg = fig.add_axes([0, 0, 1, 1])
    ax_bg.set_axis_off()

    card = FancyBboxPatch(
        (0.03, 0.05), 0.94, 0.90,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.2,
        edgecolor="#1e2d3d",
        facecolor="#111820",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(card)

    # Title + headline
    fig.text(0.06, 0.91, "Cost of Living Snapshot", color="white", fontsize=22, fontweight="bold")
    fig.text(0.06, 0.865, comparison_headline, color="#ffd166", fontsize=13, fontweight="bold")

    # CPI latest box
    cpi_box = FancyBboxPatch(
        (0.06, 0.74), 0.18, 0.10,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.0,
        edgecolor="#2a3f55",
        facecolor="#0d1117",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(cpi_box)
    fig.text(0.075, 0.807, "LATEST CPI", color="#7d92a6", fontsize=9, fontweight="bold")
    fig.text(0.075, 0.765, f"{cpi_vals[-1]:.2f} IDX", color="#00e5ff", fontsize=19, fontweight="bold")

    # Unemployment latest box
    unemp_box = FancyBboxPatch(
        (0.27, 0.74), 0.18, 0.10,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.0,
        edgecolor="#2a3f55",
        facecolor="#0d1117",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(unemp_box)
    fig.text(0.285, 0.807, "LATEST UNEMPLOYMENT", color="#7d92a6", fontsize=9, fontweight="bold")
    fig.text(0.285, 0.765, f"{unemp_vals[-1]:.2f} %", color="#00ffa3", fontsize=19, fontweight="bold")

    # Bullet insights
    bullet_box = FancyBboxPatch(
        (0.06, 0.54), 0.39, 0.14,
        boxstyle="round,pad=0.012,rounding_size=0.02",
        linewidth=1.0,
        edgecolor="#2a3f55",
        facecolor="#0d1117",
        transform=ax_bg.transAxes
    )
    ax_bg.add_patch(bullet_box)

    bullets = [
        f"CPI direction: {cpi_dir}",
        f"Unemployment direction: {unemp_dir}",
        f"CPI avg change: {cpi_summary.get('avg_change_pct', 0)}% | Unemployment avg change: {unemp_summary.get('avg_change_pct', 0)}%",
    ]
    fig.text(0.075, 0.648, "EDITORIAL TAKEAWAYS", color="#7d92a6", fontsize=9, fontweight="bold")
    for i, bullet in enumerate(bullets):
        fig.text(0.078, 0.615 - i * 0.04, f"• {bullet}", color="#c8d8e8", fontsize=10)

    # CPI chart
    ax1 = fig.add_axes([0.52, 0.54, 0.40, 0.27])
    ax1.set_facecolor("#111820")
    x1 = list(range(len(cpi_vals)))
    ax1.plot(x1, cpi_vals, color="#00e5ff", linewidth=2.5, marker="o", markersize=4)
    ax1.fill_between(x1, cpi_vals, [min(cpi_vals)] * len(cpi_vals), color="#00e5ff", alpha=0.08)
    ax1.set_title("CPI", color="white", fontsize=12, loc="left")
    ax1.set_xticks(x1)
    ax1.set_xticklabels(cpi_labels, rotation=30, ha="right", fontsize=8, color="#9fb2c4")
    ax1.tick_params(axis="y", colors="#9fb2c4", labelsize=8)
    ax1.grid(True, linestyle="--", alpha=0.14)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines["left"].set_color("#334455")
    ax1.spines["bottom"].set_color("#334455")
    ax1.annotate(f"{cpi_vals[-1]:.2f}", (x1[-1], cpi_vals[-1]), xytext=(8, -12), textcoords="offset points", color="white", fontsize=8)

    # Unemployment chart
    ax2 = fig.add_axes([0.52, 0.18, 0.40, 0.27])
    ax2.set_facecolor("#111820")
    x2 = list(range(len(unemp_vals)))
    ax2.plot(x2, unemp_vals, color="#00ffa3", linewidth=2.5, marker="o", markersize=4)
    ax2.fill_between(x2, unemp_vals, [min(unemp_vals)] * len(unemp_vals), color="#00ffa3", alpha=0.08)
    ax2.set_title("Unemployment", color="white", fontsize=12, loc="left")
    ax2.set_xticks(x2)
    ax2.set_xticklabels(unemp_labels, rotation=30, ha="right", fontsize=8, color="#9fb2c4")
    ax2.tick_params(axis="y", colors="#9fb2c4", labelsize=8)
    ax2.grid(True, linestyle="--", alpha=0.14)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.spines["left"].set_color("#334455")
    ax2.spines["bottom"].set_color("#334455")
    ax2.annotate(f"{unemp_vals[-1]:.2f}", (x2[-1], unemp_vals[-1]), xytext=(8, -12), textcoords="offset points", color="white", fontsize=8)

    # Footer
    fig.text(0.06, 0.10, "Source: ABS · Generated by Mangonomics", color="#6f8294", fontsize=9)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)

    encoded = base64.b64encode(buf.read()).decode("utf-8")
    return {"url": f"data:image/png;base64,{encoded}"}