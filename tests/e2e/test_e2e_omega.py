# tests/e2e/test_e2e.py
import requests

BASE_URL = "https://x9rgu2z2vh.execute-api.us-east-1.amazonaws.com/prod"
OMEGA_URL = "https://a683sqnr5m.execute-api.ap-southeast-2.amazonaws.com/visualise"

QUARTER_MAP = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}


def _quarter_to_timestamp(q):
    year, quarter = q.split("-")
    return f"{year}-{QUARTER_MAP[quarter]}-01 00:00:00.0000000"


def _month_to_timestamp(m):
    return f"{m}-01 00:00:00.0000000"


def test_e2e_cpi():
    """E2E: deployed API → DynamoDB → OMEGA → CPI graph"""
    resp = requests.get(f"{BASE_URL}/public/cpi?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200
    events = resp.json()["events"]
    assert len(events) > 0

    omega_events = [{
        "time_object": {"timestamp": _quarter_to_timestamp(e["time_period"]), "timezone": "+11:00", "duration": 3, "duration_unit": "month"},
        "event_type": "cpi",
        "attribute": {"value": e["cpi_value"], "unit": e.get("unit_measure", "Index")},
    } for e in events]

    graph = requests.post(OMEGA_URL, json={
        "title": "E2E: CPI 2023",
        "yAxisTitle": "CPI Value",
        "returnURL": True,
        "datasets": [{"datasetName": "CPI", "attributeName": "value", "events": omega_events}],
    })
    assert graph.status_code == 200
    data = graph.json()
    assert "url" in data
    print(f"\nE2E CPI {data['url']}\n")


def test_e2e_gdp():
    """E2E: deployed API → DynamoDB → OMEGA → GDP graph"""
    resp = requests.get(f"{BASE_URL}/public/gdp?start=2023-Q1&end=2023-Q4")
    assert resp.status_code == 200
    events = resp.json()["events"]
    assert len(events) > 0

    omega_events = [{
        "time_object": {"timestamp": _quarter_to_timestamp(e["time_period"]), "timezone": "+11:00", "duration": 3, "duration_unit": "month"},
        "event_type": "gdp",
        "attribute": {"value": e["gdp_value"], "unit": e.get("unit_measure", "")},
    } for e in events]

    graph = requests.post(OMEGA_URL, json={
        "title": "E2E: GDP 2023",
        "yAxisTitle": "GDP Value",
        "returnURL": True,
        "datasets": [{"datasetName": "GDP", "attributeName": "value", "events": omega_events}],
    })
    assert graph.status_code == 200
    data = graph.json()
    assert "url" in data
    print(f"\nE2E GDP {data['url']}\n")


def test_e2e_unemployment():
    """E2E: deployed API → DynamoDB → OMEGA → Unemployment graph"""
    resp = requests.get(f"{BASE_URL}/public/unemployment?start=2023-01&end=2023-12")
    assert resp.status_code == 200
    events = resp.json()["events"]
    assert len(events) > 0

    omega_events = [{
        "time_object": {"timestamp": _month_to_timestamp(e["time_period"]), "timezone": "+11:00", "duration": 1, "duration_unit": "month"},
        "event_type": "unemployment",
        "attribute": {"value": e["unemployment_value"], "unit": e.get("unit_measure", "%")},
    } for e in events]

    graph = requests.post(OMEGA_URL, json={
        "title": "E2E: Unemployment 2023",
        "yAxisTitle": "Unemployment Rate",
        "returnURL": True,
        "datasets": [{"datasetName": "Unemployment", "attributeName": "value", "events": omega_events}],
    })
    assert graph.status_code == 200
    data = graph.json()
    assert "url" in data
    print(f"\nE2E Unemployment {data['url']}\n")