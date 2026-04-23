# UC1 — David: quarterly GDP release coverage
# UC2 — Smriti: cost-of-living breakdown (CPI + unemployment + correlation)
# UC3 — Arya: recession signal + public sentiment
# UC4 — Jimin: CPI & unemployment trends + media sentiment for opinion piece

import uuid
import pytest
import requests
 
BASE_URL = "https://x9rgu2z2vh.execute-api.us-east-1.amazonaws.com/prod"

TEST_PASSWORD = "E2E-Test-Pass-123!"
 
#  auth fixtures — one persona per user story, created once per session
def _register_and_login(name: str, email: str) -> dict:
    """
    Ensure the persona account exists (register once, or skip the 409 if
    already registered), then log in to get a fresh token, and finally
    call /auth/details to confirm the token is accepted.
    Returns {email, token, headers, name}.
    """
    # register — 200 first time, 409 on re-runs (both are fine)
    r = requests.post(
        f"{BASE_URL}/auth/register",
        json={"email": email, "password": TEST_PASSWORD, "name": name},
    )
    assert r.status_code in (200, 409), (
        f"register failed: {r.status_code} {r.text}"
    )
 
    # login — always fresh token
    r = requests.post(
        f"{BASE_URL}/auth/login",
        json={"email": email, "password": TEST_PASSWORD},
    )
    assert r.status_code == 200, f"login failed: {r.status_code} {r.text}"
    login_body = r.json()
    token = login_body["token"]
    headers = {"Authorization": f"Bearer {token}"}
 
    # confirm token works
    r = requests.get(f"{BASE_URL}/auth/details", headers=headers)
    assert r.status_code == 200, f"/auth/details failed: {r.status_code} {r.text}"
    details = r.json()["user"]
    assert details["email"] == email
 
    return {"email": email, "token": token, "headers": headers, "name": name}
 
 
@pytest.fixture(scope="session")
def david():
    """UC1 persona — senior economics correspondent."""
    return _register_and_login("David", "e2e-david@example.com")
 
 
@pytest.fixture(scope="session")
def smriti():
    """UC2 persona — data journalist."""
    return _register_and_login("Smriti", "e2e-smriti@example.com")
 
 
@pytest.fixture(scope="session")
def arya():
    """UC3 persona — freelance financial journalist."""
    return _register_and_login("Arya", "e2e-arya@example.com")
 
 
@pytest.fixture(scope="session")
def jimin():
    """UC4 persona — junior economics reporter."""
    return _register_and_login("Jimin", "e2e-jimin@example.com")
 
 
# UC1 — David: Covering a Quarterly GDP Release
def test_uc1_gdp_release_coverage(david):
    """
    User story:
      David opens the dashboard and, within seconds, needs the latest
      GDP figures, the trend direction, and a downloadable chart.
 
    Flow:
      1. auth (fixture)
      2. GET /public/gdp                   — raw figures
      3. GET /public/analysis/trend/gdp    — direction + summary
      4. GET /visualise/gdp                — chart URL
    """
    headers = david["headers"]
 
    # 2. raw GDP figures
    r = requests.get(
        f"{BASE_URL}/public/gdp",
        params={"start": "2023-Q1", "end": "2024-Q4"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    gdp = r.json()
    assert gdp["dataset_id"] == "ABS:ANA_IND_GVA"
    assert gdp["count"] == len(gdp["events"])
    assert len(gdp["events"]) > 0
    sample = gdp["events"][0]
    assert "gdp_value" in sample
    assert "time_period" in sample
 
    # 3. trend direction
    r = requests.get(
        f"{BASE_URL}/public/analysis/trend/gdp",
        params={"start": "2023-Q1", "end": "2024-Q4"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    trend = r.json()
    assert trend["analysis_type"] == "gdp_trend"
    assert trend["summary"]["overall_direction"] in {
        "growing", "shrinking", "stable", "insufficient_data",
    }
 
    # 4. visual chart for the article
    r = requests.get(
        f"{BASE_URL}/visualise/gdp",
        params={"start": "2023-Q1", "end": "2024-Q4"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    viz = r.json()
    assert "url" in viz
    print(f"\n[UC1] GDP chart: {viz['url']}")
 
# UC2 — Smriti: Cost-of-living Breakdown
def test_uc2_cost_of_living_breakdown(smriti):
    """
    User story:
      Smriti wants several years of CPI + unemployment in a consistent
      ADAGE-compliant format, plus the CPI↔GDP correlation to back
      her 'decoupling' narrative.
 
    Flow:
      1. auth (fixture)
      2. GET /public/cpi
      3. GET /public/unemployment
      4. GET /public/analysis/cpi-gdp-correlation
    """
    headers = smriti["headers"]
 
    # 2. multi-year CPI
    r = requests.get(
        f"{BASE_URL}/public/cpi",
        params={"start": "2019-Q1", "end": "2024-Q4"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    cpi = r.json()
    # ADAGE 3.0-compliant envelope
    assert cpi["data_source"] == "Australian Bureau of Statistics (ABS)"
    assert cpi["dataset_type"] == "Government Economic Indicator"
    assert cpi["dataset_id"] == "ABS:CPI"
    assert cpi["count"] == len(cpi["events"])
    assert len(cpi["events"]) > 0
    assert "cpi_value" in cpi["events"][0]
 
    # 3. multi-year unemployment
    r = requests.get(
        f"{BASE_URL}/public/unemployment",
        params={"start": "2019-01", "end": "2024-12"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    unemp = r.json()
    assert unemp["dataset_id"] == "ABS:LF"
    assert len(unemp["events"]) > 0
    assert "unemployment_value" in unemp["events"][0]
 
    # 4. correlation supporting the "decoupling" claim
    r = requests.get(
        f"{BASE_URL}/public/analysis/cpi-gdp-correlation",
        params={"start": "2019-Q1", "end": "2024-Q4"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    corr = r.json()
    assert corr["analysis_type"] == "pearson_correlation"
    assert set(corr["datasets"]) == {"CPI", "GDP"}
    assert -1.0 <= corr["correlation_coefficient"] <= 1.0
    assert corr["interpretation"] in {
        "strong positive", "moderate positive", "weak positive",
        "weak negative", "moderate negative", "strong negative",
    }
    print(
        f"\n[UC2] CPI↔GDP correlation: {corr['correlation_coefficient']} "
        f"({corr['interpretation']}) over {corr['num_data_points']} quarters"
    )
 
 
# UC3 — Arya: Recession Signal for News Article
 
def test_uc3_recession_risk_with_sentiment(arya):
    """
    User story:
      Arya wants an at-a-glance recession-risk score paired with
      public sentiment so she can frame her article around both
      hard data and the public mood.
 
    Flow:
      1. auth (fixture)
      2. GET /public/analysis/recession-risk           — risk + signals
      3. GET /public/analysis/media/context?keyword=unemployment — sentiment
    """
    headers = arya["headers"]
 
    # 2. recession-risk index
    r = requests.get(
        f"{BASE_URL}/public/analysis/recession-risk",
        headers=headers,
    )
    assert r.status_code == 200, r.text
    risk = r.json()
    assert risk["risk_level"] in {"Low", "Moderate", "High"}
    assert 0.0 <= risk["confidence"] <= 1.0
    # three contributing indicators: Unemployment, Inflation, GDP
    indicators = {s["indicator"] for s in risk["signals"]}
    assert {"Unemployment", "Inflation", "GDP"} <= indicators
    for signal in risk["signals"]:
        assert signal["severity"] in {"Low", "Medium", "High", "Unknown"}
 
    # 3. public sentiment to add the human dimension
    r = requests.get(
        f"{BASE_URL}/public/analysis/media/context",
        params={"keyword": "unemployment", "timeframe": "7d"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    media = r.json()
    assert media["keyword"] == "unemployment"
    assert media["sentiment"]["label"] in {"positive", "negative", "neutral"}
    assert "headline" in media["story"]
    assert "summary" in media["story"]
 
    print(
        f"\n[UC3] Risk: {risk['risk_level']} (conf {risk['confidence']}) | "
        f"Unemployment sentiment: {media['sentiment']['label']} "
        f"({media['sentiment']['articleCount']} articles)"
    )
 
 
# UC4 — Jimin: Comparing Public Sentiment Across Indicators
def test_uc4_cost_of_living_opinion_piece(jimin):
    """
    User story:
      Jimin wants CPI and unemployment trend charts plus media
      sentiment for both, so she can write a data-driven opinion
      piece ahead of the federal budget.
 
    Flow:
      1. auth (fixture)
      2. GET /visualise/trend/cpi              — CPI chart + trend
      3. GET /visualise/trend/unemployment     — unemployment chart + trend
      4. GET /public/analysis/media/context?keyword=inflation
      5. GET /public/analysis/media/context?keyword=unemployment
    """
    headers = jimin["headers"]
 
    # 2. CPI trend chart
    r = requests.get(
        f"{BASE_URL}/visualise/trend/cpi",
        params={"start": "2023-Q1", "end": "2024-Q4"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    cpi_viz = r.json()
    assert "url" in cpi_viz
    print(f"\n[UC4] CPI trend chart: {cpi_viz['url']}")
 
    # 3. unemployment trend chart
    r = requests.get(
        f"{BASE_URL}/visualise/trend/unemployment",
        params={"start": "2023-01", "end": "2024-12"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    unemp_viz = r.json()
    assert "url" in unemp_viz
    print(f"[UC4] Unemployment trend chart: {unemp_viz['url']}")
 
    # 4. media sentiment for inflation
    r = requests.get(
        f"{BASE_URL}/public/analysis/media/context",
        params={"keyword": "inflation", "timeframe": "7d"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    inflation_media = r.json()
    assert inflation_media["keyword"] == "inflation"
    assert "sentiment" in inflation_media
    assert "coverage" in inflation_media
 
    # 5. media sentiment for unemployment
    r = requests.get(
        f"{BASE_URL}/public/analysis/media/context",
        params={"keyword": "unemployment", "timeframe": "7d"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    unemp_media = r.json()
    assert unemp_media["keyword"] == "unemployment"
 
    print(
        f"[UC4] Inflation sentiment: {inflation_media['sentiment']['label']} | "
        f"Unemployment sentiment: {unemp_media['sentiment']['label']}"
    )

# # tests/e2e/test_e2e.py
# import requests

# BASE_URL = "https://x9rgu2z2vh.execute-api.us-east-1.amazonaws.com/prod"
# OMEGA_URL = "https://a683sqnr5m.execute-api.ap-southeast-2.amazonaws.com/visualise"

# QUARTER_MAP = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}


# def _quarter_to_timestamp(q):
#     year, quarter = q.split("-")
#     return f"{year}-{QUARTER_MAP[quarter]}-01 00:00:00.0000000"


# def _month_to_timestamp(m):
#     return f"{m}-01 00:00:00.0000000"


# def test_e2e_cpi():
#     """E2E: deployed API → DynamoDB → OMEGA → CPI graph"""
#     resp = requests.get(f"{BASE_URL}/public/cpi?start=2023-Q1&end=2023-Q4")
#     assert resp.status_code == 200
#     events = resp.json()["events"]
#     assert len(events) > 0

#     omega_events = [{
#         "time_object": {"timestamp": _quarter_to_timestamp(e["time_period"]), "timezone": "+11:00", "duration": 3, "duration_unit": "month"},
#         "event_type": "cpi",
#         "attribute": {"value": e["cpi_value"], "unit": e.get("unit_measure", "Index")},
#     } for e in events]

#     graph = requests.post(OMEGA_URL, json={
#         "title": "E2E: CPI 2023",
#         "yAxisTitle": "CPI Value",
#         "returnURL": True,
#         "datasets": [{"datasetName": "CPI", "attributeName": "value", "events": omega_events}],
#     })
#     assert graph.status_code == 200
#     data = graph.json()
#     assert "url" in data
#     print(f"\nE2E CPI {data['url']}\n")


# # def test_e2e_gdp():
# #     """E2E: deployed API → DynamoDB → OMEGA → GDP graph"""
# #     resp = requests.get(f"{BASE_URL}/public/gdp?start=2023-Q1&end=2023-Q4")
# #     assert resp.status_code == 200
# #     events = resp.json()["events"]
# #     assert len(events) > 0

# #     omega_events = [{
# #         "time_object": {"timestamp": _quarter_to_timestamp(e["time_period"]), "timezone": "+11:00", "duration": 3, "duration_unit": "month"},
# #         "event_type": "gdp",
# #         "attribute": {"value": e["gdp_value"], "unit": e.get("unit_measure", "")},
# #     } for e in events]

# #     graph = requests.post(OMEGA_URL, json={
# #         "title": "E2E: GDP 2023",
# #         "yAxisTitle": "GDP Value",
# #         "returnURL": True,
# #         "datasets": [{"datasetName": "GDP", "attributeName": "value", "events": omega_events}],
# #     })
# #     assert graph.status_code == 200
# #     data = graph.json()
# #     assert "url" in data
# #     print(f"\nE2E GDP {data['url']}\n")


# def test_e2e_unemployment():
#     """E2E: deployed API → DynamoDB → OMEGA → Unemployment graph"""
#     resp = requests.get(f"{BASE_URL}/public/unemployment?start=2023-01&end=2023-12")
#     assert resp.status_code == 200
#     events = resp.json()["events"]
#     assert len(events) > 0

#     omega_events = [{
#         "time_object": {"timestamp": _month_to_timestamp(e["time_period"]), "timezone": "+11:00", "duration": 1, "duration_unit": "month"},
#         "event_type": "unemployment",
#         "attribute": {"value": e["unemployment_value"], "unit": e.get("unit_measure", "%")},
#     } for e in events]

#     graph = requests.post(OMEGA_URL, json={
#         "title": "E2E: Unemployment 2023",
#         "yAxisTitle": "Unemployment Rate",
#         "returnURL": True,
#         "datasets": [{"datasetName": "Unemployment", "attributeName": "value", "events": omega_events}],
#     })
#     assert graph.status_code == 200
#     data = graph.json()
#     assert "url" in data
#     print(f"\nE2E Unemployment {data['url']}\n")