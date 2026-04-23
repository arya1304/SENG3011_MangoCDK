import requests


OMEGA_URL = "https://a683sqnr5m.execute-api.ap-southeast-2.amazonaws.com/visualise"


def test_render_cpi_graph():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "CPI Rendering Test",
            "yAxisTitle": "CPI Value",
            "returnURL": True,
            "datasets": [{
                "datasetName": "CPI",
                "attributeName": "value",
                "events": [
                    {
                        "time_object": {
                            "timestamp": "2023-01-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "cpi",
                        "attribute": {"value": 130.0, "unit": "Index"},
                    },
                    {
                        "time_object": {
                            "timestamp": "2023-04-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "cpi",
                        "attribute": {"value": 132.6, "unit": "Index"},
                    },
                    {
                        "time_object": {
                            "timestamp": "2023-07-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "cpi",
                        "attribute": {"value": 135.2, "unit": "Index"},
                    },
                ],
            }],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "url" in data
    print(f"\nCPI Graph URL: {data['url']}\n")


def test_render_gdp_graph():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "GDP Rendering Test",
            "yAxisTitle": "GDP Value",
            "returnURL": True,
            "datasets": [{
                "datasetName": "GDP",
                "attributeName": "value",
                "events": [
                    {
                        "time_object": {
                            "timestamp": "2023-01-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "gdp",
                        "attribute": {"value": 48016.0},
                    },
                    {
                        "time_object": {
                            "timestamp": "2023-04-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "gdp",
                        "attribute": {"value": 49200.0},
                    },
                    {
                        "time_object": {
                            "timestamp": "2023-07-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "gdp",
                        "attribute": {"value": 50100.0},
                    },
                ],
            }],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "url" in data
    print(f"\nGDP Graph URL: {data['url']}\n")

def test_render_unemployment_graph():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "Unemployment Rendering Test",
            "yAxisTitle": "Unemployment Rate",
            "returnURL": True,
            "datasets": [{
                "datasetName": "Unemployment",
                "attributeName": "value",
                "events": [
                    {
                        "time_object": {
                            "timestamp": "2023-01-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 1,
                            "duration_unit": "month"
                            },
                        "event_type": "unemployment",
                        "attribute": {"value": 3.5, "unit": "%"},
                    },
                    {
                        "time_object": {
                            "timestamp": "2023-02-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 1,
                            "duration_unit": "month"
                            },
                        "event_type": "unemployment",
                        "attribute": {"value": 3.6, "unit": "%"},
                    },
                    {
                        "time_object": {
                            "timestamp": "2023-03-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 1,
                            "duration_unit": "month"
                            },
                        "event_type": "unemployment",
                        "attribute": {"value": 3.4, "unit": "%"},
                    },
                ],
            }],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "url" in data
    print(f"\nUnemployment Graph: {data['url']}\n")

def test_omega_empty_events():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "Empty Test",
            "yAxisTitle": "Value",
            "returnURL": True,
            "datasets": [{
                "datasetName": "CPI",
                "attributeName": "value",
                "events": [],
            }],
        },
    )
    print(f"\nEmpty events - Status: {response.status_code}")
    print(f"Response: {response.text}\n")


def test_omega_single_event():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "Single Point",
            "yAxisTitle": "CPI Value",
            "returnURL": True,
            "datasets": [{
                "datasetName": "CPI",
                "attributeName": "value",
                "events": [{
                    "time_object": {
                        "timestamp": "2023-01-01 00:00:00.0000000",
                        "timezone": "+11:00",
                        "duration": 3,
                        "duration_unit": "month",
                    },
                    "event_type": "cpi",
                    "attribute": {"value": 130.0, "unit": "Index"},
                }],
            }],
        },
    )
    assert response.status_code == 200
    print(f"\nSingle point URL: {response.json().get('url', 'N/A')}\n")


def test_omega_multiple_datasets():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "CPI vs GDP",
            "yAxisTitle": "Value",
            "returnURL": True,
            "datasets": [
                {
                    "datasetName": "CPI",
                    "attributeName": "value",
                    "events": [{
                        "time_object": {
                            "timestamp": "2023-01-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "cpi",
                        "attribute": {"value": 130.0},
                    }, {
                        "time_object": {
                            "timestamp": "2023-04-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "cpi",
                        "attribute": {"value": 132.6},
                    }],
                },
                {
                    "datasetName": "GDP",
                    "attributeName": "value",
                    "events": [{
                        "time_object": {
                            "timestamp": "2023-01-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "gdp",
                        "attribute": {"value": 48016.0},
                    }, {
                        "time_object": {
                            "timestamp": "2023-04-01 00:00:00.0000000",
                            "timezone": "+11:00",
                            "duration": 3,
                            "duration_unit": "month",
                        },
                        "event_type": "gdp",
                        "attribute": {"value": 49200.0},
                    }],
                },
            ],
        },
    )
    assert response.status_code == 200
    print(f"\nMulti-dataset URL: {response.json().get('url', 'N/A')}\n")


def test_omega_base64_response():
    response = requests.post(
        OMEGA_URL,
        json={
            "title": "Base64 Test",
            "yAxisTitle": "Value",
            "returnURL": False,
            "datasets": [{
                "datasetName": "CPI",
                "attributeName": "value",
                "events": [{
                    "time_object": {
                        "timestamp": "2023-01-01 00:00:00.0000000",
                        "timezone": "+11:00",
                        "duration": 3,
                        "duration_unit": "month",
                    },
                    "event_type": "cpi",
                    "attribute": {"value": 130.0},
                }],
            }],
        },
    )
    print(f"\nBase64 - Status: {response.status_code}")
    print(f"Response keys: {list(response.json().keys()) if response.status_code == 200 else response.text}\n")