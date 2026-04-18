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