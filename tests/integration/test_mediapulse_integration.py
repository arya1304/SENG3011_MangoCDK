import requests

MEDIAPULSE_URL = "https://i9pdxmupj7.execute-api.ap-southeast-2.amazonaws.com/api"


def test_mediapulse_sentiment_inflation():
    response = requests.get(
        f"{MEDIAPULSE_URL}/sentiment",
        params={
            "keyword": "inflation",
            "timeframe": "7d",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "averageSentiment" in data
    assert "articleCount" in data
    assert "distribution" in data
    print(f"\nInflation sentiment: {data['averageSentiment']} ({data['articleCount']} articles)\n")


def test_mediapulse_sentiment_unemployment():
    response = requests.get(
        f"{MEDIAPULSE_URL}/sentiment",
        params={
            "keyword": "unemployment",
            "timeframe": "7d",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "averageSentiment" in data
    assert "articleCount" in data
    assert "distribution" in data
    print(f"\nUnemployment sentiment: {data['averageSentiment']} ({data['articleCount']} articles)\n")


def test_mediapulse_sentiment_trend():
    response = requests.get(
        f"{MEDIAPULSE_URL}/sentiment/trend",
        params={
            "keyword": "inflation",
            "timeframe": "7d",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "dataPoints" in data
    assert isinstance(data["dataPoints"], list)
    if data["dataPoints"]:
        for dp in data["dataPoints"]:
            assert "averageSentiment" in dp
    print(f"\nSentiment trend data points: {len(data['dataPoints'])}\n")


def test_mediapulse_volume_trend():
    response = requests.get(
        f"{MEDIAPULSE_URL}/trend",
        params={
            "keyword": "unemployment",
            "timeframe": "7d",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "dataPoints" in data
    assert "totalArticles" in data
    if data["dataPoints"]:
        for dp in data["dataPoints"]:
            assert "articleCount" in dp
    print(f"\nVolume trend - total articles: {data['totalArticles']}\n")


def test_mediapulse_with_source_id():
    response = requests.get(
        f"{MEDIAPULSE_URL}/sentiment",
        params={
            "keyword": "inflation",
            "timeframe": "7d",
            "sourceId": "allSources",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "averageSentiment" in data
    print(f"\nWith sourceId - Status: {response.status_code}\n")


def test_mediapulse_unknown_keyword():
    response = requests.get(
        f"{MEDIAPULSE_URL}/sentiment",
        params={
            "keyword": "zzzz-nonsense-keyword-xyz",
            "timeframe": "7d",
        },
    )
    # unknown keyword should not crash the boundary with a 5xx
    assert response.status_code < 500
    print(f"\nUnknown keyword - Status: {response.status_code}\n")