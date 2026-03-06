import json
import os
import requests
from datetime import datetime

ABS_API_URL = "https://data.api.abs.gov.au/rest/data"
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def get_cpi(event, s3):
    """
    GET / CPI data from ABS API and return
    Query parameters:
    - startPeriod:             e.g., "2023-Q1"
    - endPeriod:               e.g., "2023-Q4"
    - format:                   e.g., "json"
    - detail:                   e.g., "dataonly"
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")

    query_params = event.get("queryStringParameters") or {}
    path_params= event.get("pathParameters") or {}

    dataflow_identifier = path_params.get("dataflowIdentifier")
    data_key = path_params.get("dataKey")

    if not dataflow_identifier or not data_key:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing required path parameters: dataflowIdentifier or dataKey"})
        }
    
    if not BUCKET_NAME:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Server configuration error: BUCKET_NAME not set"})
        }
    
    start_period = query_params.get("startPeriod")
    end_period = query_params.get("endPeriod")
    response_format = query_params.get("format", "jsondata")
    detail = query_params.get("detail", "dataonly")
    abs_query_params = {
        "format": response_format,
        "detail": detail,
    }
    
    if start_period:
        abs_query_params["startPeriod"] = start_period
    if end_period:
        abs_query_params["endPeriod"] = end_period
    
    response = requests.get(f"{ABS_API_URL}/{dataflow_identifier}/{data_key}", params=abs_query_params, timeout=10)
    status_code = response.status_code

    if status_code != 200:
        return {
            "statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"ABS API error: {response.text}"})
        }
    
    raw = response.json()
    
    s3.put_object(Bucket=BUCKET_NAME, Key=f"{dataflow_identifier}/{data_key}/{timestamp}.json", Body=response.content)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(raw)
    }
