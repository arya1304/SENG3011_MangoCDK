import json
import os
from datetime import datetime, timezone
import boto3
from fastapi import APIRouter, HTTPException

BUCKET_NAME = os.environ.get("BUCKET_NAME")

router = APIRouter(prefix="/preprocess")

s3 = boto3.client('s3')
table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])

@router.post("/cpi")
def preprocess_cpi(dataflowIdentifier: str, dataKey: str):
    """
    POST /preprocess/cpi to preprocess CPI data and return
    """
    # transforms SDMX raw CPI data -> standardised model
    
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")

    # find the latest file under {dataflowIdentifier}/{dataKey}/
    prefix = f"{dataflowIdentifier}/{dataKey}/"
    listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' not in listing or not listing['Contents']:
        raise HTTPException(status_code=404, detail=f"No CPI data found at s3://{BUCKET_NAME}/{prefix}")

    latest_key = sorted(listing['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    raw = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)['Body'].read())

    # parse raw JSON structure
    structures = raw.get('data', {}).get('structures', [])
    datasets = raw.get('data', {}).get('dataSets', [])

    if not structures or not datasets:
        raise HTTPException(status_code=500, detail="Unexpected SDMX-JSON format in stored file")

    structure = structures[0]
    dataset = datasets[0]

    series_dims = structure.get('dimensions', {}).get('series', [])
    obs_dims = structure.get('dimensions', {}).get('observation', [])

    series_dim_values = [dim.get('values', []) for dim in series_dims]
    obs_dim_values = obs_dims[0].get('values', []) if obs_dims else []

    # format dataflow string: "ABS,CPI,1.0.0" → "ABS:CPI(1.0.0)"
    parts = dataflowIdentifier.split(',')
    if len(parts) == 3:
        dataflow_str = f"{parts[0]}:{parts[1]}({parts[2]})"
    else:
        dataflow_str = dataflowIdentifier

    freq_to_unit = {'Q': 'quarter', 'M': 'month', 'A': 'year', 'S': 'semester'}

    events = []
    for series_key, series_data in dataset.get('series', {}).items():
        # decode (e.g. "0:1:0:0" → {MEASURE:"1", REGION:"AUS", ...})
        dim_indices = [int(i) for i in series_key.split(':')]
        dim_values = {}
        for i, idx in enumerate(dim_indices):
            if i < len(series_dims):
                dim_id = series_dims[i].get('id', f'DIM_{i}')
                values = series_dim_values[i]
                dim_values[dim_id] = values[idx].get('id', str(idx)) if idx < len(values) else str(idx)

        measure = dim_values.get('MEASURE', '1')
        region = dim_values.get('REGION', '')
        freq = dim_values.get('FREQ', 'Q')
        duration_unit = freq_to_unit.get(freq, 'quarter')

        for obs_key, obs_val in series_data.get('observations', {}).items():
            obs_idx = int(obs_key)
            time_period = obs_dim_values[obs_idx].get('id', str(obs_idx)) if obs_idx < len(obs_dim_values) else str(obs_idx)
            value = obs_val[0] if obs_val else None

            events.append({
                "time_object": {
                    "timestamp": time_period,
                    "duration": 1,
                    "duration_unit": duration_unit,
                    "timezone": "GMT+11"
                },
                "event_type": "cpi_observation",
                "attribute": {
                    "dataflow": dataflow_str,
                    "measure": measure,
                    "region": region,
                    "freq": freq,
                    "time_period": time_period,
                    "obs_value": value,
                    "unit_measure": "IDX",
                    "unit_mult": "0",
                    "obs_status": "A"
                }
            })

    result = {
        "data_source": "Australian Bureau of Statistics (ABS)",
        "dataset_type": "Government Economic Indicator",
        "dataset_id": f"https://data.api.abs.gov.au/rest/data/{dataflowIdentifier}/{dataKey}",
        "time_object": {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "timezone": "GMT+11"
        },
        "events": events
    }

    # save preprocessed data back to S3
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"preprocessed/{dataflowIdentifier}/{dataKey}/{timestamp}.json",
        Body=json.dumps(result)
    )

    return result

@router.post("/gdp")
def preprocess_gdp():
    """
    POST /preprocess/gdp to preprocess GDP data and return
    """
    return {"message": "Preprocessing completed"}

@router.post("/unemployment")
def preprocess_unemployment():
    """
    POST /preprocess/unemployment to preprocess unemployment data and return
    """
    return {"message": "Preprocessing completed"}




@router.post("/clean")
def preprocess_clean_cpi(dataflowIdentifier: str, dataKey: str):
    """
    POST /preprocess/clean to clean the data and return
    """
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")

    # find the latest preprocessed cpi file
    prefix = f"preprocessed/{dataflowIdentifier}/{dataKey}/"
    listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' not in listing or not listing['Contents']:
        raise HTTPException(status_code=404, detail=f"No Preprocessed CPI data found at s3://{BUCKET_NAME}/{prefix}")

    latest_key = sorted(listing['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    raw = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)['Body'].read())

    return {"message": "Data cleaning completed"}