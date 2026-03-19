import json
import os
from datetime import datetime, timezone
from decimal import Decimal
import boto3
from fastapi import APIRouter, HTTPException

BUCKET_NAME = os.environ.get("BUCKET_NAME")
router = APIRouter(prefix="/preprocess")

s3 = boto3.client('s3')
cpi_table = boto3.resource('dynamodb').Table(os.environ['CPI_TABLE_NAME'])
unemployment_table = boto3.resource('dynamodb').Table(os.environ['UNEMPLOYMENT_TABLE_NAME'])
gdp_table = boto3.resource('dynamodb').Table(os.environ['GDP_TABLE_NAME'])

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

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    # transforms ABS raw GDP data -> standardised model
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")

    dataflowIdentifier = "ABS,ANA_IND_GVA,1.0.0"
    dataKey = "......Q"

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
    
    # build dimension lookup table
    dimensions = {}
    for dim in structure["dimensions"]["observation"]:
        position = dim["keyPosition"]
        dimensions[position] = {
            i: v["id"] for i, v in enumerate(dim["values"])
        }
    
    # build attribute lookup table
    attributes = {}
    for i, attr in enumerate(structure["attributes"]["observation"]):
        attributes[i] = {
            j: v["id"] for j, v in enumerate(attr["values"])
        }

    events = []
    for obs_key, obs_value in dataset["observations"].items():
        obs_indices = [int(i) for i in obs_key.split(":")]

        measure     = dimensions[0][obs_indices[0]]
        data_item   = dimensions[1][obs_indices[1]]
        sector      = dimensions[2][obs_indices[2]]
        tsest       = dimensions[3][obs_indices[3]]
        industry    = dimensions[4][obs_indices[4]]
        region      = dimensions[5][obs_indices[5]]
        freq        = dimensions[6][obs_indices[6]]
        time_period = dimensions[7][obs_indices[7]]

        value = obs_value[0]
        unit_measure = attributes[0].get(obs_value[1]) if obs_value[1] is not None else None
        unit_mult    = attributes[1].get(obs_value[2]) if obs_value[2] is not None else None
        obs_status   = attributes[2].get(obs_value[3]) if obs_value[3] is not None else None

        events.append({
            "time_object": {
                "timestamp": time_period,
                "duration": 1,
                "duration_unit": "quarter",
                "timezone": "GMT+11"
            },
            "event_type": "gdp_observation",
            "attribute": {
                "dataflow": "ABS:ANA_IND_GVA(1.0.0)",
                "measure": measure,
                "data_item": data_item,
                "sector": sector,
                "adjustment_type": tsest,
                "industry": industry,
                "region": region,
                "freq": freq,
                "time_period": time_period,
                "obs_value": value,
                "unit_measure": unit_measure,
                "unit_mult": unit_mult,
                "obs_status": obs_status,
            }
        })

    result = {
        "data_source": "Australian Bureau of Statistics (ABS)",
        "dataset_type": "Government Economic Indicator",
        "dataset_id": f"https://data.api.abs.gov.au/rest/data/{dataflowIdentifier}/{dataKey}",
        "time_object": {
            "timestamp": timestamp,
            "timezone": "GMT+11"
        },
        "events": events
    }


    # send preprocessed data back to S3
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"preprocessed/{dataflowIdentifier}/{dataKey}/{timestamp}.json",
        Body=json.dumps(result)
    )

    return result

@router.post("/unemployment")
def preprocess_unemployment(dataflowIdentifier: str, dataKey: str):
    """
    POST /preprocess/unemployment to preprocess unemployment data and return
    """
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")

    prefix = f"{dataflowIdentifier}/{dataKey}/"
    listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' not in listing or not listing['Contents']:
        raise HTTPException(status_code=404, detail=f"No Unemployment data found at s3://{BUCKET_NAME}/{prefix}")
    
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
    series_attrs = structure.get('attributes', {}).get('series', [])

    series_dim_values = [dim.get('values', []) for dim in series_dims]
    obs_dim_values = obs_dims[0].get('values', []) if obs_dims else []
    serries_attr_values = [attr.get('values', []) for attr in series_attrs]
    
    parts = dataflowIdentifier.split(',')
    if len(parts) == 3:
        dataflow_str = f"{parts[0]}:{parts[1]}({parts[2]})"
    else:
        dataflow_str = dataflowIdentifier
    freq_to_unit = {'Q': 'quarter', 'M': 'month', 'A': 'year', 'S': 'semester'}

    events = []
    for series_key, series_data in dataset.get('series', {}).items():
        dim_indices = [int(i) for i in series_key.split(':')]
        dim_values = {}
        for i, idx in enumerate(dim_indices):
            if i < len(series_dims):
                dim_id = series_dims[i].get('id', f'DIM_{i}')
                values = series_dim_values[i]
                dim_values[dim_id] = values[idx].get('id', str(idx)) if idx < len(values) else str(idx)

        measure = dim_values.get('MEASURE', '')
        sex = dim_values.get('SEX', '')
        age = dim_values.get('AGE', '')
        tsest = dim_values.get('TSEST', '')
        region = dim_values.get('REGION', '')
        freq = dim_values.get('FREQ', '')
        duration_unit = freq_to_unit.get(freq, 'quarter')

        attr_indces = series_data.get('attributes', [])
        unit_measure = None
        unit_mult = None
        for i, attr_idx in enumerate(attr_indces):
            if attr_idx is not None and i < len(series_attrs):
                attr_values = serries_attr_values[i]
                if attr_idx < len(attr_values):
                    val = attr_values[attr_idx].get('id', str(attr_idx))
                    if i < len(series_attrs) and series_attrs[i].get('id') == 'UNIT_MEASURE':
                        unit_measure = val
                    elif i < len(series_attrs) and series_attrs[i].get('id') == 'UNIT_MULT':
                        unit_mult = val

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
                "event_type": "unemployment_observation",
                "attribute": {
                    "dataflow": dataflow_str,
                    "measure": measure,
                    "sex": sex,
                    "age": age,
                    "adjustment_type": tsest,
                    "region": region,
                    "freq": freq,
                    "time_period": time_period,
                    "obs_value": value,
                    "unit_measure": unit_measure,
                    "unit_mult": unit_mult,
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


    # send preprocessed data back to S3
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"preprocessed/{dataflowIdentifier}/{dataKey}/{timestamp}.json",
        Body=json.dumps(result)
    )

    return result


@router.post("/cleanCpi")
def preprocess_clean_cpi(dataflowIdentifier: str, dataKey: str):
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="Server configuration error: BUCKET_NAME not set")
    
    # find the latest preprocessed cpi file
    prefix = f"preprocessed/{dataflowIdentifier}/{dataKey}/"
    listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if 'Contents' not in listing or not listing['Contents']:
        raise HTTPException(status_code=404, detail=f"No Preprocessed CPI data found at s3://{BUCKET_NAME}/{prefix}")

    latest_key = sorted(listing['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    raw = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)['Body'].read())

    dataset_id = raw.get("dataset_id")
    data_source = raw.get("data_source")

    events = raw.get("events", [])
    if not events:
        raise HTTPException(status_code=404, detail="No events found in preprocessed data")
    
    # loop through the events of the data model and store it inside the db

    # CPI
    for event in events:
        attribute = event.get("attribute", {})

        time_period = attribute.get("time_period", "")
        parts = time_period.split("-")
        obs_value = attribute.get("obs_value")

        each_row = {
            "region": attribute.get("region"),
            "time_period": time_period,
            "year": parts[0] if len(parts) > 0 else None,
            "quarter": parts[1] if len(parts) > 1 else None,
            "dataset_id": dataset_id,
            "obs_value": Decimal(str(obs_value)) if obs_value is not None else None,  
            "obs_status": attribute.get("obs_status"),
            "freq": attribute.get("freq"),
            "unit_measure": attribute.get("unit_measure"),
            "data_source": data_source,
        }

        cpi_table.put_item(Item=each_row)  

    # # GDP
    # # find the latest preprocessed gdp file
    # gdp_prefix = "preprocessed/ABS,ANA_IND_GVA,1.0.0/......Q/"
    # gdp_listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=gdp_prefix)

    # if 'Contents' not in gdp_listing or not gdp_listing['Contents']:
    #     raise HTTPException(status_code=404, detail=f"No Preprocessed GDP data found at s3://{BUCKET_NAME}/{prefix}")

    # gdp_latest_key = sorted(listing['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    # gdp_raw = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key=gdp_latest_key)['Body'].read())

    # gdp_dataset_id = gdp_raw.get("dataset_id")
    # gdp_data_source = gdp_raw.get("data_source")

    # events = gdp_raw.get("events", [])
    # if not events:
    #     raise HTTPException(status_code=404, detail="No events found in preprocessed data")
    
    # for event in events:
    #     attribute = event.get("attribute", {})

    #     time_period = attribute.get("time_period", "")
    #     parts = time_period.split("-")
    #     obs_value = attribute.get("obs_value")

    #     each_row = {
    #         "dataset_id": gdp_dataset_id,
    #         "data_source": gdp_data_source,
    #         "year": parts[0] if len(parts) > 0 else None,
    #         "quarter": parts[1] if len(parts) > 1 else None,
    #         "industry": attribute.get("industry"),
    #         "region": attribute.get("region"),
    #         "time_period": attribute.get("time_period"),
    #         "obs_value": attribute.get("obs_value"),
    #         "data_item": attribute.get("data_item"),
    #         "adjustment_type": attribute.get("adjustment_type"),
    #         "obs_status":attribute.get("obs_status"),
    #     }
    #     gdp_table.put_item(Item=each_row)  
    
    
    return {"data": raw}

@router.post("/cleanGdp")
def preprocess_clean_gdp(dataflowIdentifier: str, dataKey: str):
    # GDP
    # find the latest preprocessed gdp file
    gdp_prefix = f"preprocessed/{dataflowIdentifier}/{dataKey}/"
    gdp_listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=gdp_prefix)

    if 'Contents' not in gdp_listing or not gdp_listing['Contents']:
        raise HTTPException(status_code=404, detail=f"No Preprocessed GDP data found at s3://{BUCKET_NAME}/{ gdp_prefix}")

    gdp_latest_key = sorted(gdp_listing['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    gdp_raw = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key=gdp_latest_key)['Body'].read())

    gdp_dataset_id = gdp_raw.get("dataset_id")
    gdp_data_source = gdp_raw.get("data_source")

    events = gdp_raw.get("events", [])
    if not events:
        raise HTTPException(status_code=404, detail="No events found in preprocessed data")
    
    for event in events:
        attribute = event.get("attribute", {})

        time_period = attribute.get("time_period", "")
        parts = time_period.split("-")
        obs_value = attribute.get("obs_value")
        
        each_row = {
            "dataset_id": gdp_dataset_id,
            "data_source": gdp_data_source,
            "year": parts[0] if len(parts) > 0 else None,
            "quarter": parts[1] if len(parts) > 1 else None,
            "industry": attribute.get("industry"),
            "region": attribute.get("region"),
            "time_period": attribute.get("time_period"),
            "obs_value": Decimal(str(obs_value)) if obs_value is not None else None,
            "data_item": attribute.get("data_item"),
            "adjustment_type": attribute.get("adjustment_type"),
            "obs_status":attribute.get("obs_status"),
        }
        gdp_table.put_item(Item=each_row)  
    
    
    return {"data": gdp_raw}



@router.post("/cleanUnemployment")
def preprocess_clean_gdp(dataflowIdentifier: str, dataKey: str):
    # GDP
    # find the latest preprocessed gdp file
    unemployment_prefix = f"preprocessed/{dataflowIdentifier}/{dataKey}/"
    unemployment_listing = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=unemployment_prefix)

    if 'Contents' not in unemployment_listing or not unemployment_listing['Contents']:
        raise HTTPException(status_code=404, detail=f"No Preprocessed unemployment data found at s3://{BUCKET_NAME}/{ unemployment_prefix}")

    unemployment_latest_key = sorted(unemployment_listing['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    unemployment_raw = json.loads(s3.get_object(Bucket=BUCKET_NAME, Key=unemployment_latest_key)['Body'].read())

    unemployment_dataset_id = unemployment_raw.get("dataset_id")
    unemployment_data_source = unemployment_raw.get("data_source")

    events = unemployment_raw.get("events", [])
    if not events:
        raise HTTPException(status_code=404, detail="No events found in preprocessed data")
    
    for event in events:
        attribute = event.get("attribute", {})

        time_period = attribute.get("time_period", "")
        parts = time_period.split("-")

        each_row = {
            "dataset_id": unemployment_dataset_id,
            "data_source": unemployment_data_source,
            "year": parts[0] if len(parts) > 0 else None,
            "sex": attribute.get("sex"),
            "age":  attribute.get("age"),
            "adjustment_type":  attribute.get("adjustment_type"),
            "region": attribute.get("region"),
            "measure": attribute.get("measure")
        }
        unemployment_table.put_item(Item=each_row)  
    
    
    return {"data": unemployment_raw}