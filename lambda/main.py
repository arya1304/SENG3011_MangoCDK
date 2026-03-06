import json
import os
import boto3
from abs.cpi import get_cpi

s3 = boto3.client('s3')

def handler(event, context):
    path = event.get("path", "")

    if path == "/cpi":
        return get_cpi(event, s3)

    return {
        "statusCode": 404,
        "body": json.dumps({"error": "Endpoint not found"})
    }
    
    # return {    
    #     "statusCode": 200,
    #     "body": json.dumps({"message": "Hello from Mango microservice!"})
    # }