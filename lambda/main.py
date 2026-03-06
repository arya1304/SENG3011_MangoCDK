import json
import os
import boto3

def handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Hello from Mango microservice!"})
    }