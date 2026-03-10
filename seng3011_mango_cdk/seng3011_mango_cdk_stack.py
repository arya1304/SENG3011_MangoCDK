import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct

ACCOUNT_ID = "083483698371"

class Seng3011MangoCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id,
            synthesizer=cdk.LegacyStackSynthesizer(),
            **kwargs
        )

        lab_role = iam.Role.from_role_arn(self, "LabRole",
            f"arn:aws:iam::{ACCOUNT_ID}:role/LabRole"
        )

        # Your app's shared data bucket
        app_bucket = s3.Bucket.from_bucket_name(self, "MangoSharedBucket",
            f"mango-shared-bucket-{ACCOUNT_ID}"
        )

        # Dedicated assets bucket for Lambda code
        assets_bucket = s3.Bucket.from_bucket_name(self, "AssetsBucket",
            f"cdk-assets-{ACCOUNT_ID}-us-east-1"
        )

        # Lambda loaded from assets bucket
        main_function = _lambda.Function(self, "MainFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=_lambda.Code.from_bucket(assets_bucket, "lambda.zip"),
            role=lab_role,
            environment={
                "BUCKET_NAME": app_bucket.bucket_name
            }
        )

        # New CPI Lambda
        cpi_function = _lambda.Function(self, "CpiFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="cpi.handler",
            code=_lambda.Code.from_bucket(assets_bucket, "lambda.zip"),
            role=lab_role,
            environment={"BUCKET_NAME": app_bucket.bucket_name}
        )