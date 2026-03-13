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

        app_bucket = s3.Bucket.from_bucket_name(self, "MangoSharedBucket",
            f"mango-shared-bucket-{ACCOUNT_ID}"
        )

        main_function = _lambda.Function(self, "MainFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=_lambda.Code.from_asset("lambda",
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_11.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                )
            ),
            role=lab_role,
            environment={
                "BUCKET_NAME": app_bucket.bucket_name
            }
        )