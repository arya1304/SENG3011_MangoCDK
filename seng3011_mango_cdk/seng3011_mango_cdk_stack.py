import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_s3 as s3,
    aws_iam as iam,
)
from constructs import Construct


class Seng3011MangoCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id,
            synthesizer=cdk.LegacyStackSynthesizer(),
            **kwargs
        )

        account_id = Stack.of(self).account

        lab_role = iam.Role.from_role_arn(self, "LabRole",
            f"arn:aws:iam::{account_id}:role/LabRole"
        )

        app_bucket = s3.Bucket.from_bucket_name(self, "MangoSharedBucket",
            f"mango-shared-bucket-{account_id}"
        )

        # CDK bundles and uploads lambda/ automatically
        main_function = _lambda.Function(self, "MainFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=_lambda.Code.from_asset("lambda"),
            role=lab_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "BUCKET_NAME": app_bucket.bucket_name
            }
        )

        # API Gateway to expose FastAPI routes over HTTP
        api = apigw.LambdaRestApi(self, "MangoApi",
            handler=main_function,
            proxy=True,  # forwards all routes to FastAPI/Mangum
        )

        # Print the API URL after deploy
        cdk.CfnOutput(self, "ApiUrl", value=api.url)