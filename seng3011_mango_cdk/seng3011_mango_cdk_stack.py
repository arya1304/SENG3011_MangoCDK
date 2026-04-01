import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
    aws_s3 as s3,
    aws_iam as iam,
    aws_dynamodb as dynamodb,
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

        cpi_table = dynamodb.Table(
            self, "CpiTable",
            partition_key=dynamodb.Attribute(
                name="dataset_id",
                type=dynamodb.AttributeType.STRING
            ),

            sort_key=dynamodb.Attribute(
                name="time_period",
                type=dynamodb.AttributeType.STRING
            ),

            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        unemployment_table = dynamodb.Table(
            self, "UnemploymentTable",
            partition_key=dynamodb.Attribute(
                name="dataset_id",
                type=dynamodb.AttributeType.STRING
            ),

            sort_key=dynamodb.Attribute(
                name="time_period",
                type=dynamodb.AttributeType.STRING
            ),

            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        gdp_table = dynamodb.Table(
            self, "GdpTable",
            partition_key=dynamodb.Attribute(
                name="dataset_id",
                type=dynamodb.AttributeType.STRING
            ),

            sort_key=dynamodb.Attribute(
                name="time_period",
                type=dynamodb.AttributeType.STRING
            ),

            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        users_table = dynamodb.Table(
            self, "UsersTable",
            partition_key=dynamodb.Attribute(
                name="email",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY
        )

        main_function = _lambda.Function(self, "MainFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=_lambda.Code.from_bucket(app_bucket, "lambda.zip"),
            role=lab_role,
            timeout=Duration.seconds(300),
            memory_size=256,
            environment={
                "BUCKET_NAME": app_bucket.bucket_name,
                "CPI_TABLE_NAME": cpi_table.table_name,
                "UNEMPLOYMENT_TABLE_NAME": unemployment_table.table_name,
                "GDP_TABLE_NAME": gdp_table.table_name,
                "USERS_TABLE_NAME": users_table.table_name,
                "JWT_SECRET": "mango-cdk-jwt-secret-key-2026",
            }
        )

        api = apigw.LambdaRestApi(self, "MangoApi",
            handler=main_function,
            proxy=True,
        )

        cdk.CfnOutput(self, "ApiUrl", value=api.url)