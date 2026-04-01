#!/usr/bin/env python3
import aws_cdk as cdk
from seng3011_mango_cdk.seng3011_mango_cdk_stack import Seng3011MangoCdkStack

app = cdk.App()

# staging environment
Seng3011MangoCdkStack(app, "Seng3011MangoCdkStack-staging",
    env=cdk.Environment(account='083483698371', region='us-east-1'),
)

# prod environment
Seng3011MangoCdkStack(app, "Seng3011MangoCdkStack-prod",
    env=cdk.Environment(account='083483698371', region='us-east-1'),
)
app.synth()