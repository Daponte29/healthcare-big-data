#!/usr/bin/env python3
import os
import aws_cdk as cdk
from emr_serverless_stack import WebHealthClusteringStack

app = cdk.App()
WebHealthClusteringStack(
    app, "WebHealthClusteringStack",
    env=cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),
)
app.synth()
