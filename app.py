#!/usr/bin/env python3

import os

from aws_cdk import App, Tags

from aws_stacks.hubspot_to_ryl_sfn_stack import HubspotToRYLSfnStack
from aws_stacks.hubspot_to_ryl_stack import HubspotToRYLStack
from aws_stacks.hubspot_to_ryl_table_stack import HubspotToRYLTableStack

env = {
    "account": os.environ["CDK_DEFAULT_ACCOUNT"],
    "region": os.environ["CDK_DEFAULT_REGION"],
}
app = App()

table_name = "main-street-law-firm-dev"

HubspotToRYLTableStack(
    scope=app,
    env=env,
    construct_id="hubspot-to-ryl-table-dev",
    table_name=table_name,
)

HubspotToRYLStack(
    scope=app,
    env=env,
    construct_id="hubspot-to-ryl-dev",
    table_name=table_name,
)

HubspotToRYLSfnStack(
    scope=app, env=env, construct_id="hubspot-to-ryl-sfn-dev", table_name=table_name
)

Tags.of(app).add("billing", "mslf")

app.synth()
