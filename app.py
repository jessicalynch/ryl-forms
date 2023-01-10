#!/usr/bin/env python3

import os

from aws_cdk import App, Tags

from aws_stacks.hubspot_to_ryl_stack import HubspotToRYLStack
from aws_stacks.hubspot_to_ryl_table_stack import HubspotToRYLTableStack

env = {
    "account": os.environ["CDK_DEFAULT_ACCOUNT"],
    "region": os.environ["CDK_DEFAULT_REGION"],
}
app = App()

HubspotToRYLTableStack(
    scope=app,
    env=env,
    construct_id="hubspot-to-ryl-table-dev",
    table_name="main-street-law-firm-dev",
)

HubspotToRYLStack(
    scope=app,
    env=env,
    construct_id="hubspot-to-ryl-dev",
    table_name="main-street-law-firm-dev",
)

Tags.of(app).add("billing", "mslf")

app.synth()
