from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_dynamodb as dynamo
from constructs import Construct


class HubspotToRYLTableStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, table_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        dynamo_table = dynamo.Table(
            self,
            id=f"{construct_id}-{table_name}",
            table_name=table_name,
            partition_key=dynamo.Attribute(name="pk", type=dynamo.AttributeType.STRING),
            sort_key=dynamo.Attribute(name="sk", type=dynamo.AttributeType.STRING),
            billing_mode=dynamo.BillingMode.PROVISIONED,
            read_capacity=5,
            write_capacity=5,
            time_to_live_attribute="ttl",
        )

        CfnOutput(
            self,
            id=construct_id + "tablenameoutput",
            export_name=construct_id + "-tablename",
            description=construct_id + " table name",
            value=table_name,
        )
