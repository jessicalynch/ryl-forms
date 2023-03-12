from aws_cdk import Stack
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct


class HubspotToRYLSfnStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, table_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        query_forms_task = sfn_tasks.CallAwsService(
            self,
            "Query all Hubspot form IDs",
            service="dynamodb",
            action="query",
            parameters={
                "TableName": table_name,
                "KeyConditionExpression": "#pk = :pk",
                "ExpressionAttributeNames": {"#pk": "pk"},
                "ExpressionAttributeValues": {":pk": {"S": "hsform"}},
            },
            iam_resources=[
                f"arn:aws:dynamodb:{self.region}:{self.account}:table/{table_name}"
            ],
        )

        process_forms_map = sfn.Map(
            self,
            "Process form",
            max_concurrency=40,
            items_path=sfn.JsonPath.string_at("$.Items"),
        ).iterator(sfn.Pass(self, "Placeholder"))

        definition = query_forms_task.next(process_forms_map)

        state_machine = sfn.StateMachine(
            self, "HubspotToRYLStateMachine", definition=definition
        )
