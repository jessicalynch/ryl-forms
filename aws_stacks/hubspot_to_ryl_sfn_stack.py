from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as sfn_tasks
from constructs import Construct


class HubspotToRYLSfnStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, table_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_powertools_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            construct_id + "awspowertoolslayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:{self.account}:layer:aws-lambda-powertools-python-layer:1",
        )

        requests_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            construct_id + "requestslayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:{self.account}:layer:requests-python-layer:2",
        )

        lambda_env = {"LOG_LEVEL": "DEBUG", "TABLE_NAME": table_name}

        lambda_role_policy = iam.Policy(
            self,
            f"{construct_id}ddbpolicy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "dynamodb:*",
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"arn:aws:dynamodb:{self.region}:{self.account}:table/{table_name}"
                    ],
                )
            ],
        )

        lambda_role = iam.Role(
            self,
            f"{construct_id}lmdrole",
            role_name=construct_id + "-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=construct_id + "lambda role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        lambda_role.attach_inline_policy(lambda_role_policy)

        lambda_func = _lambda.Function(
            self,
            construct_id + "lambda",
            function_name=construct_id + "lambdafunc",
            runtime=_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.minutes(1),
            handler="main.lambda_handler",
            code=_lambda.Code.from_asset("src"),
            layers=[aws_powertools_layer, requests_layer],
            environment=lambda_env,
            role=lambda_role,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        CfnOutput(
            self,
            id=construct_id + "lmdarncfnoutput",
            export_name=construct_id + "-lambdaarn",
            description=construct_id + " lambda arn",
            value=lambda_func.function_arn,
        )

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
            "Process forms map state",
            max_concurrency=3,
            items_path=sfn.JsonPath.string_at("$.Items"),
            result_selector={"flatten.$": "$[*].Payload"},
            output_path="$.flatten",
        ).iterator(
            sfn_tasks.LambdaInvoke(self, "Process form", lambda_function=lambda_func)
        )

        definition = query_forms_task.next(process_forms_map)

        state_machine = sfn.StateMachine(
            self, "HubspotToRYLStateMachine", definition=definition
        )

        state_machine_target = targets.SfnStateMachine(machine=state_machine)

        events.Rule(
            self,
            construct_id + "cronrule",
            rule_name=construct_id + "-rule",
            description="Hubspot to RYL step function run schedule",
            enabled=True,
            schedule=events.Schedule.cron(minute="*/5"),
            targets=[state_machine_target],
        )
