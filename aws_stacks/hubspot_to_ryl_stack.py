from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from aws_cdk import Duration
from constructs import Construct


class HubspotToRYLStack(Stack):
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

        rule = events.Rule(
            self,
            "Rule",
            schedule=events.Schedule.cron(minute="*"),
        )

        rule.add_target(targets.LambdaFunction(lambda_func))

        CfnOutput(
            self,
            id=construct_id + "lmdarncfnoutput",
            export_name=construct_id + "-lambdaarn",
            description=construct_id + " lambda arn",
            value=lambda_func.function_arn,
        )
