from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_logs as logs
from constructs import Construct


class HubspotToRYLStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, table_name: str, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_powertools_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            construct_id + "awspowertoolslayer",
            layer_version_arn="arn:aws:lambda:us-east-1:930529236463:layer:aws-lambda-powertools-python-layer:1",
        )

        requests_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            construct_id + "requestslayer",
            layer_version_arn="arn:aws:lambda:us-east-1:930529236463:layer:requests-python-layer:2",
        )

        lmd_env = {"LOG_LEVEL": "DEBUG", "TABLE_NAME": table_name}

        role_policy = iam.Policy(
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
                        f"arn:aws:dynamodb:{self.region}:{self.account}:table/{table_name}/index/*",
                    ],
                )
            ],
        )

        lmd_role = iam.Role(
            self,
            f"{construct_id}lmdrole",
            role_name=construct_id + "-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=construct_id + "lambda role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaRole"
                ),
            ],
        )

        lmd_role.attach_inline_policy(role_policy)

        lmd_func = _lambda.Function(
            self,
            construct_id + "lambda",
            function_name=construct_id + "lambdafunc",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="main.lambda_handler",
            code=_lambda.Code.from_asset("src"),
            layers=[aws_powertools_layer, requests_layer],
            environment=lmd_env,
            role=lmd_role,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )
