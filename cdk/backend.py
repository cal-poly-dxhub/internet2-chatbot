from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
)
from aws_cdk import (
    aws_apigateway as apigw,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from constructs import Construct


class RagBackend(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        opensearch_endpoint: str,
        opensearch_index_name: str,
        opensearch_collection_arn: str,
        chat_model: str,
        embedding_model: str,
        chat_prompt: str,
        bucket_arn: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        #################################################################################
        # CDK FOR THE LAMBDA WHICH SERVES THE API
        #################################################################################

        # Define the Lambda function
        chat_lambda = _lambda.Function(
            self,
            "ChatbotConversationHandler",
            runtime=_lambda.Runtime.PYTHON_3_13,
            code=_lambda.Code.from_asset(
                "src/backend",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_13.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install --platform manylinux2014_x86_64 --implementation cp --python-version 3.13 --only-binary=:all: --target /asset-output -r requirements.txt && cp -au . /asset-output",
                    ],
                ),
            ),
            handler="chatbot_backend.lambda_handler",
            timeout=Duration.seconds(60),
            environment={
                "OPENSEARCH_ENDPOINT": opensearch_endpoint,
                "OPENSEARCH_INDEX": opensearch_index_name,
                "CHAT_MODEL_ID": chat_model,
                "EMBEDDING_MODEL_ID": embedding_model,
                "CHAT_PROMPT": chat_prompt,
            },
        )

        chat_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[bucket_arn],
                effect=iam.Effect.ALLOW,
            )
        )

        # Attach AWS managed policies
        chat_lambda.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonBedrockFullAccess"
            )
        )
        chat_lambda.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonOpenSearchServiceFullAccess"
            )
        )

        # Define custom inline policy
        opensearch_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "aoss:APIAccessAll",
            ],
            resources=[opensearch_collection_arn],
        )

        # Attach inline policy to the Lambda role
        chat_lambda.role.add_to_policy(opensearch_policy)

        #################################################################################
        # CDK FOR API
        #################################################################################
        # Define the API Gateway
        api = apigw.RestApi(
            self,
            "RagAPI",
            rest_api_name="RagChatbotAPI",
            description="API Gateway to be served by a lambda",
        )

        # Create a resource and method
        resource = api.root.add_resource("chat-response")
        integration = apigw.LambdaIntegration(chat_lambda, proxy=True)

        # Add method to the resource
        resource.add_method("POST", integration)

        # Add CORS support
        resource.add_method(
            "OPTIONS",
            apigw.MockIntegration(
                integration_responses=[
                    apigw.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Headers": "'Content-Type,X-Amz-Date,Authorization,X-Api-Key'",
                            "method.response.header.Access-Control-Allow-Origin": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,POST'",
                        },
                    )
                ],
                request_templates={"application/json": '{"statusCode": 200}'},
            ),
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                    },
                )
            ],
        )

        # Create API Key
        api_key = apigw.ApiKey(
            self,
            "RagChatbotApiKey",
            api_key_name="RagChatAPIKey",
            description="API key for accessing RagChatbotAPI",
        )

        # Create Usage Plan and associate with API Key and API Stage
        usage_plan = apigw.UsagePlan(
            self,
            "RagChatUsagePlan",
            name="RagChatUsagePlan",
            throttle=apigw.ThrottleSettings(
                rate_limit=10,
                burst_limit=2,
            ),
            quota=apigw.QuotaSettings(limit=1000, period=apigw.Period.DAY),
        )

        usage_plan.add_api_key(api_key)
        usage_plan.add_api_stage(stage=api.deployment_stage)

        self.api_url = api.url

        CfnOutput(
            self,
            "OpensearchAPIEndpoint",
            value=opensearch_endpoint,
        )
