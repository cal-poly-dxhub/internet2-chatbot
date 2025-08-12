from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
    RemovalPolicy,
)
from aws_cdk import (
    aws_apigateway as apigw,
)
from aws_cdk import (
    aws_dynamodb as dynamodb,
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
        
        # Create DynamoDB table for conversation history
        conversation_table = dynamodb.Table(
            self,
            "ConversationHistory",
            partition_key=dynamodb.Attribute(
                name="session_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        
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
                "CONVERSATION_TABLE": conversation_table.table_name,
            },
        )

        chat_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[bucket_arn],
                effect=iam.Effect.ALLOW,
            )
        )

        # Grant DynamoDB permissions
        conversation_table.grant_read_write_data(chat_lambda)

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

        # Define the Lambda function for feedback
        feedback_lambda = _lambda.Function(
            self,
            "FeedbackHandler",
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
            handler="feedback.feedback_handler",
            timeout=Duration.seconds(30),
            environment={
                "CONVERSATION_TABLE": conversation_table.table_name,
            },
        )

        # Grant DynamoDB permissions to feedback lambda
        conversation_table.grant_read_write_data(feedback_lambda)

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

        # Create chat-response resource and method
        chat_resource = api.root.add_resource("chat-response")
        chat_integration = apigw.LambdaIntegration(chat_lambda, proxy=True)
        chat_resource.add_method("POST", chat_integration)

        # Create feedback resource and method
        feedback_resource = api.root.add_resource("feedback")
        feedback_integration = apigw.LambdaIntegration(feedback_lambda, proxy=True)
        feedback_resource.add_method("POST", feedback_integration)

        # Add CORS support for chat-response
        chat_resource.add_method(
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

        # Add CORS support for feedback
        feedback_resource.add_method(
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
        
        CfnOutput(
            self,
            "ConversationTableName",
            value=conversation_table.table_name,
        )
