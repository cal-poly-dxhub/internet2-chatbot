from aws_cdk import BundlingOptions, CfnOutput, Duration, RemovalPolicy
from aws_cdk import (
    aws_ec2 as ec2,
)
from aws_cdk import (
    aws_ecr as ecr,
)
from aws_cdk import (
    aws_ecs as ecs,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as lambda_,
)
from aws_cdk import (
    aws_logs as logs,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_stepfunctions as sfn,
)
from aws_cdk import (
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class RagIngest(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        index_name: str,
        opensearch_endpoint: str,
        embeddings_model_id: str,
        video_text_model_id: str,
        region: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create VPC (required for ECS Fargate)
        vpc = ec2.Vpc(
            self,
            "IngestVPC",
            max_azs=2,
            nat_gateways=0,
        )

        # Create S3 bucket
        input_assets_bucket = s3.Bucket(
            self,
            "InputAssetsBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # Add bucket policy for Transcribe access
        input_assets_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                principals=[iam.ServicePrincipal("transcribe.amazonaws.com")],
                resources=[
                    input_assets_bucket.bucket_arn,
                    f"{input_assets_bucket.bucket_arn}/*",
                ],
            )
        )

        #################################################################################
        # ECS CLUSTER AND CONTAINER SETUP
        #################################################################################

        # Create ECS Cluster
        cluster = ecs.Cluster(
            self,
            "IngestCluster",
            vpc=vpc,
            enable_fargate_capacity_providers=True,
        )

        # Create ECR repositories for each service
        video_repo = ecr.Repository(
            self,
            "VideoRepo",
            repository_name="video-service",
            removal_policy=RemovalPolicy.DESTROY,
        )

        pdf_repo = ecr.Repository(
            self,
            "PDFRepo",
            repository_name="pdf-service",
            removal_policy=RemovalPolicy.DESTROY,
        )

        audio_repo = ecr.Repository(
            self,
            "AudioRepo",
            repository_name="audio-service",
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Create IAM execution role for ECS tasks
        execution_role = iam.Role(
            self,
            "EcsTaskExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )

        # Create IAM task role with necessary permissions
        task_role = iam.Role(
            self,
            "EcsTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonTextractFullAccess"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonBedrockFullAccess"
                ),
            ],
        )

        # Grant S3 permissions to task role
        input_assets_bucket.grant_read_write(task_role)

        # Add Transcribe permissions
        transcribe_policy = iam.PolicyStatement(
            actions=[
                "transcribe:StartTranscriptionJob",
                "transcribe:GetTranscriptionJob",
                "transcribe:DeleteTranscriptionJob",
                "transcribe:TagResource",
                "transcribe:ListTagsForResource",
            ],
            resources=["*"],
        )
        task_role.add_to_policy(transcribe_policy)

        # Add OpenSearch Permissions
        opensearch_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "aoss:APIAccessAll",
            ],
            resources=["*"],
        )
        task_role.add_to_policy(opensearch_policy)

        video_log_group = logs.LogGroup(
            self,
            "VideoLogGroup",
            log_group_name="/ecs/video-service",
            removal_policy=RemovalPolicy.DESTROY,
        )

        pdf_log_group = logs.LogGroup(
            self,
            "PDFLogGroup",
            log_group_name="/ecs/pdf-service",
            removal_policy=RemovalPolicy.DESTROY,
        )

        audio_log_group = logs.LogGroup(
            self,
            "AudioLogGroup",
            log_group_name="/ecs/audio-service",
            removal_policy=RemovalPolicy.DESTROY,
        )

        routing_lambda = lambda_.Function(
            self,
            "RoutingLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="routing_lambda.lambda_handler",
            code=lambda_.Code.from_asset("src/ingest/routing"),
            timeout=Duration.seconds(10),
            memory_size=128,
        )

        text_lambda = lambda_.Function(
            self,
            "TextLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="text_lambda.lambda_handler",
            code=lambda_.Code.from_asset(
                "src/ingest/text",
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_13.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output",
                    ],
                ),
            ),
            timeout=Duration.minutes(1),
            memory_size=128,
            environment={
                "INDEX_NAME": index_name,
                "OPENSEARCH_ENDPOINT": opensearch_endpoint,
                "EMBEDDINGS_MODEL_ID": embeddings_model_id,
            },
        )

        input_assets_bucket.grant_read(text_lambda)
        text_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"], resources=["*"]
            )
        )
        text_lambda.add_to_role_policy(
            iam.PolicyStatement(actions=["aoss:APIAccessAll"], resources=["*"])
        )

        video_task_definition = ecs.FargateTaskDefinition(
            self,
            "VideoTaskDefinition",
            memory_limit_mib=16384,
            cpu=8192,
            execution_role=execution_role,
            task_role=task_role,
        )

        video_container = video_task_definition.add_container(
            "VideoContainer",
            image=ecs.ContainerImage.from_ecr_repository(video_repo, "latest"),
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="video-service",
                log_group=video_log_group,
            ),
            environment={
                "INDEX_NAME": index_name,
                "OPENSEARCH_ENDPOINT": opensearch_endpoint,
                "EMBEDDINGS_MODEL_ID": embeddings_model_id,
                "VIDEO_TEXT_MODEL_ID": video_text_model_id,
                "AWS_DEFAULT_REGION": region,
            },
        )

        pdf_task_definition = ecs.FargateTaskDefinition(
            self,
            "PDFTaskDefinition",
            memory_limit_mib=8192,
            cpu=4096,
            execution_role=execution_role,
            task_role=task_role,
        )

        pdf_container = pdf_task_definition.add_container(
            "PDFContainer",
            image=ecs.ContainerImage.from_ecr_repository(pdf_repo, "latest"),
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="pdf-service",
                log_group=pdf_log_group,
            ),
            environment={
                "INDEX_NAME": index_name,
                "OPENSEARCH_ENDPOINT": opensearch_endpoint,
                "EMBEDDINGS_MODEL_ID": embeddings_model_id,
                "AWS_DEFAULT_REGION": region,
            },
        )

        audio_task_definition = ecs.FargateTaskDefinition(
            self,
            "AudioTaskDefinition",
            memory_limit_mib=2048,
            cpu=1024,
            execution_role=execution_role,
            task_role=task_role,
        )

        audio_container = audio_task_definition.add_container(
            "AudioContainer",
            image=ecs.ContainerImage.from_ecr_repository(audio_repo, "latest"),
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="audio-service",
                log_group=audio_log_group,
            ),
            environment={
                "INDEX_NAME": index_name,
                "OPENSEARCH_ENDPOINT": opensearch_endpoint,
                "EMBEDDINGS_MODEL_ID": embeddings_model_id,
                "AUDIO_TEXT_MODEL_ID": video_text_model_id,
                "AWS_DEFAULT_REGION": region,
            },
        )

        #################################################################################
        # STEP FUNCTION DEFINITION
        #################################################################################

        # Create security group for ECS tasks
        security_group = ec2.SecurityGroup(
            self,
            "EcsSecurityGroup",
            vpc=vpc,
            description="Security group for ECS tasks",
            allow_all_outbound=True,
        )

        process_bucket_data = tasks.LambdaInvoke(
            self,
            "routing-task",
            lambda_function=routing_lambda,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.routing_result",
            retry_on_service_exceptions=True,
        )

        input_assets_bucket.grant_read(routing_lambda)

        # Define the nested state machine for the Map state
        choice = sfn.Choice(self, "Choice")

        # Audio branch
        start_audio_transcription = tasks.CallAwsService(
            self,
            "StartAudioTranscriptionJob",
            service="transcribe",
            action="startTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "States.Format('{}-{}', $.lambda_name, States.UUID())",
                "LanguageCode": "en-US",
                "MediaFormat.$": "$.data_type",
                "Media": {"MediaFileUri.$": "$.s3_uri"},
                "Settings": {"ShowSpeakerLabels": True, "MaxSpeakerLabels": 5},
                "Tags": [
                    {"Key": "source-url", "Value.$": "$.metadata.source-url"},
                    {
                        "Key": "member-content",
                        "Value.$": "$.metadata.member-content",
                    },
                ],
            },
            iam_resources=["*"],
        )

        wait_audio_transcribe = sfn.Wait(
            self,
            "WaitAudioTranscribe",
            time=sfn.WaitTime.duration(Duration.seconds(10)),
        )

        get_audio_transcription = tasks.CallAwsService(
            self,
            "GetAudioTranscriptionJob",
            service="transcribe",
            action="getTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "$.TranscriptionJob.TranscriptionJobName"
            },
            iam_resources=["*"],
        )

        audio_job_complete = sfn.Choice(self, "AudioJobComplete")

        process_audio = tasks.EcsRunTask(
            self,
            "audio-task",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=audio_task_definition,
            assign_public_ip=True,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=audio_container,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name="STEP_FUNCTION_INPUT",
                            value=sfn.JsonPath.string_at(
                                "States.JsonToString($)"
                            ),
                        ),
                    ],
                )
            ],
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST,
            ),
            security_groups=[security_group],
            result_path="$.audio_result",
        )

        delete_transcription_job = tasks.CallAwsService(
            self,
            "DeleteTranscriptionJob",
            service="transcribe",
            action="deleteTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "$.TranscriptionJob.TranscriptionJobName"
            },
            iam_resources=["*"],
        )

        # Video branch
        start_video_transcription = tasks.CallAwsService(
            self,
            "StartVideoTranscriptionJob",
            service="transcribe",
            action="startTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "States.Format('{}-{}', $.lambda_name, States.UUID())",
                "LanguageCode": "en-US",
                "MediaFormat.$": "$.data_type",
                "Media": {"MediaFileUri.$": "$.s3_uri"},
                "Settings": {"ShowSpeakerLabels": True, "MaxSpeakerLabels": 5},
                "Tags": [
                    {
                        "Key": "source-url",
                        "Value.$": "$.metadata['source-url']",
                    },
                    {
                        "Key": "member-content",
                        "Value.$": "$.metadata['member-content']",
                    },
                ],
            },
            iam_resources=["*"],
        )

        wait_video_transcribe = sfn.Wait(
            self,
            "WaitVideoTranscribe",
            time=sfn.WaitTime.duration(Duration.seconds(10)),
        )

        get_video_transcription = tasks.CallAwsService(
            self,
            "GetVideoTranscriptionJob",
            service="transcribe",
            action="getTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "$.TranscriptionJob.TranscriptionJobName"
            },
            iam_resources=["*"],
        )

        video_job_complete = sfn.Choice(self, "VideoJobComplete")

        process_video = tasks.EcsRunTask(
            self,
            "video-task",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=video_task_definition,
            assign_public_ip=True,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=video_container,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name="STEP_FUNCTION_INPUT",
                            value=sfn.JsonPath.string_at(
                                "States.JsonToString($)"
                            ),
                        ),
                    ],
                )
            ],
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST,
            ),
            security_groups=[security_group],
            result_path="$.video_result",
        )

        # PDF branch
        process_pdf = tasks.EcsRunTask(
            self,
            "pdf-task",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            cluster=cluster,
            task_definition=pdf_task_definition,
            assign_public_ip=True,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=pdf_container,
                    environment=[
                        tasks.TaskEnvironmentVariable(
                            name="STEP_FUNCTION_INPUT",
                            value=sfn.JsonPath.string_at(
                                "States.JsonToString($)"
                            ),
                        ),
                    ],
                )
            ],
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST,
            ),
            security_groups=[security_group],
            result_path="$.pdf_result",
        )

        # Text branch
        process_text = tasks.LambdaInvoke(
            self,
            "text-task",
            lambda_function=text_lambda,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.text_result",
            retry_on_service_exceptions=True,
        )

        # Connect audio branch
        start_audio_transcription.next(wait_audio_transcribe)
        wait_audio_transcribe.next(get_audio_transcription)
        get_audio_transcription.next(audio_job_complete)

        audio_job_complete.when(
            sfn.Condition.string_equals(
                "$.TranscriptionJob.TranscriptionJobStatus", "COMPLETED"
            ),
            process_audio,
        )
        audio_job_complete.when(
            sfn.Condition.string_equals(
                "$.TranscriptionJob.TranscriptionJobStatus", "FAILED"
            ),
            wait_audio_transcribe,
        )
        audio_job_complete.otherwise(wait_audio_transcribe)

        process_audio.next(delete_transcription_job)

        # Connect video branch
        start_video_transcription.next(wait_video_transcribe)
        wait_video_transcribe.next(get_video_transcription)
        get_video_transcription.next(video_job_complete)

        video_job_complete.when(
            sfn.Condition.string_equals(
                "$.TranscriptionJob.TranscriptionJobStatus", "COMPLETED"
            ),
            process_video,
        )
        video_job_complete.when(
            sfn.Condition.string_equals(
                "$.TranscriptionJob.TranscriptionJobStatus", "FAILED"
            ),
            wait_video_transcribe,
        )
        video_job_complete.otherwise(wait_video_transcribe)

        process_video.next(delete_transcription_job)

        # Connect the branches to the choice state
        choice.when(
            sfn.Condition.string_matches("$.lambda_name", "process-video"),
            start_video_transcription,
        )
        choice.when(
            sfn.Condition.string_matches("$.lambda_name", "process-audio"),
            start_audio_transcription,
        )
        choice.when(
            sfn.Condition.string_matches("$.lambda_name", "process-pdf"),
            process_pdf,
        )
        choice.otherwise(process_text)

        # Define the Map state
        map_state = sfn.Map(
            self,
            "Map",
            max_concurrency=7,
            input_path="$",
            items_path="$.routing_result.Payload",
        ).item_processor(choice)

        # Connect Process-Bucket-Data to Map state
        process_bucket_data.next(map_state)

        # Create the state machine
        state_machine = sfn.StateMachine(
            self,
            "DataIngestionStateMachine",
            definition=process_bucket_data,
            timeout=Duration.hours(2),
        )

        state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:PutObject",
                    "s3:DeleteObject",
                ],
                resources=[
                    input_assets_bucket.bucket_arn,
                    f"{input_assets_bucket.bucket_arn}/*",
                ],
            )
        )

        state_machine.add_to_role_policy(transcribe_policy)

        self.bucket_arn = input_assets_bucket.bucket_arn

        CfnOutput(
            self,
            "InputBucketName",
            value=input_assets_bucket.bucket_name,
        )
