# stacks/main_stack.py


from aws_cdk import Stack
from constructs import Construct

from .backend import RagBackend
from .ingest import RagIngest


class RagChatbotStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        embeddings_model_id: str,
        video_text_model_id: str,
        opensearch_collection_name: str,
        opensearch_index_name: str,
        chat_model: str,
        embedding_model: str,
        chat_prompt: str,
        config_path: str,
        max_concurrency: int,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ingest_stack = RagIngest(
            self,
            "RagIngest",
            opensearch_index_name=opensearch_index_name,
            opensearch_collection_name=opensearch_collection_name,
            embeddings_model_id=embeddings_model_id,
            video_text_model_id=video_text_model_id,
            region=self.region,
            max_concurrency=max_concurrency,
        )
        rag_api_stack = RagBackend(
            self,
            "RagBackend",
            opensearch_endpoint=ingest_stack.opensearch_endpoint,
            opensearch_index_name=opensearch_index_name,
            opensearch_collection_arn=ingest_stack.collection_arn,
            chat_model=chat_model,
            embedding_model=embedding_model,
            chat_prompt=chat_prompt,
            bucket_arn=ingest_stack.bucket_arn,
        )
