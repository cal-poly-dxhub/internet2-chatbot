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
        classifier_model: str,
        document_filter_model: str,
        platform_classifier_prompt: str,
        document_filter_prompt: str,
        config_path: str,
        max_concurrency: int,
        step_function_timeout_hours: int,
        chunk_size: str,
        overlap: str,
        docs_retrieved: int,
        docs_after_falloff: int,
        conversation_history_turns: int = 4,
        max_history_characters: int = 100000,
        temperature: float = 1.0,
        top_p: float = 0.999,
        max_tokens: int = 4096,
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
            step_function_timeout_hours=step_function_timeout_hours,
            chunk_size=chunk_size,
            overlap=overlap,
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
            classifier_model=classifier_model,
            document_filter_model=document_filter_model,
            platform_classifier_prompt=platform_classifier_prompt,
            document_filter_prompt=document_filter_prompt,
            bucket_arn=ingest_stack.bucket_arn,
            docs_retrieved=docs_retrieved,
            docs_after_falloff=docs_after_falloff,
            conversation_history_turns=conversation_history_turns,
            max_history_characters=max_history_characters,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
        )
