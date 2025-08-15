#!/usr/bin/env python3
import aws_cdk as cdk
import yaml

from cdk.main import RagChatbotStack

CONFIG_PATH = "./config.yaml"
config = yaml.safe_load(open(CONFIG_PATH))

app = cdk.App()
RagChatbotStack(
    app,
    "RagChatbotStack",
    embeddings_model_id=config["model"]["embedding"],
    opensearch_collection_name=config["opensearch_collection_name"],
    opensearch_index_name=config["opensearch_index_name"],
    chat_model=config["model"]["chat"],
    embedding_model=config["model"]["embedding"],
    video_text_model_id=config["model"]["video_ingest"],
    chat_prompt=config["chat_prompt"],
    config_path=CONFIG_PATH,
    max_concurrency=int(config["max_concurrency"]),
    step_function_timeout_hours=int(config["step_function_timeout_hours"]),
    chunk_size=config["chunk_size"],
    overlap=config["overlap"],
    docs_retrieved=int(config["docs_retrieved"]),
    docs_after_falloff=int(config["docs_after_falloff"]),
    conversation_history_turns=int(config["conversation_history_turns"]),
    max_history_characters=int(config["max_history_characters"]),
)

app.synth()
