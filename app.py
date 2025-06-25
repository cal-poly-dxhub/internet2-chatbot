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
)

app.synth()
