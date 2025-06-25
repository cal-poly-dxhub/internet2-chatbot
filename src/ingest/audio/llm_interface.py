from typing import Dict, List

import boto3
from langchain_aws.chat_models import ChatBedrockConverse
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

# Initialize the Bedrock client
bedrock_runtime = boto3.client(service_name="bedrock-runtime")


def create_chat_model(model_id: str, temperature: float) -> ChatBedrockConverse:
    return ChatBedrockConverse(
        model=model_id,
        client=bedrock_runtime,
        temperature=temperature,
        max_tokens=1000,
    )


def generate_response(
    messages: List[Dict[str, str]], model_id: str, temperature: float
) -> str:
    chat = create_chat_model(model_id, temperature)
    formatted_messages = [
        SystemMessage(content="You are a helpful AI assistant.")
    ] + [
        HumanMessage(content=msg["content"])
        if msg["role"] == "user"
        else AIMessage(content=msg["content"])
        for msg in messages
    ]

    try:
        response = chat.invoke(formatted_messages)
        return (
            response.content[0]["text"]
            if isinstance(response.content, list)
            else response.content
        )
    except Exception as e:
        print(f"An error occurred: {e}")
        return "I'm sorry, but I encountered an error while processing your request. Please try again."
