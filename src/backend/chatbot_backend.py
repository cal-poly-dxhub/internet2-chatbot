import json
import logging
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from opensearch_query import generate_short_uuid, get_documents
from search_utils import generate_text_embedding

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")


def get_conversation_history(session_id: str) -> List[Dict[str, str]]:
    """Get last 5 messages from conversation history."""
    table = dynamodb.Table(os.getenv("CONVERSATION_TABLE"))

    response = table.query(
        KeyConditionExpression=Key("session_id").eq(session_id),
        ScanIndexForward=False,
        Limit=10,  # Get 10 to have 5 pairs (user + assistant)
    )

    messages = []
    for item in response["Items"]:
        messages.append({"role": item["role"], "content": item["content"]})

    # Return last 5 messages (reverse to chronological order)
    return list(reversed(messages[-5:]))


def save_message(session_id: str, role: str, content: str, document_ids: List[str] = None) -> int:
    """Save a message to conversation history and return timestamp."""
    table = dynamodb.Table(os.getenv("CONVERSATION_TABLE"))

    timestamp = int(time.time() * 1000)
    item = {
        "session_id": session_id,
        "timestamp": timestamp,
        "role": role,
        "content": content,
    }
    
    if document_ids:
        item["document_ids"] = document_ids
    
    table.put_item(Item=item)
    return timestamp


def extract_document_ids(documents: List[Dict[str, Any]]) -> List[str]:
    """Extract document IDs from OpenSearch results."""
    doc_ids = []
    for doc in documents:
        if doc.get("_id"):
            doc_ids.append(doc["_id"])
    return doc_ids


def build_conversation_context(
    history: List[Dict[str, str]], current_query: str
) -> str:
    """Build conversation context from history."""
    if not history:
        return current_query

    context_parts = []
    for msg in history:
        if msg["role"] == "user":
            context_parts.append(f"Previous question: {msg['content']}")
        else:
            context_parts.append(f"Previous answer: {msg['content']}")

    context_parts.append(f"Current question: {current_query}")
    return "\n\n".join(context_parts)


def invoke_model(
    prompt: str, model_id: str, max_tokens: int = 4096
) -> Optional[str]:
    """Calls Bedrock for a given model id.

    Args:
        prompt (str): The text prompt to send to the model
        model_id (str): The Bedrock model identifier
        max_tokens (int): Maximum number of tokens to generate

    Returns:
        str: The text response from the model
    """

    bedrock = boto3.client("bedrock-runtime")

    try:
        inference_config = {
            "maxTokens": max_tokens,
            "temperature": 1,
            "topP": 0.999,
        }
        messages = [{"role": "user", "content": [{"text": prompt}]}]

        logger.info(f"Prompt: {prompt}")
        response = bedrock.converse(
            modelId=model_id,
            messages=messages,
            inferenceConfig=inference_config,
        )

        return response["output"]["message"]["content"][0]["text"]

    except Exception as e:
        logger.error(f"Error invoking the model: {str(e)}")
        return None


def process_text(
    text: str,
    uuid_mapping: Dict[str, Dict[str, Any]],
    metadata_mapping: Dict[str, Dict[str, Any]],
) -> str:
    """Replaces uuids with urls for sources using provided mappings.
       If there is an invalid angle bracket <too short> or <too long> we simply remove them.

    Args:
        text (str): Input text containing UUID references in format <uuid>
        uuid_mapping (Dict[str, Dict[str, Any]]): Mapping of UUIDs to source URLs
            Format: {"uuid": {"source_url": "url"}}
        metadata_mapping (Dict[str, Dict[str, Any]]): Mapping of UUIDs to metadata
            Format: {"uuid": {"title": str, "doc_type": str, "start_time": str,
                            "member_content_flag": str}}

    Returns:
        Text (str): The text with uuids substituted

    Example:
        >>> text = "This is a response with a source <sja84nak>"
        >>> uuid_mapping = {"sja84nak": {"source_url": "example.com}}
        >>> metadata_mapping = {"sja84nak": {"title": "example_website", "member_content_flag": "false"}}
        >>> print(process_text(text, uuid_mapping, metadata_mapping))
        >>> "This is a response with a source [example_website](example.com) — _[Public]_"
    """

    uuid_pattern = r"<([a-f0-9]{8})>"

    def replace_uuid(match: re.Match[str]) -> str:
        uuid_match = match.group(1)
        source_data = uuid_mapping.get(uuid_match)
        metadata_info = metadata_mapping.get(uuid_match)

        if source_data and metadata_info:
            source_url = source_data["source_url"]
            doc_type = metadata_info["doc_type"]
            start_time = metadata_info.get("start_time")
            is_member = metadata_info["member_content_flag"]
            title = metadata_info["title"]

            # Create member content badge
            badge = "[Subscriber-only]" if is_member == "true" else "[Public]"

            # Add timestamp for video/audio content
            if doc_type in ["video", "podcast"] and start_time:
                url_with_timestamp = f"{source_url}#t={start_time}"
                return f"[{title}]({url_with_timestamp}) — _{badge}_"
            else:
                return f"[{title}]({source_url}) — _{badge}_"
        return match.group(
            0
        )  # Return the original match if UUID not found in mappings

    # Process valid UUIDs first
    text = re.sub(uuid_pattern, replace_uuid, text)

    # Then remove any remaining angle brackets and their contents
    text = re.sub(r"<[^>]*>", "", text)

    return text


def add_meeting_list(
    text: str, metadata_mapping: Dict[str, Dict[str, Any]]
) -> str:
    """Add formatted meeting list to end of text based on UUIDs referenced.

    Args:
        text (str): Input text containing UUID references in format <uuid>
        metadata_mapping (Dict[str, Dict[str, Any]]): Mapping of UUIDs to metadata
            Format: {"uuid": {
                "parent_folder_name": str,
                "parent_folder_url": str,
                "member_content_flag": str
            }}

    Returns:
        str: Original text with appended meeting list in markdown format.
                If meetings are found, adds a section "Meetings referenced:"
                followed by formatted links with content badges.

    Example:
        >>> text = "Discussion from meeting <12345678>"
        >>> metadata_mapping = {
            "12345678": {
                "parent_folder_name": "Q4 Review",
                "parent_folder_url": "https://example.com/meetings/q4",
                "member_content_flag": "false"
            }
        >>> result = add_meeting_list(text, metadata_mapping)
        >>> print(result)
        Passed in text with source <12345678>

        **Meetings referenced:**
        - [Q4 Review](https://example.com/meetings/q4) — *[Public]*
    """

    meetings: Set[Tuple[str, str]] = set()

    # Extract UUIDs that appear in the LLM response
    uuid_pattern = r"([a-f0-9]{8})"
    referenced_uuids = set(re.findall(uuid_pattern, text))

    # Only include meetings for UUIDs that were referenced in the response
    for uuid_match in referenced_uuids:
        metadata = metadata_mapping.get(uuid_match, {})
        parent_folder_name = metadata.get("parent_folder_name", "")
        parent_folder_url = metadata.get("parent_folder_url", "")
        member_content = metadata.get("member_content_flag", "")
        if parent_folder_name and parent_folder_url:
            meetings.add(
                (parent_folder_name, parent_folder_url, member_content)
            )

    if meetings:
        text += "\n\n**Meetings referenced:**\n"
        for folder_name, meeting_url, member_content in sorted(meetings):
            badge = (
                "*[Subscriber-only]*"
                if member_content == "true"
                else "*[Public]*"
            )
            text += f"- [{folder_name}]({meeting_url}) — {badge}\n"

    return text


def format_documents_for_llm(
    documents: List[Dict[str, Any]], source_mapping: Dict[str, Dict[str, Any]]
) -> List[Dict[str, str]]:
    """Format documents for LLM to read with only UUID, passage content, and file name."""

    formatted_docs: List[Dict[str, str]] = []

    for i, (doc_uuid, _) in enumerate(source_mapping.items()):
        if i < len(documents) and documents[i].get("_source"):
            document = documents[i]["_source"]
            passage = document.get("passage", "")
            doc_id = document.get("metadata", {}).get("doc_id", "")

            formatted_doc: Dict[str, str] = {
                "uuid": doc_uuid,
                "document_name": doc_id,
                "passage": passage,
            }
            formatted_docs.append(formatted_doc)

    return formatted_docs


def extract_metadata_for_substitution(
    documents: List[Dict[str, Any]], source_mapping: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Extract all metadata that will be substituted back after LLM response."""

    metadata_mapping: Dict[str, Dict[str, Any]] = {}

    # Convert source_mapping to a list to maintain order
    source_items: List[Tuple[str, Dict[str, Any]]] = list(
        source_mapping.items()
    )

    for i, item in enumerate(documents):
        if item.get("_source") and i < len(source_items):
            document = item.get("_source")
            metadata = document.get("metadata", {})
            doc_type = document.get("type", "")

            # Use the UUID at the same index position
            doc_uuid = source_items[i][0]

            # Get title based on document type
            title = metadata.get("doc_id", "Document")

            metadata_info: Dict[str, Any] = {
                "title": title,
                "parent_folder_name": metadata.get("parent-folder-name", ""),
                "parent_folder_url": metadata.get("parent-folder-url", ""),
                "member_content_flag": metadata.get("member-content", ""),
                "doc_type": doc_type,
            }

            # Add start time for video/audio content
            if doc_type in ["video", "podcast"]:
                metadata_info["start_time"] = metadata.get("start_time", 0)

            metadata_mapping[doc_uuid] = metadata_info

    return metadata_mapping


def generate_source_mapping(
    documents: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Generates a mapping from uuid to source URL with timestamp info for LLM to read."""

    source_mapping: Dict[str, Dict[str, Any]] = {}
    for item in documents:
        if item.get("_source"):
            document = item.get("_source")
            metadata = document.get("metadata", {})

            source_id = generate_short_uuid()
            source_url = metadata.get("source-url", "")
            doc_type = document.get("type", "")
            member_content = metadata.get("member-content", "")
            title = metadata.get("doc_id", "Document")

            # Store source URL, timestamp info, member content flag, and title
            source_data: Dict[str, Any] = {
                "source_url": source_url,
                "doc_type": doc_type,
                "start_time": metadata.get("start_time", 0)
                if doc_type in ["video", "podcast"]
                else None,
                "member_content": member_content,
                "title": title,
            }

            source_mapping[source_id] = source_data

    return source_mapping


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        body_data: Dict[str, Any] = json.loads(event["body"])
        user_query: str = body_data["query"]
        session_id: str = body_data.get("session_id", str(uuid.uuid4()))

        # Get conversation history
        history = get_conversation_history(session_id)

        # Build context with conversation history
        contextual_query = build_conversation_context(history, user_query)

        embedding: List[float] = generate_text_embedding(contextual_query)

        selected_docs: List[Dict[str, Any]] = get_documents(
            contextual_query, embedding
        )

        source_mapping: Dict[str, Dict[str, Any]] = generate_source_mapping(
            selected_docs
        )

        formatted_docs: List[Dict[str, str]] = format_documents_for_llm(
            selected_docs, source_mapping
        )

        # Extract metadata separately for post-processing
        metadata_mapping: Dict[str, Dict[str, Any]] = (
            extract_metadata_for_substitution(selected_docs, source_mapping)
        )

        # Create simplified mapping for LLM prompt (only UUIDs and source URLs)
        simplified_mapping: Dict[str, str] = {}
        for uuid_key, data in source_mapping.items():
            simplified_mapping[uuid_key] = data["source_url"]

        # Include conversation history in prompt
        history_context = ""
        if history:
            history_context += "<conversation_history>"
            for msg in history[-4:]:  # Last 4 messages for context
                history_context += f"{msg['role'].title()}: {msg['content']}\n"
            history_context += "\n"
            history_context += "</conversation_history>"

        prompt: str = (
            history_context
            + "User: "
            + user_query
            + "\n"
            + os.getenv("CHAT_PROMPT").format(
                documents=formatted_docs, citations=str(simplified_mapping)
            )
        )

        logger.info(f"User query length: {len(user_query)}")
        logger.info(f"Formatted documents length: {len(str(formatted_docs))}")
        logger.info(f"Prompt length: {len(prompt)}")

        model_response: Optional[str] = invoke_model(
            prompt, os.getenv("CHAT_MODEL_ID")
        )

        logger.info(f"Model: {model_response}")

        # Add meeting list at the bottom
        meeting_response: str = add_meeting_list(
            model_response, metadata_mapping
        )

        # Use source mapping and metadata mapping for text processing
        final_response: str = process_text(
            meeting_response, source_mapping, metadata_mapping
        )

        # Extract document IDs for storage
        document_ids = extract_document_ids(selected_docs)

        # Save conversation to history
        save_message(session_id, "user", user_query)
        assistant_timestamp = save_message(
            session_id, "assistant", final_response, document_ids
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "response": final_response,
                    "session_id": session_id,
                    "timestamp": assistant_timestamp,
                }
            ),
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("Error processing message"),
        }
