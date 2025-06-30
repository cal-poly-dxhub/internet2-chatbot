import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from opensearch_query import generate_short_uuid, get_documents
from search_utils import generate_text_embedding

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

    # Then replace all UUIDs with their corresponding source URLs using metadata
    uuid_pattern = r"<([a-f0-9]{8})>"

    def replace_uuid(match: re.Match[str]) -> str:
        uuid = match.group(1)
        source_data = uuid_mapping.get(uuid)
        metadata_info = metadata_mapping.get(uuid)

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
        return "[]()"

    text = re.sub(uuid_pattern, replace_uuid, text)

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
    for uuid in referenced_uuids:
        metadata = metadata_mapping.get(uuid, {})
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

    # Convert source_mapping to a list to maintain order
    source_items: List[Tuple[str, Dict[str, Any]]] = list(
        source_mapping.items()
    )

    for i, item in enumerate(documents):
        if item.get("_source") and i < len(source_items):
            document = item.get("_source")
            passage = document.get("passage", "")
            doc_id = document.get("metadata").get("doc_id", "")

            # Use the UUID at the same index position
            doc_uuid = source_items[i][0]

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
            if doc_type == "video":
                title = metadata.get("video_id", "Video")
            elif doc_type == "podcast":
                title = metadata.get("podcast_id", "Podcast")
            elif doc_type == "pdf":
                title = metadata.get("doc_id", "PDF Document")
            else:
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

            # Get title based on document type
            if doc_type == "video":
                title = metadata.get("video_id", "Video")
            elif doc_type == "podcast":
                title = metadata.get("podcast_id", "Podcast")
            elif doc_type == "pdf":
                title = metadata.get("doc_id", "PDF Document")
            else:
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

        embedding: List[float] = generate_text_embedding(user_query)

        selected_docs: List[Dict[str, Any]] = get_documents(
            user_query, embedding
        )

        source_mapping: Dict[str, Dict[str, Any]] = generate_source_mapping(
            selected_docs
        )

        # Format documents with only UUID and passage for LLM
        formatted_docs: List[Dict[str, str]] = format_documents_for_llm(
            selected_docs, source_mapping
        )

        # Extract metadata separately for post-processing
        metadata_mapping: Dict[str, Dict[str, Any]] = (
            extract_metadata_for_substitution(selected_docs, source_mapping)
        )

        # Create simplified mapping for LLM prompt (only UUIDs and source URLs)
        simplified_mapping: Dict[str, str] = {}
        for uuid, data in source_mapping.items():
            simplified_mapping[uuid] = data["source_url"]

        prompt: str = (
            "User:"
            + user_query
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

        return {"statusCode": 200, "body": json.dumps(final_response)}

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("Error processing message"),
        }


test_event = {"body": json.dumps({"query": "What is the weather today?"})}
lambda_handler(test_event, None)
