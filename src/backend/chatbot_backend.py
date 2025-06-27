import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError
from opensearch_query import generate_short_uuid, get_documents
from search_utils import generate_text_embedding

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


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

        print(prompt)
        print(messages)
        print(len(messages))
        response = bedrock.converse(
            modelId=model_id,
            messages=messages,
            inferenceConfig=inference_config,
        )

        return response["output"]["message"]["content"][0]["text"]

    except Exception as e:
        print(f"Error calling the model: {str(e)}")
        return None


def s3_uri_to_presigned_url(
    s3_uri: str, expiration: int = 3600
) -> Optional[str]:
    """Convert an S3 URI to a presigned URL.

    Args:
        s3_uri (str): S3 URI in format 's3://bucket-name/path/to/file'
        expiration (int): URL expiration time in seconds (default: 1 hour)

    Returns:
        str: Presigned URL or None if there's an error
    """

    try:
        # Parse the S3 URI
        parsed_uri = urlparse(s3_uri)
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path.lstrip("/")

        # Create S3 client
        s3_client = boto3.client("s3")

        # Generate presigned URL
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration,
        )
        return presigned_url

    except NoCredentialsError:
        print("AWS credentials not found")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def get_filename_from_s3_uri(s3_uri: str) -> str:
    """Extract just the filename from an S3 URI"""
    parsed_uri = urlparse(s3_uri)
    # Get the full path and extract the filename using os.path.basename
    return os.path.basename(parsed_uri.path)


def process_text(
    text: str,
    uuid_mapping: Dict[str, Dict[str, Any]],
    metadata_mapping: Dict[str, Dict[str, Any]],
) -> str:
    """Replaces s3 uris and uuids with presign urls to sources using metadata mapping."""

    def replace_image_uri(match: re.Match[str]) -> str:
        s3_uri = match.group(0)[4:-1]
        if s3_uri:
            presigned_url = s3_uri_to_presigned_url(s3_uri)
            file_name = get_filename_from_s3_uri(s3_uri)
            return f"![{file_name}]({presigned_url})"
        return "![]()"

    # First replace all S3 URIs in image markdown
    image_pattern = r"!\[\]\(s3://[^\)]+\)"
    text = re.sub(image_pattern, replace_image_uri, text)

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
    """Add meeting list at the bottom of the response based on UUIDs referenced in the LLM response."""
    meetings: Set[Tuple[str, str]] = set()

    # Extract UUIDs that appear in the LLM response
    uuid_pattern = r"([a-f0-9]{8})"
    referenced_uuids = set(re.findall(uuid_pattern, text))

    # Only include meetings for UUIDs that were referenced in the response
    for uuid in referenced_uuids:
        metadata = metadata_mapping.get(uuid, {})
        parent_folder_name = metadata.get("parent_folder_name", "")
        parent_folder_url = metadata.get("parent_folder_url", "")
        if parent_folder_name and parent_folder_url:
            meetings.add((parent_folder_name, parent_folder_url))

    if meetings:
        text += "\n\n**Meetings referenced:**\n"
        for folder_name, meeting_url in sorted(meetings):
            text += f"- [{folder_name}]({meeting_url})\n"

    return text


def format_documents_for_llm(
    documents: List[Dict[str, Any]], source_mapping: Dict[str, Dict[str, Any]]
) -> List[Dict[str, str]]:
    """Format documents for LLM with only UUID and passage content."""
    formatted_docs: List[Dict[str, str]] = []

    # Convert source_mapping to a list to maintain order
    source_items: List[Tuple[str, Dict[str, Any]]] = list(
        source_mapping.items()
    )

    for i, item in enumerate(documents):
        if item.get("_source") and i < len(source_items):
            document = item.get("_source")
            passage = document.get("passage", "")

            # Use the UUID at the same index position
            doc_uuid = source_items[i][0]

            formatted_doc: Dict[str, str] = {
                "uuid": doc_uuid,
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
