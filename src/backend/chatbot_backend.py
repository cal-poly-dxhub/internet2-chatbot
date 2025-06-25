import json
import logging
import os
import re
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError
from opensearch_query import generate_short_uuid, get_documents
from search_utils import generate_text_embedding

# set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def invoke_model(prompt, model_id, max_tokens=4096):
    """
    Calls Bedrock for a given modelid

    Args:
        prompt (str): The text prompt to send to the model
        model_id (str): The model identifier
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


def s3_uri_to_presigned_url(s3_uri, expiration=3600):
    """
    Convert an S3 URI to a presigned URL

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


def get_filename_from_s3_uri(s3_uri):
    """Extract just the filename from an S3 URI"""
    parsed_uri = urlparse(s3_uri)
    # Get the full path and extract the filename using os.path.basename
    return os.path.basename(parsed_uri.path)


def get_filename_from_url(url):
    """Extract just the filename from a URL"""
    if not url:
        return "source"
    parsed_url = urlparse(url)
    path = parsed_url.path
    return os.path.basename(path) if path else "source"


SOURCE_URL = 0
FILE_TYPE = 1
START_TIME = 2
MEMBER_CONTENT = 3
FILE_NAME = 4


def process_text(text, uuid_mapping):
    """Replaces s3 uris and uuids with presign urls to sources."""

    def replace_image_uri(match):
        s3_uri = match.group(0)[4:-1]
        if s3_uri:
            presigned_url = s3_uri_to_presigned_url(s3_uri)
            file_name = get_filename_from_s3_uri(s3_uri)
            return f"![{file_name}]({presigned_url})"
        return "![]()"

    # First replace all S3 URIs in image markdown
    image_pattern = r"!\[\]\(s3://[^\)]+\)"
    text = re.sub(image_pattern, replace_image_uri, text)

    # Then replace all UUIDs with their corresponding sources
    uuid_pattern = r"<([a-f0-9]{8})>"

    def replace_uuid(match):
        uuid = match.group(1)
        source_object = uuid_mapping.get(uuid)
        if source_object:
            source_url = source_object[SOURCE_URL]
            file_type = source_object[FILE_TYPE]
            start_time = source_object[START_TIME]
            is_member = source_object[MEMBER_CONTENT]
            file_name = source_object[FILE_NAME]

            badge = "[Subscriber-only]" if is_member == "true" else "[Public]"

            if file_type == "audio/video":
                url_with_timestamp = (
                    f"{source_url}#t={start_time}" if start_time else source_url
                )
                return f"[{file_name}]({url_with_timestamp}) — _{badge}_"
            elif file_type == "pdf":
                url_with_timestamp = (
                    f"{source_url}#page={start_time}"
                    if start_time
                    else source_url
                )
                return f"[{file_name}]({url_with_timestamp}) — _{badge}_"
            else:
                return f"[{file_name}]({source_url}) — _{badge}_"
        return "[]()"

    text = re.sub(uuid_pattern, replace_uuid, text)

    return text


def generate_source_mapping(documents):
    """Generates a mapping from a generated uuid:(source url, file_type, start_time) for llm to read."""
    source_mapping = {}
    for item in documents:
        if item.get("_source"):
            document = item.get("_source")
            metadata = document.get("metadata", {})

            source_id = generate_short_uuid()

            # Use the URL from metadata
            source_url = metadata.get("source-url", "")

            is_member_content = metadata.get("member-content", "")
            print(f"SOURCE: {source_url}")
            print(f"MEMBER-CONTENT: {is_member_content}")

            # Determine document type and get appropriate metadata
            doc_type = document.get("type", "")

            if doc_type == "video":
                # Get start time from metadata
                start_time = metadata.get("start_time", 0)
                file_name = metadata.get("video_id", "")
                source_mapping[source_id] = (
                    source_url,
                    "audio/video",
                    start_time,
                    is_member_content,
                    file_name,
                )
            elif doc_type == "podcast":
                # Get start time from metadata
                start_time = metadata.get("start_time", 0)
                file_name = metadata.get("podcast_id", "")
                source_mapping[source_id] = (
                    source_url,
                    "audio/video",
                    start_time,
                    is_member_content,
                    file_name,
                )
            elif doc_type == "pdf":
                file_name = metadata.get("doc_id", "")
                page_number = metadata.get("page_number", "")
                source_mapping[source_id] = (
                    source_url,
                    "pdf",
                    page_number,
                    is_member_content,
                    file_name,
                )
            else:
                file_name = metadata.get("doc_id", "")
                source_mapping[source_id] = (
                    source_url,
                    "text",
                    0,
                    is_member_content,
                    file_name,
                )

    return source_mapping


def lambda_handler(event, context):
    try:
        body_data = json.loads(event["body"])
        user_query = body_data["query"]

        embedding = generate_text_embedding(user_query)

        selected_docs = get_documents(user_query, embedding)

        source_mapping = generate_source_mapping(selected_docs)

        prompt = (
            "User:"
            + user_query
            + os.getenv("CHAT_PROMPT").format(
                documents=selected_docs, citations=str(source_mapping)
            )
        )

        logger.info(f"User query length: {len(user_query)}")
        logger.info(f"Documents length: {len(str(selected_docs))}")
        logger.info(f"Prompt length: {len(prompt)}")

        model_response = invoke_model(prompt, os.getenv("CHAT_MODEL_ID"))

        logger.info(f"Model: {model_response}")

        parsed_chat_respose = process_text(model_response, source_mapping)

        return {"statusCode": 200, "body": json.dumps(parsed_chat_respose)}

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps("Error processing message"),
        }
