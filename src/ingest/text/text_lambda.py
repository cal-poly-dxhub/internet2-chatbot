import json
import os
import time
from typing import List
from urllib.parse import urlparse

import boto3
from opensearch_utils import bulk_add_to_opensearch, create_document

REGION = os.getenv("AWS_REGION")
OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
INDEX_NAME = os.getenv("INDEX_NAME")
EMBEDDINGS_MODEL_ID = os.getenv("EMBEDDINGS_MODEL_ID")

# Get chunk size and overlap from environment variables, with fallback defaults
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500"))  # in characters
OVERLAP = float(os.getenv("OVERLAP", "0.1"))  # overlap percentage

client = boto3.client("bedrock-runtime")


def get_text_from_s3_uri(s3_uri: str) -> str:
    """
    Retrieve text content from an S3 file using its URI.
    """
    try:
        # Parse the S3 URI
        parts = s3_uri.replace("s3://", "").split("/")
        bucket = parts[0]
        key = "/".join(parts[1:])

        # Initialize S3 client
        s3_client = boto3.client("s3")

        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)

        # Read the content first
        content = response["Body"].read()
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            pass

        # If none of the encodings work, use 'utf-8' with error handling
        return content.decode("utf-8", errors="replace")

    except Exception as e:
        raise Exception(f"Error reading from S3: {str(e)}")


def create_chunks(
    text: str,
    chunk_size: int = None,
    overlap: float = None,
) -> List[str]:
    """
    Create chunks of text with specified size and overlap.
    Simple implementation that avoids infinite loops.
    """
    # Use environment variables if parameters not provided
    if chunk_size is None:
        chunk_size = CHUNK_SIZE
    if overlap is None:
        overlap = OVERLAP
    
    # If text is smaller than chunk_size, return it as a single chunk
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    overlap_chars = int(chunk_size * overlap)

    start = 0
    while start < len(text):
        # Calculate the end position for this chunk
        end = min(start + chunk_size, len(text))

        # Try to end at a space for cleaner chunks (but don't get stuck)
        if end < len(text):
            space_pos = text.rfind(" ", start, end)
            if space_pos > start:
                end = space_pos

        # Add the chunk
        chunk = text[start:end].strip()
        if chunk:  # Only add non-empty chunks
            chunks.append(chunk)

        # Move to next position accounting for overlap
        if end == len(text):
            # We've reached the end
            break

        # Move forward by chunk size minus overlap
        start = end - overlap_chars
        if start < 0:
            start = 0
        if start < len(text) and text[start] == " ":
            start += 1  # Skip the space we broke on

    return chunks


def invoke_embedding(input_text: str, retries: int = 0) -> list[float]:
    """
    Generate embeddings using Amazon Titan Text Embeddings V2.
    Returns just the embedding vector.
    """
    try:
        # Create the request for the model
        native_request = {"inputText": input_text}
        request = json.dumps(native_request)

        # Invoke the model
        response = client.invoke_model(
            modelId=EMBEDDINGS_MODEL_ID, body=request
        )

        # Decode the model's response
        model_response = json.loads(response["body"].read())

        # Return just the embedding vector
        return model_response["embedding"]

    except Exception as e:
        if "(ThrottlingException)" in str(e) and retries < 3:
            time.sleep((retries + 1) * 8)
            return invoke_embedding(input_text, retries + 1)
        print(e)
        exit(1)


def get_filename_from_s3_uri(s3_uri):
    """Extract just the filename from an S3 URI"""
    parsed_uri = urlparse(s3_uri)
    # Get the full path and extract the filename using os.path.basename
    return os.path.basename(parsed_uri.path)


def lambda_handler(event, context):
    try:
        s3_uri = event["s3_uri"]
        metadata = event.get("metadata", {})

        text = get_text_from_s3_uri(s3_uri)

        chunks = create_chunks(text)

        documents = []
        for chunk in chunks:
            embedding = invoke_embedding(chunk)

            metadata.update({"doc_id": get_filename_from_s3_uri(s3_uri)})

            doc = create_document(
                passage=chunk,
                embedding=embedding,
                type="text",
                metadata=metadata,
            )
            documents.append(doc)

        success = bulk_add_to_opensearch(documents)

        if not success:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Failed to upload documents"}),
            }

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Successfully processed and indexed text chunks",
                    "s3_uri": s3_uri,
                    "chunks_processed": len(chunks),
                    "documents_indexed": len(documents),
                }
            ),
        }

    except Exception as e:
        print(f"Error processing documents: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
