import json
import os
import sys
import time

import boto3
import requests
from process_podcast import process_transcript_and_add_to_opensearch

# Get DynamoDB table name from environment variable
PROCESSED_FILES_TABLE = os.getenv("PROCESSED_FILES_TABLE")

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
processed_files_table = dynamodb.Table(PROCESSED_FILES_TABLE)


def is_file_processed(s3_uri):
    """Check if a file has already been processed by looking it up in DynamoDB"""
    try:
        response = processed_files_table.get_item(Key={"s3_uri": s3_uri})
        return "Item" in response
    except Exception as e:
        print(f"Error checking if file is processed: {str(e)}")
        return False


def mark_file_processed(s3_uri):
    """Mark a file as processed in DynamoDB"""
    try:
        processed_files_table.put_item(
            Item={
                "s3_uri": s3_uri,
                "timestamp": int(time.time()),
                "processor": "audio_main"
            }
        )
        print(f"Marked {s3_uri} as processed in DynamoDB")
    except Exception as e:
        print(f"Error marking file as processed: {str(e)}")


def get_s3_metadata(s3_uri):
    """Extract metadata from S3 object custom metadata."""
    s3_client = boto3.client("s3")

    # Parse S3 URI
    if s3_uri.startswith("s3://"):
        path = s3_uri[5:]
    else:
        path = s3_uri

    parts = path.split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])

    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        # Extract custom metadata (x-amz-meta-* headers)
        custom_metadata = {
            k.replace("x-amz-meta-", ""): v.replace("=", "-").replace("?", "")
            if isinstance(v, str)
            else v
            for k, v in response.get("Metadata", {}).items()
        }
        return custom_metadata
    except Exception as e:
        print(f"Error fetching S3 metadata for {s3_uri}: {str(e)}")
        return {}


def main(transcript_uri, media_file_uri, job_name, metadata):
    # Check if file has already been processed
    if is_file_processed(media_file_uri):
        print(f"File {media_file_uri} has already been processed. Skipping.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "File already processed", "s3_uri": media_file_uri}),
            "TranscriptionJobName": job_name,
        }
    
    # Fetch the JSON file from the URI
    response = requests.get(transcript_uri)

    # Load the content as JSON
    transcript_json = response.json()

    transcribe_json_file = "/tmp/transcript_output.json"
    with open(transcribe_json_file, "w") as f:
        json.dump(transcript_json, f, indent=4)

    success = process_transcript_and_add_to_opensearch(
        transcribe_json_file, media_file_uri, metadata
    )
    
    if success:
        # Mark file as processed in DynamoDB
        mark_file_processed(media_file_uri)
        print(
            f"Processed and added {os.path.basename(media_file_uri)} to OpenSearch successfully."
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                f"Processed and added {os.path.basename(media_file_uri)} to OpenSearch successfully."
            ),
            "TranscriptionJobName": job_name,
        }
    else:
        print(f"Failed to process {os.path.basename(media_file_uri)}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Failed to process {os.path.basename(media_file_uri)}"),
            "TranscriptionJobName": job_name,
        }


if __name__ == "__main__":
    step_function_input = os.environ.get("STEP_FUNCTION_INPUT")

    # Test event
    # step_function_input = """{
    #     "TranscriptionJob": {
    #         "Transcript": {
    #             "TranscriptFileUri": "https://example.com/transcript.json"
    #         },
    #         "Media": {"MediaFileUri": "s3://example-bucket/example-file.mp4"},
    #         "TranscriptionJobName": "Example-Transcription-Job",
    #     }
    # }"""

    input_data = json.loads(step_function_input)

    try:
        transcript_uri = input_data["TranscriptionJob"]["Transcript"][
            "TranscriptFileUri"
        ]

        job_name = input_data["TranscriptionJob"]["TranscriptionJobName"]
        media_file_uri = input_data["TranscriptionJob"]["Media"]["MediaFileUri"]

        # Get metadata from S3 custom metadata instead of transcribe tags
        metadata = get_s3_metadata(media_file_uri)

    except Exception as e:
        print(f"Error processing step function input: {e}")

    result = main(transcript_uri, media_file_uri, job_name, metadata)
    print(result)

    sys.exit(0)
