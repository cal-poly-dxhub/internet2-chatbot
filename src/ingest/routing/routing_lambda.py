import os
import time

import boto3
from botocore.exceptions import ClientError

# Get DynamoDB table name from environment variable
PROCESSED_FILES_TABLE = os.getenv("PROCESSED_FILES_TABLE")

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
processed_files_table = dynamodb.Table(PROCESSED_FILES_TABLE)


def get_processed_files():
    """
    Retrieve all processed files from DynamoDB.
    Returns a set of processed S3 URIs.
    """
    try:
        response = processed_files_table.scan()
        processed_files = set()
        
        for item in response['Items']:
            processed_files.add(item['s3_uri'])
        
        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = processed_files_table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response['Items']:
                processed_files.add(item['s3_uri'])
        
        return processed_files
    except Exception as e:
        print(f"Error reading processed files from DynamoDB: {str(e)}")
        return set()


def reset_processed_files():
    """
    Reset the processed files by clearing the DynamoDB table.
    """
    try:
        # Scan the table to get all items
        response = processed_files_table.scan()
        
        # Delete all items
        with processed_files_table.batch_writer() as batch:
            for item in response['Items']:
                batch.delete_item(Key={'s3_uri': item['s3_uri']})
        
        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = processed_files_table.scan(
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            with processed_files_table.batch_writer() as batch:
                for item in response['Items']:
                    batch.delete_item(Key={'s3_uri': item['s3_uri']})
        
        print("Processed files table has been reset (emptied)")
        return {"status": "success", "message": "Processed files table has been reset"}
    except Exception as e:
        print(f"Error resetting processed files table: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to reset processed files table: {str(e)}",
        }


def lambda_handler(event, context):
    """
    Takes in a bucket and folder name and returns
    a list of objects and their metadata.

    Example event:
    {
        "Bucket": "media-processing-bucket",
        "Prefix": "input-folder/"
    }

    If event is empty, uses DEFAULT_BUCKET from environment variables
    with root directory as prefix.

    Special event for cache reset:
    {
        "cache": "reset"
    }
    """

    s3 = boto3.client("s3")

    # Handle empty event by using environment variables
    if not event or (
        not event.get("Bucket")
        and not event.get("Prefix")
        and not event.get("cache")
    ):
        bucket = os.environ.get("DEFAULT_BUCKET")
        if not bucket:
            raise ValueError(
                "No bucket specified in event and DEFAULT_BUCKET environment variable not set"
            )
        prefix = ""  # Use root directory
        print(f"Using default bucket: {bucket} with root directory")
    elif event.get("Bucket"):
        bucket = event["Bucket"]
        prefix = event.get("Prefix", "")
    else:
        # For cache operations, still need to determine bucket
        bucket = os.environ.get("DEFAULT_BUCKET")
        if not bucket:
            raise ValueError(
                "No bucket specified in event and DEFAULT_BUCKET environment variable not set"
            )
        prefix = ""

    # Handle cache reset event
    if event.get("cache") == "reset":
        return reset_processed_files()

    lambda_mappings = {
        "mp4": "process-video",
        "webm": "process-video",
        "pdf": "process-pdf",
        "mp3": "process-audio",
        "wav": "process-audio",
        "flac": "process-audio",
        "m4a": "process-audio",
        "txt": "process-text",
        "vtt": "process-text",
    }

    # Get processed files to avoid reprocessing
    processed_uris = get_processed_files()
    print(f"Found {len(processed_uris)} files already processed")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    results = []

    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith("/"):
                continue

            s3_uri = f"s3://{bucket}/{key}"

            # Skip files that are already processed
            if s3_uri in processed_uris:
                print(f"Skipping already processed file: {key}")
                continue

            file_extension = key.split(".")[-1].lower()
            lambda_name = lambda_mappings.get(file_extension)
            if lambda_name:
                results.append(
                    {
                        "bucket": bucket,
                        "key": key,
                        "lambda_name": lambda_name,
                        "s3_uri": s3_uri,
                        "data_type": file_extension,
                        "timestamp": context.aws_request_id,
                    }
                )
            else:
                print(f"Skipping unsupported file type: {key}")

    print(f"Found {len(results)} new files to process")

    return results
