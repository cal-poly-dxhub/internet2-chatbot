import os

import boto3
from botocore.exceptions import ClientError


def get_cache_contents(s3_client, bucket):
    """
    Retrieve the contents of the cache file from S3.
    Returns a set of cached S3 URIs.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key="cache_file.txt")
        content = response["Body"].read().decode("utf-8").strip()
        if content:
            return set(
                line.strip() for line in content.split("\n") if line.strip()
            )
        return set()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            # Cache file doesn't exist, create an empty one
            print("Cache file doesn't exist, creating empty cache file")
            s3_client.put_object(Bucket=bucket, Key="cache_file.txt", Body="")
            return set()
        else:
            print(f"Error reading cache file: {str(e)}")
            return set()


def update_cache_file(s3_client, bucket, new_uris):
    """
    Add new S3 URIs to the cache file.
    """
    if not new_uris:
        return

    try:
        # Get existing cache contents
        existing_cache = get_cache_contents(s3_client, bucket)

        # Add new URIs
        updated_cache = existing_cache.union(set(new_uris))

        # Write back to S3
        cache_content = "\n".join(sorted(updated_cache))
        s3_client.put_object(
            Bucket=bucket, Key="cache_file.txt", Body=cache_content
        )

        print(f"Updated cache file with {len(new_uris)} new entries")

    except Exception as e:
        print(f"Error updating cache file: {str(e)}")


def reset_cache_file(s3_client, bucket):
    """
    Reset the cache file by uploading an empty file.
    """
    try:
        s3_client.put_object(Bucket=bucket, Key="cache_file.txt", Body="")
        print("Cache file has been reset (emptied)")
        return {"status": "success", "message": "Cache file has been reset"}
    except Exception as e:
        print(f"Error resetting cache file: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to reset cache file: {str(e)}",
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
        return reset_cache_file(s3, bucket)

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

    # Get cached files to avoid reprocessing
    cached_uris = get_cache_contents(s3, bucket)
    print(f"Found {len(cached_uris)} files in cache")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    results = []
    new_cache_entries = []

    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if key == "cache_file.txt":
                continue

            s3_uri = f"s3://{bucket}/{key}"

            # Skip files that are already in cache
            if s3_uri in cached_uris:
                print(f"Skipping cached file: {key}")
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
                # Add to cache entries for this batch
                new_cache_entries.append(s3_uri)
            else:
                print(f"Skipping unsupported file type: {key}")

    # Update cache file with newly processed files
    if new_cache_entries:
        update_cache_file(s3, bucket, new_cache_entries)
        print(
            f"Processing {len(results)} new files, added {len(new_cache_entries)} to cache"
        )
    else:
        print("No new files to process")

    return results
