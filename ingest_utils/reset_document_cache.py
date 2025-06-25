"""Script to reset the document cache held in s3."""

import boto3
import yaml

config = yaml.safe_load(open("../config.yaml"))


def reset_processed_uris_cache(bucket_name: str) -> None:
    """
    Deletes and recreates the processed URIs cache file in S3.

    Args:
        bucket_name (str): Name of the S3 bucket
    """
    s3 = boto3.client("s3")
    key = config["ingest_cache_file"]

    try:
        # Delete existing file if it exists
        try:
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted existing cache file: {key}")
        except s3.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "404":  # Ignore if file doesn't exist
                raise

        # Create new empty file
        s3.put_object(Bucket=bucket_name, Key=key, Body="")
        print(f"Created new empty cache file: {key}")

    except Exception as e:
        print(f"Error resetting URI cache file: {str(e)}")


if __name__ == "__main__":
    reset_processed_uris_cache(config["input_bucket_name"])
