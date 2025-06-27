import logging
import os
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def get_filename_from_s3_uri(s3_uri: str) -> str:
    """Extract just the filename from an S3 URI"""

    parsed_uri = urlparse(s3_uri)
    # Get the full path and extract the filename using os.path.basename
    return os.path.basename(parsed_uri.path)


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
        logger.error("AWS credentials not found")
        return None
    except Exception as e:
        logger.error(e)
        return None
