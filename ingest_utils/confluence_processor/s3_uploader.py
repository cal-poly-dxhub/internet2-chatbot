import os
from typing import Optional

import boto3 #type: ignore


class S3Uploader:
    def __init__(self, bucket_name: str, region_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3", region_name=region_name)

    def upload_file(
        self,
        file_path: str,
        s3_object_name_relative_to_subfolder: str,
        is_subscriber_content: bool,
        source_url: Optional[str] = None,
        parent_folder_name: Optional[str] = None,
        parent_folder_url: Optional[str] = None,
        relative_start_time: Optional[int] = None,
    ) -> bool:
        """
        Uploads a file to S3 with specified metadata.
        """
        s3_object_key = s3_object_name_relative_to_subfolder.replace("\\", "/")

        extra_args = {
            "Metadata": {
                "member-content": "true" if is_subscriber_content else "false"
            }
        }

        if source_url:
            extra_args["Metadata"]["source-url"] = source_url
        if parent_folder_name:
            extra_args["Metadata"]["parent-folder-name"] = parent_folder_name
        if parent_folder_url:
            extra_args["Metadata"]["parent-folder-url"] = parent_folder_url
        if relative_start_time is not None:
            extra_args["Metadata"]["relative-start-time"] = str(
                relative_start_time
            )

        try:
            print(
                f"Uploading {file_path} to s3://{self.bucket_name}/{s3_object_key} with metadata member-content: {'true' if is_subscriber_content else 'false'}{', source-url: ' + source_url if source_url else ''}{', parent-folder-name: ' + parent_folder_name if parent_folder_name else ''}{', parent-folder-url: ' + parent_folder_url if parent_folder_url else ''}"
            )
            self.s3_client.upload_file(
                file_path, self.bucket_name, s3_object_key, ExtraArgs=extra_args
            )
            print(f"Successfully uploaded {os.path.basename(file_path)}")
            return True
        except Exception as e:
            print(f"Error uploading {os.path.basename(file_path)}: {e}")
            return False
