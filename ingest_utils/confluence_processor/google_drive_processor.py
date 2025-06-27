# TODO: Rename this file to google_drive_processor.py as part of the refactor for client delivery.
import json
import os
import re  # Added for regular expressions
import subprocess
from typing import Dict, List, Optional

import pandas as pd
import yaml
from dotenv import dotenv_values
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from s3_uploader import S3Uploader

# Added for developerKey

# just incase
native_types = {
    "application/vnd.google-apps.document": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.google-apps.drawing": "image/png",
    "application/vnd.google-apps.script": "application/vnd.google-apps.script+json",
    "application/vnd.google-apps.form": "application/zip",
    "application/vnd.google-apps.site": "text/html",
    "application/vnd.google-apps.fusiontable": "application/zip",
}
blob_types = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "text/plain",
    "application/zip",
    "video/mp4",
    "audio/mpeg",
    "audio/wav",
    "text/vtt",
    "audio/mp4",
}


class GoogleDriveProcessor:
    def _get_normalized_file_name(self, file_name: str) -> str:
        """
        Normalizes a file name by removing common extensions and  suffixes
         make comparison for MP4 dominance logic
        """
        name_without_ext = os.path.splitext(file_name)[0]
        # Remove common suffixes that might differentiate files of the same content
        # Order matters: removes longer more specific patterns first
        suffixes_to_remove = [
            "_recording",
            "_chat",
            "_audio",
            "_transcript",
            "_video",
        ]
        for suffix in suffixes_to_remove:
            if name_without_ext.lower().endswith(suffix):
                name_without_ext = name_without_ext[: -len(suffix)]
                break  # Assuming only one such suffix per file for simplicity

        # Remove common file extensions for audio/video/text if they are still there
        # This is a fallback in case os.path.splitext didn't catch a complex extension
        name_without_ext = re.sub(
            r"\.(mp4|vtt|m4a|mp3|wav|txt|pdf|docx|xlsx|pptx)$/i",
            "",
            name_without_ext,
            flags=re.IGNORECASE,
        )

        return name_without_ext.strip().lower()

    def __init__(
        self,
        service_account_json: str,
        s3_uploader: S3Uploader,
        output_dir: str,
    ):
        self.creds = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )

        # Get API key from environment and pass  if available
        api_key = os.getenv("GOOGLE_API_KEY")
        if api_key:
            print(
                "DEBUG: GOOGLE_API_KEY found. Initializing Drive service with developerKey."
            )
            self.service = build(
                "drive", "v3", credentials=self.creds, developerKey=api_key
            )
        else:
            print(
                "DEBUG: GOOGLE_API_KEY not found. Initializing Drive service without developerKey."
            )
            self.service = build("drive", "v3", credentials=self.creds)

        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.s3_uploader = s3_uploader

    def _get_folder_id(self, url: str) -> str:
        """Extract folder ID from Google Drive URL"""
        try:
            # Handle various Google Drive URL formats
            if "/folders/" in url:
                return url.split("/folders/")[1].split("?")[0]
            elif (
                "id=" in url
            ):  # For direct file links that might be used for folders implicitly
                return url.split("id=")[1].split("&")[0]
            else:
                raise ValueError(f"Could not extract folder ID from URL: {url}")
        except IndexError:
            raise ValueError(f"Could not extract folder ID from URL: {url}")

    def _get_folder_name(self, folder_id: str) -> str:
        """Fetches the name of a Google Drive folder given ID"""
        try:
            folder = (
                self.service.files()
                .get(
                    fileId=folder_id,
                    fields="name",
                    # needs to be true (cant access if not true big bug )
                    supportsAllDrives=True,
                )
                .execute()
            )
            return folder.get("name", f"unknown_folder_name_{folder_id}")
        except HttpError as e:
            print(
                f"ERROR: Could not get name for folder ID {folder_id} due to HTTP error: {e}"
            )
            return f"error_folder_name_{folder_id}"
        except Exception as e:
            print(
                f"ERROR: Unexpected error getting folder name for {folder_id}: {e}"
            )
            return f"error_folder_name_{folder_id}"

    def _list_files_in_folder(self, folder_id: str) -> List[Dict]:
        """List all files and subfolders in a given folder"""
        print(f"DEBUG: Listing items in folder ID: {folder_id}")
        # Query for files AND folders within the given folder_id
        query = f"'{folder_id}' in parents and trashed = false"
        results = (
            self.service.files()
            .list(
                q=query,
                pageSize=1000,
                fields="files(id, name, mimeType, webViewLink, webContentLink)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        items = results.get("files", [])
        print(f"DEBUG: Found {len(items)} items in folder ID {folder_id}")
        for item in items:
            print(
                f'DEBUG:   Item found: Name="{item.get("name")}", MimeType="{item.get("mimeType")}", ID="{item.get("id")}", WebViewLink="{item.get("webViewLink", "N/A")}"'
            )
        return items  # Now returns both files and folders

    def _sanitize_drive_file_url(self, url: Optional[str]) -> Optional[str]:
        """Removes /view and any query parameters from a Google Drive file URL, returning clean link"""
        if url and "https://drive.google.com/file/d/" in url:
            # Extract up to the file ID
            parts = url.split("/")
            try:
                idx = parts.index("d")
                file_id = parts[idx + 1]
                return f"https://drive.google.com/file/d/{file_id}"
            except (ValueError, IndexError):
                return url  # fallback to original if unexpected format
        return url

    def convert_to_pdf(self, input_path: str, output_dir: str) -> str | None:
        """Convert a document to PDF using LibreOffice"""
        try:
            cmd = [
                "soffice",
                "--headless",
                "--convert-to",
                "pdf",
                "--outdir",
                output_dir,
                input_path,
            ]
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            output, error = process.communicate()
            pdf_path = os.path.join(
                output_dir,
                os.path.splitext(os.path.basename(input_path))[0] + ".pdf",
            )
            if process.returncode == 0 and os.path.exists(pdf_path):
                return pdf_path
            else:
                print(f"PDF conversion failed: {error.decode()}")
                return None
        except Exception as e:
            print(f"Error converting file to PDF: {e}")
            return None

    def is_file_public(self, file_id: str) -> bool:
        """Check if a Google Drive file is public (anyone can read)."""
        try:
            permissions = (
                self.service.permissions()
                .list(fileId=file_id)
                .execute()
                .get("permissions", [])
            )
            for perm in permissions:
                if perm.get("type") == "anyone" and perm.get("role") in (
                    "reader",
                    "commenter",
                    "writer",
                ):
                    return True
            return False
        except Exception as e:
            print(
                f"WARNING: Could not check permissions for file {file_id}: {e}"
            )
            return False

    def _download_and_upload_file(
        self,
        file: Dict,
        is_subscriber_content: bool,
        source_url: Optional[str] = None,
        parent_folder_name: Optional[str] = None,
        parent_folder_url: Optional[str] = None,
    ) -> Optional[str]:
        """Download a file from Google Drive and upload it to S3"""
        file_id = file["id"]
        mime_type = file["mimeType"]
        name = file["name"]

        print(
            f"DEBUG: Attempting to download file '{name}' (ID: {file_id}, MimeType: {mime_type})"
        )

        output_path = os.path.join(self.output_dir, name)

        try:
            request = None
            export_mime_type = None

            if mime_type in native_types:
                export_mime_type = native_types[mime_type]
                request = self.service.files().export(
                    fileId=file_id, mimeType=export_mime_type
                )
                print(
                    f"DEBUG: Exporting Google Workspace file '{name}' to MimeType: {export_mime_type}"
                )
            elif mime_type in blob_types:
                request = self.service.files().get_media(fileId=file_id)
                print(
                    f"DEBUG: Getting media for blob file '{name}' (MimeType: {mime_type})"
                )
            else:
                print(
                    f"DEBUG: Skipping unsupported Google Drive file type: {mime_type} - {name}"
                )
                return None

            # Download the file
            if request:
                response = request.execute()
                print(
                    f"DEBUG: Google Drive API request executed successfully for '{name}'."
                )
            else:
                print(
                    f"DEBUG: No request object created for '{name}', skipping download."
                )
                return None

            # For exported files response is bytes  get_media it's bytes directly
            if export_mime_type and export_mime_type.startswith(
                "application/vnd.openxmlformats"
            ):
                # Append appropriate extension for exported Google Workspace files
                if "wordprocessingml" in export_mime_type:
                    name += ".docx"
                elif "spreadsheetml" in export_mime_type:
                    name += ".xlsx"
                elif "presentationml" in export_mime_type:
                    name += ".pptx"
                output_path = os.path.join(self.output_dir, name)
                print(
                    f"DEBUG: Appended extension for exported file. New path: {output_path}"
                )

            with open(output_path, "wb") as f:
                f.write(response)
            print(f"Downloaded from Drive: {name} -> {output_path}")

            # Sanitize the source_url if present
            sanitized_source_url = (
                self._sanitize_drive_file_url(source_url)
                if source_url
                else None
            )

            #  Check if file is public and adjust is_subscriber_content
            is_public = self.is_file_public(file_id)
            if is_public:
                print(
                    f"INFO: File '{name}' is public. Setting member-content to false in S3 metadata."
                )
                is_subscriber_content = False

            # Convert DOCX/PPTX to PDF and upload PDF only
            ext = os.path.splitext(output_path)[1].lower()
            if ext in [".docx", ".pptx"]:
                pdf_path = self.convert_to_pdf(output_path, self.output_dir)
                if pdf_path and os.path.exists(pdf_path):
                    print(f"Converted {output_path} to PDF: {pdf_path}")
                    # Upload the PDF instead
                    s3_object_name = os.path.basename(pdf_path)
                    upload_success = self.s3_uploader.upload_file(
                        pdf_path,
                        s3_object_name,
                        is_subscriber_content,
                        source_url=sanitized_source_url,
                        parent_folder_name=parent_folder_name,
                        parent_folder_url=parent_folder_url,
                    )
                    if upload_success:
                        print(
                            f"DEBUG: S3 upload successful for {pdf_path}. Removing local files."
                        )
                        os.remove(output_path)
                        os.remove(pdf_path)
                    else:
                        print(
                            f"ERROR: S3 upload failed for {pdf_path}. Keeping local files for inspection."
                        )
                    return pdf_path if upload_success else None
                else:
                    print(
                        f"ERROR: PDF conversion failed for {output_path}. Uploading original file instead."
                    )
                    # Fallback to uploading the original file

            # Upload all other files to the configured s3_subfolder (no separate type-based folders)
            s3_object_name = f"{name}"
            upload_success = self.s3_uploader.upload_file(
                output_path,
                s3_object_name,
                is_subscriber_content,
                source_url=sanitized_source_url,
                parent_folder_name=parent_folder_name,
                parent_folder_url=parent_folder_url,
            )
            if upload_success:
                print(
                    f"DEBUG: S3 upload successful for {name}. Removing local file."
                )
                os.remove(output_path)  # Clean up local file after upload
            else:
                print(
                    f"ERROR: S3 upload failed for {name}. Keeping local file for inspection."
                )
            return output_path if upload_success else None

        except Exception as e:
            print(
                f"ERROR: Error during download or upload of {name} from Google Drive: {str(e)}"
            )
            if os.path.exists(output_path):
                os.remove(output_path)
            return None

    def _process_folder_recursively(
        self,
        folder_id: str,
        is_subscriber: bool,
        parent_folder_name: Optional[str] = None,
        parent_folder_url: Optional[str] = None,
    ):
        """
        1. Recursively processes a Google Drive folder, including subfolders and files,
        2. MP4 dominance logic and uploading to S3 with type-based folders
        3. Passes immediate parent folder name and URL to file uploads
        """
        print(f"DEBUG: Recursively processing folder ID: {folder_id}")

        # Get current folder's name and URL
        current_folder_name = self._get_folder_name(folder_id)
        current_folder_url = (
            f"https://drive.google.com/drive/folders/{folder_id}"
        )

        items_in_folder = self._list_files_in_folder(folder_id)
        files = [
            item
            for item in items_in_folder
            if item["mimeType"] != "application/vnd.google-apps.folder"
        ]
        folders = [
            item
            for item in items_in_folder
            if item["mimeType"] == "application/vnd.google-apps.folder"
        ]

        # Group files by normalized name for MP4 dominance logic
        file_groups_by_normalized_name: dict[str, List[Dict]] = {}
        for file_item in files:
            normalized_name = self._get_normalized_file_name(file_item["name"])
            if normalized_name not in file_groups_by_normalized_name:
                file_groups_by_normalized_name[normalized_name] = []
            file_groups_by_normalized_name[normalized_name].append(file_item)

        # Process files within the current folder, applying MP4 dominance
        for (
            normalized_name,
            group_files,
        ) in file_groups_by_normalized_name.items():
            has_mp4_in_group = any(
                f["mimeType"] == "video/mp4" for f in group_files
            )

            for file_item in group_files:
                file_mime_type = file_item["mimeType"]
                file_name = file_item["name"]
                # Get the source link for the individual file
                file_source_url = file_item.get("webViewLink") or file_item.get(
                    "webContentLink"
                )

                if file_mime_type == "video/mp4":
                    # Always download MP4
                    self._download_and_upload_file(
                        file_item,
                        is_subscriber,
                        source_url=file_source_url,
                        parent_folder_name=current_folder_name,
                        parent_folder_url=current_folder_url,
                    )
                elif has_mp4_in_group and file_mime_type in [
                    "text/vtt",
                    "audio/mp4",
                    "audio/mpeg",
                ]:
                    # Skip VTT/M4A if an MP4 with the same normalized name is present in this group
                    print(
                        f"DEBUG: Skipping {file_name} ({file_mime_type}) because an MP4 with a similar name ({normalized_name}) is present in the group."
                    )
                    continue
                else:
                    # Download all other file types, and VTT/M4A if no MP4 in this group
                    self._download_and_upload_file(
                        file_item,
                        is_subscriber,
                        source_url=file_source_url,
                        parent_folder_name=current_folder_name,
                        parent_folder_url=current_folder_url,
                    )

        # Recursively process subfolders
        for subfolder_item in folders:
            subfolder_id = subfolder_item["id"]
            self._process_folder_recursively(
                subfolder_id,
                is_subscriber,
                parent_folder_name=current_folder_name,
                parent_folder_url=current_folder_url,
            )


def main():
    service_account_json_data = None

    print("DEBUG: Starting main function for Google Drive processing.")

    # Load environment variables from names.env
    env_vars = dotenv_values("names.env")
    print(
        f"DEBUG: Loaded environment variables from names.env: {list(env_vars.keys())}"
    )

    # Load config from config.yaml
    with open("../../config.yaml", "r") as f:
        config = yaml.safe_load(f)

    s3_bucket_name = config["s3_bucket_name"]
    aws_region = config.get("aws_region", "us-west-2")
    download_dir = config.get(
        "google_drive_download_dir", "google_drive_downloads"
    )

    # First, try to get the path to the service account JSON file from loaded environment variables
    service_account_json_path = env_vars.get("GOOGLE_DRIVE_CREDENTIALS")
    print(
        f"DEBUG: GOOGLE_DRIVE_CREDENTIALS path from dotenv: {service_account_json_path}"
    )

    if service_account_json_path:
        try:
            with open(service_account_json_path, "r") as f:
                service_account_json_data = json.load(f)
            print(
                f"Successfully loaded Google Service Account JSON from file: {service_account_json_path}"
            )
            print(
                f"DEBUG: Type of loaded service_account_json_data (from file): {type(service_account_json_data)}"
            )
        except FileNotFoundError:
            print(
                f"ERROR: Google Service Account JSON file not found at {service_account_json_path}"
            )
        except json.JSONDecodeError as e:
            print(
                f"ERROR: Decoding JSON from Google Service Account file {service_account_json_path} failed: {e}"
            )
        except Exception as e:
            print(
                f"ERROR: Unexpected error reading Google Service Account JSON file {service_account_json_path}: {e}"
            )

    # If not found or failed from path, try to get the content directly from an environment variable (fallback)
    if not service_account_json_data:
        print(
            "DEBUG: Attempting to load from GOOGLE_SERVICE_ACCOUNT_JSON environment variable (direct content)."
        )
        service_account_json_content = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if service_account_json_content:
            try:
                service_account_json_data = json.loads(
                    service_account_json_content
                )
                print(
                    "Successfully loaded Google Service Account JSON from GOOGLE_SERVICE_ACCOUNT_JSON environment variable."
                )
                print(
                    f"DEBUG: Type of loaded service_account_json_data (from env var): {type(service_account_json_data)}"
                )
            except json.JSONDecodeError as e:
                print(
                    f"ERROR: Decoding JSON from GOOGLE_SERVICE_ACCOUNT_JSON environment variable failed: {e}"
                )
        else:
            print(
                "DEBUG: GOOGLE_SERVICE_ACCOUNT_JSON environment variable is not set or empty."
            )

    if not service_account_json_data:
        print(
            "DEBUG: service_account_json_data is still None. Raising ValueError."
        )
        raise ValueError(
            "Google Service Account JSON credentials not found. Please set GOOGLE_DRIVE_CREDENTIALS (path in names.env) or GOOGLE_SERVICE_ACCOUNT_JSON (content) environment variable."
        )

    # Convert the loaded JSON data back to a string for the credential constructor
    service_account_json_content_string = json.dumps(service_account_json_data)
    print(
        f"DEBUG: Converted JSON data to string for credential constructor. Length: {len(service_account_json_content_string)}"
    )

    s3_uploader = S3Uploader(bucket_name=s3_bucket_name, region_name=aws_region)
    drive_processor = GoogleDriveProcessor(
        service_account_json_content_string, s3_uploader, download_dir
    )

    # Read URLs from confluence_asset_links.csv
    try:
        # Expecting 'url' and 'is_subscriber_content' columns
        assets_df = pd.read_csv("confluence_asset_links.csv")
        if (
            "url" not in assets_df.columns
            or "is_subscriber_content" not in assets_df.columns
        ):
            raise ValueError(
                "'confluence_asset_links.csv' must contain 'url' and 'is_subscriber_content' columns."
            )
        print(
            f"Found {len(assets_df)} asset links in confluence_asset_links.csv"
        )
    except FileNotFoundError:
        print(
            "Error: confluence_asset_links.csv not found. Please run confluence_processor.py first."
        )
        return
    except ValueError as e:
        print(f"Error reading confluence_asset_links.csv: {e}")
        return

    # Process only Google Drive folder URLs from the CSV
    google_drive_urls_to_process = []
    for index, row in assets_df.iterrows():
        url = str(row["url"]).strip()
        is_subscriber = (
            str(row["is_subscriber_content"]).strip().lower() == "true"
        )

        # Attempt to get folder ID to confirm it's a Google Drive folder URL
        try:
            folder_id = drive_processor._get_folder_id(url)
            google_drive_urls_to_process.append(
                {
                    "url": url,
                    "is_subscriber": is_subscriber,
                    "folder_id": folder_id,
                }
            )
        except ValueError:
            print(f"Skipping non-Google Drive URL from CSV: {url}")
            continue

    if not google_drive_urls_to_process:
        print(
            "No Google Drive folder URLs found in confluence_asset_links.csv. Exiting."
        )
        return

    print(
        f"Found {len(google_drive_urls_to_process)} Google Drive folders to process."
    )

    # Process each Google Drive URL
    for item in google_drive_urls_to_process:
        url = item["url"]
        is_subscriber = item["is_subscriber"]
        folder_id = item["folder_id"]

        print(
            f"\nProcessing Google Drive folder: {url} (Subscriber: {is_subscriber})"
        )

        try:
            # Initiate recursive processing for the top-level folder
            drive_processor._process_folder_recursively(
                folder_id, is_subscriber
            )

        except Exception as e:
            print(f"An unexpected error occurred for URL {url}: {e}")

    print("\nGoogle Drive asset processing complete.")


if __name__ == "__main__":
    main()
