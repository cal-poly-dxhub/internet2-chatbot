import os
import pandas as pd# type: ignore
from googleapiclient.discovery import build# type: ignore
from google.oauth2 import service_account# type: ignore
import json
from typing import List, Dict, Optional
from s3_uploader import S3Uploader
from dotenv import dotenv_values
import re  # Added for regular expressions
from googleapiclient.errors import HttpError# type: ignore
import subprocess
import yaml # type: ignore
import logging
import pymediainfo # type: ignore


# Configure logging
logger = logging.getLogger(__name__)

# Constants for video processing
DURATION_THRESHOLD_HOURS = 3  # Videos longer than 3 hours will be split
CHUNK_AMOUNT = 14  # 14 minutes
CHUNK_DURATION_HOURS = 1.5  # Each chunk will be 1.5 hours long
SIZE_THRESHOLD_GB = 2  # Videos bigger than 2GB will be split
MAX_CHUNK_SIZE_GB = 2  # Maximum size per chunk for Lambda processing
CHUNK_DURATION_MINUTES = 20  # Each chunk will be 20 minutes long
CONFLUENCE_ASSETS_LINKS = "confluence_asset_links.csv"
NAMES_ENV = "names.env"

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
    def __init__(
        self,
        service_account_json: str,
        s3_uploader: S3Uploader,
        output_dir: str
    ):
        self.creds = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )

        # Get API key from environment and pass  if available
        api_key = os.getenv("GOOGLE_API_KEY")
        if api_key:
            logger.debug(
                "GOOGLE_API_KEY found. Initializing Drive service with developerKey."
            )
            self.service = build(
                "drive", "v3", credentials=self.creds, developerKey=api_key
            )
        else:
            logger.debug("GOOGLE_API_KEY not found. Initializing Drive service without developerKey.")
            self.service = build("drive", "v3", credentials=self.creds)

        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.s3_uploader = s3_uploader

        # Global timestamp tracking across all videos
        self.global_timestamp_seconds = 0

    def _get_normalized_file_name(self, file_name: str) -> str:
        """
        Normalizes a file name by removing common extensions and  suffixes
         make comparison for MP4 dominance logic
        """
        name_without_ext = os.path.splitext(file_name)[0]
        # Remove common suffixes that might differentiate files of the same content
        # Order matters: removes longer more specific patterns first
        suffixes_to_remove = ["_recording", "_chat", "_audio", "_transcript", "_video"]
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
            logger.error(
                f"Could not get name for folder ID {folder_id} due to HTTP error: {e}"
            )
            return f"error_folder_name_{folder_id}"
        except Exception as e:
            logger.error(f"Unexpected error getting folder name for {folder_id}: {e}")
            return f"error_folder_name_{folder_id}"

    def _list_files_in_folder(self, folder_id: str) -> List[Dict]:
        """List all files and subfolders in a given folder"""

        logger.debug(f"Listing items in folder ID: {folder_id}")
        # Query for files AND folders within the given folder_id (direct children with id that givne)
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
        logger.debug(f"Found {len(items)} items in folder ID {folder_id}")
        for item in items:
            logger.debug(
                f'DEBUG:   Item found: Name="{item.get("name")}", MimeType="{item.get("mimeType")}", ID="{item.get("id")}", WebViewLink="{item.get("webViewLink", "N/A")}"'
            )
        return items  # Now returns both files and folders

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
                output_dir, os.path.splitext(os.path.basename(input_path))[0] + ".pdf"
            )
            if process.returncode == 0 and os.path.exists(pdf_path):
                return pdf_path
            else:
                logger.error(f"PDF conversion failed: {error.decode()}")
                return None
        except Exception as e:
            logger.error(f"Error converting file to PDF: {e}")
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
            logger.warning(f"Could not check permissions for file {file_id}: {e}")
            return False

    def _sanitize_drive_file_url(self, url: Optional[str]) -> Optional[str]:
        """
        Removes query parameters and /view, /edit, etc. from Google Drive/Docs/Sheets/Slides URLs,
        returning a clean link with just the file/folder/document ID.
        """
        if not url:
            return url
        # Google Drive file
        match = re.match(r"(https://drive\.google\.com/file/d/[^/]+)", url)
        if match:
            return match.group(1)
        # Google Docs/Sheets/Slides
        match = re.match(
            r"(https://docs\.google\.com/(?:document|spreadsheets|presentation)/d/[^/]+)",
            url,
        )
        if match:
            return match.group(1)
        # Google Drive folder
        match = re.match(r"(https://drive\.google\.com/drive/folders/[^/?]+)", url)
        if match:
            return match.group(1)
        return url

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

        logger.debug(
            f"Attempting to download file '{name}' (ID: {file_id}, MimeType: {mime_type})"
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
                logger.debug(
                    f"Exporting Google Workspace file '{name}' to MimeType: {export_mime_type}"
                )
            elif mime_type in blob_types:
                request = self.service.files().get_media(fileId=file_id)
                logger.debug(
                    f"Getting media for blob file '{name}' (MimeType: {mime_type})"
                )
            else:
                logger.debug(
                    f"Skipping unsupported Google Drive file type: {mime_type} - {name}"
                )
                return None

            # Download the file
            if request:
                response = request.execute()
                logger.debug(
                    f"Google Drive API request executed successfully for '{name}'."
                )
            else:
                logger.debug(
                    f"No request object created for '{name}', skipping download."
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
                logger.debug(
                    f"Appended extension for exported file. New path: {output_path}"
                )

            with open(output_path, "wb") as f:
                f.write(response)
            logger.info(f"Downloaded from Drive: {name} -> {output_path}")

            #  Check if file is public and adjust is_subscriber_content
            is_public = self.is_file_public(file_id)
            if is_public:
                logger.info(
                    f"File '{name}' is public. Setting member-content to false in S3 metadata."
                )
                is_subscriber_content = False

            # Convert DOCX/PPTX to PDF and upload PDF only
            ext = os.path.splitext(output_path)[1].lower()
            if ext in [".docx", ".pptx"]:
                pdf_path = self.convert_to_pdf(output_path, self.output_dir)
                if pdf_path and os.path.exists(pdf_path):
                    logger.info(f"Converted {output_path} to PDF: {pdf_path}")
                    # Upload the PDF instead
                    s3_object_name = os.path.basename(pdf_path)
                    sanitized_source_url = self._sanitize_drive_file_url(source_url) if source_url else None
                    upload_success = self.s3_uploader.upload_file(
                        pdf_path,
                        s3_object_name,
                        is_subscriber_content,
                        source_url=sanitized_source_url,
                        parent_folder_name=parent_folder_name,
                        parent_folder_url=parent_folder_url,
                    )
                    if upload_success:
                        logger.debug(
                            f"S3 upload successful for {pdf_path}. Removing local files."
                        )
                        os.remove(output_path)
                        os.remove(pdf_path)
                    else:
                        logger.error(
                            f"S3 upload failed for {pdf_path}. Keeping local files for inspection."
                        )
                    return pdf_path if upload_success else None
                else:
                    logger.error(
                        f"PDF conversion failed for {output_path}. Uploading original file instead."
                    )
                    # Fallback to uploading the original file

            # Upload all other files to the configured s3_subfolder (no separate type-based folders)
            s3_object_name = f"{name}"

            # Check if this is a video file and process it before uploading
            file_ext = os.path.splitext(output_path)[1].lower()
            if file_ext in [".mp4", ".avi", ".mov", ".mkv", ".wmv", ".flv", ".webm"]:
                # Process video file with splitting if needed
                logger.info(f"Video file detected, checking for processing: {name}")
                sanitized_source_url = self._sanitize_drive_file_url(source_url) if source_url else None
                chunked = self.process_video_file(
                    output_path,
                    s3_object_name,
                    is_subscriber_content,
                    source_url=sanitized_source_url,
                    parent_folder_name=parent_folder_name,
                    parent_folder_url=parent_folder_url,
                )
                if chunked:
                    logger.info(
                        f"INFO: {name} was chunked. Skipping upload of the original file to S3."
                    )

                    os.remove(output_path)  # Clean up local file after chunking
                    return output_path

            sanitized_source_url = self._sanitize_drive_file_url(source_url) if source_url else None
            upload_success = self.s3_uploader.upload_file(
                output_path,
                s3_object_name,
                is_subscriber_content,
                source_url=sanitized_source_url,
                parent_folder_name=parent_folder_name,
                parent_folder_url=parent_folder_url,
            )
            if upload_success:
                logger.debug(f"S3 upload successful for {name}. Removing local file.")
                os.remove(output_path)  # Clean up local file after upload
            else:
                logger.error(
                    f"S3 upload failed for {name}. Keeping local file for inspection."
                )
            return output_path if upload_success else None

        except Exception as e:
            logger.error(
                f"Error during download or upload of {name} from Google Drive: {str(e)}"
            )
            if os.path.exists(output_path):
                os.remove(output_path)
            return None

    def get_video_duration_seconds(self, file_path: str) -> Optional[float]:
        try:
            media_info = pymediainfo.MediaInfo.parse(file_path)

            # Get duration from video track
            for track in media_info.tracks:
                if track.track_type == "Video":
                    if hasattr(track, "duration") and track.duration:
                        # Duration is in milliseconds, convert to seconds
                        duration_seconds = float(track.duration) / 1000.0
                        return duration_seconds

            logger.warning(f"No video duration found for {file_path}")
            return None

        except Exception as e:
            logger.error(f"Error getting video duration for {file_path}: {e}")
            return None

    def get_video_size_gb(self, file_path: str) -> Optional[float]:
        try:
            size_bytes = os.path.getsize(file_path)
            size_gb = size_bytes / (1024 * 1024 * 1024)  # Convert to GB
            return size_gb
        except Exception as e:
            logger.error(f"Error getting video size for {file_path}: {e}")
            return None

    def should_split_video(self, duration_seconds: float, file_size_bytes: int) -> bool:
        duration_hours = duration_seconds / 3600.0
        file_size_gb = file_size_bytes / (1024**3)
        # Split if duration > 3 hours OR size > 2GB
        return (
            duration_hours > DURATION_THRESHOLD_HOURS
            or file_size_gb > SIZE_THRESHOLD_GB
        )

    def calculate_chunks(
        self, duration_seconds: float, file_size_bytes: int
    ) -> List[Dict]:
        duration_hours = duration_seconds / 3600.0
        file_size_gb = file_size_bytes / (1024**3)

        chunks = []
        MIN_CHUNK_DURATION = 2  # seconds, skip chunks smaller than this

        # --- Adaptive Chunker  ---
        # Determine chunk duration based on what creates smaller chunks
        if file_size_gb > SIZE_THRESHOLD_GB:
            # Size-based splitting: ensure each chunk is under 2GB
            num_chunks_for_size = max(2, int(file_size_gb / MAX_CHUNK_SIZE_GB) + 1)
            chunk_duration_seconds = duration_seconds / num_chunks_for_size
        elif duration_hours > DURATION_THRESHOLD_HOURS:
            # Duration-based splitting: 1.5-hour chunks
            chunk_duration_seconds = CHUNK_DURATION_HOURS * 3600
        else:
            return [
                {
                    "start_time": 0,
                    "duration": duration_seconds,
                    "chunk_num": 1,
                    "relative_time": 0.0,
                }
            ]

        num_chunks = int(duration_seconds / chunk_duration_seconds) + 1

        cumulative_time = 0.0
        for part in range(1, num_chunks + 1):
            start_time = (part - 1) * chunk_duration_seconds
            if start_time >= duration_seconds:
                continue
            chunk_duration = min(chunk_duration_seconds, duration_seconds - start_time)
            if chunk_duration < MIN_CHUNK_DURATION:
                # Skip tiny last chunk
                continue
            relative_time = cumulative_time
            chunks.append(
                {
                    "start_time": start_time,
                    "duration": chunk_duration,
                    "chunk_num": part,
                    "relative_time": relative_time,
                }
            )
            cumulative_time += chunk_duration

        return chunks

    def process_video_file(
        self,
        file_path: str,
        s3_key: str,
        is_subscriber_content: bool,
        source_url: Optional[str] = None,
        parent_folder_name: Optional[str] = None,
        parent_folder_url: Optional[str] = None,
    ) -> bool:
        try:
            # Get video duration and file size
            duration_seconds = self.get_video_duration_seconds(file_path)
            file_size_bytes = os.path.getsize(file_path)

            if duration_seconds is None:
                logger.error(f"Could not get duration for {file_path}")
                return False

            # Check if splitting is needed
            if not self.should_split_video(duration_seconds, file_size_bytes):
                logger.info(f"Video {file_path} does not need splitting")
                # Upload as single file with global timestamp
                sanitized_source_url = self._sanitize_drive_file_url(source_url) if source_url else None
                success = self.s3_uploader.upload_file(
                    file_path,
                    s3_key,
                    is_subscriber_content,
                    source_url=sanitized_source_url,
                    parent_folder_name=parent_folder_name,
                    parent_folder_url=parent_folder_url,
                    relative_start_time=0,  # Always include, even if zero
                )

                if success:
                    self.global_timestamp_seconds += int(duration_seconds)

                return success
            else:
                logger.info(
                    f"Video {file_path} is being chunked. The original large file will NOT be uploaded to S3."
                )

            # Calculate chunks
            chunks = self.calculate_chunks(duration_seconds, file_size_bytes)
            logger.info(f"Splitting {file_path} into {len(chunks)} chunks")

            success = True
            for chunk in chunks:
                # Create chunk file using ffmpeg
                chunk_file_path = self._create_video_chunk(
                    file_path,
                    chunk["start_time"],
                    chunk["duration"],
                    chunk["chunk_num"],
                )

                if chunk_file_path:
                    # Create S3 key for chunk
                    base_name = os.path.splitext(s3_key)[0]
                    chunk_s3_key = f"{base_name}_chunk_{chunk['chunk_num']:03d}.mp4"

                    sanitized_source_url = self._sanitize_drive_file_url(source_url) if source_url else None
                    chunk_success = self.s3_uploader.upload_file(
                        chunk_file_path,
                        chunk_s3_key,
                        is_subscriber_content,
                        source_url=sanitized_source_url,
                        parent_folder_name=parent_folder_name,
                        parent_folder_url=parent_folder_url,
                        relative_start_time=int(chunk["relative_time"]),
                    )

                    if chunk_success:
                        self.global_timestamp_seconds += int(chunk["duration"])
                    else:
                        success = False
                    # add other meta data stuff
                    chunk["source_url"] = source_url
                    chunk["parent_folder_name"] = parent_folder_name
                    chunk["parent_folder_url"] = parent_folder_url

                    # Clean up chunk file
                    os.remove(chunk_file_path)
                else:
                    success = False

            return success

        except Exception as e:
            logger.error(f"Error processing video file {file_path}: {e}")
            return False

    def _create_video_chunk(
        self, input_file: str, start_time: float, duration: float, chunk_num: int
    ) -> Optional[str]:
        # ffmpeg
        try:
            output_file = f"{input_file}_chunk_{chunk_num:03d}.mp4"

            # ffmpeg  to slice
            cmd = [
                "ffmpeg",
                "-i",
                input_file,
                "-ss",
                str(start_time),
                "-t",
                str(duration),
                "-c",
                "copy",  # Copy without re-encoding for speed
                "-avoid_negative_ts",
                "make_zero",
                "-y",  # Overwrite output file
                output_file,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"Created chunk {chunk_num}: {output_file}")
                return output_file
            else:
                logger.error(f"ffmpeg failed for chunk {chunk_num}: {result.stderr}")
                return None

        except Exception as e:
            logger.error(f"Error creating video chunk {chunk_num}: {e}")
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
        logger.debug(f"Recursively processing folder ID: {folder_id}")

        # Get current folder's name and URL
        current_folder_name = self._get_folder_name(folder_id)
        current_folder_url = f"https://drive.google.com/drive/folders/{folder_id}"

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
        for normalized_name, group_files in file_groups_by_normalized_name.items():
            has_mp4_in_group = any(f["mimeType"] == "video/mp4" for f in group_files)

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
                    logger.debug(
                        f"Skipping {file_name} ({file_mime_type}) because an MP4 with a similar name ({normalized_name}) is present in the group."
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

    logger.debug(f"Starting main function for Google Drive processing.")
    # Load config and get env file path
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    config_path = os.path.join(project_root, "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    env_file = config.get("env_file", "names.env")
    env_vars = dotenv_values(env_file)
    logger.debug(f"Loaded environment variables from {env_file}: {list(env_vars.keys())}")

    s3_bucket_name = config["s3_bucket_name"]
    aws_region = config.get("aws_region", "us-west-2")
    download_dir = config.get("google_drive_download_dir", "google_drive_downloads")

    service_account_json_path = env_vars.get("GOOGLE_DRIVE_CREDENTIALS")
    logger.debug(
        f"GOOGLE_DRIVE_CREDENTIALS path from dotenv: {service_account_json_path}"
    )

    if service_account_json_path:
        with open(service_account_json_path, "r") as f:
            service_account_json_data = json.load(f)
        logger.info(
            f"Successfully loaded Google Service Account JSON from file: {service_account_json_path}"
        )


    # Convert the loaded JSON data back to a string for the credential constructor
    service_account_json_content_string = json.dumps(service_account_json_data)
    logger.debug(
        f"Converted JSON data to string for credential constructor. Length: {len(service_account_json_content_string)}"
    )

    s3_uploader = S3Uploader(bucket_name=s3_bucket_name, region_name=aws_region)
    drive_processor = GoogleDriveProcessor(
        service_account_json_content_string, s3_uploader, download_dir
    )

    # Read URLs from long_video.csv
    try:
        # Expecting 'url' and 'is_subscriber_content' columns
        assets_df = pd.read_csv(CONFLUENCE_ASSETS_LINKS)
        if (
            "url" not in assets_df.columns
            or "is_subscriber_content" not in assets_df.columns
        ):
            raise ValueError(
                "'confluence_asset_links' must contain 'url' and 'is_subscriber_content' columns."
            )
        logger.info(f"Found {len(assets_df)} asset links in t.csv")
    except FileNotFoundError:
        logger.error(
            "Error: confluence_asset_links not found. Please create the file with Google Drive folder URLs."
        )
        return
    except ValueError as e:
        logger.error(f"Error reading confluence_asset_links: {e}")
        return

    # Process only Google Drive folder URLs from the CSV
    google_drive_urls_to_process = []
    for index, row in assets_df.iterrows():
        url = str(row["url"]).strip()
        is_subscriber = str(row["is_subscriber_content"]).strip().lower() == "true"

        # Attempt to get folder ID to confirm it's a Google Drive folder URL
        try:
            folder_id = drive_processor._get_folder_id(url)
            google_drive_urls_to_process.append(
                {"url": url, "is_subscriber": is_subscriber, "folder_id": folder_id}
            )
        except ValueError:
            logger.warning(f"Skipping non-Google Drive URL from CSV: {url}")
            continue

    if not google_drive_urls_to_process:
        logger.info("No Google Drive folder URLs found in .csv. Exiting.")
        return

    logger.info(f"Found {len(google_drive_urls_to_process)} Google Drive folders to process.")

    # Process each Google Drive URL
    for item in google_drive_urls_to_process:
        url = item["url"]
        is_subscriber = item["is_subscriber"]
        folder_id = item["folder_id"]

        logger.info(f"\nProcessing Google Drive folder: {url} (Subscriber: {is_subscriber})")

        try:
            # Initiate recursive processing for the top-level folder
            drive_processor._process_folder_recursively(folder_id, is_subscriber)

        except Exception as e:
            logger.error(f"An unexpected error occurred for URL {url}: {e}")

    logger.info("\nGoogle Drive asset processing complete.")


if __name__ == "__main__":
    main()
