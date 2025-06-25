import asyncio
import json
import logging
import os
import sys
import time

import boto3
import requests
from add_video import process_video_data_and_add_to_opensearch
from chunker import chunk_transcriptions
from detect_scenes import run_scenedetect
from log_config import get_logger, set_log_level, setup_logger
from ocr_scene_processor import process_scenes_with_ocr_async
from transcript_splitter import process_video_scenes_and_transcription


def parse_s3_uri(s3_uri):
    """Parse an S3 URI into bucket name and object key."""
    # Remove 's3://' prefix
    if s3_uri.startswith("s3://"):
        path = s3_uri[5:]
    else:
        path = s3_uri

    # Split into bucket and key
    parts = path.split("/")
    bucket = parts[0]
    key = "/".join(parts[1:])

    return bucket, key

def get_s3_metadata(s3_uri):
    """Extract metadata from S3 object custom metadata."""
    s3_client = boto3.client('s3')
    
    bucket, key = parse_s3_uri(s3_uri)
    
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        # Extract custom metadata (x-amz-meta-* headers)
        custom_metadata = {
            k.replace("x-amz-meta-", ""): v.replace("=", "-").replace("?", "")
            if isinstance(v, str) else v
            for k, v in response.get("Metadata", {}).items()
        }
        return custom_metadata
    except Exception as e:
        print(f"Error fetching S3 metadata for {s3_uri}: {str(e)}")
        return {}

def main(media_file_uri, transcribe_uri, metadata):
    pipeline_start_time = time.time()

    setup_logger()
    set_log_level(logging.INFO)
    logger = get_logger(__name__)

    video_filename = os.path.basename(media_file_uri)
    video_name_without_extension = video_filename.split(".")[0]

    logger.info(f"Processing video: {media_file_uri}")

    # Fetch the JSON file from the URI
    response = requests.get(transcribe_uri)

    # Load the content as JSON
    transcript_json = response.json()

    output_path = "./images/transcript.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(transcript_json, f, indent=4)

    # Step 1: Download video from S3
    logger.info("Step 2: Downloading video from S3...")
    s3 = boto3.client("s3")
    bucket_name, object_key = parse_s3_uri(media_file_uri)
    local_video_path = os.path.join("./tmp/", video_filename)
    os.makedirs("./tmp/mp4_files", exist_ok=True)
    start_time = time.time()
    s3.download_file(bucket_name, object_key, local_video_path)
    end_time = time.time()
    logger.info(
        f"Video downloaded to: {local_video_path}, took %.2f seconds."
        % (end_time - start_time)
    )

    # Step 2: Detect scenes
    logger.info("Step 3: Detecting scenes...")
    scenes_dir = os.path.join("./tmp", video_name_without_extension)
    os.makedirs(scenes_dir, exist_ok=True)
    start_time = time.time()
    csv_file = os.path.join(
        scenes_dir, f"csv-{video_name_without_extension}-Scenes.csv"
    )
    success = run_scenedetect(local_video_path, scenes_dir, csv_file)
    end_time = time.time()
    if not success:
        logger.error("Scene detection failed.")
        return
    logger.info(
        "Scene detection completed successfully, took %.2f seconds."
        % (end_time - start_time)
    )

    # Step 3: Split transcript based on scene timestamps
    logger.info("Step 4: Splitting transcript based on scene timestamps...")

    matched_scenes_file = "./tmp/matched_scenes.json"
    start_time = time.time()
    time.sleep(1)
    success = process_video_scenes_and_transcription(
        scenes_dir, csv_file, output_path, matched_scenes_file
    )
    end_time = time.time()
    if not success:
        logger.error("Transcript splitting failed.")
        return
    logger.info(
        "Transcript splitting completed successfully, took %.2f seconds."
        % (end_time - start_time)
    )

    # Step 4: Chunk transcriptions
    logger.info("Step 5: Chunking transcriptions...")
    chunked_scenes_file = "./tmp/matched_scenes_chunked.json"
    start_time = time.time()
    success = chunk_transcriptions(matched_scenes_file, chunked_scenes_file)
    end_time = time.time()
    if not success:
        logger.error("Chunking transcriptions failed.")
        return
    logger.info(
        "Chunking transcriptions completed successfully, took %.2f seconds."
        % (end_time - start_time)
    )

    # Step 5: Process scenes with OCR
    logger.info("Step 6: Processing scenes with OCR...")
    scenes_with_ocr_file = "./tmp/scenes_with_ocr.json"
    start_time = time.time()
    success = asyncio.run(
        process_scenes_with_ocr_async(
            chunked_scenes_file,
            scenes_with_ocr_file,
            scenes_dir,
            batch_size=10,  # TODO
        )
    )
    end_time = time.time()
    if not success:
        logger.error("OCR processing failed.")
        return
    logger.info(
        "OCR processing completed successfully, took %.2f seconds."
        % (end_time - start_time)
    )

    # Step 7: Add video data to OpenSearch
    logger.info("Step 8: Adding video data to OpenSearch...")
    special_instructions = (
        "The above document is a transcription chunk from a video scene."
    )
    success = process_video_data_and_add_to_opensearch(
        scenes_with_ocr_file,
        True,
        special_instructions,
        media_file_uri,
        metadata,
    )
    if not success:
        logger.error("Adding data to OpenSearch failed.")
        return
    logger.info(
        "Video data successfully added to OpenSearch, pipeline completed."
    )
    pipeline_end_time = time.time()
    logger.info("Video processing pipeline completed successfully!")
    logger.info(
        f"Total time taken: {pipeline_end_time - pipeline_start_time:.2f} seconds"
    )


# Example usage
if __name__ == "__main__":
    step_function_input = os.environ.get("STEP_FUNCTION_INPUT")

    # Test event
    # step_function_input = json.dumps(
    #     {
    #         "TranscriptionJob": {
    #             "Transcript": {
    #                 "TranscriptFileUri": "https://example.com/transcript.json"
    #             },
    #             "Media": {
    #                 "MediaFileUri": "s3://example-bucket/example-file.mp4"
    #             },
    #             "TranscriptionJobName": "Example-Transcription-Job",
    #         }
    #     }
    # )

    try:
        input_data = json.loads(step_function_input)

        transcript_uri = input_data["TranscriptionJob"]["Transcript"][
            "TranscriptFileUri"
        ]

        job_name = input_data["TranscriptionJob"]["TranscriptionJobName"]
        media_file_uri = input_data["TranscriptionJob"]["Media"]["MediaFileUri"]
        
        # Get metadata from S3 custom metadata instead of transcribe tags
        metadata = get_s3_metadata(media_file_uri)

    except Exception as e:
        print(f"Error processing step function input: {e}")

    result = main(media_file_uri, transcript_uri, metadata)
    print(result)

    sys.exit(0)
