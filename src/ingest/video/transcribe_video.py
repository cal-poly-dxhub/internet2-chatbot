import json
import logging
import os
import time

import boto3
import requests
from botocore.exceptions import ClientError

# Configure logger
logger = logging.getLogger(__name__)


def create_directory(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def transcribe_and_save(s3_url, output_path, vocabulary_name=None):
    try:
        # Parse out bucket name and object key from S3 URL
        s3_parts = s3_url.replace("s3://", "").split("/", 1)
        bucket_name = s3_parts[0]
        object_key = s3_parts[1]

        # Determine file type from the object key
        file_type = object_key.split(".")[-1].lower()

        # Create a unique job name
        job_name = f"transcribe-job-{int(time.time())}"

        transcribe_client = boto3.client("transcribe")

        settings = {
            "ShowSpeakerLabels": True,
            "MaxSpeakerLabels": 10,
        }

        # Add VocabularyName to settings if provided
        if vocabulary_name:
            settings["VocabularyName"] = vocabulary_name

        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={"MediaFileUri": s3_url},
            MediaFormat=file_type,
            LanguageCode="en-US",
            Settings=settings,
        )

        logger.info(f"Started transcription job: {job_name}")

        start_time = time.time()
        while True:
            job = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            job_status = job["TranscriptionJob"]["TranscriptionJobStatus"]

            if job_status in ["COMPLETED", "FAILED"]:
                logger.info(f"Transcription job {job_name} {job_status}")
                break

            elapsed_time = int(time.time() - start_time)
            logger.info(
                f"Job {job_name} in progress. Status: {job_status}. Time elapsed: {elapsed_time // 60}m {elapsed_time % 60}s"
            )
            time.sleep(60)  # Check every 1 minute

        # If job completed successfully, download and save the transcript
        if job_status == "COMPLETED":
            transcript_uri = job["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
            response = requests.get(transcript_uri)

            create_directory(output_path)

            with open(output_path, "w") as f:
                json.dump(response.json(), f, indent=4)

            logger.info(f"Transcription saved to: {output_path}")
        else:
            logger.error("Transcription job failed.")

    except ClientError as e:
        logger.error(f"AWS ClientError occurred: {e}")
    except requests.RequestException as e:
        logger.error(f"Error downloading transcript: {e}")
    except IOError as e:
        logger.error(f"Error writing to file: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


# Sample function call
if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    s3_url = "s3://video.mp4"
    output_path = "transcriptions/video/transcription.json"

    try:
        transcribe_and_save(s3_url, output_path)
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
