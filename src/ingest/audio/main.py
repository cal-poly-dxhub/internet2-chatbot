import json
import os
import sys

import boto3
import requests
from process_podcast import process_transcript_and_add_to_opensearch


def get_s3_metadata(s3_uri):
    """Extract metadata from S3 object custom metadata."""
    s3_client = boto3.client('s3')
    
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
            if isinstance(v, str) else v
            for k, v in response.get("Metadata", {}).items()
        }
        return custom_metadata
    except Exception as e:
        print(f"Error fetching S3 metadata for {s3_uri}: {str(e)}")
        return {}


def main(transcript_uri, media_file_uri, job_name, metadata):
    # Fetch the JSON file from the URI
    response = requests.get(transcript_uri)

    # Load the content as JSON
    transcript_json = response.json()

    transcribe_json_file = "/tmp/transcript_output.json"
    with open(transcribe_json_file, "w") as f:
        json.dump(transcript_json, f, indent=4)

    process_transcript_and_add_to_opensearch(
        transcribe_json_file, media_file_uri, metadata
    )
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
