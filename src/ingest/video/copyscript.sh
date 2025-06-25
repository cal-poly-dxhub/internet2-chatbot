#!/bin/bash

# Load AWS credentials from environment variables
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION

# Get transcription URI and copy to clipboard
aws transcribe get-transcription-job --transcription-job-name process-video-16f43559-0313-4e33-a5e4-9b8610ecf8ea | jq -r '.TranscriptionJob.Transcript.TranscriptFileUri' | pbcopy

echo "TranscriptFileUri copied to clipboard!"
