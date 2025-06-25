import json
import os
import re
import sys
from typing import Any, Dict

from langchain_aws import BedrockEmbeddings
from llm_interface import generate_response
from opensearch_utils import bulk_add_to_opensearch, create_document

OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
INDEX_NAME = os.getenv("INDEX_NAME")
REGION = os.getenv("AWS_REGION")
EMBEDDINGS_MODEL_ID = os.getenv("EMBEDDINGS_MODEL_ID")


def _get_emb_(passage):
    """
    This function takes a passage of text and a model name as input, and returns the corresponding text embedding.
    The function first checks the provided model name and then invokes the appropriate model or API to generate the text embedding.
    After invoking the appropriate model or API, the function extracts the text embedding from the response and returns it.
    """
    embeddings_client = BedrockEmbeddings(model_id=EMBEDDINGS_MODEL_ID)

    # Invoke the model
    embedding = embeddings_client.embed_query(passage)
    return embedding


def add_speaker_labels(transcribe_json: str) -> tuple:
    print("Adding speaker labels to transcript...")

    with open(transcribe_json, "r") as file:
        data = json.load(file)

    items = data["results"]["items"]
    speaker_labels = data["results"]["speaker_labels"]["segments"]

    time_to_speaker = {}
    for segment in speaker_labels:
        speaker = segment["speaker_label"]
        start_time = float(segment["start_time"])
        end_time = float(segment["end_time"])
        time_to_speaker[(start_time, end_time)] = speaker

    def find_speaker(start_time):
        for (start, end), speaker in time_to_speaker.items():
            if start <= start_time < end:
                return speaker, start, end
        return "Unknown", None, None

    modified_transcript = ""
    current_speaker = None
    segment_start_time = None
    segment_end_time = None
    segments = []

    for item in items:
        if item["type"] == "pronunciation":
            start_time = float(item["start_time"])
            speaker, start, end = find_speaker(start_time)

            if speaker != current_speaker:
                if current_speaker is not None:
                    segments.append(
                        (
                            modified_transcript.strip(),
                            segment_start_time,
                            segment_end_time,
                        )
                    )
                    modified_transcript = ""
                modified_transcript += f"[{speaker}] "
                current_speaker = speaker
                segment_start_time = start
                segment_end_time = end

            modified_transcript += item["alternatives"][0]["content"] + " "
            segment_end_time = float(item["end_time"])
        elif item["type"] == "punctuation":
            modified_transcript = (
                modified_transcript.rstrip() + item["alternatives"][0]["content"] + " "
            )

    if modified_transcript:
        segments.append(
            (modified_transcript.strip(), segment_start_time, segment_end_time)
        )

    print("Speaker labels added successfully.")
    return segments


def extract_analysis_content(text):
    match = re.search(r"<analysis>(.*?)</analysis>", text, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return None


def identify_speakers(labeled_transcript: str) -> Dict[str, Any]:
    print("Identifying speakers...")

    with open("speaker_identifier_prompt.txt", "r") as file:
        prompt_template = file.read()

    # Escape any curly braces in the prompt template (for Python's format method)
    # This is necessary because the prompt template includes a json schema
    prompt_template = prompt_template.replace("{", "{{").replace("}", "}}")
    # Now add back the placeholder for the transcript
    prompt_template = prompt_template.replace("{{transcript}}", "{transcript}")

    prompt = prompt_template.format(transcript=labeled_transcript)

    messages = [{"role": "user", "content": prompt}]
    model_id = os.getenv("AUDIO_TEXT_MODEL_ID")
    temperature = 0
    response = generate_response(messages, model_id, temperature)
    print(f"LLM response:\n {response}")
    print("Speakers identified. Parsing response...")

    try:
        speaker_data = extract_analysis_content(response)
        print("Speaker data parsed successfully.")
        print(f"Speaker data: {speaker_data}")
        return speaker_data
    except json.JSONDecodeError:
        print("Error: Unable to parse LLM response as JSON.")
        return None


def create_modified_transcript(
    labeled_transcript: str, speaker_data: Dict[str, Any]
) -> str:
    print("Creating modified transcript with speaker details...")

    print(labeled_transcript)

    modified_transcript = []
    speaker_map = {
        speaker["speakerId"]: speaker for speaker in speaker_data["speakers"]
    }

    segments = re.split(r"\[spk_\d+\]", labeled_transcript)
    speakers = re.findall(r"\[spk_\d+\]", labeled_transcript)

    for speaker, segment in zip(speakers, segments[1:]):  # Skip the first empty segment
        speaker_id = speaker.strip("[]")
        speaker_info = speaker_map.get(
            speaker_id,
            {"fullName": "Unknown", "bio": "No information available"},
        )

        modified_segment = {
            "speaker": {
                "fullName": speaker_info["fullName"],
                "speakerId": speaker_id,
                "bio": speaker_info["bio"],
            },
            "text": segment.strip(),
        }

        modified_transcript.append(modified_segment)

    print("Modified transcript created successfully.")
    return json.dumps(modified_transcript, indent=2)


def process_transcript_and_add_to_opensearch(
    transcribe_json_file: str, s3_uri: str, source_url: str
):
    print("Starting transcript processing...")

    try:
        labeled_segments = add_speaker_labels(transcribe_json_file)
        speaker_data = identify_speakers(
            "\n".join([segment[0] for segment in labeled_segments])
        )

        if speaker_data:
            print("Speaker data:")
            print(json.dumps(speaker_data, indent=2))

            modified_transcript = create_modified_transcript(
                "\n".join([segment[0] for segment in labeled_segments]),
                speaker_data,
            )
            modified_transcript_data = json.loads(modified_transcript)

            podcast_id = os.path.splitext(os.path.basename(transcribe_json_file))[0]
            podcast_id = podcast_id.replace("-", "")

            # Prepare documents for bulk upload
            documents = []

            for sequence_number, (segment, start_time, end_time) in enumerate(
                zip(
                    modified_transcript_data,
                    [s[1] for s in labeled_segments],
                    [s[2] for s in labeled_segments],
                ),
                start=1,
            ):
                doc_id = f"{podcast_id}_{sequence_number}_{os.urandom(4).hex()}"
                passage = f"Podcast_id: {podcast_id}\nSpeaker: {segment['speaker']['fullName']}\nBio: {segment['speaker']['bio']}\nTranscript: {segment['text']}"

                # Generate embedding
                embedding = _get_emb_(passage)

                # Create metadata dictionary with all additional fields
                metadata = {
                    "doc_id": doc_id,
                    "url": source_url,
                    "is_training_resource": False,
                    "special_instructions": "The above is a snippet from the following podcast episode: "
                    + podcast_id,
                    "is_podcast": True,
                    "podcast_id": podcast_id,
                    "sequence_number": sequence_number,
                    "start_time": start_time,
                    "end_time": end_time,
                    "speaker": {
                        "fullName": segment["speaker"]["fullName"],
                        "bio": segment["speaker"]["bio"],
                    },
                }

                document = create_document(
                    passage=passage,
                    embedding=embedding,
                    type="podcast",
                    metadata=metadata,
                )

                documents.append(document)
                print(
                    f"Prepared document {sequence_number} for speaker {segment['speaker']['fullName']}"
                )

            # Bulk upload all documents
            success = bulk_add_to_opensearch(documents)

            if success:
                print("All segments added to OpenSearch successfully.")
            else:
                print("Failed to add segments to OpenSearch.")

        else:
            print("Failed to process transcript due to speaker identification error.")

    except Exception as e:
        print(f"An error occurred during transcript processing: {str(e)}")
        raise


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            "Usage: python script_name.py <path_to_transcribe_json_file> <s3_uri> <source_url>"
        )
        sys.exit(1)

    transcribe_json_file = sys.argv[1]
    s3_uri = sys.argv[2]
    source_url = sys.argv[3]

    process_transcript_and_add_to_opensearch(transcribe_json_file, s3_uri, source_url)
    print("Transcript processing and OpenSearch indexing completed.")
