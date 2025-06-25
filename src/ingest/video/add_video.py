import json
import os
import re

from langchain_aws import BedrockEmbeddings
from log_config import get_logger
from opensearch_utils import bulk_add_to_opensearch, create_document

logger = get_logger(__name__)

REGION = os.getenv("AWS_REGION")
EMBEDDINGS_MODEL_ID = os.getenv("EMBEDDINGS_MODEL_ID")


def get_emb(embeddings_client, passage):
    embedding = embeddings_client.embed_query(passage)
    return embedding


def process_video_data_and_add_to_opensearch(
    json_file: str,
    is_video: bool,
    special_instructions: str,
    s3_uri: str,
    metadata: dict[str, str],
) -> bool:
    try:
        # Load the JSON data from file
        with open(json_file, "r") as file:
            scenes_data = json.load(file)

        if not scenes_data:
            raise ValueError("Scenes data is empty or None.")

        logger.info(
            f"Successfully read JSON file. Found {len(scenes_data)} scenes."
        )

        embeddings_client = BedrockEmbeddings(
            model_id=EMBEDDINGS_MODEL_ID, region_name=REGION
        )

        documents = []

        # Iterate over scenes in the JSON data
        for scene in scenes_data:
            scene_file = scene.get("scene_file")

            # Ensure scene_file exists
            if scene_file is None:
                logger.warning("Scene file is missing, skipping this scene.")
                continue

            video_id = re.sub(r"-Scene-\d+-\d+\.jpg$", "", scene_file)
            scene_number = scene.get("scene_number", 0)
            start_time = scene.get("start_time", 0)
            end_time = scene.get("end_time", 0)

            # Common metadata for all documents from this scene
            combined_metadata = {
                "start_time": start_time,
                "end_time": end_time,
                "is_video": is_video,
                "video_id": video_id,
                "slide_number": scene_number,
                "special_instructions": special_instructions,
            }
            combined_metadata.update(metadata)

            # If the scene has text process it
            if scene.get("ocr_text"):
                ocr_passage = f"OCR OUTPUT:\n{scene.get('ocr_text')}"
                ocr_embedding = get_emb(embeddings_client, ocr_passage)

                ocr_metadata = combined_metadata.copy()
                ocr_metadata["sequence_number"] = 0

                documents.append(
                    create_document(
                        passage=ocr_passage,
                        embedding=ocr_embedding,
                        type="video",
                        metadata=ocr_metadata,
                    )
                )

            # Process transcription chunks
            transcription_chunks = scene.get("transcription_chunks")
            if isinstance(transcription_chunks, list):
                for chunk_index, chunk in enumerate(
                    transcription_chunks, start=1
                ):
                    embedding = get_emb(embeddings_client, chunk)

                    chunk_metadata = combined_metadata.copy()
                    chunk_metadata["sequence_number"] = chunk_index

                    documents.append(
                        create_document(
                            passage=chunk,
                            embedding=embedding,
                            type="video",
                            metadata=chunk_metadata,
                        )
                    )
            else:
                logger.warning(
                    f"Transcription chunks missing or not a list for scene {scene_number}."
                )

        logger.info(f"Number of documents created: {len(documents)}")
        return bulk_add_to_opensearch(documents)

    except Exception as e:
        logger.error(
            f"An error occurred during video data processing: {str(e)}"
        )
        return False


if __name__ == "__main__":
    json_file_path = "./videos/scenes_with_ocr.json"
    is_video = True
    special_instructions = (
        "The above document is a transcription chunk from a video scene."
    )
    s3_uri = "s3://general-data-processing-test/video.mp4"
    source_url = "https://example.com/video"

    logger.info(
        f"Adding video data from {json_file_path}, with s3_uri: {s3_uri}"
    )
    success = process_video_data_and_add_to_opensearch(
        json_file_path, is_video, special_instructions, s3_uri, source_url
    )
    logger.info("Success" if success else "Failure")
