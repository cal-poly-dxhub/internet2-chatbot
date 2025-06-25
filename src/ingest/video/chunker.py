import json
import os
from typing import Any, Dict, List

from langchain.text_splitter import RecursiveCharacterTextSplitter
from log_config import get_logger
from tqdm import tqdm

logger = get_logger(__name__)


def ensure_directory_exists(file_path: str) -> None:
    """Ensure that the directory for the given file path exists."""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")


def process_chunks(chunks: List[str]) -> List[str]:
    """Process chunks to ensure periods are at the end of chunks where appropriate."""
    processed_chunks = []
    for i, chunk in enumerate(chunks):
        chunk = chunk.strip()
        if i < len(chunks) - 1 and chunks[i + 1].startswith("."):
            chunk += "."
            chunks[i + 1] = chunks[i + 1][1:].strip()
        processed_chunks.append(chunk)
    logger.info(f"Processed {len(chunks)} chunks")
    return processed_chunks


def chunk_transcriptions(
    input_json_path: str,
    output_json_path: str,
    chunk_size: int = 800,
    chunk_overlap: int = 80,
) -> bool:
    """Process transcriptions in a JSON file, splitting them into chunks."""
    try:
        # Ensure input file exists
        if not os.path.exists(input_json_path):
            logger.error(f"Input file not found: {input_json_path}")
            return False

        # Ensure output directory exists
        ensure_directory_exists(output_json_path)

        logger.info(f"Loading data from {input_json_path}")
        with open(input_json_path, "r") as file:
            data = json.load(file)

        if not isinstance(data, list):
            logger.error("Data is not a list. Unable to process.")
            return False

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ".", "?", " ", ""],
        )

        logger.info("Chunking transcriptions...")
        for i, scene in tqdm(
            enumerate(data), total=len(data), desc="Processing scenes"
        ):
            if not isinstance(scene, dict):
                tqdm.write(f"Warning: Scene {i} is not a dictionary. Skipping.")
                continue

            if "transcription" in scene:
                chunks = text_splitter.split_text(scene["transcription"])

                # Process chunks to move periods as needed
                processed_chunks = process_chunks(chunks)

                # Replace the original transcription with the processed chunks
                scene["transcription_chunks"] = processed_chunks
                tqdm.write(
                    f"Processed scene {i + 1}: Split into {len(processed_chunks)} chunks"
                )
            else:
                tqdm.write(f"Warning: Scene {i + 1} has no transcription. Skipping.")

        # Save the updated data back to a new JSON file
        logger.info(f"Saving processed data to {output_json_path}")
        with open(output_json_path, "w") as file:
            json.dump(data, file, indent=2)

        logger.info(f"Successfully saved chunked data to {output_json_path}")
        return True

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON file: {e}")
    except PermissionError as e:
        logger.error(f"Error: Permission denied - {e}")
    except OSError as e:
        logger.error(f"Error: OS error - {e}")
    except Exception as e:
        logger.error(f"An error occurred during chunking: {e}")

    return False


if __name__ == "__main__":
    input_path = "./videos/matched_scenes.json"
    output_path = "./videos/matched_scenes.json"

    success = chunk_transcriptions(input_path, output_path)
    if success:
        logger.info("Processing completed successfully.")
    else:
        logger.error("Processing failed.")
