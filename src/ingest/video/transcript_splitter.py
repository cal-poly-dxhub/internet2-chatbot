import json
import logging
import os
from datetime import timedelta
from typing import Any, Dict, List

from tqdm import tqdm

# Configure logger
logger = logging.getLogger(__name__)


def parse_timecode(timecode: str) -> float:
    hours, minutes, seconds = map(float, timecode.split(":"))
    return timedelta(
        hours=hours, minutes=minutes, seconds=seconds
    ).total_seconds()


def load_scenes_from_csv(csv_file: str) -> List[Dict[str, Any]]:
    scenes = []
    try:
        with open(csv_file, "r") as f:
            lines = f.readlines()

        # Skip the first line (assuming it's the timecode list)
        data_lines = lines[1:]

        # Find the header line
        header_line = next(
            line for line in data_lines if "Scene Number" in line
        )
        headers = header_line.strip().split(",")

        # Get indices for required columns
        scene_number_index = headers.index("Scene Number")
        start_timecode_index = headers.index("Start Timecode")
        end_timecode_index = headers.index("End Timecode")

        for line in data_lines[data_lines.index(header_line) + 1 :]:
            values = line.strip().split(",")
            if len(values) > max(
                scene_number_index, start_timecode_index, end_timecode_index
            ):
                scenes.append(
                    {
                        "number": int(values[scene_number_index]),
                        "start": parse_timecode(values[start_timecode_index]),
                        "end": parse_timecode(values[end_timecode_index]),
                    }
                )

        logger.info(f"Loaded {len(scenes)} scenes from CSV")
        return scenes
    except FileNotFoundError:
        logger.error(f"Error: CSV file not found: {csv_file}")
        raise
    except ValueError as e:
        logger.error(f"Error parsing CSV file: {e}")
        raise


def load_transcription_from_json(json_file: str) -> List[Dict[str, Any]]:
    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        items = data["results"]["items"]
        logger.info(f"Loaded {len(items)} transcription items from JSON")
        return items
    except FileNotFoundError:
        logger.error(f"Error: JSON file not found: {json_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON file: {e}")
        raise


def match_scenes_with_transcription(
    scenes: List[Dict[str, Any]], transcription_items: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    matched_scenes = []
    current_scene_index = 0
    current_scene = scenes[current_scene_index]
    current_text = []
    current_speaker = None

    for item in tqdm(
        transcription_items, desc="Matching scenes with transcription"
    ):
        if item["type"] == "pronunciation":
            start_time = float(item["start_time"])
            content = item["alternatives"][0]["content"]
            speaker = item.get("speaker_label", "Unknown")

            # Move to the correct scene based on the timestamp
            while (
                start_time >= current_scene["end"]
                and current_scene_index < len(scenes) - 1
            ):
                # Save the current scene before moving to the next one
                if current_text:
                    matched_scenes.append(
                        {
                            "number": current_scene["number"],
                            "start": current_scene["start"],
                            "end": current_scene["end"],
                            "text": " ".join(current_text),
                        }
                    )
                    current_text = []
                    current_speaker = (
                        None  # Reset the speaker when changing scenes
                    )

                current_scene_index += 1
                current_scene = scenes[current_scene_index]

            # If the item falls within the current scene
            if (
                start_time >= current_scene["start"]
                and start_time < current_scene["end"]
            ):
                # Handle speaker change
                if speaker != current_speaker:
                    prefix = (
                        f"\n[{speaker}]: " if current_text else f"[{speaker}]: "
                    )
                    current_text.append(prefix)
                    current_speaker = speaker

                current_text.append(content)

        elif item["type"] == "punctuation" and current_text:
            current_text[-1] += item["alternatives"][0]["content"]

    # Don't forget the last scene
    if current_text:
        matched_scenes.append(
            {
                "number": current_scene["number"],
                "start": current_scene["start"],
                "end": current_scene["end"],
                "text": " ".join(current_text),
            }
        )

    logger.info(f"Matched {len(matched_scenes)} scenes with transcription")
    return matched_scenes


def consolidate_short_scenes(
    matched_scenes: List[Dict[str, Any]], min_length: int = 400
) -> List[Dict[str, Any]]:
    """
    Consolidates short scenes to ensure each chunk has at least min_length characters
    where possible. Retains only the first scene number but uses the full text span.
    """
    if not matched_scenes:
        return []

    consolidated = []
    current_chunk = {
        "number": matched_scenes[0][
            "number"
        ],  # Keep only the first scene number
        "start": matched_scenes[0]["start"],
        "end": matched_scenes[0]["end"],
        "text": matched_scenes[0]["text"],
    }

    for scene in matched_scenes[1:]:
        # If the current chunk already exceeds the minimum length, finalize it and start a new one
        if len(current_chunk["text"]) >= min_length:
            consolidated.append(current_chunk)
            current_chunk = {
                "number": scene["number"],
                "start": scene["start"],
                "end": scene["end"],
                "text": scene["text"],
            }
        else:
            # Otherwise keep accumulating scenes, but keep the original scene number
            current_chunk["end"] = scene["end"]
            # Make sure to join the text properly to avoid breaking speaker annotations
            current_chunk["text"] += " " + scene["text"]

    # Add the final chunk if it's not empty
    if current_chunk:
        consolidated.append(current_chunk)

    logger.info(
        f"Consolidated {len(matched_scenes)} scenes into {len(consolidated)} chunks"
    )
    return consolidated


def process_video_scenes_and_transcription(
    scenes_folder: str,
    csv_file: str,
    transcription_file: str,
    output_file: str,
    min_chunk_length: int = 400,
) -> bool:
    try:
        logger.info(f"Loading scenes from {csv_file}")
        scenes = load_scenes_from_csv(csv_file)

        logger.info(f"Loading transcription from {transcription_file}")
        transcription_items = load_transcription_from_json(transcription_file)

        logger.info("Matching scenes with transcription")
        matched_scenes = match_scenes_with_transcription(
            scenes, transcription_items
        )

        logger.info(
            f"Consolidating scenes to ensure minimum chunk length of {min_chunk_length} characters"
        )
        consolidated_chunks = consolidate_short_scenes(
            matched_scenes, min_chunk_length
        )

        logger.info(f"Writing output to {output_file}")
        output_data = []

        for chunk in tqdm(
            consolidated_chunks, desc="Processing consolidated chunks"
        ):
            scene_file = next(
                (
                    file
                    for file in os.listdir(scenes_folder)
                    if f"-Scene-{chunk['number']:03d}-" in file
                ),
                None,
            )

            if scene_file:
                output_data.append(
                    {
                        "scene_number": chunk["number"],
                        "scene_file": scene_file,
                        "start_time": chunk["start"],
                        "end_time": chunk["end"],
                        "transcription": chunk["text"],
                    }
                )

        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)

        logger.info(f"Total chunks written: {len(output_data)}")
        return True
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False


# Example usage:
if __name__ == "__main__":
    scenes_folder = "./video/one_shot/scenes/video"
    csv_file = "./video/one_shot/scenes/video-scences.csv"
    transcription_file = (
        "./video/one_shot/transcriptions/video/transcription.json"
    )
    output_file = "matched_scenes.json"

    success = process_video_scenes_and_transcription(
        scenes_folder,
        csv_file,
        transcription_file,
        output_file,
        min_chunk_length=400,
    )
    if success:
        logger.info("Processing completed successfully.")
    else:
        logger.error("Processing failed.")
