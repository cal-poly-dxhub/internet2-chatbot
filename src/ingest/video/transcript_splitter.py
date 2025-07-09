import json
import logging
import os
from datetime import timedelta
from typing import Any, Dict, List

from tqdm import tqdm

# Configure logger
logger = logging.getLogger(__name__)


def parse_timecode(timecode: str) -> float:
    """Convert a timecode string to seconds."""
    try:
        h, m, s = timecode.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception as e:
        logger.error(f"Error parsing timecode {timecode}: {e}")
        return 0.0


def get_scene_timestamps(csv_path: str) -> List[Dict[str, Any]]:
    """Extract scene timestamps from CSV file."""
    scenes = []
    try:
        with open(csv_path, "r") as file:
            lines = file.readlines()
            
            # Skip header line
            for line in lines[1:]:
                parts = line.strip().split(",")
                if len(parts) >= 4:
                    scene_number = int(parts[0])
                    start_time = parse_timecode(parts[1])
                    end_time = parse_timecode(parts[2])
                    
                    scenes.append({
                        "scene_number": scene_number,
                        "start_time": start_time,
                        "end_time": end_time
                    })
        
        return scenes
    except Exception as e:
        logger.error(f"Error reading scene CSV file: {e}")
        return []


def get_scene_files(scenes_dir: str) -> Dict[int, str]:
    """Get mapping of scene numbers to image files."""
    scene_files = {}
    try:
        for filename in os.listdir(scenes_dir):
            if filename.endswith(".jpg") and "Scene" in filename:
                # Extract scene number from filename (format: *-Scene-001-01.jpg)
                parts = filename.split("-Scene-")
                if len(parts) == 2:
                    scene_part = parts[1].split("-")[0]
                    try:
                        scene_number = int(scene_part)
                        scene_files[scene_number] = filename
                    except ValueError:
                        continue
        return scene_files
    except Exception as e:
        logger.error(f"Error getting scene files: {e}")
        return {}


def get_transcript_items(transcript_path: str) -> List[Dict[str, Any]]:
    """Extract transcript items from JSON file."""
    try:
        with open(transcript_path, "r") as file:
            data = json.load(file)
            
        # Handle AWS Transcribe format
        if "results" in data and "items" in data["results"]:
            return data["results"]["items"]
        
        logger.error("Unexpected transcript format")
        return []
    except Exception as e:
        logger.error(f"Error reading transcript file: {e}")
        return []


def match_transcript_to_scenes(
    transcript_items: List[Dict[str, Any]], 
    scenes: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Match transcript items to scenes based on timestamps."""
    result = []
    
    # Initialize scenes with empty transcriptions
    for scene in scenes:
        scene_copy = scene.copy()
        scene_copy["transcription"] = ""
        result.append(scene_copy)
    
    # Process transcript items
    current_word = ""
    current_scene_idx = 0
    
    for item in transcript_items:
        # Skip non-pronunciation items without start_time
        if "start_time" not in item:
            if item["type"] == "punctuation":
                current_word += item["alternatives"][0]["content"]
            continue
            
        start_time = float(item["start_time"])
        
        # Find the appropriate scene for this timestamp
        while (current_scene_idx < len(result) - 1 and 
               start_time >= result[current_scene_idx + 1]["start_time"]):
            # If we have a pending word, add it to the current scene before moving on
            if current_word:
                result[current_scene_idx]["transcription"] += current_word + " "
                current_word = ""
            current_scene_idx += 1
        
        # Add the word to the current word buffer
        word = item["alternatives"][0]["content"]
        current_word += word
        
        # If it's the end of a sentence or has punctuation, add to transcription
        if "." in word or "?" in word or "!" in word or "," in word:
            result[current_scene_idx]["transcription"] += current_word + " "
            current_word = ""
    
    # Add any remaining word
    if current_word:
        result[current_scene_idx]["transcription"] += current_word
    
    # Clean up transcriptions
    for scene in result:
        scene["transcription"] = scene["transcription"].strip()
    
    return result


def process_video_scenes_and_transcription(
    scenes_dir: str,
    csv_path: str,
    transcript_path: str,
    output_path: str
) -> bool:
    """Process video scenes and match with transcription."""
    try:
        # Step 1: Get scene timestamps from CSV
        logger.info("Getting scene timestamps from CSV...")
        scenes = get_scene_timestamps(csv_path)
        if not scenes:
            logger.error("No scenes found in CSV file")
            return False
        
        # Step 2: Get scene image files
        logger.info("Getting scene image files...")
        scene_files = get_scene_files(scenes_dir)
        
        # Step 3: Add scene files to scenes data
        for scene in scenes:
            scene_number = scene["scene_number"]
            if scene_number in scene_files:
                scene["scene_file"] = scene_files[scene_number]
            else:
                scene["scene_file"] = None
                logger.warning(f"No image file found for scene {scene_number}")
        
        # Step 4: Get transcript items
        logger.info("Getting transcript items...")
        transcript_items = get_transcript_items(transcript_path)
        if not transcript_items:
            logger.error("No transcript items found")
            return False
        
        # Step 5: Match transcript to scenes
        logger.info("Matching transcript to scenes...")
        matched_scenes = match_transcript_to_scenes(transcript_items, scenes)
        
        # Step 6: Save results
        logger.info(f"Saving results to {output_path}...")
        with open(output_path, "w") as file:
            json.dump(matched_scenes, file, indent=2)
        
        logger.info("Processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error processing video scenes and transcription: {e}")
        return False


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Example usage
    scenes_dir = "./tmp/video"
    csv_path = "./tmp/video/csv-video-Scenes.csv"
    transcript_path = "./tmp/transcript.json"
    output_path = "./tmp/matched_scenes.json"
    
    success = process_video_scenes_and_transcription(
        scenes_dir, csv_path, transcript_path, output_path
    )
    
    if success:
        logger.info("Processing completed successfully")
    else:
        logger.error("Processing failed")
