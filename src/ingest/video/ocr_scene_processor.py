import asyncio
import json
import logging
import os
from multiprocessing import Pool, cpu_count

import aioboto3
import numpy as np
from botocore.exceptions import ClientError
from PIL import Image
from tqdm import tqdm

# Configure logger
logger = logging.getLogger(__name__)


def has_meaningful_text(image_data):
    """Check if image has meaningful text. Takes tuple of (scene, full_path)"""
    scene, image_path = image_data
    try:
        # Load and aggressively resize to tiny size
        image = Image.open(image_path).convert("L")

        # Early exit for very small images
        if image.size[0] * image.size[1] < 5000:
            return (scene, image_path, False, "")

        # Resize to very small size for extremely quick processing
        image.thumbnail((300, 300))

        # Convert to numpy array
        img_array = np.array(image)

        # Quick check for image variance
        if np.var(img_array) < 100:
            return (scene, image_path, False, "")

        # Compute simple gradient magnitude
        dx = np.abs(np.diff(img_array, axis=1))
        dy = np.abs(np.diff(img_array, axis=0))

        # Add zero columns/rows to make shapes match
        dx = np.pad(dx, ((0, 0), (0, 1)), mode="constant")
        dy = np.pad(dy, ((0, 1), (0, 0)), mode="constant")

        # Combine gradients
        grad_mag = np.sqrt(dx**2 + dy**2)

        # Threshold gradient magnitude
        threshold = 30
        edges = (grad_mag > threshold).astype(np.uint8)

        # Calculate edge statistics
        edge_density = np.mean(edges)

        # Calculate horizontal run statistics (for text detection)
        runs = []
        for row in edges:
            run_start = -1
            for i, val in enumerate(row):
                if val == 1 and run_start == -1:
                    run_start = i
                elif val == 0 and run_start != -1:
                    runs.append(i - run_start)
                    run_start = -1
            if run_start != -1:
                runs.append(len(row) - run_start)

        # Calculate vertical run statistics
        v_runs = []
        for col in edges.T:
            run_start = -1
            for i, val in enumerate(col):
                if val == 1 and run_start == -1:
                    run_start = i
                elif val == 0 and run_start != -1:
                    v_runs.append(i - run_start)
                    run_start = -1
            if run_start != -1:
                v_runs.append(len(col) - run_start)

        # If no runs found, there's likely no text
        if len(runs) < 10 or len(v_runs) < 10:
            return (scene, image_path, False, "")

        # Calculate run statistics
        avg_run = np.mean(runs) if runs else 0
        avg_v_run = np.mean(v_runs) if v_runs else 0

        # Text typically has:
        # 1. Moderate edge density (not too low, not too high)
        # 2. Certain range of run lengths (not too short, not too long)
        has_text = 0.05 < edge_density < 0.3 and 2 < avg_run < 15 and 2 < avg_v_run < 15

        return (scene, image_path, has_text, "")

    except Exception as e:
        return (scene, image_path, False, str(e))


def filter_scenes_with_text(scenes, parent_directory):
    """Process all images in parallel to detect text"""
    # Initialize ocr_text as empty string for all scenes first
    for scene in scenes:
        scene["ocr_text"] = ""

    # Prepare data for parallel processing
    image_data = []
    for scene in scenes:
        scene_file = scene.get("scene_file")
        if scene_file:
            full_path = os.path.join(parent_directory, scene_file)
            if os.path.isfile(full_path):
                image_data.append((scene, full_path))
            else:
                logger.error(f"File not found: {full_path}")
        else:
            logger.error("Scene file path missing in JSON data.")

    # Process images in parallel
    with Pool(processes=cpu_count()) as pool:
        results = list(
            tqdm(
                pool.imap(has_meaningful_text, image_data),
                total=len(image_data),
                desc="Checking for text",
            )
        )

    # Separate scenes with and without text
    scenes_with_text = []
    for scene, path, has_text, pytesseract_text in results:
        if has_text:
            scenes_with_text.append((scene, path))
            # Store the pytesseract text temporarily
            scene["pytesseract_text"] = pytesseract_text

    return scenes_with_text


async def perform_ocr_async(file_path, textract):
    try:
        with open(file_path, "rb") as image:
            image_bytes = image.read()

        response = await textract.detect_document_text(Document={"Bytes": image_bytes})

        return " ".join(
            [item["Text"] for item in response["Blocks"] if item["BlockType"] == "LINE"]
        )
    except ClientError as e:
        logger.error(f"An error occurred with Textract for {file_path}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred for {file_path}: {e}")
    return None


async def process_batch_async(scenes_batch, textract):
    tasks = []
    for scene, path in scenes_batch:
        task = perform_ocr_async(path, textract)
        tasks.append((scene, task))

    for scene, task in tasks:
        try:
            ocr_text = await task
            scene["ocr_text"] = (
                ocr_text if ocr_text else scene.get("pytesseract_text", "")
            )
            # Clean up temporary pytesseract text
            scene.pop("pytesseract_text", None)
        except Exception as e:
            scene["ocr_text"] = scene.get("pytesseract_text", "")
            scene.pop("pytesseract_text", None)
            logger.error(f"Error processing scene: {e}")


async def process_scenes_with_ocr_async(
    input_file_path, output_file_name, parent_directory, batch_size=10
):
    try:
        if not os.path.isfile(input_file_path):
            logger.error(f"Input file not found: {input_file_path}")
            return False
        if not os.path.isdir(parent_directory):
            logger.error(f"Parent directory not found: {parent_directory}")
            return False

        output_file_path = os.path.join(os.getcwd(), output_file_name)

        with open(input_file_path, "r") as f:
            data = json.load(f)

        if not isinstance(data, list):
            logger.error("Input JSON should contain a list of scenes")
            return False

        logger.info(f"Processing {len(data)} scenes...")

        # Initialize ocr_text as empty string for all scenes
        for scene in data:
            scene["ocr_text"] = ""

        # Filter scenes that contain text
        scenes_with_text = filter_scenes_with_text(data, parent_directory)
        logger.info(f"Found {len(scenes_with_text)} scenes with meaningful text")

        # Process those scenes with Textract
        session = aioboto3.Session()
        async with session.client("textract") as textract:
            for i in tqdm(
                range(0, len(scenes_with_text), batch_size),
                desc="Processing with Textract",
            ):
                batch = scenes_with_text[i : i + batch_size]
                await process_batch_async(batch, textract)

        # Add transcription chunks if missing
        for scene in data:
            if "transcription_chunks" not in scene:
                scene["transcription_chunks"] = [scene.get("transcription", "")]

        # Save processed data
        with open(output_file_path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Processing complete. Results saved in '{output_file_path}'")
        return True

    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON file: {e}")
    except IOError as e:
        logger.error(f"Error reading input file: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

    return False


# Example usage
if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    input_file = "matched_scenes_chunked.json"
    output_file = "scenes_with_ocr.json"
    parent_dir = "/tmp/video_processing"
    batch_size = 10  # Number of concurrent requests

    # Run the async function
    success = asyncio.run(
        process_scenes_with_ocr_async(input_file, output_file, parent_dir, batch_size)
    )
    logger.info("Processing successful" if success else "Processing failed")
