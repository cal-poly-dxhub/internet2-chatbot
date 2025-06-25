import logging
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pytesseract
from PIL import Image

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def original_has_meaningful_text(image_data):
    """Original function"""
    scene, image_path = image_data
    try:
        text = pytesseract.image_to_string(Image.open(image_path).convert("L"))
        cleaned_text = " ".join(re.sub(r"[^a-zA-Z0-9\s]", "", text).split())
        words = [word for word in cleaned_text.split() if len(word) > 2]
        has_text = len(cleaned_text) >= 50 and len(words) >= 20
        return (scene, image_path, has_text, cleaned_text if has_text else "")
    except Exception as e:
        logger.error(f"Error checking for text in {image_path}: {e}")
        return (scene, image_path, False, "")


def optimized_has_meaningful_text(image_data):
    """Optimized function"""
    scene, image_path = image_data
    try:
        # Load and aggressively resize
        image = Image.open(image_path).convert("L")

        # Early exit for very small images
        if image.size[0] * image.size[1] < 10000:
            return (scene, image_path, False, "")

        # Resize to very small size for quick processing
        image.thumbnail((1200, 800))

        # Quick check for image variance
        image_array = np.array(image)
        if np.var(image_array) < 100:
            return (scene, image_path, False, "")

        # Ultra fast tesseract config
        custom_config = '--oem 3 --psm 6 -c tessedit_do_invert=0 -c tessedit_char_whitelist="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz "'

        # Just get word count
        word_count = len(
            pytesseract.image_to_string(image, config=custom_config).split()
        )

        return (scene, image_path, word_count >= 20, "")

    except Exception as e:
        logger.error(f"Error checking for text in {image_path}: {e}")
        return (scene, image_path, False, "")


def fast_has_text(image_data):
    """Ultra-fast function to detect presence of any text in image"""
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


def test_functions(folder_path):
    # Get all image files
    image_files = []
    valid_extensions = {".jpg", ".jpeg", ".png", ".bmp", ".tiff"}

    for ext in valid_extensions:
        image_files.extend(Path(folder_path).glob(f"*{ext}"))

    # Prepare input data
    image_data = [(i, str(path)) for i, path in enumerate(image_files)]

    results = {}

    # Test original function
    print("\nTesting original function...")
    start_time = time.time()

    with ProcessPoolExecutor() as executor:
        original_results = list(executor.map(original_has_meaningful_text, image_data))

    original_time = time.time() - start_time
    original_text_count = sum(1 for _, _, has_text, _ in original_results if has_text)

    # Test optimized function
    print("\nTesting optimized function...")
    start_time = time.time()

    with ProcessPoolExecutor() as executor:
        optimized_results = list(executor.map(fast_has_text, image_data))

    optimized_time = time.time() - start_time
    optimized_text_count = sum(1 for _, _, has_text, _ in optimized_results if has_text)

    # Print results
    print("\nResults:")
    print("-" * 50)
    print(f"Number of images processed: {len(image_data)}")
    print("\nOriginal function:")
    print(f"Time taken: {original_time:.2f} seconds")
    print(f"Images with text: {original_text_count}")
    print(f"Average time per image: {original_time / len(image_data):.2f} seconds")

    print("\nOptimized function:")
    print(f"Time taken: {optimized_time:.2f} seconds")
    print(f"Images with text: {optimized_text_count}")
    print(f"Average time per image: {optimized_time / len(image_data):.2f} seconds")

    print("\nComparison:")
    print(f"Speed improvement: {(original_time / optimized_time):.2f}x faster")

    # Print disagreements
    print("\nDisagreements between functions:")
    disagreements = 0
    for orig, opt in zip(original_results, optimized_results):
        if orig[2] != opt[2]:  # Compare has_text results
            disagreements += 1
            print(f"Image: {orig[1]}")
            print(f"Original function: {'Has text' if orig[2] else 'No text'}")
            print(f"Optimized function: {'Has text' if opt[2] else 'No text'}")
            print("-" * 30)

    print(f"\nTotal disagreements: {disagreements}")
    print(
        f"Agreement rate: {((len(image_data) - disagreements) / len(image_data)) * 100:.2f}%"
    )


if __name__ == "__main__":
    import sys

    folder_path = "/Users/njriley/dxhub/ingestion/rag-data-ingestion/src/video/tmp/Answers About Google's AI Tools"

    if not os.path.isdir(folder_path):
        print(f"Error: {folder_path} is not a valid directory")
        sys.exit(1)

    test_functions(folder_path)
