import scenedetect as sd
from log_config import get_logger
from scenedetect import SceneManager
from scenedetect.detectors import ContentDetector
from scenedetect.scene_manager import save_images, write_scene_list

logger = get_logger(__name__)


def run_scenedetect(
    input_file, output_dir, csv_path, min_scene_len=5.0, frame_skip=100
):
    # try:
    # Create the scene manager and content detector
    scene_manager = SceneManager()
    scene_manager.auto_downscale = False
    scene_manager.downscale = 100

    scene_manager.add_detector(ContentDetector(threshold=min_scene_len))

    # Use OpenCV video reader (new recommended approach)
    video = sd.open_video(input_file)

    # Perform scene detection
    scene_manager.detect_scenes(video, frame_skip=frame_skip)

    # Get list of detected scenes
    scene_list = scene_manager.get_scene_list()

    # Save scene images
    save_images(scene_list, video, num_images=1, output_dir=output_dir)

    # if os.path.exists(csv_path):
    #     os.remove(csv_path)

    # Open the CSV file for writing
    with open(csv_path, "w", newline="") as csv_file:
        # Write the scene list to the CSV file
        write_scene_list(csv_file, scene_list, include_cut_list=True)

    if len(scene_list) == 0:
        logger.warning("No scenes detected.")
        return False

    # Optionally, print out the list of detected scenes
    for i, scene in enumerate(scene_list):
        print(
            f"Scene {i + 1}: Start {scene[0].get_timecode()}, End {scene[1].get_timecode()}"
        )

    logger.info("Scene detection completed successfully.")
    logger.info(f"Scene images saved to: {output_dir}")
    return True
    # except Exception as e:
    #     logger.error(f"An error occurred during scene detection: {e}")
    #     return False


# # Example usage
if __name__ == "__main__":
    input_file = "./video/mp4_files/video.mp4"
    output_dir = "./video/one_shot/temp/video.mp4"
    success = run_scenedetect(input_file, output_dir)
    logger.info(
        "Scene detection successful" if success else "Scene detection failed"
    )
