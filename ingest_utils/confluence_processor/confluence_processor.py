import os
from typing import Optional

import pandas as pd
import requests
import yaml
from confluence_scraper import ConfluenceScraper
from s3_uploader import S3Uploader

DOWNLOAD_DIR = "confluence_downloads"
ASSET_LINKS_CSV = "confluence_asset_links.csv"


def download_file(url: str, output_path: str) -> Optional[str]:
    """
    Downloads a file from a given URL to the specified output path.
    Returns the path if successful, None otherwise.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {url} -> {output_path}")
        return output_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None


def main():
    # Load config from config.yaml
    with open("../../config.yaml", "r") as f:
        config = yaml.safe_load(f)
    confluence_url = config["confluence_url"]
    s3_bucket_name = config["s3_bucket_name"]
    aws_region = config.get("aws_region", "us-west-2")

    # Create download directory if it doesn't exist
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Initialize scraper and uploader
    scraper = ConfluenceScraper(base_url=confluence_url)
    uploader = S3Uploader(bucket_name=s3_bucket_name, region_name=aws_region)

    print(f"Scraping assets from: {confluence_url}")
    all_assets = scraper.scrape_assets()

    if not all_assets:
        print("No assets found or error during scraping. Exiting.")
        return

    # Save all extracted links to a CSV file
    print(f"Saving extracted asset links to {ASSET_LINKS_CSV}")
    df_links = pd.DataFrame(all_assets)
    df_links.to_csv(ASSET_LINKS_CSV, index=False)
    print(f"Found {len(all_assets)} asset links.")

    # Process and upload files based on rules
    for asset in all_assets:
        url = asset["url"]
        file_type = asset["file_type"]
        is_subscriber_content = asset["is_subscriber_content"]

        # Determine local file name from URL
        file_name = url.split("/")[-1].split("?")[
            0
        ]  # Get filename and remove query params
        if (
            not file_name or "." not in file_name
        ):  # Handle cases where filename is not clear
            file_name = f"downloaded_asset.{file_type}"

        output_path = os.path.join(DOWNLOAD_DIR, file_name)

        print(f"Processing asset: {url}")

        downloaded_file_path = download_file(url, output_path)

        if downloaded_file_path:
            # Always upload using just the filename (no subfolder)
            uploader.upload_file(
                downloaded_file_path, file_name, is_subscriber_content
            )
            os.remove(downloaded_file_path)  # Clean up local file after upload

    print("\nConfluence asset processing complete.")


if __name__ == "__main__":
    main()
