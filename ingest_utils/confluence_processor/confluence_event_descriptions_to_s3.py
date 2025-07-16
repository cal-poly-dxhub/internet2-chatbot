import os
import re
import requests
from bs4 import BeautifulSoup
import yaml # type: ignore
import boto3 #type: ignore
from dotenv import load_dotenv

# Load config and get env file path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
config_path = os.path.join(project_root, "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

env_file = config.get("env_file", "names.env")
load_dotenv(env_file)

CONFLUENCE_URL = config["confluence_url"]
S3_BUCKET = config["s3_bucket_name"]
AWS_REGION = config.get("aws_region", "us-west-2")
S3_SUBFOLDER = config.get("s3_subfolder", "").strip()
DOWNLOAD_DIR = "confluence_downloads"
CONFLUENCE_API_TOKEN = os.getenv("CONFLUENCE_API")
HEADERS = (
    {"Authorization": f"Bearer {CONFLUENCE_API_TOKEN}"} if CONFLUENCE_API_TOKEN else {}
)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
s3_client = boto3.client("s3", region_name=AWS_REGION)


def sanitize_filename(name):
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    return "_".join(name.strip().split())


#  to ensure ASCII-only metadata
def to_ascii(s):
    return s.encode("ascii", errors="ignore").decode("ascii") if s else s


def format_event_txt(date, title, description, who, when, where):
    """Helper to format event description."""
    lines = [
        date.strip(),
        "",
        title.strip(),
        "",
        description.strip(),
        "",
        f"Who: {who}",
        "",
        f"When: {when}",
        "",
        f"Where: {where}",
    ]
    return "\n".join([line for line in lines if line is not None])


# sanitize Google Drive/Docs/Sheets/Slides URLs
def sanitize_drive_file_url(url):
    if not url:
        return url
    import re
    match = re.match(r"(https://drive\.google\.com/file/d/[^/]+)", url)
    if match:
        return match.group(1)
    match = re.match(
        r"(https://docs\.google\.com/(?:document|spreadsheets|presentation)/d/[^/]+)",
        url,
    )
    if match:
        return match.group(1)
    match = re.match(r"(https://drive\.google\.com/drive/folders/[^/?]+)", url)
    if match:
        return match.group(1)
    return url


# Fetch and parse the Confluence page
resp = requests.get(CONFLUENCE_URL, headers=HEADERS)
resp.raise_for_status()
soup = BeautifulSoup(resp.content, "html.parser")

event_count = 0
found_table = False
# Find the first table with 'Assets' or 'Meeting Assets' in the header
for table in soup.find_all("table"):
    if table.find(
        "th", string=lambda s: s and ("Assets" in s or "Meeting Assets" in s)
    ):
        found_table = True
        for row in table.find("tbody").find_all("tr"):
            cols = row.find_all("td")

            if len(cols) < 5:
                continue  # skip bad rows
            
            date = cols[0].get_text(strip=True)
            title = cols[1].get_text(strip=True)
            description = cols[2].get_text(" ", strip=True)
            speaker = cols[3].get_text(" ", strip=True)
            assets_col = cols[4]

            meeting_link = ""
            a_tag = assets_col.find("a", href=True)
            if a_tag:
                meeting_link = a_tag["href"]
            who = speaker or "N/A"
            when = date
            where = sanitize_drive_file_url(meeting_link)
            sanitized_where = sanitize_drive_file_url(where)
            txt_content = format_event_txt(date, title, description, who, when, where)
            safe_title = (
                sanitize_filename(title)
                or f"event_{date.replace('/', '-') or 'unknown'}"
            )
            txt_file_name = f"{safe_title}.txt"
            txt_file_path = os.path.join(DOWNLOAD_DIR, txt_file_name)
            with open(txt_file_path, "w", encoding="utf-8") as f:
                f.write(txt_content)
            # S3 key
            s3_key = (
                f"{S3_SUBFOLDER}/{txt_file_name}" if S3_SUBFOLDER else txt_file_name
            )
            # Metadata
            metadata = {
                "member-content": to_ascii("False"),
                "parent-folder-name": to_ascii(title),
                "parent-folder-url": to_ascii(sanitized_where),
                "source-url": to_ascii(sanitized_where),
            }
            # Remove empty metadata
            metadata = {k: v for k, v in metadata.items() if v}
            # Upload to S3
            print(f"Uploading {txt_file_name} to s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_file(
                txt_file_path, S3_BUCKET, s3_key, ExtraArgs={"Metadata": metadata}
            )
            os.remove(txt_file_path)
            event_count += 1

if not found_table:
    print("No event table with 'Assets' or 'Meeting Assets' found on the page.")
elif event_count == 0:
    print("No valid event rows found in the event table.")
else:
    print(f"All {event_count} event descriptions uploaded to S3.")
