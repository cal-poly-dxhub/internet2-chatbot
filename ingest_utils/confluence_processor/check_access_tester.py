import json
import os

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv("names.env")

# Load your service account key
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_FILE = os.get("GOOGLE_DRIVE_CREDENTIALS")

# Print service account info
with open(SERVICE_ACCOUNT_FILE) as f:
    sa_info = json.load(f)
    print("Service Account Details:")
    print(f"Email: {sa_info.get('client_email')}")
    print(f"Project ID: {sa_info.get('project_id')}")
    print(f"Token URI: {sa_info.get('token_uri')}")
    print("-" * 50)

# Create credentials
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

# Get API key
api_key = os.getenv("GOOGLE_API_KEY")
if not api_key:
    print("Error: GOOGLE_API_KEY not found in environment variables")
    exit(1)

# Build services with both authentication methods
service_account_service = build(
    "drive", "v3", credentials=creds, developerKey=api_key
)

# Original folder to test
original_folder_id = "13CiH9pKBxAI5zl-LrgjY-ZiV_zC5FxmX"
original_folder_url = (
    f"https://drive.google.com/drive/folders/{original_folder_id}"
)

print("\nTesting access to Original Folder:")
print(f"URL: {original_folder_url}")
print(f"ID: {original_folder_id}")
print("-" * 50)


def test_folder_access(service, auth_type):
    try:
        # Try to get the folder metadata
        folder = (
            service.files()
            .get(
                fileId=original_folder_id,
                fields="id, name, permissions, owners, shared, capabilities",
                supportsAllDrives=True,
            )
            .execute()
        )

        print(f"\n✅ Success with {auth_type} authentication:")
        print(f"Name: {folder.get('name')}")
        print(f"ID: {folder.get('id')}")
        print(f"Shared: {folder.get('shared', 'Unknown')}")

        # Try to list files
        query = f"'{original_folder_id}' in parents and trashed = false"
        results = (
            service.files()
            .list(
                q=query,
                pageSize=5,
                fields="files(id, name, mimeType)",
                supportsAllDrives=True,
            )
            .execute()
        )

        files = results.get("files", [])
        print(f"\nFound {len(files)} files/folders:")
        for file in files:
            print(f"- {file['name']} ({file['mimeType']})")

    except Exception as e:
        print(f"\n❌ Failed with {auth_type} authentication:")
        print(f"Error: {str(e)}")
        print("\nPlease check:")
        print("1. The folder ID is correct")
        print("2. The folder is shared with the service account")
        print("3. The service account has the correct IAM roles")
        print("4. The Google Drive API is enabled in your project")


# Test both authentication methods
print("\nTesting with Service Account authentication...")
test_folder_access(service_account_service, "Service Account")


# Folder IDs to check
folder_ids = {
    "Test Folder (my own fodler)": "1GTodmYU8YS7KLQGf79Kr4x4dqRFXlZWn",
    "New Shared Folder (owned by someone else)": "1WBKjjfOEF-wLy4hzYtgf_GPRzip6ZsK8",
    "NICK": "16A4U7s4VpReJZ52xbPEHMdCMIhEyoZkR",
}

for folder_name, folder_id in folder_ids.items():
    try:
        # Try to get the folder metadata with more fields
        folder = (
            service_account_service.files()
            .get(
                fileId=folder_id,
                fields="id, name, permissions, owners, shared, capabilities",
            )
            .execute()
        )
        print(f"\n✅ Service account CAN access {folder_name}:")
        print(f"Name: {folder.get('name')}")
        print(f"ID: {folder.get('id')}")
        print(f"Shared: {folder.get('shared', 'Unknown')}")
        if "capabilities" in folder:
            print("Capabilities:")
            for key, value in folder["capabilities"].items():
                print(f"  - {key}: {value}")
    except Exception as e:
        print(f"\n❌ Service account CANNOT access {folder_name} {folder_id}:")
        print(f"Error: {str(e)}")
        print("Please check:")
        print("1. The folder ID is correct")
        print("2. The folder is shared with the service account")
        print("3. The service account has the correct IAM roles")
        print("4. The Google Drive API is enabled in your project")
