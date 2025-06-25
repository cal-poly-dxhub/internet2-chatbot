#!/bin/bash

# Folders to delete
folders=(
    "mp4_files"
    "transcriptions"
    "__pycache__"
    "scenes"
)

# Loop through the folders and delete them
for folder in "${folders[@]}"; do
    if [ -d "$folder" ]; then
        echo "Deleting $folder..."
        rm -rf "$folder"
    else
        echo "$folder does not exist, skipping."
    fi
done

echo "Deletion complete."