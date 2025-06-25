import os
from typing import List

from textractor import Textractor
from textractor.data.constants import TextractFeatures

REGION_NAME = os.getenv("AWS_REGION")


def generate_embedding(embeddings_client, passage: str) -> List[float]:
    """Get embedding for a given passage."""
    # Invoke the model
    embedding = embeddings_client.embed_query(passage)
    return embedding


def extract_textract_data(s3, s3_file, bucket_name):
    """Extract structured text data using Textract."""

    extractor = Textractor(region_name=REGION_NAME)

    file_name, ext = os.path.splitext(s3_file)

    document = extractor.start_document_analysis(
        file_source=s3_file,
        features=[TextractFeatures.LAYOUT, TextractFeatures.TABLES],
        save_image=False,
        s3_output_path=f"s3://{bucket_name}/textract-output/{file_name}/",
    )

    print("Document analysis started... ")

    # Download pdf from s3
    local_pdf_path = f"/tmp/{os.path.basename(file_name)}"
    download_from_s3(s3, s3_file, local_pdf_path)

    return document, local_pdf_path


def download_from_s3(s3, s3_path, local_path):
    s3_bucket, s3_key = s3_path.replace("s3://", "").split("/", 1)
    s3.download_file(s3_bucket, s3_key, local_path)
