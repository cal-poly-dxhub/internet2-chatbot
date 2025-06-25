import os

import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

REGION = os.getenv("AWS_REGION")
OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
INDEX_NAME = os.getenv("INDEX_NAME")


def initialize_opensearch():
    service = "aoss"
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, REGION, service)
    client = OpenSearch(
        hosts=[{"host": OPENSEARCH_ENDPOINT, "port": 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )
    return client


def create_document(
    passage: str, embedding: list[float], type: str, metadata: dict[str, str]
) -> dict:
    """Creates a document object with the specified fields."""
    return {
        "passage": passage,
        "embedding": embedding,
        "type": type,
        "metadata": metadata,
    }


def bulk_add_to_opensearch(documents: list[dict]) -> bool:
    """
    Bulk add documents to OpenSearch.

    Args:
        documents: List of dictionaries, each containing:
            - passage: str
            - embedding: list[float]
            - type: str
            - metadata: dict

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client = initialize_opensearch()
        bulk_operations = []

        for doc in documents:
            # Create the bulk operation array in the correct format
            bulk_operations.extend([{"index": {"_index": INDEX_NAME}}, doc])

        if bulk_operations:
            print(
                f"Executing bulk upload with {len(bulk_operations) // 2} documents..."
            )
            response = client.bulk(body=bulk_operations)

            if response.get("errors", False):
                print(f"Errors occurred during bulk upload: {response}")
                return False

            print(
                f"Bulk upload completed: took {response.get('took', 'N/A')}ms with {len(response.get('items', []))} items"
            )
            return True
        else:
            print("No documents provided for bulk upload.")
            return False

    except Exception as e:
        print(f"An error occurred during bulk upload: {str(e)}")
        return False


# Example usage
if __name__ == "__main__":
    # Example documents
    sample_documents = [
        {
            "passage": "Sample text 1",
            "embedding": [0.1, 0.2, 0.3],
            "type": "document",
            "metadata": {
                "source": "example.com",
                "author": "John Doe",
                "date": "2024-01-01",
            },
        },
        {
            "passage": "Sample text 2",
            "embedding": [0.4, 0.5, 0.6],
            "type": "document",
            "metadata": {
                "source": "example.com",
                "author": "Jane Doe",
                "date": "2024-01-02",
            },
        },
    ]

    success = bulk_add_to_opensearch(sample_documents)
    print("Success" if success else "Failure")
