import json
import logging
import os
import uuid

import boto3
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

# Configure logger
logger = logging.getLogger(__name__)

REGION = os.getenv("AWS_REGION")
OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
INDEX_NAME = os.getenv("INDEX_NAME")


def get_opensearch_client():
    """Create and return an OpenSearch client with AWS authentication."""
    try:
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, REGION, "aoss")
        
        client = OpenSearch(
            hosts=[{"host": OPENSEARCH_ENDPOINT, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30
        )
        return client
    except Exception as e:
        logger.error(f"Error creating OpenSearch client: {e}")
        return None


def create_document(passage, embedding, type="video", metadata=None):
    """Create a document for OpenSearch indexing."""
    if metadata is None:
        metadata = {}
        
    doc_id = str(uuid.uuid4())
    
    document = {
        "id": doc_id,
        "passage": passage,
        "embedding": embedding,
        "type": type,
        "metadata": metadata
    }
    
    return document


def bulk_add_to_opensearch(documents):
    """Add multiple documents to OpenSearch using bulk API."""
    try:
        if not documents:
            logger.warning("No documents to add to OpenSearch")
            return True
            
        client = get_opensearch_client()
        if not client:
            return False
            
        bulk_body = []
        
        for doc in documents:
            # Add the index action
            bulk_body.append({"index": {"_index": INDEX_NAME, "_id": doc["id"]}})
            # Add the document
            bulk_body.append(doc)
        
        # Execute bulk operation
        response = client.bulk(body=bulk_body, refresh=True)
        
        # Check for errors
        if response.get("errors", False):
            error_items = [item for item in response.get("items", []) if item.get("index", {}).get("error")]
            logger.error(f"Bulk indexing errors: {error_items}")
            return False
            
        logger.info(f"Successfully added {len(documents)} documents to OpenSearch")
        return True
        
    except Exception as e:
        logger.error(f"Error adding documents to OpenSearch: {e}")
        return False


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Example usage
    test_doc = create_document(
        passage="This is a test document",
        embedding=[0.1, 0.2, 0.3],  # This would be a real embedding vector
        type="test",
        metadata={"source": "test"}
    )
    
    success = bulk_add_to_opensearch([test_doc])
    logger.info("Indexing successful" if success else "Indexing failed")
