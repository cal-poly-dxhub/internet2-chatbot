"""
Script to delete all the documents in a given opensearch index.
By default size is set to delete 1000 docs. Change according to your needs.
"""

import asyncio
from datetime import datetime

import boto3
import yaml
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

config = yaml.safe_load(open("../config.yaml"))
SIZE = 1000  # Number of docs to delete

region = config["aws_region"]
service = "aoss"
host = config["opensearch_endpoint"]
index_name = config["opensearch_index_name"]

# Get AWS credentials and create auth
session = boto3.Session()
credentials = session.get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

# Create OpenSearch client for getting document IDs
client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)


def get_document_ids():
    """Fetch document IDs using the OpenSearch client"""
    search_body = {"_source": False, "size": SIZE, "query": {"match_all": {}}}
    response = client.search(index=index_name, body=search_body)
    return [hit["_id"] for hit in response["hits"]["hits"]]


def delete_document(doc_id):
    """Delete a single document"""
    try:
        delete_response = client.delete(index=index_name, id=doc_id)
        print(
            f"Deleted document ID: {doc_id} | Result: {delete_response['result']}"
        )
    except Exception as e:
        print(f"Error deleting document ID: {doc_id} | Error: {str(e)}")


async def delete_documents(doc_ids):
    """Delete multiple documents concurrently"""
    # Create a semaphore to limit concurrent operations
    semaphore = asyncio.Semaphore(10)  # Adjust this value based on your needs

    async def delete_with_semaphore(doc_id):
        async with semaphore:
            await asyncio.to_thread(delete_document, doc_id)

    tasks = [delete_with_semaphore(doc_id) for doc_id in doc_ids]
    await asyncio.gather(*tasks)


async def main():
    start_time = datetime.now()

    # Get document IDs
    doc_ids = get_document_ids()
    print(f"Found {len(doc_ids)} documents to delete")

    # Delete documents concurrently
    await delete_documents(doc_ids)

    end_time = datetime.now()
    print(f"Total time taken: {end_time - start_time}")


if __name__ == "__main__":
    asyncio.run(main())
