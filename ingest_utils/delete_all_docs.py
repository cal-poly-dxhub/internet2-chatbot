#!/usr/bin/env python3
"""
Script to delete all documents from an OpenSearch index using the bulk API.
This script fetches all document IDs and then uses bulk delete operations.
Uses the same configuration setup as other ingest_utils scripts.
"""

import argparse
import logging
import sys
from typing import Any, Dict, List

import boto3
import yaml
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load configuration
config = yaml.safe_load(open("../config.yaml"))


class OpenSearchBulkDeleter:
    def __init__(self):
        """
        Initialize OpenSearch client using configuration from config.yaml.
        Uses AWS OpenSearch Serverless (AOSS) with AWS authentication.
        """
        self.region = config["aws_region"]
        self.service = "aoss"
        self.host = config["opensearch_endpoint"]
        self.index_name = config["opensearch_index_name"]

        # Get AWS credentials and create auth
        session = boto3.Session()
        credentials = session.get_credentials()
        auth = AWSV4SignerAuth(credentials, self.region, self.service)

        # Create OpenSearch client
        self.client = OpenSearch(
            hosts=[{"host": self.host, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )

        logger.info(f"Connected to OpenSearch at {self.host}")
        logger.info(f"Target index: {self.index_name}")

    def get_all_document_ids(self) -> List[str]:
        """
        Get all document IDs from the configured index using search with pagination.
        OpenSearch Serverless doesn't support scroll API.

        Returns:
            List of document IDs
        """
        logger.info(f"Fetching all document IDs from index: {self.index_name}")

        try:
            doc_ids = []
            size = 1000  # Number of documents per page
            from_offset = 0

            while True:
                # Search for documents with pagination
                response = self.client.search(
                    index=self.index_name,
                    body={
                        "query": {"match_all": {}},
                        "size": size,
                        "from": from_offset,
                        "_source": False,  # Only get document IDs, not the full source
                    },
                )

                hits = response["hits"]["hits"]
                if not hits:
                    break

                # Extract document IDs from this batch
                batch_ids = [hit["_id"] for hit in hits]
                doc_ids.extend(batch_ids)

                logger.info(
                    f"Fetched {len(batch_ids)} document IDs (total: {len(doc_ids)})"
                )

                # Check if we've reached the end
                if len(hits) < size:
                    break

                from_offset += size

            logger.info(f"Found {len(doc_ids)} documents to delete")
            return doc_ids

        except Exception as e:
            logger.error(f"Error fetching document IDs: {str(e)}")
            raise

    def create_bulk_delete_actions(
        self, doc_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Create bulk delete actions for the given document IDs.

        Args:
            doc_ids: List of document IDs to delete

        Returns:
            List of bulk delete actions
        """
        actions = []
        for doc_id in doc_ids:
            action = {
                "_op_type": "delete",
                "_index": self.index_name,
                "_id": doc_id,
            }
            actions.append(action)

        return actions

    def delete_all_documents(self, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Delete all documents from the configured index using bulk API.

        Args:
            batch_size: Number of documents to delete in each batch

        Returns:
            Dictionary with deletion results
        """
        try:
            # Check if index exists by trying to get document count
            doc_count = self.get_document_count()

            if doc_count == 0:
                logger.info(f"No documents found in index '{self.index_name}'")
                return {"deleted": 0, "errors": []}

            # Get all document IDs
            doc_ids = self.get_all_document_ids()

            if not doc_ids:
                logger.info(f"No documents found in index '{self.index_name}'")
                return {"deleted": 0, "errors": []}

            # Create bulk delete actions
            actions = self.create_bulk_delete_actions(doc_ids)

            # Execute bulk delete in batches
            logger.info(
                f"Starting bulk delete operation with batch size: {batch_size}"
            )

            success_count = 0
            error_count = 0
            errors = []

            # Process in batches
            for i in range(0, len(actions), batch_size):
                batch = actions[i : i + batch_size]
                logger.info(
                    f"Processing batch {i // batch_size + 1}/{(len(actions) + batch_size - 1) // batch_size}"
                )

                try:
                    # Execute bulk delete
                    response = bulk(
                        self.client,
                        batch,
                        index=self.index_name,
                        # Note: refresh=True is not supported in OpenSearch Serverless
                    )

                    # Parse response
                    if isinstance(response, tuple):
                        success_count += response[0]
                        if response[1]:  # If there are errors
                            errors.extend(response[1])
                            error_count += len(response[1])
                    else:
                        success_count += len(batch)

                except Exception as e:
                    logger.error(f"Error in batch deletion: {str(e)}")
                    error_count += len(batch)
                    errors.append(str(e))

            result = {
                "total_documents": len(doc_ids),
                "deleted": success_count,
                "errors": error_count,
                "error_details": errors,
            }

            logger.info(
                f"Bulk delete completed. Deleted: {success_count}, Errors: {error_count}"
            )
            return result

        except Exception as e:
            logger.error(f"Error during bulk delete operation: {str(e)}")
            raise

    def get_document_count(self) -> int:
        """
        Get document count for the configured index using count API.
        OpenSearch Serverless doesn't support _stats endpoint.

        Returns:
            Number of documents in the index
        """
        try:
            response = self.client.count(
                index=self.index_name, body={"query": {"match_all": {}}}
            )
            return response["count"]
        except Exception as e:
            logger.error(f"Error getting document count: {str(e)}")
            return 0


def main():
    parser = argparse.ArgumentParser(
        description="Delete all documents from the configured OpenSearch index using bulk API"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for bulk operations (default: 1000)",
    )
    parser.add_argument(
        "--confirm", action="store_true", help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    # Initialize the deleter
    deleter = OpenSearchBulkDeleter()

    try:
        # Get current document count
        doc_count = deleter.get_document_count()

        print(
            f"Index '{deleter.index_name}' currently contains {doc_count} documents."
        )

        if doc_count == 0:
            print("No documents to delete.")
            return

        # Confirmation prompt
        if not args.confirm:
            response = input(
                f"Are you sure you want to delete ALL {doc_count} documents from index '{deleter.index_name}'? (yes/no): "
            )
            if response.lower() not in ["yes", "y"]:
                print("Operation cancelled.")
                return

        # Perform bulk delete
        result = deleter.delete_all_documents(args.batch_size)

        # Print results
        print("\n" + "=" * 50)
        print("BULK DELETE RESULTS")
        print("=" * 50)
        print(f"Total documents found: {result['total_documents']}")
        print(f"Successfully deleted: {result['deleted']}")
        print(f"Errors: {result['errors']}")

        if result["error_details"]:
            print("\nError details:")
            for error in result["error_details"][:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(result["error_details"]) > 5:
                print(
                    f"  ... and {len(result['error_details']) - 5} more errors"
                )

        # Verify deletion
        final_count = deleter.get_document_count()
        print(f"\nFinal document count in index: {final_count}")

        if final_count == 0:
            print("All documents successfully deleted!")
        else:
            print(f"{final_count} documents remain in the index")

    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
