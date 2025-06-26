"""
Script to check the number of unique metadata.source-url values in the OpenSearch index.
This helps track how many unique documents/sources have been ingested.
"""

import boto3
import yaml
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

# Load configuration
config = yaml.safe_load(open("../config.yaml"))

# OpenSearch configuration
service = "aoss"
domain_endpoint = config["opensearch_endpoint"]
domain_index = config["opensearch_index_name"]
region = config["aws_region"]

# Set up AWS authentication
credentials = boto3.Session().get_credentials()
awsauth = AWSV4SignerAuth(credentials, region, service)

# Create OpenSearch client
os_client = OpenSearch(
    hosts=[{"host": domain_endpoint, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    timeout=300,
    connection_class=RequestsHttpConnection,
)


def check_unique_source_urls():
    """
    Check the number of unique metadata.source-url values in the OpenSearch index.
    Uses aggregation to get unique count efficiently.
    """
    try:
        # Check if index exists
        if not os_client.indices.exists(index=domain_index):
            print(f"Index '{domain_index}' does not exist!")
            return
        
        # Get total document count first
        total_docs_response = os_client.count(index=domain_index)
        total_docs = total_docs_response['count']
        
        # Aggregation query to get unique source URLs
        agg_query = {
            "size": 0,  # We don't need the actual documents, just the aggregation
            "aggs": {
                "unique_sources": {
                    "terms": {
                        "field": "metadata.source-url.keyword",  # Use keyword field for exact matches
                        "size": 10000  # Adjust if you expect more than 10k unique sources
                    }
                }
            }
        }
        
        response = os_client.search(index=domain_index, body=agg_query)
        
        # Extract unique source URLs from aggregation
        buckets = response['aggregations']['unique_sources']['buckets']
        unique_count = len(buckets)
        
        print(f"OpenSearch Index: {domain_index}")
        print(f"Total documents in index: {total_docs:,}")
        print(f"Unique source URLs: {unique_count:,}")
        print(f"Average chunks per source: {total_docs/unique_count:.1f}" if unique_count > 0 else "No sources found")
        
        # Show top 10 sources by document count
        if buckets:
            print(f"\nTop 10 sources by chunk count:")
            print("-" * 80)
            for i, bucket in enumerate(buckets[:10], 1):
                source_url = bucket['key']
                doc_count = bucket['doc_count']
                print(f"{i:2d}. {source_url} ({doc_count} chunks)")
        
        return {
            'total_documents': total_docs,
            'unique_sources': unique_count,
            'sources': [bucket['key'] for bucket in buckets]
        }
        
    except Exception as e:
        print(f"Error checking unique source URLs: {str(e)}")
        return None


def get_source_details():
    """
    Get detailed information about each unique source URL.
    """
    try:
        # Get all unique sources with their document counts
        agg_query = {
            "size": 0,
            "aggs": {
                "unique_sources": {
                    "terms": {
                        "field": "metadata.source-url.keyword",
                        "size": 10000,
                        "order": {"_count": "desc"}  # Order by document count descending
                    }
                }
            }
        }
        
        response = os_client.search(index=domain_index, body=agg_query)
        buckets = response['aggregations']['unique_sources']['buckets']
        
        print(f"\nDetailed source breakdown:")
        print("=" * 100)
        
        for i, bucket in enumerate(buckets, 1):
            source_url = bucket['key']
            doc_count = bucket['doc_count']
            print(f"{i:3d}. {source_url}")
            print(f"     Chunks: {doc_count}")
            print()
        
        return buckets
        
    except Exception as e:
        print(f"Error getting source details: {str(e)}")
        return None


if __name__ == "__main__":
    print("Checking unique source URLs in OpenSearch index...")
    print("=" * 60)
    
    # Get basic statistics
    stats = check_unique_source_urls()
    
    if stats and stats['unique_sources'] > 0:
        # Ask if user wants detailed breakdown
        response = input(f"\nWould you like to see all {stats['unique_sources']} sources in detail? (y/n): ")
        if response.lower().startswith('y'):
            get_source_details()
    
    print("\nDone!")
