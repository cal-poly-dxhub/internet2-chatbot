import os
import uuid

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from search_utils import hybrid_search


def initialize_opensearch():
    region = os.getenv("AWS_REGION")
    service = "aoss"
    host = os.getenv("OPENSEARCH_ENDPOINT")

    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token,
    )

    client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    return client


def select_top_documents(hybrid_results, max_docs=5):
    documents = hybrid_results["hits"]["hits"]
    sorted_docs = sorted(documents, key=lambda x: x["_score"], reverse=True)

    if len(sorted_docs) <= max_docs:
        return sorted_docs

    selected_docs = sorted_docs[:max_docs]
    scores = [doc["_score"] for doc in selected_docs]

    score_diffs = [scores[i] - scores[i + 1] for i in range(len(scores) - 1)]
    if score_diffs:
        max_drop_index = max(score_diffs.index(max(score_diffs)), 4)
        return sorted_docs[: max_drop_index + 1]
    else:
        return selected_docs


def get_documents(prompt, embedding, size=10):
    osClient = initialize_opensearch()

    lexical_query = {
        "query": {"match": {"passage": prompt}},
        "size": size,
        "_source": {"exclude": ["embedding"]},
    }

    semantic_query = {
        "query": {"knn": {"embedding": {"vector": embedding, "k": size}}},
        "size": size,
        "_source": {"exclude": ["embedding"]},
    }

    lexical_results = osClient.search(
        index=os.getenv("OPENSEARCH_INDEX"), body=lexical_query
    )
    semantic_results = osClient.search(
        index=os.getenv("OPENSEARCH_INDEX"), body=semantic_query
    )

    hybrid_results = hybrid_search(
        20,
        lexical_results,
        semantic_results,
        interpolation_weight=0.5,
        normalizer="minmax",
        use_rrf=False,
    )

    selected_docs = select_top_documents(hybrid_results)

    return selected_docs


def generate_short_uuid():
    return uuid.uuid4().hex[:8]
