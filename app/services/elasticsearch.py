"""(c) 2024, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>
"""

from typing import Optional
import bigframes.pandas as bf
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config.logging_config import setup_logger
from config.settings import ES_URL, ES_API_KEY, ES_VECTOR_INDEX_NAME

logger = setup_logger(__name__)


def get_elasticsearch_client() -> Elasticsearch:
    """
    Create and return a singleton Elasticsearch client.

    Returns:
        Elasticsearch: The Elasticsearch client instance.
    """
    if not hasattr(get_elasticsearch_client, "client"):
        get_elasticsearch_client.client = Elasticsearch(ES_URL).options(
            api_key=ES_API_KEY
        )
        logger.info("🔗 Created new Elasticsearch client")

    return get_elasticsearch_client.client


def create_elastic_index(es_client: Elasticsearch, index_name: str) -> None:
    """
    Delete the Elasticsearch index if it exists, then create a new one.

    Args:
        es_client (Elasticsearch): The Elasticsearch client.
        index_name (str): The name of the index to be created.
    """
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)
        logger.info(f"🗑️  Deleted existing Elasticsearch index: {index_name}")

    es_client.indices.create(index=index_name)
    logger.info(f"✨ Created new Elasticsearch index: {index_name}")


def insert_dataframe_to_elasticsearch(
    es_client: Elasticsearch,
    index_name: str,
    dataframe: bf.DataFrame,
    doc_type: str = "kb",  # 'kb' or 'news'
    chunk_size: int = 500,
) -> None:
    """
    Insert the DataFrame into the Elasticsearch index in bulk, using chunks.

    Args:
        es_client (Elasticsearch): The Elasticsearch client.
        index_name (str): The name of the index to insert data into.
        dataframe (bf.DataFrame): The DataFrame to be inserted.
        doc_type (str): The type of document ('kb' or 'news'). Defaults to 'kb'.
        chunk_size (int): The number of documents to insert in each bulk operation. Defaults to 500.
    """

    def generate_actions():
        for _, row in dataframe.iterrows():
            doc = row.to_dict()
            doc["doc_type"] = doc_type  # Add document type
            yield {"_index": index_name, "_source": doc}

    total_documents = len(dataframe)
    success, _ = bulk(
        es_client, generate_actions(), chunk_size=chunk_size, refresh=True
    )

    logger.info(
        f"📥 Inserted {success}/{total_documents} {doc_type} documents into Elasticsearch index: {index_name}"
    )


def check_article_id_and_hash(
    client: Elasticsearch, index: str, article_id: str
) -> Optional[str]:
    """Check if an article with the given ID exists and return its hash if it does."""
    query = {"query": {"term": {"metadata.article_id": article_id}}}
    response = client.search(index=index, body=query)

    if response["hits"]["total"]["value"] > 0:
        return response["hits"]["hits"][0]["_source"]["article_hash"]

    return None


def delete_embeddings_by_article_id(client: Elasticsearch, index: str, article_id: str):
    """Delete all documents for an article with the given ID from the given index."""
    query = {"query": {"term": {"metadata.article_id": article_id}}}
    client.delete_by_query(index=index, body=query)
    logger.info(f"🗑️  Deleted embeddings for article {article_id}")


def create_vector_index(client: Elasticsearch):
    """Create a vector index."""
    client.indices.create(
        index=ES_VECTOR_INDEX_NAME,
        ignore=400,
        body={
            "mappings": {
                "properties": {
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 1536,
                    },
                    "page_content": {"type": "text"},
                    "metadata": {"type": "object"},
                    "article_id": {"type": "keyword"},
                    "chunk_id": {"type": "text"},
                    "article_hash": {"type": "keyword"},
                }
            }
        },
    )
