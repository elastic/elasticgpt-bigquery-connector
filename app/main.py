# Import necessary libraries
import os
import time
import logging
import json
import hashlib
from typing import List, Optional, Dict, Any, Generator

from google.cloud import bigquery
import bigframes.pandas as bf
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from langchain_openai import AzureOpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_text_splitters.base import Language
from tqdm import tqdm
from markdownify import markdownify

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()
logger.info("Environment variables loaded from .env file")

# Retrieve configuration from environment variables
GBQ_PROJECT_ID: Optional[str] = os.getenv("GBQ_PROJECT_ID")
GBQ_LOCATION: Optional[str] = os.getenv("GBQ_LOCATION")
GBQ_DATASET: Optional[str] = os.getenv("GBQ_DATASET")
GBQ_TABLE: Optional[str] = os.getenv("GBQ_TABLE")
GBQ_MAX_RESULTS: int = int(os.getenv("GBQ_MAX_RESULTS", "10"))
OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data")
ES_URL: Optional[str] = os.getenv("ELASTICSEARCH_URL")
ES_API_KEY: Optional[str] = os.getenv("ELASTICSEARCH_API_KEY")
ES_INDEX_NAME: str = os.getenv("ES_INDEX_NAME", "test-bq-snow-ingest")
ES_VECTOR_INDEX_NAME: str = os.getenv(
    "ES_VECTOR_INDEX_NAME", "test-bq-embeddings-openai"
)
AZURE_EMBEDDING_DEPLOYMENT_NAME: Optional[str] = os.getenv(
    "AZURE_EMBEDDING_DEPLOYMENT_NAME"
)
AZURE_EMBEDDING_API_VERSION: Optional[str] = os.getenv("AZURE_EMBEDDING_API_VERSION")
AZURE_OPENAI_API_KEY: Optional[str] = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT: Optional[str] = os.getenv("AZURE_OPENAI_ENDPOINT")
SNOW_BASE_URL: Optional[str] = os.getenv("SNOW_BASE_URL")
KB_KNOWLEDGE_BASE_VALUES: str = os.getenv("KB_KNOWLEDGE_BASE_VALUES", "")

logger.info("Configuration retrieved from environment variables")

# Constants
BATCH_SIZE: int = 20
QUERY_SIZE: int = 10000

# Initialize text splitter
TEXT_SPLITTER: RecursiveCharacterTextSplitter = (
    RecursiveCharacterTextSplitter.from_language(
        chunk_size=2048, chunk_overlap=256, language=Language.MARKDOWN
    )
)

# Initialize Azure OpenAI Embeddings model
EMBEDDING_MODEL: AzureOpenAIEmbeddings = AzureOpenAIEmbeddings(
    azure_deployment=AZURE_EMBEDDING_DEPLOYMENT_NAME,
    openai_api_version=AZURE_EMBEDDING_API_VERSION,
    api_key=AZURE_OPENAI_API_KEY,
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
)


def init() -> None:
    """
    Initialize the application by creating the output directory if it doesn't exist.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"Created output directory: {OUTPUT_DIR}")


def connect_to_bigquery() -> bigquery.Client:
    """
    Establish a connection to BigQuery.

    Returns:
        bigquery.Client: A BigQuery client object.
    """
    client: bigquery.Client = bigquery.Client()
    logger.info("Connected to BigQuery")
    return client


def query_bigquery() -> bf.DataFrame:
    """
    Query BigQuery to retrieve data.

    Returns:
        bf.DataFrame: A BigFrames DataFrame containing the query results.
    """
    logger.info("Starting BigQuery query process")
    client: bigquery.Client = connect_to_bigquery()
    bf.options.bigquery.client = client
    bf.options.bigquery.location = GBQ_LOCATION
    bf.options.bigquery.project = GBQ_PROJECT_ID

    columns: List[str] = [
        "active",
        "article_id",
        "article_type",
        "flagged",
        "meta",
        "meta_description",
        "number",
        "published",
        "short_description",
        "text",
        "topic",
        "version_link",
        "workflow_state",
        "sys_updated_on",
        "sys_created_on",
        "sys_id",
        "kb_knowledge_base_value",
        "can_read_user_criteria"
    ]

    # Check if required values exist before constructing the query
    if not all([GBQ_PROJECT_ID, GBQ_DATASET, GBQ_TABLE, GBQ_MAX_RESULTS]):
        raise ValueError("Missing required BigQuery configuration values")

    query: str = f"""
        SELECT      {', '.join(columns)}
        FROM        `{GBQ_PROJECT_ID}.{GBQ_DATASET}.{GBQ_TABLE}`
        WHERE       workflow_state = 'published'
                    AND (
                        (kb_knowledge_base_value = 'a7e8a78bff0221009b20ffffffffff17')
                        OR
                        (kb_knowledge_base_value = 'bb0370019f22120047a2d126c42e7073' AND (can_read_user_criteria IS NULL OR can_read_user_criteria = ''))
                    )
        LIMIT       {GBQ_MAX_RESULTS}
    """

    logger.info(f"\nQuery: {query}\n")

    df: bf.DataFrame = bf.read_gbq(query)
    logger.info(f"Query executed, retrieved {len(df)} rows")

    return df


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
        logger.info("Created new Elasticsearch client")

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
        logger.info(f"Deleted existing Elasticsearch index: {index_name}")

    es_client.indices.create(index=index_name)
    logger.info(f"Created new Elasticsearch index: {index_name}")


def insert_dataframe_to_elasticsearch(
    es_client: Elasticsearch,
    index_name: str,
    dataframe: bf.DataFrame,
    chunk_size: int = 500,
) -> None:
    """
    Insert the DataFrame into the Elasticsearch index in bulk, using chunks.

    Args:
        es_client (Elasticsearch): The Elasticsearch client.
        index_name (str): The name of the index to insert data into.
        dataframe (bf.DataFrame): The DataFrame to be inserted.
        chunk_size (int): The number of documents to insert in each bulk operation. Defaults to 500.
    """

    def generate_actions():
        for _, row in dataframe.iterrows():
            yield {"_index": index_name, "_source": row.to_dict()}

    total_documents = len(dataframe)
    success, _ = bulk(
        es_client, generate_actions(), chunk_size=chunk_size, refresh=True
    )

    logger.info(
        f"Inserted {success}/{total_documents} documents into Elasticsearch index: {index_name}"
    )


def generate_hash(data: Any) -> str:
    """Generate a hash for the given data."""
    data_string = json.dumps(data, sort_keys=True)
    return hashlib.md5(data_string.encode()).hexdigest()


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
    logger.info(f"Deleted embeddings for article {article_id}")


def process_batch(
    batch_to_process: List[Dict[str, Any]],
    embedding_model: AzureOpenAIEmbeddings,
    es_client: Elasticsearch,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Process a batch of documents by splitting them into chunks and embedding them."""
    embedded_docs: List[Dict[str, Any]] = []
    batch_error_chunks: List[Dict[str, Any]] = []
    total_chunks_count: int = 0

    for temp_doc in batch_to_process:
        if temp_doc["workflow_state"] != "published":
            continue

        article_id: str = temp_doc["article_id"]
        doc_body: str = markdownify(temp_doc["text"])  # Convert HTML to Markdown
        body_hash: str = generate_hash(doc_body)
        existing_body_hash: Optional[str] = check_article_id_and_hash(
            es_client, ES_VECTOR_INDEX_NAME, article_id
        )

        if existing_body_hash == body_hash:
            logger.info(
                f"Article ID {article_id} with the same hash already exists. Skipping update."
            )
            continue

        if existing_body_hash and existing_body_hash != body_hash:
            delete_embeddings_by_article_id(es_client, ES_VECTOR_INDEX_NAME, article_id)

        # https://elasticprod.service-now.com/esc?id=kb_article&table=kb_knowledge&sys_id=43f43838476f3d50ffad4438946d43a3&recordUrl=kb_view.do?sysparm_article%3DKB0012917
        sys_id: Optional[str] = temp_doc.get("sys_id")
        kb_article_id: Optional[str] = temp_doc.get("number")
        url = f"{SNOW_BASE_URL}/esc?id=kb_article&table=kb_knowledge&sys_id={sys_id}&recordUrl=kb_view.do?sysparm_article%3D{kb_article_id}"

        metadata: Dict[str, Any] = {
            "kb_number": temp_doc.get("number"),
            "article_id": temp_doc.get("number"),
            "title": temp_doc.get("short_description"),
            "timestamp": temp_doc.get("sys_updated_on"),
            "url": url
        }

        chunks: List[str] = TEXT_SPLITTER.split_text(doc_body)
        total_chunks_count += len(chunks)

        try:
            embeddings: List[List[float]] = embedding_model.embed_documents(chunks)
        except Exception as e:
            logger.error(f"Error embedding document: {metadata['article_id']}: {e}")
            batch_error_chunks.append({"metadata": metadata, "chunks": chunks})
            continue

        for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            unique_identifier = f"{metadata['article_id']}_chunk_{idx}"
            embedded_doc = {
                "_index": ES_VECTOR_INDEX_NAME,
                "_id": unique_identifier,
                "_op_type": "index",
                "_source": {
                    "embedding": embedding,
                    "page_content": chunk,
                    "metadata": metadata,
                    "article_id": metadata.get("article_id"),
                    "chunk_id": idx,
                    "article_hash": body_hash,
                },
            }
            embedded_docs.append(embedded_doc)

    return embedded_docs, batch_error_chunks, total_chunks_count


def batch_documents(
    documents: List[Dict[str, Any]], batch_size: int
) -> Generator[List[Dict[str, Any]], None, None]:
    """Split the documents into batches."""
    for i in range(0, len(documents), batch_size):
        yield documents[i : i + batch_size]


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


if __name__ == "__main__":
    logger.info("Starting main application")
    init()

    results: bf.DataFrame = query_bigquery()
    # exit(0)

    es_client = get_elasticsearch_client()

    create_elastic_index(es_client, ES_INDEX_NAME)
    insert_dataframe_to_elasticsearch(es_client, ES_INDEX_NAME, results)

    logger.info("Processing and embedding documents...")

    # Delete and recreate the vector index
    if es_client.indices.exists(index=ES_VECTOR_INDEX_NAME):
        es_client.indices.delete(index=ES_VECTOR_INDEX_NAME)
        logger.info(f"Deleted existing vector index: {ES_VECTOR_INDEX_NAME}")
    create_vector_index(es_client)
    logger.info(f"Created new vector index: {ES_VECTOR_INDEX_NAME}")

    embedding_model: AzureOpenAIEmbeddings = EMBEDDING_MODEL
    all_error_chunks: List[Dict[str, Any]] = []
    total_chunks = 0

    documents = results.to_dict("records")
    total_batches = (len(documents) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch in tqdm(
        batch_documents(documents, BATCH_SIZE), total=total_batches, unit="batch"
    ):
        batch_embedded_docs, error_chunks, chunks_count = process_batch(
            batch, embedding_model, es_client
        )
        total_chunks += chunks_count

        if batch_embedded_docs:
            success, failed = bulk(
                es_client,
                batch_embedded_docs,
                chunk_size=500,
                request_timeout=60,
                raise_on_error=False,
            )
            logger.info(f"Bulk insert: {success} succeeded, {len(failed)} failed")

        if error_chunks:
            all_error_chunks.extend(error_chunks)

        time.sleep(5)  # Sleep for 5 seconds to avoid RateLimit and overload

    logger.info(f"Total chunks processed: {total_chunks}")
    logger.info(f"Total errors: {len(all_error_chunks)}")

    logger.info("Application completed successfully")
