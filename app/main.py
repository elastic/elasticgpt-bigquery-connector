# app/main.py

"""(c) 2025, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>


Script Flow:
--------------
1. Set up the project root and update the Python path.
2. Import necessary modules and configure logging.
3. Initialize the output directory.
4. Query KB and News articles from BigQuery.
5. Obtain an Elasticsearch client.
6. Create the raw data index and store KB and News articles.
7. Delete and recreate the vector index for embedding storage.
8. Process KB documents in batches:
   - Embed each document batch.
   - Bulk insert embedded documents into Elasticsearch.
   - Collect any errors and enforce a delay to avoid rate limits.
9. Process News documents similarly in batches.
10. Log the total processed chunks and any errors.
11. Conclude the application run.
"""

import sys
from pathlib import Path

# Add the project root directory to the Python path to allow local imports
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

import time
from typing import List, Dict, Any
from tqdm import tqdm  # Progress bar for monitoring batch processing
from elasticsearch.helpers import bulk  # Bulk helper for Elasticsearch insertion

# Import project-specific configurations and utility functions
from app.config.logging_config import setup_logger
from app.config.settings import (
    OUTPUT_DIR,
    ES_INDEX_NAME,
    ES_VECTOR_INDEX_NAME,
    BATCH_SIZE,
)
from app.utils.helpers import init, batch_documents
from app.services.bigquery import query_bigquery, query_news_articles
from app.services.elasticsearch import (
    get_elasticsearch_client,
    create_elastic_index,
    insert_dataframe_to_elasticsearch,
    create_vector_index,
)
from app.services.embeddings import process_batch, EMBEDDING_MODEL

# Set up the logger using the project's logging configuration
logger = setup_logger(__name__)

if __name__ == "__main__":
    # Log the start of the application
    logger.info("🚀 Starting main application")

    # Initialize the output directory (e.g., create necessary folders)
    init(OUTPUT_DIR)

    # -------------------------------
    # Querying Data from BigQuery
    # -------------------------------
    # Process Knowledge Base (KB) Articles
    logger.info("📚 Processing KB Articles...")
    kb_results = query_bigquery()

    # Process News Articles
    logger.info("📰 Processing News Articles...")
    news_results = query_news_articles()

    # -------------------------------
    # Elasticsearch Setup
    # -------------------------------
    # Get the Elasticsearch client instance
    es_client = get_elasticsearch_client()

    # Create a regular index to store raw data and insert queried documents
    create_elastic_index(es_client, ES_INDEX_NAME)
    insert_dataframe_to_elasticsearch(es_client, ES_INDEX_NAME, kb_results, doc_type="kb")
    insert_dataframe_to_elasticsearch(es_client, ES_INDEX_NAME, news_results, doc_type="news")

    logger.info("🔄 Processing and embedding documents...")

    # -------------------------------
    # Vector Index Management
    # -------------------------------
    # Delete the existing vector index if it exists to ensure a clean slate
    if es_client.indices.exists(index=ES_VECTOR_INDEX_NAME):
        es_client.indices.delete(index=ES_VECTOR_INDEX_NAME)
        logger.info(f"🗑️  Deleted existing vector index: {ES_VECTOR_INDEX_NAME}")

    # Create a new vector index to store embedded documents
    create_vector_index(es_client)
    logger.info(f"✨ Created new vector index: {ES_VECTOR_INDEX_NAME}")

    # Initialize error collection and chunk counter
    all_error_chunks: List[Dict[str, Any]] = []
    total_chunks = 0

    # -------------------------------
    # Processing KB Documents
    # -------------------------------
    # Convert the KB articles from DataFrame to list of dictionary records
    kb_documents = kb_results.to_dict("records")
    total_kb_batches = (len(kb_documents) + BATCH_SIZE - 1) // BATCH_SIZE

    logger.info(f"📚 Processing {len(kb_documents)} KB articles...")
    # Process documents in batches using a progress bar for visibility
    for batch in tqdm(
        list(batch_documents(kb_documents, BATCH_SIZE)),
        total=total_kb_batches,
        unit="batch"
    ):
        # Process each batch: generate embeddings and capture errors
        batch_embedded_docs, error_chunks, chunks_count = process_batch(
            batch, EMBEDDING_MODEL, es_client, source_type="kb"
        )
        total_chunks += chunks_count

        # If there are embedded documents, bulk insert them into Elasticsearch
        if batch_embedded_docs:
            success, failed = bulk(
                es_client.options(request_timeout=60),
                batch_embedded_docs,
                chunk_size=500,
                raise_on_error=False,
            )
            logger.info(f"📦 KB Bulk insert: {success} succeeded, {len(failed)} failed")

        # Collect any error chunks from processing
        if error_chunks:
            all_error_chunks.extend(error_chunks)

        # Pause for 2 seconds to avoid rate limiting and potential overload
        time.sleep(2)

    # -------------------------------
    # Processing News Documents
    # -------------------------------
    # Convert the News articles from DataFrame to list of dictionary records
    news_documents = news_results.to_dict("records")
    total_news_batches = (len(news_documents) + BATCH_SIZE - 1) // BATCH_SIZE

    logger.info(f"📰 Processing {len(news_documents)} News articles...")
    # Process News documents in batches
    for batch in tqdm(
        list(batch_documents(news_documents, BATCH_SIZE)),
        total=total_news_batches,
        unit="batch"
    ):
        # Process each batch: generate embeddings and capture errors
        batch_embedded_docs, error_chunks, chunks_count = process_batch(
            batch, EMBEDDING_MODEL, es_client, source_type="news"
        )
        total_chunks += chunks_count

        # Bulk insert embedded documents if available
        if batch_embedded_docs:
            success, failed = bulk(
                es_client.options(request_timeout=60),
                batch_embedded_docs,
                chunk_size=500,
                raise_on_error=False,
            )
            logger.info(f"📦 News Bulk insert: {success} succeeded, {len(failed)} failed")

        # Collect any error chunks from processing
        if error_chunks:
            all_error_chunks.extend(error_chunks)

        # Pause for 5 seconds to avoid rate limits and overload
        time.sleep(5)

    # -------------------------------
    # Final Logging and Completion
    # -------------------------------
    # Log the total number of document chunks processed and any errors encountered
    logger.info(f"📊 Total chunks processed: {total_chunks}")
    logger.info(f"⚠️  Total errors: {len(all_error_chunks)}")

    # Indicate that the application has completed successfully
    logger.info("✅ Application completed successfully")
