import os
import sys
from pathlib import Path

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

import time
from typing import List, Dict, Any
from tqdm import tqdm
from elasticsearch.helpers import bulk

from app.config.logging_config import setup_logger
from app.config.settings import (
    OUTPUT_DIR,
    ES_INDEX_NAME,
    ES_VECTOR_INDEX_NAME,
    BATCH_SIZE,
)
from app.utils.helpers import init, batch_documents
from app.services.bigquery import query_bigquery
from app.services.elasticsearch import (
    get_elasticsearch_client,
    create_elastic_index,
    insert_dataframe_to_elasticsearch,
    create_vector_index,
)
from app.services.embeddings import process_batch, EMBEDDING_MODEL

logger = setup_logger(__name__)

if __name__ == "__main__":
    logger.info("🚀 Starting main application")
    init(OUTPUT_DIR)

    results = query_bigquery()

    es_client = get_elasticsearch_client()

    create_elastic_index(es_client, ES_INDEX_NAME)
    insert_dataframe_to_elasticsearch(es_client, ES_INDEX_NAME, results)

    logger.info("🔄 Processing and embedding documents...")

    # Delete and recreate the vector index
    if es_client.indices.exists(index=ES_VECTOR_INDEX_NAME):
        es_client.indices.delete(index=ES_VECTOR_INDEX_NAME)
        logger.info(f"🗑️  Deleted existing vector index: {ES_VECTOR_INDEX_NAME}")
    create_vector_index(es_client)
    logger.info(f"✨ Created new vector index: {ES_VECTOR_INDEX_NAME}")

    all_error_chunks: List[Dict[str, Any]] = []
    total_chunks = 0

    documents = results.to_dict("records")
    total_batches = (len(documents) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch in tqdm(
        batch_documents(documents, BATCH_SIZE), total=total_batches, unit="batch"
    ):
        batch_embedded_docs, error_chunks, chunks_count = process_batch(
            batch, EMBEDDING_MODEL, es_client
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
            logger.info(f"📦 Bulk insert: {success} succeeded, {len(failed)} failed")

        if error_chunks:
            all_error_chunks.extend(error_chunks)

        time.sleep(5)  # Sleep for 5 seconds to avoid RateLimit and overload

    logger.info(f"📊 Total chunks processed: {total_chunks}")
    logger.info(f"⚠️  Total errors: {len(all_error_chunks)}")

    logger.info("✅ Application completed successfully")
