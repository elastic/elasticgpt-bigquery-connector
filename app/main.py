# app/main.py

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
from app.services.bigquery import query_bigquery, query_news_articles
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

    # Process KB Articles
    logger.info("📚 Processing KB Articles...")
    kb_results = query_bigquery()
    
    # Process News Articles
    logger.info("📰 Processing News Articles...")
    news_results = query_news_articles()

    es_client = get_elasticsearch_client()

    # Store raw data in regular index
    create_elastic_index(es_client, ES_INDEX_NAME)
    insert_dataframe_to_elasticsearch(es_client, ES_INDEX_NAME, kb_results, doc_type="kb")
    insert_dataframe_to_elasticsearch(es_client, ES_INDEX_NAME, news_results, doc_type="news")

    logger.info("🔄 Processing and embedding documents...")

    # Delete and recreate the vector index
    if es_client.indices.exists(index=ES_VECTOR_INDEX_NAME):
        es_client.indices.delete(index=ES_VECTOR_INDEX_NAME)
        logger.info(f"🗑️  Deleted existing vector index: {ES_VECTOR_INDEX_NAME}")
    
    create_vector_index(es_client)
    logger.info(f"✨ Created new vector index: {ES_VECTOR_INDEX_NAME}")

    all_error_chunks: List[Dict[str, Any]] = []
    total_chunks = 0

    # Process KB documents
    kb_documents = kb_results.to_dict("records")
    total_kb_batches = (len(kb_documents) + BATCH_SIZE - 1) // BATCH_SIZE

    logger.info(f"📚 Processing {len(kb_documents)} KB articles...")
    for batch in tqdm(
        list(batch_documents(kb_documents, BATCH_SIZE)), 
        total=total_kb_batches, 
        unit="batch"
    ):
        batch_embedded_docs, error_chunks, chunks_count = process_batch(
            batch, EMBEDDING_MODEL, es_client, source_type="kb"
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
            logger.info(f"📦 KB Bulk insert: {success} succeeded, {len(failed)} failed")

        if error_chunks:
            all_error_chunks.extend(error_chunks)

        time.sleep(5)  # Sleep for 5 seconds to avoid RateLimit and overload

    # Process News documents
    news_documents = news_results.to_dict("records")
    total_news_batches = (len(news_documents) + BATCH_SIZE - 1) // BATCH_SIZE

    logger.info(f"📰 Processing {len(news_documents)} News articles...")
    for batch in tqdm(
        list(batch_documents(news_documents, BATCH_SIZE)), 
        total=total_news_batches, 
        unit="batch"
    ):
        batch_embedded_docs, error_chunks, chunks_count = process_batch(
            batch, EMBEDDING_MODEL, es_client, source_type="news"
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
            logger.info(f"📦 News Bulk insert: {success} succeeded, {len(failed)} failed")

        if error_chunks:
            all_error_chunks.extend(error_chunks)

        time.sleep(5)  # Sleep for 5 seconds to avoid RateLimit and overload

    logger.info(f"📊 Total chunks processed: {total_chunks}")
    logger.info(f"⚠️  Total errors: {len(all_error_chunks)}")

    logger.info("✅ Application completed successfully")
