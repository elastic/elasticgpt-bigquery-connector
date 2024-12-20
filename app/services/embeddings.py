from typing import List, Dict, Any, Tuple, Optional
from markdownify import markdownify
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_text_splitters.base import Language
from langchain_openai import AzureOpenAIEmbeddings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config.logging_config import setup_logger
from config.settings import (
    AZURE_EMBEDDING_DEPLOYMENT_NAME,
    AZURE_EMBEDDING_API_VERSION,
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    SNOW_BASE_URL,
    ES_VECTOR_INDEX_NAME,
)
from services.elasticsearch import delete_embeddings_by_article_id, check_article_id_and_hash
from utils.helpers import generate_hash

logger = setup_logger(__name__)

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

def process_batch(
    batch_to_process: List[Dict[str, Any]],
    embedding_model: AzureOpenAIEmbeddings,
    es_client: Elasticsearch,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
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
                f"⏭️  Article ID {article_id} with the same hash already exists. Skipping update."
            )
            continue

        if existing_body_hash and existing_body_hash != body_hash:
            delete_embeddings_by_article_id(es_client, ES_VECTOR_INDEX_NAME, article_id)

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
            logger.error(f"❌ Error embedding document: {metadata['article_id']}: {e}")
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