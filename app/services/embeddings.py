"""(c) 2024, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>
"""

from typing import List, Dict, Any, Tuple, Optional
from markdownify import markdownify
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_text_splitters.base import Language
from langchain_openai import AzureOpenAIEmbeddings
from elasticsearch import Elasticsearch

from config.logging_config import setup_logger
from config.settings import (
    AZURE_EMBEDDING_DEPLOYMENT_NAME,
    AZURE_EMBEDDING_API_VERSION,
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_ENDPOINT,
    SNOW_BASE_URL,
    ES_VECTOR_INDEX_NAME,
)
from services.elasticsearch import (
    delete_embeddings_by_article_id,
    check_article_id_and_hash,
)
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
    source_type: str = "kb",  # 'kb' or 'news'
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """Process a batch of documents by splitting them into chunks and embedding them."""
    embedded_docs: List[Dict[str, Any]] = []
    batch_error_chunks: List[Dict[str, Any]] = []
    total_chunks_count: int = 0

    for temp_doc in batch_to_process:
        # Handle KB articles
        if source_type == "kb":
            if temp_doc["workflow_state"] != "published":
                continue
            article_id: str = temp_doc["article_id"]
            doc_body: str = markdownify(temp_doc["text"])
            title: str = temp_doc.get("short_description", "")
            sys_id: Optional[str] = temp_doc.get("sys_id")
            kb_article_id: Optional[str] = temp_doc.get("number")
            url = f"{SNOW_BASE_URL}/esc?id=kb_article&table=kb_knowledge&sys_id={sys_id}&recordUrl=kb_view.do?sysparm_article%3D{kb_article_id}"
            
        # Handle News articles
        else:
            article_id: str = temp_doc["sys_id"]
            headline: str = temp_doc.get("headline", "")
            subheadline: str = temp_doc.get("subheadline", "")
            rich_content: str = temp_doc.get("rich_content_html", "")
            
            # Combine headline, subheadline, and content
            doc_body: str = f"# {headline}\n\n{subheadline}\n\n{markdownify(rich_content)}"
            title: str = headline
            sys_id: Optional[str] = temp_doc.get("sys_id")
            url = f"{SNOW_BASE_URL}/now/nav/ui/classic/params/target/sn_cd_content_news.do%3Fsys_id%3D{sys_id}"

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

        metadata: Dict[str, Any] = {
            "article_id": article_id,
            "title": title,
            "timestamp": temp_doc.get("sys_updated_on"),
            "url": url,
            "source": source_type,  # Add source type to metadata
        }

        # Add KB-specific metadata
        if source_type == "kb":
            metadata["kb_number"] = temp_doc.get("number")

        # Add News-specific metadata
        else:
            metadata["news_start_date"] = temp_doc.get("news_start_date")
            metadata["news_end_date"] = temp_doc.get("news_end_date")
            metadata["thumbnail"] = temp_doc.get("thumbnail")

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
                    "source": source_type,  # Add source type to root level
                },
            }
            embedded_docs.append(embedded_doc)

    return embedded_docs, batch_error_chunks, total_chunks_count
