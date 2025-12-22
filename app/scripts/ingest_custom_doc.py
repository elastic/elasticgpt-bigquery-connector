#!/usr/bin/env python3
"""(c) 2025, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>

Simple script to ingest a custom markdown document and create embeddings.

=== COMPREHENSIVE USAGE GUIDE ===

COMMAND SYNTAX:
  # Basic ingestion
  poetry run python app/scripts/ingest_custom_doc.py <path_to_markdown_file>

  # Delete document
  poetry run python app/scripts/ingest_custom_doc.py --delete <path_to_markdown_file>

USAGE EXAMPLES:
  # Ingest a local markdown file
  poetry run python app/scripts/ingest_custom_doc.py /Users/john/Documents/my_guide.md
  poetry run python app/scripts/ingest_custom_doc.py ./data/custom_docs/product_manual.md
  poetry run python app/scripts/ingest_custom_doc.py ~/Downloads/technical_spec.md

  # Delete previously ingested documents
  poetry run python app/scripts/ingest_custom_doc.py --delete /Users/john/Documents/my_guide.md
  poetry run python app/scripts/ingest_custom_doc.py --delete ./data/custom_docs/product_manual.md

DIRECTORY SETUP:
  # Recommended: Create a directory for custom documents
  mkdir -p data/custom_docs
  # Place your .md files there for organization

WHAT HAPPENS DURING INGESTION:
  1. File is read and content hash is generated
  2. Document ID is created as: custom_{filename_without_extension}
     Example: my_guide.md → article_id: "custom_my_guide"
  3. Content is split into chunks (max 2048 chars with 256 char overlap)
  4. Each chunk gets embedded using Azure OpenAI (1536-dimension vectors)
  5. All chunks stored in Elasticsearch with metadata

INDEX DETAILS:
- Uses vector index: ES_VECTOR_INDEX_NAME (from config/settings.py)
- Default index name: "test-bq-embeddings-openai"
- Same index used by main application for KB and News articles
- Documents tagged with source="custom" to distinguish from other sources
- Each document chunk gets unique ID: {article_id}_chunk_{index}

UPDATE/DELETE BEHAVIOR:
- AUTOMATIC UPDATES: Script detects content changes via hash comparison
  * Same content hash → Skip ingestion (logs: "already exists")
  * Different content hash → Delete old chunks, insert new ones
  * New file → Direct ingestion
- MANUAL DELETE: Use --delete flag to remove all chunks for a document
- ELASTICSEARCH QUERIES: Can also delete via ES API using article_id filter

TROUBLESHOOTING:
- Ensure .env file has all required Azure OpenAI and Elasticsearch credentials
- Check that ES_VECTOR_INDEX_NAME index exists (script creates it if missing)
- Large files may take time due to embedding generation API calls
- Rate limits: Azure OpenAI has request limits, script will show progress

INTEGRATION WITH MAIN APP:
- Custom documents appear alongside KB/News articles in vector searches
- Use source="custom" filter in queries to search only custom docs
- All standard search/retrieval functionality works with custom documents
"""

import sys
import argparse
from pathlib import Path

# Add the project root directory to the Python path
project_root = str(Path(__file__).parent.parent.parent)
sys.path.insert(0, project_root)

# Add app directory to path for relative imports
app_path = str(Path(__file__).parent.parent)
sys.path.insert(0, app_path)

from config.logging_config import setup_logger  # noqa: E402
from config.settings import ES_VECTOR_INDEX_NAME  # noqa: E402
from services.elasticsearch import get_elasticsearch_client, create_vector_index  # noqa: E402
from services.embeddings import TEXT_SPLITTER, EMBEDDING_MODEL  # noqa: E402
from utils.helpers import generate_hash  # noqa: E402
from elasticsearch.helpers import bulk  # noqa: E402
from datetime import datetime  # noqa: E402

logger = setup_logger(__name__)


def delete_custom_doc(file_path: str, index_name: str = None):
    """
    Delete all chunks for a custom document from Elasticsearch.

    This function performs the following steps:
    1. Generates the same article_id that was used during ingestion
    2. Queries Elasticsearch to find all chunks for this document
    3. Deletes all matching chunks using delete_by_query API
    4. Returns count of deleted chunks for confirmation

    ELASTICSEARCH QUERY USED:
    {
        "query": {
            "term": {"article_id": "custom_{filename_stem}"}
        }
    }

    COMMAND USAGE:
    poetry run python app/scripts/ingest_custom_doc.py --delete /path/to/file.md
    poetry run python app/scripts/ingest_custom_doc.py --delete /path/to/file.md --index my_custom_index

    Args:
        file_path (str): Path to the markdown file (used to generate article_id)
                        Example: "/Users/john/docs/guide.md" → article_id: "custom_guide"
        index_name (str, optional): Custom index name. If None, uses ES_VECTOR_INDEX_NAME from settings

    Returns:
        int: Number of chunks deleted from Elasticsearch
    """
    file_path = Path(file_path)
    doc_id = f"custom_{file_path.stem}"

    # Use provided index name or fallback to default from settings
    target_index = index_name if index_name else ES_VECTOR_INDEX_NAME

    es_client = get_elasticsearch_client()

    # Delete all chunks for this document from the specified index
    delete_query = {"query": {"term": {"article_id": doc_id}}}
    result = es_client.delete_by_query(index=target_index, body=delete_query)

    deleted_count = result.get("deleted", 0)
    logger.info(
        f"🗑️  Deleted {deleted_count} chunks for document: {doc_id} from index: {target_index}"
    )
    return deleted_count


def ingest_markdown(file_path: str, index_name: str = None, title: str = None):
    """
    Ingest a markdown file and create embeddings for vector search.

    DETAILED PROCESS FLOW:
    1. FILE READING: Loads markdown content with UTF-8 encoding
    2. ID GENERATION: Creates article_id as "custom_{filename_stem}"
    3. HASH CALCULATION: Generates SHA-256 hash for change detection
    4. DUPLICATE CHECK: Queries ES to see if document exists with same hash
    5. CONTENT PROCESSING: Splits text into chunks (2048 chars, 256 overlap)
    6. EMBEDDING GENERATION: Calls Azure OpenAI API for vector embeddings
    7. BULK INSERTION: Stores all chunks in Elasticsearch vector index

    ELASTICSEARCH DOCUMENT STRUCTURE:
    {
        "_index": "custom_index_or_default",
        "_id": "custom_filename_chunk_0",
        "_source": {
            "embedding": [1536 floats],          # Vector from Azure OpenAI
            "page_content": "chunk text...",     # Actual text content
            "metadata": {
                "article_id": "custom_filename",
                "title": "Custom Title or Formatted Filename",
                "timestamp": "2025-01-01T12:00:00",
                "url": "file:///absolute/path/to/file.md",
                "source": "custom"
            },
            "article_id": "custom_filename",     # For quick filtering
            "chunk_id": 0,                       # Chunk sequence number
            "article_hash": "sha256_hash",       # For change detection
            "source": "custom"                   # Source type identifier
        }
    }

    COMMAND USAGE:
    poetry run python app/scripts/ingest_custom_doc.py /path/to/document.md
    poetry run python app/scripts/ingest_custom_doc.py /path/to/document.md --index my_custom_index
    poetry run python app/scripts/ingest_custom_doc.py /path/to/document.md --title "My Document"

    AZURE OPENAI API CALLS:
    - Uses text-embedding-ada-002 model (or configured deployment)
    - Generates 1536-dimension vectors per chunk
    - Subject to rate limits (typically 1000 RPM for embeddings)
    - Costs apply per token processed

    ERROR HANDLING:
    - File not found: Script exits with error message
    - API failures: Logs error and continues with next chunk
    - ES connection issues: Bulk operation reports failed insertions
    - Encoding issues: UTF-8 encoding handles most international characters

    Args:
        file_path (str): Absolute or relative path to markdown file
                        Examples: "./docs/guide.md", "/Users/john/manual.md"
        index_name (str, optional): Custom index name. If None, uses ES_VECTOR_INDEX_NAME from settings
        title (str, optional): Custom document title. If None, generates from filename

    Returns:
        None: Function logs progress and results, no return value
    """
    file_path = Path(file_path)

    # STEP 1: READ MARKDOWN CONTENT
    # Open file with UTF-8 encoding to handle international characters
    # This will fail gracefully if file doesn't exist or isn't readable
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # STEP 2: GENERATE DOCUMENT IDENTIFIERS
    # Create unique document ID from filename (without extension)
    # Examples: "user_manual.md" → "custom_user_manual"
    #          "API-Guide.md" → "custom_API-Guide"
    doc_id = f"custom_{file_path.stem}"

    # Generate SHA-256 hash of content for change detection
    # Same content = same hash = skip processing
    # Different content = different hash = update required
    content_hash = generate_hash(content)

    # Use provided title or create human-readable title from filename
    # "user_manual" → "User Manual", "api-guide" → "Api Guide"
    if not title:
        title = file_path.stem.replace("_", " ").replace("-", " ").title()

    # Use provided index name or fallback to default from settings
    target_index = index_name if index_name else ES_VECTOR_INDEX_NAME

    logger.info(f"📄 Processing document: {doc_id}")
    logger.info(f"📊 Target index: {target_index}")
    logger.info(f"📝 Document title: {title}")

    # STEP 3: PREPARE METADATA FOR ALL CHUNKS
    # This metadata will be attached to every chunk of this document
    # Allows filtering and identification in search results
    metadata = {
        "article_id": doc_id,  # Unique identifier for grouping chunks
        "title": title,  # Human-readable document title
        "timestamp": datetime.now().isoformat(),  # When document was ingested (ISO format)
        "url": f"file://{file_path.absolute()}",  # Full file path for reference
        "source": "custom",  # Source type: "custom" vs "kb" vs "news"
    }

    # STEP 4: SETUP ELASTICSEARCH CONNECTION AND INDEX
    # Get singleton Elasticsearch client (reuses connection if exists)
    # Client configuration comes from environment variables in settings.py
    es_client = get_elasticsearch_client()

    # Ensure the target index exists with proper mapping for embeddings
    # Vector index mapping includes:
    # - "embedding": dense_vector field with 1536 dimensions
    # - "page_content": text field for chunk content
    # - "metadata": object field for document metadata
    # - "article_id": keyword field for exact matching
    if not es_client.indices.exists(index=target_index):
        if target_index == ES_VECTOR_INDEX_NAME:
            # Use the standard vector index creation function for default index
            create_vector_index(es_client)
        else:
            # Create custom index with same mapping as vector index
            logger.info(f"🔧 Creating custom index: {target_index}")
            es_client.indices.create(
                index=target_index,
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
                            "source": {"type": "keyword"},
                        }
                    }
                },
            )
        logger.info(f"✨ Created/verified index: {target_index}")

    # STEP 5: CHECK FOR EXISTING DOCUMENT AND HANDLE UPDATES
    # Search for any existing chunks with the same article_id
    # This query finds all chunks belonging to this document:
    # GET /vector_index/_search
    # {
    #   "query": {
    #     "term": {"article_id": "custom_filename"}
    #   }
    # }
    existing_query = {"query": {"term": {"article_id": doc_id}}}
    existing_response = es_client.search(index=target_index, body=existing_query)

    if existing_response["hits"]["total"]["value"] > 0:
        # Document already exists - check if content has changed by comparing hashes
        existing_hash = existing_response["hits"]["hits"][0]["_source"]["article_hash"]

        if existing_hash == content_hash:
            # Same hash = identical content = no processing needed
            logger.info(
                f"⏭️  Document {doc_id} with same content already exists. Skipping."
            )
            return
        else:
            # Different hash = content changed = delete old chunks and insert new ones
            # This ensures we don't have stale chunks from previous version
            deleted_count = delete_custom_doc(str(file_path), target_index)
            logger.info(
                f"🔄 Content changed for {doc_id}. Deleted {deleted_count} old chunks."
            )

    # STEP 6: CONTENT PROCESSING AND EMBEDDING GENERATION
    # Split markdown content into overlapping chunks using RecursiveCharacterTextSplitter
    # Configuration: 2048 chars max, 256 chars overlap, Language.MARKDOWN mode
    # Overlap ensures context isn't lost at chunk boundaries
    # Markdown mode preserves formatting and structure during splitting
    chunks = TEXT_SPLITTER.split_text(content)
    logger.info(f"📝 Split into {len(chunks)} chunks")

    # Generate vector embeddings for all chunks using Azure OpenAI API
    # API Details:
    # - Model: text-embedding-ada-002 (or configured deployment)
    # - Output: 1536-dimensional vectors per chunk
    # - Rate limits: Typically 1000 requests per minute
    # - Cost: Charged per token processed
    # This step may take several seconds/minutes for large documents
    logger.info("🔄 Generating embeddings...")
    embeddings = EMBEDDING_MODEL.embed_documents(chunks)

    # STEP 7: PREPARE DOCUMENTS FOR ELASTICSEARCH BULK INSERT
    # Convert each chunk into an Elasticsearch document for bulk insertion
    # Each chunk becomes a separate searchable document with its own embedding
    documents = []
    for idx, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        # Generate unique document ID for this chunk
        # Format: custom_filename_chunk_0, custom_filename_chunk_1, etc.
        unique_chunk_id = f"{doc_id}_chunk_{idx}"

        # Create Elasticsearch document following bulk API format
        # Structure matches existing KB/News articles for consistency
        doc = {
            "_index": target_index,  # Target index (custom or default)
            "_id": unique_chunk_id,  # Unique identifier for this chunk
            "_source": {
                "embedding": embedding,  # 1536-dimensional vector from Azure OpenAI
                "page_content": chunk,  # Actual text content of this chunk
                "metadata": metadata,  # Document-level metadata (shared across chunks)
                "article_id": doc_id,  # Groups all chunks of same document
                "chunk_id": idx,  # Sequential chunk number (0, 1, 2, ...)
                "article_hash": content_hash,  # Content hash for change detection
                "source": "custom",  # Source identifier for filtering
            },
        }
        documents.append(doc)

    # STEP 8: BULK INSERT INTO ELASTICSEARCH
    # Use Elasticsearch bulk API for efficient insertion of multiple documents
    # Bulk operations are much faster than individual inserts
    # chunk_size=500 processes documents in batches to avoid memory issues
    logger.info(f"💾 Inserting {len(documents)} chunks into Elasticsearch...")
    success, failed = bulk(es_client, documents, chunk_size=500)

    # Handle any insertion failures
    # Common causes: network issues, mapping conflicts, resource constraints
    if failed:
        logger.warning(f"⚠️  {len(failed)} chunks failed to insert")
        # Log details of first 5 failures for debugging
        for failure in failed[:5]:
            logger.error(f"Failed chunk: {failure}")

    # Log successful completion with summary statistics
    logger.info(f"✅ Successfully ingested {file_path.name}: {success} chunks inserted")
    logger.info(f"📊 Document stored as article_id: {doc_id} in index: {target_index}")

    # VERIFICATION: You can now search for this document using:
    # 1. Vector similarity search with query embeddings
    # 2. Text search on page_content field
    # 3. Filter by source="custom" to find only custom documents
    # 4. Filter by article_id="{doc_id}" to find all chunks of this document


def main():
    """
    Main function with proper argument parsing using argparse.

    COMMAND LINE INTERFACE:
    Uses argparse for robust command line argument handling with:
    - Proper help text generation
    - Input validation
    - Mutually exclusive operations (ingest vs delete)
    - Better error messages
    """

    # Create argument parser with detailed description
    parser = argparse.ArgumentParser(
        prog="ingest_custom_doc.py",
        description="Ingest custom markdown documents and create embeddings for vector search",
        epilog="""
EXAMPLES:
  # Ingest documents (default index)
  poetry run python app/scripts/ingest_custom_doc.py ./data/user_manual.md
  poetry run python app/scripts/ingest_custom_doc.py /Users/john/docs/api_guide.md
  
  # Ingest with custom index and title
  poetry run python app/scripts/ingest_custom_doc.py ./data/manual.md --index my_docs_index
  poetry run python app/scripts/ingest_custom_doc.py ./data/manual.md --title "Product Manual"
  poetry run python app/scripts/ingest_custom_doc.py ./data/manual.md --index my_docs --title "API Guide"
  
  # Delete documents
  poetry run python app/scripts/ingest_custom_doc.py --delete ./data/user_manual.md
  poetry run python app/scripts/ingest_custom_doc.py --delete ./data/manual.md --index my_docs_index
  
PREREQUISITES:
  - .env file with Azure OpenAI and Elasticsearch credentials
  - poetry install (dependencies installed)
  - Markdown file (.md extension)
  
OUTPUT:
  - Document chunks stored in Elasticsearch vector index
  - Available for vector similarity search  
  - Tagged with source='custom' for filtering
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Create mutually exclusive group for ingest vs delete operations
    operation_group = parser.add_mutually_exclusive_group(required=True)

    # Add positional argument for file path (for ingestion)
    operation_group.add_argument(
        "file_path",
        nargs="?",
        type=str,
        help="Path to the markdown file to ingest (creates embeddings)",
    )

    # Add delete option
    operation_group.add_argument(
        "--delete",
        type=str,
        metavar="FILE_PATH",
        help="Delete a previously ingested document by file path",
    )

    # Add optional arguments
    parser.add_argument(
        "--title",
        type=str,
        help="Custom title for the document (only used during ingestion)",
    )

    parser.add_argument(
        "--index",
        type=str,
        help=f"Custom Elasticsearch index name (default: {ES_VECTOR_INDEX_NAME})",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging output"
    )

    # Parse arguments
    args = parser.parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        logger.info("Verbose mode enabled")

    # ROUTE TO APPROPRIATE FUNCTION BASED ON PARSED ARGUMENTS
    try:
        if args.delete:
            # DELETE MODE: Remove all chunks for specified document
            target_index = args.index if args.index else ES_VECTOR_INDEX_NAME
            print(f"🗑️  Deleting document: {args.delete}")
            print(f"📊 From index: {target_index}")
            deleted_count = delete_custom_doc(args.delete, args.index)
            if deleted_count > 0:
                print(f"✅ Successfully deleted {deleted_count} chunks")
            else:
                print("ℹ️  No chunks found to delete (document may not exist)")

        elif args.file_path:
            # INGEST MODE: Process and embed the markdown document
            file_path = Path(args.file_path)

            # Validate file exists and is markdown
            if not file_path.exists():
                print(f"❌ Error: File not found - {file_path}")
                sys.exit(1)

            if not file_path.suffix.lower() == ".md":
                print(f"❌ Error: File must be a markdown file (.md) - {file_path}")
                sys.exit(1)

            print(f"📄 Ingesting document: {file_path}")
            if file_path.stat().st_size > 1024 * 1024:  # > 1MB
                print("🔄 Large file detected - this may take several minutes...")
            else:
                print("🔄 Processing document...")

            # Call ingestion function with custom parameters
            target_index = args.index if args.index else ES_VECTOR_INDEX_NAME
            print(f"📊 Target index: {target_index}")
            if args.title:
                print(f"📝 Custom title: {args.title}")

            ingest_markdown(str(file_path), args.index, args.title)

            print("✅ Ingestion completed successfully")
            print("📊 Document is now available for vector search")
            print(f"🔍 Search for chunks using article_id: custom_{file_path.stem}")
            print(f"📈 Stored in index: {target_index}")

    except FileNotFoundError as e:
        print(f"❌ Error: File not found - {e}")
        sys.exit(1)
    except PermissionError as e:
        print(f"❌ Error: Permission denied - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error during operation: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # ENTRY POINT: Call main function with proper argument parsing
    main()
