"""(c) 2025, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>
"""

import os
from typing import Optional
from dotenv import load_dotenv

from config.logging_config import setup_logger

# Set up logger
logger = setup_logger(__name__)

# Load environment variables
load_dotenv()
logger.info("Environment variables loaded from .env file")

# BigQuery Settings
GBQ_PROJECT_ID: Optional[str] = os.getenv("GBQ_PROJECT_ID")
GBQ_LOCATION: Optional[str] = os.getenv("GBQ_LOCATION")
GBQ_DATASET: Optional[str] = os.getenv("GBQ_DATASET")
GBQ_TABLE: Optional[str] = os.getenv("GBQ_TABLE")
GBQ_NEWS_TABLE: Optional[str] = os.getenv("GBQ_NEWS_TABLE")
GBQ_MAX_RESULTS: int = int(os.getenv("GBQ_MAX_RESULTS", "10"))

# File System Settings
OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data")

# Elasticsearch Settings
ES_URL: Optional[str] = os.getenv("ELASTICSEARCH_URL")
ES_API_KEY: Optional[str] = os.getenv("ELASTICSEARCH_API_KEY")
ES_INDEX_NAME: str = os.getenv("ES_INDEX_NAME", "test-bq-snow-ingest")
ES_VECTOR_INDEX_NAME: str = os.getenv(
    "ES_VECTOR_INDEX_NAME", "test-bq-embeddings-openai"
)

# Azure OpenAI Settings
AZURE_EMBEDDING_DEPLOYMENT_NAME: Optional[str] = os.getenv(
    "AZURE_EMBEDDING_DEPLOYMENT_NAME"
)
AZURE_EMBEDDING_API_VERSION: Optional[str] = os.getenv("AZURE_EMBEDDING_API_VERSION")
AZURE_OPENAI_API_KEY: Optional[str] = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT: Optional[str] = os.getenv("AZURE_OPENAI_ENDPOINT")

# ServiceNow Settings
SNOW_BASE_URL: Optional[str] = os.getenv("SNOW_BASE_URL")
KB_KNOWLEDGE_BASE_VALUES: str = os.getenv("KB_KNOWLEDGE_BASE_VALUES", "")

# Processing Settings
BATCH_SIZE: int = 20
QUERY_SIZE: int = 10000

# Validate critical environment variables
missing_vars = []
for var_name, var_value in [
    ("GBQ_PROJECT_ID", GBQ_PROJECT_ID),
    ("GBQ_DATASET", GBQ_DATASET),
    ("GBQ_TABLE", GBQ_TABLE),
    ("ES_URL", ES_URL),
    ("ES_API_KEY", ES_API_KEY),
    ("AZURE_EMBEDDING_DEPLOYMENT_NAME", AZURE_EMBEDDING_DEPLOYMENT_NAME),
    ("AZURE_OPENAI_API_KEY", AZURE_OPENAI_API_KEY),
    ("AZURE_OPENAI_ENDPOINT", AZURE_OPENAI_ENDPOINT),
]:
    if not var_value:
        missing_vars.append(var_name)

if missing_vars:
    logger.warning(f"Missing required environment variables: {', '.join(missing_vars)}")
    logger.warning("Please check your .env file and ensure all required variables are set.")
else:
    logger.info("All required environment variables are set.")

logger.info("Configuration retrieved from environment variables")
