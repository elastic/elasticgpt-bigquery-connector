from typing import List
from google.cloud import bigquery
import bigframes.pandas as bf

from config.logging_config import setup_logger
from config.settings import (
    GBQ_PROJECT_ID,
    GBQ_DATASET,
    GBQ_TABLE,
    GBQ_MAX_RESULTS,
    GBQ_LOCATION,
)

logger = setup_logger(__name__)

def connect_to_bigquery() -> bigquery.Client:
    """
    Establish a connection to BigQuery.

    Returns:
        bigquery.Client: A BigQuery client object.
    """
    client: bigquery.Client = bigquery.Client()
    logger.info("🔌 Connected to BigQuery")
    return client

def query_bigquery() -> bf.DataFrame:
    """
    Query BigQuery to retrieve data.

    Returns:
        bf.DataFrame: A BigFrames DataFrame containing the query results.
    """
    logger.info("🔍 Starting BigQuery query process")
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

    logger.info(f"\n📝 Query: {query}\n")

    df: bf.DataFrame = bf.read_gbq(query)
    logger.info(f"✨ Query executed, retrieved {len(df)} rows")

    return df 