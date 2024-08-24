# Import necessary libraries
import os
from datetime import datetime
from typing import List, Optional

from google.cloud import bigquery
import bigframes.pandas as bf
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve BigQuery configuration from environment variables
GBQ_PROJECT_ID: Optional[str] = os.getenv("GBQ_PROJECT_ID")
GBQ_LOCATION: Optional[str] = os.getenv("GBQ_LOCATION")
GBQ_DATASET: Optional[str] = os.getenv("GBQ_DATASET")
GBQ_TABLE: Optional[str] = os.getenv("GBQ_TABLE")
GBQ_MAX_RESULTS: int = int(os.getenv("GBQ_MAX_RESULTS", "10"))
OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data")


def init() -> None:
    """
    Initialize the application by creating the output directory if it doesn't exist.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def connect_to_bigquery() -> bigquery.Client:
    """
    Establish a connection to BigQuery.

    Returns:
        bigquery.Client: A BigQuery client object.
    """
    client: bigquery.Client = bigquery.Client()
    return client


def query_bigquery() -> bf.DataFrame:
    """
    Query BigQuery to retrieve data.

    Returns:
        bf.DataFrame: A BigFrames DataFrame containing the query results.
    """
    # Connect to BigQuery and set up the client
    client: bigquery.Client = connect_to_bigquery()
    bf.options.bigquery.client = client
    bf.options.bigquery.location = GBQ_LOCATION
    bf.options.bigquery.project = GBQ_PROJECT_ID

    # Define the columns to be retrieved
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
    ]

    # Construct the SQL query
    query: str = f"""
    SELECT {', '.join(columns)}
    FROM `{GBQ_PROJECT_ID}.{GBQ_DATASET}.{GBQ_TABLE}`
    LIMIT {GBQ_MAX_RESULTS}
    """

    # Execute the query and return the results as a DataFrame
    df: bf.DataFrame = bf.read_gbq(query)

    return df


def save_df_to_csv(dataframe: bf.DataFrame) -> None:
    """
    Save a BigFrames DataFrame to a CSV file.

    Args:
        dataframe (bf.DataFrame): The DataFrame to be saved.
    """
    # Generate a unique filename using the current timestamp
    filename: str = f"{GBQ_TABLE}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    
    # Save the DataFrame to a CSV file
    dataframe.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)
    
    # Print a confirmation message
    print(f"\nSaved dataframe to {filename}\n")


if __name__ == "__main__":
    # Initialize the application
    init()

    # Query BigQuery and get the results
    results: bf.DataFrame = query_bigquery()

    # Save the results to a CSV file
    save_df_to_csv(dataframe=results)
