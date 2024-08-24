import os
from datetime import datetime

from google.cloud import bigquery
import bigframes.pandas as bf
from dotenv import load_dotenv

load_dotenv()

GBQ_PROJECT_ID = os.getenv("GBQ_PROJECT_ID")
GBQ_LOCATION = os.getenv("GBQ_LOCATION")
GBQ_DATASET = os.getenv("GBQ_DATASET")
GBQ_TABLE = os.getenv("GBQ_TABLE")
GBQ_MAX_RESULTS = int(os.getenv("GBQ_MAX_RESULTS"), 10)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")


def init():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def connect_to_bigquery():
    client = bigquery.Client()
    return client


def query_bigquery():
    client = connect_to_bigquery()
    bf.options.bigquery.client = client
    bf.options.bigquery.location = GBQ_LOCATION
    bf.options.bigquery.project = GBQ_PROJECT_ID

    df = bf.read_gbq(f"{GBQ_PROJECT_ID}.{GBQ_DATASET}.{GBQ_TABLE}", max_results=GBQ_MAX_RESULTS)

    return df


def save_df_to_csv(dataframe: bf.DataFrame):
    filename: str = f"{GBQ_TABLE}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    dataframe.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)
    print(f"\nSaved dataframe to {filename}\n")


if __name__ == "__main__":
    init()
    
    results = query_bigquery()


    save_df_to_csv(dataframe=results)