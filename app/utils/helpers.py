"""(c) 2025, Elastic Co.
Author: Adhish Thite <adhish.thite@elastic.co>
"""

import os
import json
import hashlib
from typing import List, Dict, Any, Generator

from app.config.logging_config import setup_logger

logger = setup_logger(__name__)


def init(output_dir: str) -> None:
    """Initialize the application by creating the output directory if it doesn't exist."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info(f"📁 Created output directory: {output_dir}")


def generate_hash(data: Any) -> str:
    """Generate a hash for the given data."""
    data_string = json.dumps(data, sort_keys=True)
    return hashlib.md5(data_string.encode()).hexdigest()


def batch_documents(
    documents: List[Dict[str, Any]], batch_size: int
) -> Generator[List[Dict[str, Any]], None, None]:
    """Split the documents into batches."""
    for i in range(0, len(documents), batch_size):
        yield documents[i : i + batch_size]
