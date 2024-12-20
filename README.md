# BigQuery to Elasticsearch Data Pipeline

A Python-based data pipeline that extracts knowledge base articles from BigQuery, processes them, and indexes them into Elasticsearch with vector embeddings for semantic search capabilities.

## Features

- Extracts data from BigQuery using specified queries
- Converts HTML content to Markdown format
- Splits documents into chunks for better search granularity
- Generates embeddings using Azure OpenAI's embedding model
- Indexes both raw documents and vector embeddings into Elasticsearch
- Handles batch processing with error handling and rate limiting
- Supports incremental updates by checking document hashes

## Prerequisites

- Python 3.11 or higher
- Poetry for dependency management
- Google Cloud Platform account with BigQuery access
- Elasticsearch cluster
- Azure OpenAI API access

## Environment Variables

Create a `.env` file with the following variables:

```env
# BigQuery Configuration
GBQ_PROJECT_ID=your_project_id
GBQ_LOCATION=your_location
GBQ_DATASET=your_dataset
GBQ_TABLE=your_table
GBQ_MAX_RESULTS=10  # Adjust as needed

# Elasticsearch Configuration
ELASTICSEARCH_URL=your_elasticsearch_url
ELASTICSEARCH_API_KEY=your_api_key
ES_INDEX_NAME=test-bq-snow-ingest  # Default value, can be changed
ES_VECTOR_INDEX_NAME=test-bq-embeddings-openai  # Default value, can be changed

# Azure OpenAI Configuration
AZURE_EMBEDDING_DEPLOYMENT_NAME=your_deployment_name
AZURE_EMBEDDING_API_VERSION=your_api_version
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=your_endpoint

# ServiceNow Configuration
SNOW_BASE_URL=your_snow_base_url

# Other Configuration
OUTPUT_DIR=data  # Default value, can be changed
KB_KNOWLEDGE_BASE_VALUES=your_kb_values
```

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd bq-es-project
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Activate the virtual environment:
```bash
poetry shell
```

## Usage

Run the main script:
```bash
python app/main.py
```

The script will:
1. Connect to BigQuery and extract knowledge base articles
2. Create/recreate Elasticsearch indices
3. Process documents and generate embeddings
4. Index both raw documents and vector embeddings
5. Handle errors and provide progress updates

## Project Structure

```
bq-es-project/
├── app/
│   ├── __init__.py
│   └── main.py          # Main application logic
├── data/                # Output directory for temporary files
├── .env                 # Environment variables
├── pyproject.toml       # Poetry configuration and dependencies
└── README.md           # This file
```

## Development

The project uses several development tools:
- Black for code formatting
- Ruff for linting
- IPython/Jupyter for interactive development
- Pyright for type checking

To run development tools:
```bash
# Format code
poetry run black .

# Lint code
poetry run ruff check .

# Type check
poetry run pyright
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
