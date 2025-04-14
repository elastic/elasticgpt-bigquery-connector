# BigQuery to Elasticsearch Data Pipeline

A Python-based data pipeline that extracts knowledge base articles from BigQuery, processes them, and indexes them into Elasticsearch with vector embeddings for semantic search capabilities.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Overview

This pipeline automates the process of:
- Extracting knowledge base articles from BigQuery
- Processing and cleaning the content
- Generating vector embeddings using Azure OpenAI
- Indexing both raw documents and embeddings into Elasticsearch
- Supporting incremental updates with document versioning

## Project Structure

```
.
├── app/
│   ├── config/         # Configuration and settings
│   ├── services/       # Core service implementations
│   ├── utils/          # Utility functions and helpers
│   ├── __init__.py
│   └── main.py         # Application entry point
├── data/               # Data storage and temporary files
├── .env               # Environment configuration
├── .env.example       # Example environment variables
├── poetry.lock        # Locked dependencies
├── pyproject.toml     # Project configuration and dependencies
└── README.md
```

## Prerequisites

- Python 3.11+
- Poetry for dependency management (Version 1.8)
- Access to:
  - Google Cloud Platform (BigQuery)
  - Elasticsearch cluster
  - Azure OpenAI API
  - ServiceNow instance (optional)

## Setup and Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd bq-es-project
```

2. Install dependencies:
```bash
poetry install
```

3. Set up environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your configuration values

## Configuration

The following environment variables are required:

### BigQuery Configuration
```env
GBQ_PROJECT_ID=your_project_id
GBQ_LOCATION=your_location
GBQ_DATASET=your_dataset
GBQ_TABLE=your_table
GBQ_MAX_RESULTS=10
```

### Elasticsearch Configuration
```env
ELASTICSEARCH_URL=your_elasticsearch_url
ELASTICSEARCH_API_KEY=your_api_key
ES_INDEX_NAME=test-bq-snow-ingest
ES_VECTOR_INDEX_NAME=test-bq-embeddings-openai
```

### Azure OpenAI Configuration
```env
AZURE_EMBEDDING_DEPLOYMENT_NAME=your_deployment_name
AZURE_EMBEDDING_API_VERSION=your_api_version
AZURE_OPENAI_API_KEY=your_api_key
AZURE_OPENAI_ENDPOINT=your_endpoint
```

### Additional Configuration
```env
SNOW_BASE_URL=your_snow_base_url
OUTPUT_DIR=data
KB_KNOWLEDGE_BASE_VALUES=your_kb_values
```

## Usage

1. Activate the virtual environment:
```bash
poetry shell
```

2. Run the pipeline:
```bash
python app/main.py
```

The pipeline will:
1. Connect to BigQuery and extract articles
2. Process and clean the content
3. Generate embeddings using Azure OpenAI
4. Create or update Elasticsearch indices
5. Index documents and embeddings
6. Handle errors and provide progress updates

## Development

### Code Quality Tools

The project uses modern Python development tools:

```bash
# Format code
poetry run black .
```

### Project Components

- **config/**: Configuration management and settings
- **services/**: Core business logic and external service integrations
- **utils/**: Helper functions and utilities
- **main.py**: Pipeline orchestration and execution

## Error Handling

The pipeline includes:
- Comprehensive error logging
- Retry mechanisms for API calls
- Batch processing with failure recovery
- Document versioning for incremental updates

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Security

If you discover a security vulnerability within this project, please follow the responsible disclosure principles. Please do NOT create publicly viewable issues for suspected security vulnerabilities.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
