# Contributing to BigQuery to Elasticsearch Data Pipeline

First off, thank you for considering contributing to this project! We appreciate the time and effort you put into making this tool better.

## Code of Conduct

By participating in this project, you agree to abide by the [Elastic Community Code of Conduct](https://www.elastic.co/community/codeofconduct).

## How Can I Contribute?

### Reporting Bugs

Before submitting a bug report:

1. Check if the bug has already been reported in the Issues section.
2. Use the latest version of the project to see if the bug still exists.
3. Include detailed information:
   - Clear, descriptive title
   - Steps to reproduce the issue
   - Expected behavior versus actual behavior
   - Environment details (OS, Python version, dependency versions)
   - Any relevant logs or error messages

For security vulnerabilities, please follow the instructions in the [SECURITY.md](SECURITY.md) file.

### Suggesting Enhancements

Enhancement suggestions are welcome. When submitting an enhancement suggestion:

1. Provide a clear, descriptive title.
2. Explain why the enhancement would be useful.
3. Suggest an implementation approach if possible.

### Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add or update tests as needed
5. Run tests and verify they pass (`poetry run pytest`)
6. Format code using Black (`poetry run black .`) 
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

### Coding Conventions

- Follow the existing code style (Black formatting)
- Include type hints for function parameters and return values
- Add docstrings to all functions, classes, and modules
- Write tests for your code
- Keep the codebase organized according to the existing structure

## Development Setup

1. Clone the repository
2. Install dependencies with Poetry:
```bash
poetry install
```
3. Copy `.env.example` to `.env` and configure for your development environment
4. Run tests to verify your setup:
```bash
poetry run pytest
```

## License

By contributing to this project, you agree that your contributions will be licensed under the project's [Apache 2.0 License](LICENSE).